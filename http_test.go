package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// httpFixture is a Server with a few tools behind an HTTPHandler on a real
// listener, so the tests speak the wire the way a client does.
type httpFixture struct {
	srv       *Server
	ts        *httptest.Server
	h         *httpHandler
	slowStart chan struct{} // closed-per-call signal that the slow tool began
	slowDone  chan string   // what the slow tool's context said when it ended
}

func newHTTPFixture(t *testing.T, opts HTTPOptions, mod ...func(*Options)) *httpFixture {
	t.Helper()
	so := Options{Name: "httptest", Version: "0", Logger: log.New(io.Discard, "", 0)}
	for _, f := range mod {
		f(&so)
	}
	srv := New(so)
	fx := &httpFixture{srv: srv, slowStart: make(chan struct{}, 16), slowDone: make(chan string, 16)}
	schema := json.RawMessage(`{"type":"object"}`)
	add := func(name string, fn ToolHandler) {
		if err := srv.AddTool(Tool{Name: name, InputSchema: schema}, fn); err != nil {
			t.Fatal(err)
		}
	}
	add("echo", func(ctx context.Context, s *Session, args json.RawMessage) (*CallToolResult, error) {
		var a struct{ Text string }
		_ = json.Unmarshal(args, &a)
		if a.Text == "boom" {
			return ToolError("kaboom"), nil
		}
		return ToolText("echo:%s", a.Text), nil
	})
	add("who", func(ctx context.Context, s *Session, _ json.RawMessage) (*CallToolResult, error) {
		return ToolText("%s", s.Identity()), nil
	})
	add("elicit", func(ctx context.Context, s *Session, _ json.RawMessage) (*CallToolResult, error) {
		res, err := s.Elicit(ctx, "name?", json.RawMessage(`{"type":"object"}`))
		if err != nil {
			return ToolError("%v", err), nil
		}
		return ToolText("elicit:%s:%s", res.Action, res.Content), nil
	})
	add("chatty", func(ctx context.Context, s *Session, _ json.RawMessage) (*CallToolResult, error) {
		s.Log(LogInfo, "t", "one")
		return ToolText("done"), nil
	})
	add("slow", func(ctx context.Context, s *Session, _ json.RawMessage) (*CallToolResult, error) {
		fx.slowStart <- struct{}{}
		select {
		case <-ctx.Done():
			fx.slowDone <- "cancelled"
			return ToolError("cancelled"), nil
		case <-time.After(30 * time.Second):
			fx.slowDone <- "timeout"
			return ToolText("late"), nil
		}
	})
	fx.h = srv.HTTPHandler(opts).(*httpHandler)
	fx.ts = httptest.NewServer(fx.h)
	t.Cleanup(func() { _ = srv.Close(); fx.ts.Close() })
	return fx
}

func (fx *httpFixture) sessionCount() int {
	fx.h.mu.Lock()
	defer fx.h.mu.Unlock()
	return len(fx.h.sessions)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---- raw wire helpers ----

const initBody = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{"elicitation":{}},"clientInfo":{"name":"raw","version":"0"}}}`

// rpc POSTs body with the headers a conformant client sends, plus extras.
func rpc(t *testing.T, url, sid, body string, extra ...string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if sid != "" {
		req.Header.Set(headerSessionID, sid)
	}
	for i := 0; i+1 < len(extra); i += 2 {
		req.Header.Set(extra[i], extra[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// rpcAsync is rpc for a call that will not answer until the test lets it: the
// POST runs on its own goroutine and the outcome arrives on the channel.
func rpcAsync(url, sid, body string) <-chan string {
	out := make(chan string, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
		if err != nil {
			out <- "ERR " + err.Error()
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set(headerSessionID, sid)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			out <- "ERR " + err.Error()
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		out <- fmt.Sprintf("%d %s", resp.StatusCode, b)
	}()
	return out
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// rawInit performs the handshake and returns the session id.
func rawInit(t *testing.T, url string) string {
	t.Helper()
	resp := rpc(t, url, "", initBody)
	sid := resp.Header.Get(headerSessionID)
	if resp.StatusCode != 200 || sid == "" {
		t.Fatalf("initialize: status %d, session id %q, body %s", resp.StatusCode, sid, readBody(t, resp))
	}
	_ = readBody(t, resp)
	if r := rpc(t, url, sid, `{"jsonrpc":"2.0","method":"notifications/initialized"}`); r.StatusCode != http.StatusAccepted {
		t.Fatalf("initialized: status %d", r.StatusCode)
	}
	return sid
}

// events reads SSE events off a response, delivering each on a channel.
func events(resp *http.Response) <-chan wireMessage {
	ch := make(chan wireMessage, 16)
	go func() {
		defer close(ch)
		rd := newSSEReader(resp.Body, DefaultMaxMessageBytes)
		for {
			_, data, err := rd.next()
			if err != nil {
				return
			}
			var m wireMessage
			if json.Unmarshal(data, &m) == nil {
				ch <- m
			}
		}
	}()
	return ch
}

func nextEvent(t *testing.T, ch <-chan wireMessage) wireMessage {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("stream ended before the expected event")
		}
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an SSE event")
	}
	return wireMessage{}
}

func toolCall(id int, name string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":%q,"arguments":{}}}`, id, name)
}

// ---- round trips ----

func TestHTTPRoundTripThroughClient(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, fx.ts.URL, ClientOptions{Info: Implementation{Name: "c", Version: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if fx.sessionCount() != 1 {
		t.Fatalf("sessions after initialize = %d, want 1", fx.sessionCount())
	}
	tools, err := c.ListTools(ctx)
	if err != nil || len(tools) != 5 {
		t.Fatalf("ListTools = %d tools, err %v; want 5", len(tools), err)
	}
	res, err := c.CallTool(ctx, "echo", map[string]any{"text": "hi"})
	if err != nil || res.IsError || !strings.Contains(res.Content[0].Text, "echo:hi") {
		t.Fatalf("echo = %+v, %v", res, err)
	}
	if res, err = c.CallTool(ctx, "echo", map[string]any{"text": "boom"}); err != nil || !res.IsError {
		t.Fatalf("a failing tool must be a result with IsError, got %+v, %v", res, err)
	}
	if _, err := c.CallTool(ctx, "nope", nil); err == nil {
		t.Fatal("an unknown tool must be a JSON-RPC error")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Close deletes the server's session.
	eventually(t, "the session to end after Close", func() bool { return fx.sessionCount() == 0 })
}

// One Session, two transports, the same answer.
func TestHTTPDifferentialAgainstPipe(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hc, err := DialHTTP(ctx, fx.ts.URL, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer hc.Close()

	a, b := net.Pipe()
	go func() {
		_ = fx.srv.ServeConn(ctx, NewFramer(a, a, FramerOptions{Closer: a}), ConnConfig{Remote: "pipe"})
	}()
	pc := Dial(b, ClientOptions{})
	defer pc.Close()

	for _, c := range []*Client{hc, pc} {
		if _, err := c.Initialize(ctx); err != nil {
			t.Fatal(err)
		}
	}
	norm := func(r *CallToolResult, err error) string {
		if err != nil {
			return "ERR " + err.Error()
		}
		j, _ := json.Marshal(r)
		return string(j)
	}
	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"echo", map[string]any{"text": "x"}},
		{"echo", map[string]any{"text": "boom"}},
		{"echo", nil},
		{"nope", nil},
		{"chatty", nil},
	} {
		got, want := norm(hc.CallTool(ctx, tc.tool, tc.args)), norm(pc.CallTool(ctx, tc.tool, tc.args))
		if got != want {
			t.Errorf("%s %v:\n http: %s\n pipe: %s", tc.tool, tc.args, got, want)
		}
	}
	ht, _ := hc.ListTools(ctx)
	pt, _ := pc.ListTools(ctx)
	hj, _ := json.Marshal(ht)
	pj, _ := json.Marshal(pt)
	if string(hj) != string(pj) {
		t.Errorf("tools/list differs:\n http: %s\n pipe: %s", hj, pj)
	}
}

// The reply to a request is a plain JSON body when the server has nothing else to
// say first, and an SSE stream when it does. Both are the same answer.
func TestHTTPReplyIsJSONOrSSE(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	sid := rawInit(t, fx.ts.URL)

	resp := rpc(t, fx.ts.URL, sid, toolCall(2, "echo"))
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("quiet request: Content-Type %q, want application/json", ct)
	}
	var m wireMessage
	if err := json.Unmarshal([]byte(readBody(t, resp)), &m); err != nil || string(m.ID) != "2" || m.Error != nil {
		t.Fatalf("quiet request: %+v, %v", m, err)
	}

	// chatty logs before it returns: no stream is open, so the notification rides
	// this POST's stream, ahead of the response.
	resp = rpc(t, fx.ts.URL, sid, toolCall(3, "chatty"))
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("chatty request: Content-Type %q, want text/event-stream", ct)
	}
	ev := events(resp)
	first, second := nextEvent(t, ev), nextEvent(t, ev)
	if first.Method != NotifyMessage {
		t.Fatalf("first event %q, want the log notification", first.Method)
	}
	if string(second.ID) != "3" || second.Result == nil {
		t.Fatalf("second event %+v, want the response to id 3", second)
	}
}

// The hard part of the transport: a server→client request mid-call, answered by
// a separate POST, on a client with no GET stream.
func TestHTTPElicitationOverPostStream(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	sid := rawInit(t, fx.ts.URL)

	resp := rpc(t, fx.ts.URL, sid, toolCall(2, "elicit"))
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type %q, want text/event-stream", ct)
	}
	ev := events(resp)
	req := nextEvent(t, ev)
	if req.Method != MethodElicitationCreate || len(req.ID) == 0 {
		t.Fatalf("first event %+v, want an elicitation/create request", req)
	}
	answer := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"action":"accept","content":{"name":"ada"}}}`, req.ID)
	if r := rpc(t, fx.ts.URL, sid, answer); r.StatusCode != http.StatusAccepted {
		t.Fatalf("answering the elicitation: status %d, want 202", r.StatusCode)
	}
	final := nextEvent(t, ev)
	var res CallToolResult
	_ = json.Unmarshal(final.Result, &res)
	if len(res.Content) == 0 || !strings.Contains(res.Content[0].Text, `elicit:accept:{"name":"ada"}`) {
		t.Fatalf("final result %s", final.Result)
	}
}

// The same elicitation, through DialHTTP, which opens the GET stream because it
// has a handler.
func TestHTTPElicitationThroughClient(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, fx.ts.URL, ClientOptions{OnRequest: func(_ context.Context, method string, _ json.RawMessage) (any, *RPCError) {
		if method != MethodElicitationCreate {
			return nil, Errorf(CodeMethodNotFound, "no")
		}
		return ElicitResult{Action: "accept", Content: json.RawMessage(`{"name":"grace"}`)}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	// Wait for the GET stream so the elicitation takes the stream path.
	eventually(t, "the GET stream to attach", func() bool {
		fx.h.mu.Lock()
		defer fx.h.mu.Unlock()
		for _, s := range fx.h.sessions {
			s.mu.Lock()
			g := s.get != nil
			s.mu.Unlock()
			return g
		}
		return false
	})
	res, err := c.CallTool(ctx, "elicit", nil)
	if err != nil || res.IsError || !strings.Contains(res.Content[0].Text, `elicit:accept:{"name":"grace"}`) {
		t.Fatalf("elicit = %+v, %v", res, err)
	}
}

// Concurrent calls must run concurrently: a POST that waits for its response must
// not hold the client's write lock.
func TestHTTPClientCallsRunConcurrently(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, fx.ts.URL, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	// Two slow calls in flight at once; if they were serialised only one could
	// have started before we release them.
	callCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = c.CallTool(callCtx, "slow", nil) }()
	}
	for range 2 {
		select {
		case <-fx.slowStart:
		case <-time.After(5 * time.Second):
			t.Fatal("the second slow call never started: calls are serialised")
		}
	}
	stop()
	wg.Wait()
}

// ---- admission ----

func TestHTTPOriginAndHost(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{AllowedOrigins: []string{"https://app.example.com"}, AllowedHosts: []string{"svc.internal"}})
	post := func(host, origin string) int {
		req, _ := http.NewRequest(http.MethodPost, fx.ts.URL, strings.NewReader(initBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Host = host
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	for _, tc := range []struct {
		name, host, origin string
		want               int
	}{
		{"no origin (non-browser client)", "svc.internal", "", 200},
		{"listed origin", "svc.internal", "https://app.example.com", 200},
		{"unlisted origin", "svc.internal", "https://evil.example", 403},
		{"opaque origin", "svc.internal", "null", 403},
		{"rebinding: right origin shape, wrong host", "evil.example", "", 403},
	} {
		if got := post(tc.host, tc.origin); got != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, got, tc.want)
		}
	}
}

// With no AllowedOrigins at all, no browser page can reach the server.
func TestHTTPDefaultRefusesAnyOrigin(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	resp := rpc(t, fx.ts.URL, "", initBody, "Origin", "http://localhost:3000")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status %d, want 403", resp.StatusCode)
	}
}

func TestHTTPMalformedRequests(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{}, func(o *Options) { o.MaxMessageBytes = 1024 })
	sid := rawInit(t, fx.ts.URL)
	big := `{"jsonrpc":"2.0","id":9,"method":"ping","params":{"pad":"` + strings.Repeat("x", 2048) + `"}}`

	do := func(method, body string, h map[string]string) int {
		req, _ := http.NewRequest(method, fx.ts.URL, strings.NewReader(body))
		for k, v := range h {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	ok := map[string]string{"Content-Type": "application/json", "Accept": "application/json, text/event-stream", headerSessionID: sid}
	with := func(k, v string) map[string]string {
		m := map[string]string{}
		for kk, vv := range ok {
			m[kk] = vv
		}
		if v == "" {
			delete(m, k)
		} else {
			m[k] = v
		}
		return m
	}
	ping := `{"jsonrpc":"2.0","id":2,"method":"ping"}`
	for _, tc := range []struct {
		name   string
		method string
		body   string
		h      map[string]string
		want   int
	}{
		{"wrong content type", "POST", ping, with("Content-Type", "text/plain"), 415},
		{"accept lacks event-stream", "POST", ping, with("Accept", "application/json"), 406},
		{"accept lacks json", "POST", ping, with("Accept", "text/event-stream"), 406},
		{"accept q=0 refuses", "POST", ping, with("Accept", "application/json, text/event-stream;q=0"), 406},
		{"accept wildcard is fine", "POST", ping, with("Accept", "*/*"), 200},
		{"not json", "POST", `{`, ok, 400},
		{"batch", "POST", `[` + ping + `]`, ok, 400},
		{"not a jsonrpc message", "POST", `{"jsonrpc":"2.0"}`, ok, 400},
		{"wrong jsonrpc version", "POST", `{"jsonrpc":"1.0","id":3,"method":"ping"}`, ok, 400},
		{"oversized", "POST", big, ok, 413},
		{"no session id on a non-initialize", "POST", ping, with(headerSessionID, ""), 400},
		{"unknown session", "POST", ping, with(headerSessionID, "00000000000000000000000000000000"), 404},
		{"protocol version unsupported", "POST", ping, with(headerProtocolVersion, "1999-01-01"), 400},
		{"protocol version differs from negotiated", "POST", ping, with(headerProtocolVersion, "2024-11-05"), 400},
		{"protocol version matches", "POST", ping, with(headerProtocolVersion, "2025-06-18"), 200},
		{"get without accept", "GET", "", map[string]string{"Accept": "application/json", headerSessionID: sid}, 406},
		{"get without session id", "GET", "", map[string]string{"Accept": "text/event-stream"}, 400},
		{"get unknown session", "GET", "", map[string]string{"Accept": "text/event-stream", headerSessionID: "nope"}, 404},
		{"delete without session id", "DELETE", "", nil, 400},
		{"put", "PUT", "", ok, 405},
	} {
		if got := do(tc.method, tc.body, tc.h); got != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestHTTPRequestIDInFlightTwice(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	sid := rawInit(t, fx.ts.URL)
	first := rpcAsync(fx.ts.URL, sid, toolCall(7, "slow"))
	<-fx.slowStart
	if r := rpc(t, fx.ts.URL, sid, toolCall(7, "echo")); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate in-flight id: status %d, want 400", r.StatusCode)
	}
	// The first call is undisturbed: cancelling it ends it.
	rpc(t, fx.ts.URL, sid, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7}}`)
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("the original request never ended")
	}
}

// ---- sessions ----

func TestHTTPInitializeFailureLeavesNoSession(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	resp := rpc(t, fx.ts.URL, "", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":7}}`)
	if resp.Header.Get(headerSessionID) != "" {
		t.Fatal("a failed handshake must not hand out a session id")
	}
	_ = readBody(t, resp)
	eventually(t, "the failed session to be released", func() bool { return fx.sessionCount() == 0 })
}

func TestHTTPDeleteEndsSession(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	sid := rawInit(t, fx.ts.URL)
	req, _ := http.NewRequest(http.MethodDelete, fx.ts.URL, nil)
	req.Header.Set(headerSessionID, sid)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status %d, want 204", resp.StatusCode)
	}
	if r := rpc(t, fx.ts.URL, sid, `{"jsonrpc":"2.0","id":2,"method":"ping"}`); r.StatusCode != http.StatusNotFound {
		t.Fatalf("request on a deleted session: status %d, want 404", r.StatusCode)
	}
}

// A session is bound to the identity that created it.
func TestHTTPSessionBoundToIdentity(t *testing.T) {
	who := func(r *http.Request) string { return r.Header.Get("X-Caller") }
	fx := newHTTPFixture(t, HTTPOptions{Identify: who})

	resp := rpc(t, fx.ts.URL, "", initBody, "X-Caller", "alice")
	sid := resp.Header.Get(headerSessionID)
	_ = readBody(t, resp)
	rpc(t, fx.ts.URL, sid, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, "X-Caller", "alice")

	ping := `{"jsonrpc":"2.0","id":2,"method":"ping"}`
	if r := rpc(t, fx.ts.URL, sid, ping, "X-Caller", "bob"); r.StatusCode != http.StatusNotFound {
		t.Fatalf("another identity using alice's session id: status %d, want 404", r.StatusCode)
	}
	if r := rpc(t, fx.ts.URL, sid, ping); r.StatusCode != http.StatusNotFound {
		t.Fatalf("no identity: status %d, want 404", r.StatusCode)
	}
	if r := rpc(t, fx.ts.URL, sid, ping, "X-Caller", "alice"); r.StatusCode != 200 {
		t.Fatalf("the owner: status %d, want 200", r.StatusCode)
	}
	// And the handler learns who it is serving.
	var m wireMessage
	r := rpc(t, fx.ts.URL, sid, toolCall(3, "who"), "X-Caller", "alice")
	if err := json.Unmarshal([]byte(readBody(t, r)), &m); err != nil || !strings.Contains(string(m.Result), `"alice"`) {
		t.Fatalf("who = %s, %v", m.Result, err)
	}
}

func TestHTTPSessionIdleEviction(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{SessionTTL: 60 * time.Millisecond, HandshakeTimeout: time.Second})
	sid := rawInit(t, fx.ts.URL)
	eventually(t, "the idle session to be evicted", func() bool { return fx.sessionCount() == 0 })
	if r := rpc(t, fx.ts.URL, sid, `{"jsonrpc":"2.0","id":2,"method":"ping"}`); r.StatusCode != http.StatusNotFound {
		t.Fatalf("evicted session: status %d, want 404", r.StatusCode)
	}
}

// A session that never finishes the handshake is dropped on the short timer, not
// the full TTL: unauthenticated initialize requests cannot park on slots.
func TestHTTPHandshakeTimeout(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{SessionTTL: time.Hour, HandshakeTimeout: 60 * time.Millisecond})
	resp := rpc(t, fx.ts.URL, "", initBody) // no notifications/initialized
	if resp.StatusCode != 200 {
		t.Fatalf("initialize status %d", resp.StatusCode)
	}
	_ = readBody(t, resp)
	eventually(t, "the half-open session to be evicted", func() bool { return fx.sessionCount() == 0 })
}

// A request in flight keeps its session alive past the TTL.
func TestHTTPActiveRequestHoldsSessionOpen(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{SessionTTL: 50 * time.Millisecond})
	sid := rawInit(t, fx.ts.URL)
	first := rpcAsync(fx.ts.URL, sid, toolCall(2, "slow"))
	<-fx.slowStart
	time.Sleep(200 * time.Millisecond) // four TTLs
	if fx.sessionCount() != 1 {
		t.Fatal("the session was evicted under a request in flight")
	}
	rpc(t, fx.ts.URL, sid, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}`)
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never ended")
	}
}

func TestHTTPMaxSessions(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{MaxSessions: 2})
	a := rawInit(t, fx.ts.URL)
	rawInit(t, fx.ts.URL)
	if r := rpc(t, fx.ts.URL, "", initBody); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("third session: status %d, want 503", r.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodDelete, fx.ts.URL, nil)
	req.Header.Set(headerSessionID, a)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
	if r := rpc(t, fx.ts.URL, "", initBody); r.StatusCode != 200 {
		t.Fatalf("after freeing a slot: status %d, want 200", r.StatusCode)
	}
}

// ---- cancellation and shutdown ----

// notifications/cancelled ends the POST of the request it names (the Session
// suppresses that request's reply, so nothing else would), and stops the handler.
func TestHTTPCancelEndsPost(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	sid := rawInit(t, fx.ts.URL)

	type result struct {
		status int
		body   string
	}
	done := make(chan result, 1)
	pending := rpcAsync(fx.ts.URL, sid, toolCall(5, "slow"))
	go func() {
		out := <-pending
		var r result
		_, _ = fmt.Sscanf(out, "%d", &r.status)
		r.body = out
		done <- r
	}()
	<-fx.slowStart
	if r := rpc(t, fx.ts.URL, sid, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":5}}`); r.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel notification status %d, want 202", r.StatusCode)
	}
	select {
	case r := <-done:
		if !strings.Contains(r.body, fmt.Sprint(CodeRequestCancelled)) {
			t.Fatalf("cancelled POST ended with %d %s, want the cancelled error", r.status, r.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled request's POST never ended")
	}
	select {
	case what := <-fx.slowDone:
		if what != "cancelled" {
			t.Fatalf("the handler ended with %q, want its context cancelled", what)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was not cancelled")
	}
}

// Dropping the connection is not a cancellation: the request keeps running.
func TestHTTPDisconnectDoesNotCancel(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	sid := rawInit(t, fx.ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, fx.ts.URL, strings.NewReader(toolCall(5, "slow")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(headerSessionID, sid)
	go func() { _, _ = http.DefaultClient.Do(req) }()
	<-fx.slowStart
	cancel() // the client walks away
	select {
	case what := <-fx.slowDone:
		t.Fatalf("a dropped connection ended the handler (%s); only notifications/cancelled should", what)
	case <-time.After(300 * time.Millisecond):
	}
	// The waiter is gone: nothing leaks in the session.
	eventually(t, "the abandoned POST's waiter to be released", func() bool {
		fx.h.mu.Lock()
		defer fx.h.mu.Unlock()
		for _, s := range fx.h.sessions {
			s.mu.Lock()
			n := len(s.waiters)
			s.mu.Unlock()
			return n == 0
		}
		return false
	})
}

func TestHTTPServerCloseEndsInFlightAndSessions(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	sid := rawInit(t, fx.ts.URL)
	pending := rpcAsync(fx.ts.URL, sid, toolCall(5, "slow"))
	<-fx.slowStart
	_ = fx.srv.Close()
	select {
	case <-pending:
	case <-time.After(5 * time.Second):
		t.Fatal("Server.Close left a POST hanging")
	}
	eventually(t, "sessions to be released", func() bool { return fx.sessionCount() == 0 })
	if r := rpc(t, fx.ts.URL, "", initBody); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("initialize on a closed server: status %d, want 503", r.StatusCode)
	}
}

func TestHTTPClientSessionExpired(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, fx.ts.URL, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	var live []*httpSession
	fx.h.mu.Lock()
	for _, s := range fx.h.sessions {
		live = append(live, s)
	}
	fx.h.mu.Unlock()
	for _, s := range live { // Close takes the registry lock, so not under it
		s.terminate()
	}
	_, err = c.CallTool(ctx, "echo", nil)
	if err == nil {
		t.Fatal("a call on an expired session must fail")
	}
	// The failure is terminal and names the cause.
	if _, err := c.CallTool(ctx, "echo", nil); err == nil || !strings.Contains(err.Error(), ErrSessionExpired.Error()) {
		t.Fatalf("second call err = %v, want %v", err, ErrSessionExpired)
	}
}

func TestDialHTTPRejectsBadEndpoint(t *testing.T) {
	for _, ep := range []string{"", "localhost:8080", "ftp://x/y", "http://", "://bad"} {
		if _, err := DialHTTP(context.Background(), ep, ClientOptions{}); err == nil {
			t.Errorf("DialHTTP(%q) succeeded", ep)
		}
	}
}

// The protocol version header rides every request after initialize.
func TestHTTPClientSendsNegotiatedVersion(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	var mu sync.Mutex
	seen := map[string]string{}
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m wireMessage
		_ = json.Unmarshal(b, &m)
		mu.Lock()
		seen[m.Method] = r.Header.Get(headerProtocolVersion) + "|" + r.Header.Get("X-Token")
		mu.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(b))
		fx.h.ServeHTTP(w, r)
	}))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, front.URL, ClientOptions{Header: http.Header{"X-Token": {"s3cret"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListTools(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := seen[MethodInitialize]; got != "|s3cret" {
		t.Errorf("initialize carried %q, want no version and the caller's header", got)
	}
	if got := seen[MethodToolsList]; got != "2025-06-18|s3cret" {
		t.Errorf("tools/list carried %q, want the negotiated version and the caller's header", got)
	}
}

// ---- the bridge itself ----

// A GET stream whose reader stops draining must not block the Session's writer:
// WriteMessage is called from the one writer goroutine every handler's reply
// queues behind.
func TestHTTPStreamThatStopsDrainingDoesNotBlockWriter(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	s, status := fx.h.create(httptest.NewRequest(http.MethodPost, "/", nil))
	if s == nil {
		t.Fatalf("create: %d", status)
	}
	st := s.attach() // never drained
	done := make(chan struct{})
	go func() {
		for i := range 3 * httpStreamQueue {
			if err := s.WriteMessage([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"n","params":{"i":%d}}`, i))); err != nil {
				t.Errorf("WriteMessage: %v", err)
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WriteMessage blocked on a stream nobody is draining")
	}
	select {
	case <-st.kicked:
	default:
		t.Fatal("the stuck stream was not closed")
	}
	s.mu.Lock()
	held := len(s.backlog)
	s.mu.Unlock()
	if held == 0 || held > httpBacklogMax {
		t.Fatalf("backlog holds %d frames, want 1..%d", held, httpBacklogMax)
	}
	// A new stream gets what was held.
	st2 := s.attach()
	if len(st2.out) == 0 {
		t.Fatal("a reconnecting stream was not handed the backlog")
	}
	s.terminate()
}

// Frames written while no stream and no POST is waiting are held, bounded, with
// the oldest dropped.
func TestHTTPBacklogIsBounded(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	s, _ := fx.h.create(httptest.NewRequest(http.MethodPost, "/", nil))
	defer s.terminate()
	for i := range 3 * httpBacklogMax {
		_ = s.WriteMessage([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"n","params":{"i":%d}}`, i)))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.backlog) != httpBacklogMax {
		t.Fatalf("backlog = %d, want %d", len(s.backlog), httpBacklogMax)
	}
	if !bytes.Contains(s.backlog[0], []byte(fmt.Sprintf(`"i":%d`, 2*httpBacklogMax))) {
		t.Fatalf("oldest retained frame is %s; the oldest should have been dropped", s.backlog[0])
	}
}

// Sessions created and ended leave nothing behind.
func TestHTTPNoGoroutineLeak(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	baseline := func() int { runtime.GC(); return runtime.NumGoroutine() }
	before := baseline()
	for range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		c, err := DialHTTP(ctx, fx.ts.URL, ClientOptions{OnRequest: func(context.Context, string, json.RawMessage) (any, *RPCError) {
			return struct{}{}, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Initialize(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := c.CallTool(ctx, "echo", nil); err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
		cancel()
	}
	eventually(t, "sessions to end", func() bool { return fx.sessionCount() == 0 })
	fx.ts.CloseClientConnections()
	http.DefaultClient.CloseIdleConnections()
	eventually(t, "goroutines to return to baseline", func() bool { return baseline() <= before+2 })
}

// await delivers what the Session queued ahead of the response before the
// response, in order. The writer sends both from one goroutine, but the handler's
// select may see the response first; this puts the two channels in exactly that
// state.
func TestHTTPAwaitDeliversQueuedFramesBeforeResponse(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{KeepAlive: -1})
	s, _ := fx.h.create(httptest.NewRequest(http.MethodPost, "/", nil))
	defer s.terminate()
	s.enter()
	wt, ok := s.register(json.RawMessage(`9`))
	if !ok {
		t.Fatal("register")
	}
	for i := range 3 {
		wt.out <- []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"n%d"}`, i))
	}
	wt.resp <- []byte(`{"jsonrpc":"2.0","id":9,"result":{}}`)

	rec := httptest.NewRecorder()
	fx.h.await(rec, httptest.NewRequest(http.MethodPost, "/", nil), s, wt, false)

	var got []string
	rd := newSSEReader(rec.Body, DefaultMaxMessageBytes)
	for {
		_, data, err := rd.next()
		if err != nil {
			break
		}
		var m wireMessage
		_ = json.Unmarshal(data, &m)
		got = append(got, m.Method+string(m.ID))
	}
	if want := "n0 n1 n2 9"; strings.Join(got, " ") != want {
		t.Fatalf("events %q, want %q (queued frames first, response last)", strings.Join(got, " "), want)
	}
}

// notifications/initialized must reach the server before the request the client
// sends next. Separate HTTP requests have no order, so the client waits for the
// notification's 202; here the server is slow to give it, and a request that
// overtook the notification would arrive during the wait.
func TestHTTPClientSendsNotificationBeforeNextRequest(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	var mu sync.Mutex
	var order []string
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m wireMessage
		_ = json.Unmarshal(b, &m)
		mu.Lock()
		order = append(order, "arrive:"+m.Method)
		mu.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(b))
		fx.h.ServeHTTP(w, r)
		if m.Method == NotifyInitialized {
			time.Sleep(150 * time.Millisecond) // the 202 is already sent; the client sees it only after the handler returns
		}
		mu.Lock()
		order = append(order, "done:"+m.Method)
		mu.Unlock()
	}))
	defer front.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, front.URL, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListTools(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	at := func(ev string) int {
		for i, o := range order {
			if o == ev {
				return i
			}
		}
		return -1
	}
	if n, l := at("done:"+NotifyInitialized), at("arrive:"+MethodToolsList); n < 0 || l < 0 || l < n {
		t.Fatalf("tools/list reached the server before the initialized notification finished: %v", order)
	}
}

// enqueue answers the POST itself when the session cannot take the body.
func TestHTTPEnqueueRefusals(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	s := &httpSession{in: make(chan []byte, 1), closed: make(chan struct{})}

	rec := httptest.NewRecorder()
	if !fx.h.enqueue(rec, s, []byte(`{}`)) {
		t.Fatal("an empty queue refused a body")
	}
	rec = httptest.NewRecorder()
	if fx.h.enqueue(rec, s, []byte(`{}`)) || rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("a full queue: %d, Retry-After %q; want 503 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
	close(s.closed)
	<-s.in // room again, but the session is closed
	rec = httptest.NewRecorder()
	fx.h.enqueue(rec, s, []byte(`{}`))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("a closed session answered %d, want 404", rec.Code)
	}
}

// The idle timer is armed when a POST ends, possibly just before the Session has
// processed notifications/initialized, so it may be the short handshake timer on a
// session that is in fact ready. When it fires it must judge the session by what
// it is now: ready, and idle for far less than SessionTTL.
func TestHTTPStaleHandshakeTimerDoesNotKillReadySession(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{SessionTTL: time.Hour, HandshakeTimeout: 20 * time.Millisecond})
	sid := rawInit(t, fx.ts.URL)
	fx.h.mu.Lock()
	s := fx.h.sessions[sid]
	fx.h.mu.Unlock()
	if s == nil {
		t.Fatal("no session")
	}
	eventually(t, "the session to be ready", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.sess != nil && s.sess.State() == StateReady
	})
	s.expire() // the short timer, firing late
	if fx.sessionCount() != 1 {
		t.Fatal("a ready session that had been idle for milliseconds was evicted by the handshake timer")
	}
}
