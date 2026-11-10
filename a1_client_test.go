package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// front puts a handler in front of the fixture's real HTTP handler. intercept sees
// every POST body and returns true when it has answered it itself.
func front(t *testing.T, fx *httpFixture, intercept func(w http.ResponseWriter, r *http.Request, m wireMessage) bool) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m wireMessage
		if r.Method == http.MethodPost {
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &m)
			r.Body = io.NopCloser(bytes.NewReader(b))
			if intercept(w, r, m) {
				return
			}
		}
		fx.h.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// A1 C-5: an HTTP 200 JSON reply that is not the response to the request (a null-id
// error, as JSON-RPC §5 has a server answer a request it cannot attribute) must fail
// the call at once, not leave it waiting for its deadline.
func TestHTTPClientFailsARequestAnsweredWithSomethingElse(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ts := front(t, fx, func(w http.ResponseWriter, r *http.Request, m wireMessage) bool {
		if m.Method != MethodToolsList {
			return false
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}`))
		return true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, ts.URL, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if _, err := c.ListTools(callCtx); err == nil || callCtx.Err() != nil || time.Since(start) > 2*time.Second {
		t.Fatalf("ListTools = %v after %v; want an immediate error, not the caller's deadline", err, time.Since(start))
	}
}

// A1 C-6: a notification the server rejects is a failed write. The handshake in
// particular must not look complete when notifications/initialized was refused.
func TestHTTPClientInitializeFailsWhenInitializedIsRefused(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ts := front(t, fx, func(w http.ResponseWriter, r *http.Request, m wireMessage) bool {
		if m.Method != NotifyInitialized {
			return false
		}
		http.Error(w, "token expired", http.StatusUnauthorized)
		return true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, ts.URL, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("Initialize err = %v, want it to report the 401", err)
	}
}

// A1 C-11: by default DialHTTP follows no redirect. The credential headers the
// caller supplied would be replayed to wherever it points, and net/http strips only
// Authorization and Cookie when the host changes.
func TestDialHTTPDoesNotFollowRedirectsByDefault(t *testing.T) {
	var mu sync.Mutex
	var leaked []string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		leaked = append(leaked, r.Header.Get("X-Api-Key"))
		mu.Unlock()
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusTemporaryRedirect))
	defer redirector.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, redirector.URL, ClientOptions{Header: http.Header{"X-Api-Key": {"SECRET"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Initialize(ctx)
	if err == nil || !strings.Contains(err.Error(), "307") {
		t.Fatalf("Initialize err = %v, want the 307 reported", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(leaked) != 0 {
		t.Fatalf("the redirect target received the request carrying X-Api-Key=%q", leaked)
	}
}

// A1 C-12: a server that pages forever must not make the client hold everything it
// is sent.
func TestClientListToolsBoundsWhatItRetains(t *testing.T) {
	c, fr := rawPeer(t, ClientOptions{})
	big := strings.Repeat("x", 4<<20)
	go func() {
		for i := 0; ; i++ {
			m, err := readFrame(fr)
			if err != nil {
				return
			}
			page := fmt.Sprintf(`{"tools":[{"name":"t%d","description":%q,"inputSchema":{}}],"nextCursor":"c%d"}`, i, big, i)
			if fr.WriteMessage([]byte(`{"jsonrpc":"2.0","id":`+string(m.ID)+`,"result":`+page+`}`)) != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := c.ListTools(ctx)
	if err == nil || ctx.Err() != nil || !strings.Contains(err.Error(), "listing exceeds") {
		t.Fatalf("ListTools err = %v (ctx err %v), want the listing-size bound", err, ctx.Err())
	}
}

// A1 C-4: a call that gives up frees what it held. Sixty-four calls to a server
// that never answers must not leave the sixty-fifth unable to be made at all.
func TestHTTPClientCancelledCallsFreeTheirSlots(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ts := front(t, fx, func(w http.ResponseWriter, r *http.Request, m wireMessage) bool {
		if m.Method != MethodToolsCall {
			return false
		}
		<-r.Context().Done() // never answers; ignores the protocol's cancellation
		return true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := DialHTTP(ctx, ts.URL, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range maxHTTPClientExchanges + 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc, stop := context.WithTimeout(ctx, 50*time.Millisecond)
			defer stop()
			_, _ = c.CallTool(cc, "echo", nil)
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("calls with a 50ms deadline were still blocked after 10s: the client stopped honouring its callers' contexts")
	}
	// Their slots are free again: a call the server does answer goes through.
	cc, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if _, err := c.ListTools(cc); err != nil {
		t.Fatalf("after the stalled calls gave up, ListTools = %v; their slots were not released", err)
	}
}

// A1 C-4: with every slot held by calls that cannot be cancelled, the next request is
// refused at once rather than left waiting behind a server it cannot see.
func TestHTTPClientRefusesRequestsBeyondItsSlots(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	ts := front(t, fx, func(w http.ResponseWriter, r *http.Request, m wireMessage) bool {
		if m.Method != MethodToolsCall {
			return false
		}
		<-r.Context().Done()
		return true
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := DialHTTP(ctx, ts.URL, ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	for range maxHTTPClientExchanges {
		go func() { _, _ = c.CallTool(ctx, "echo", nil) }() // never cancelled, never answered
	}
	eventually(t, "every slot to be held", func() bool { return len(c.fr.(*httpClientFramer).sem) == maxHTTPClientExchanges })
	start := time.Now()
	_, err = c.ListTools(ctx)
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("a request beyond the slots = %v after %v; want an immediate error", err, time.Since(start))
	}
}

// A1 B-1: the Client leaves params out of a notification that has none, as the
// Session does, instead of sending "params":null (not a legal params value).
func TestClientNotificationWithoutParamsOmitsThem(t *testing.T) {
	c, fr := rawPeer(t, ClientOptions{})
	got := make(chan []byte, 1)
	go func() {
		m, err := readFrame(fr)
		if err != nil {
			return
		}
		_ = fr.WriteMessage([]byte(`{"jsonrpc":"2.0","id":` + string(m.ID) + `,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"x","version":"0"}}}`))
		raw, _ := fr.ReadMessage() // net.Pipe is synchronous: Initialize's notification is read here
		got <- bytes.Clone(raw)
	}()
	if _, err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case raw := <-got:
		if bytes.Contains(raw, []byte(`"params"`)) {
			t.Fatalf("notifications/initialized carried params: %s", raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no notification seen")
	}
}

// A1 C-7: an open GET stream must not keep a session that never finished the
// handshake alive, or one connection per slot defeats HandshakeTimeout.
func TestHTTPGetStreamDoesNotKeepAHalfOpenSessionAlive(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{HandshakeTimeout: 100 * time.Millisecond, SessionTTL: time.Hour, KeepAlive: -1})
	resp := rpc(t, fx.ts.URL, "", initBody) // initialize only: no notifications/initialized
	sid := resp.Header.Get(headerSessionID)
	_ = readBody(t, resp)

	req, _ := http.NewRequest(http.MethodGet, fx.ts.URL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(headerSessionID, sid)
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	if stream.StatusCode != 200 {
		t.Fatalf("GET status %d", stream.StatusCode)
	}
	eventually(t, "the half-open session to be dropped despite its open GET", func() bool { return fx.sessionCount() == 0 })
}

// ...and a session that did finish the handshake keeps its stream open past the
// handshake timeout.
func TestHTTPGetStreamOfAReadySessionOutlivesHandshakeTimeout(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{HandshakeTimeout: 80 * time.Millisecond, SessionTTL: time.Hour, KeepAlive: -1})
	sid := rawInit(t, fx.ts.URL)
	eventually(t, "the session to be ready", func() bool {
		fx.h.mu.Lock()
		s := fx.h.sessions[sid]
		fx.h.mu.Unlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.sess != nil && s.sess.State() == StateReady
	})
	req, _ := http.NewRequest(http.MethodGet, fx.ts.URL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(headerSessionID, sid)
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	time.Sleep(400 * time.Millisecond) // five handshake timeouts
	if fx.sessionCount() != 1 {
		t.Fatal("a Ready session with an open stream was dropped by the handshake timer")
	}
}

// A1 C-3: a call's context bounds the call even while the write to a peer that has
// stopped reading is blocked, and a second caller queued behind that write is not
// held past its own deadline either.
func TestClientCallHonoursContextDuringABlockedWrite(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close() // never read: net.Pipe writes block
	c := Dial(a, ClientOptions{})
	defer c.Close()
	for i, d := range []time.Duration{200 * time.Millisecond, 200 * time.Millisecond} {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		done := make(chan error, 1)
		go func() { _, err := c.CallTool(ctx, "x", map[string]any{"pad": strings.Repeat("y", 1<<16)}); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("caller %d: no error from a peer that never reads", i)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("caller %d: still blocked 3s after a 200ms deadline", i)
		}
		cancel()
	}
	closed := make(chan struct{})
	go func() { _ = c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked behind the stalled write")
	}
}

// A1 C-1: an id stays in flight until the Session has answered it, even after the
// POST that carried it has gone. Otherwise a retry reusing the id is handed the
// first call's reply.
func TestHTTPIDStaysReservedAfterItsPostDrops(t *testing.T) {
	fx := newHTTPFixture(t, HTTPOptions{})
	sid := rawInit(t, fx.ts.URL)

	ctx, drop := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, fx.ts.URL, strings.NewReader(toolCall(7, "slow")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(headerSessionID, sid)
	go func() { _, _ = http.DefaultClient.Do(req) }()
	<-fx.slowStart
	drop() // the client walks away; the call keeps running
	time.Sleep(100 * time.Millisecond)

	if r := rpc(t, fx.ts.URL, sid, toolCall(7, "echo")); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("reusing id 7 while its first call still runs: status %d, want 400", r.StatusCode)
	}
	// Cancelling the call releases the id.
	rpc(t, fx.ts.URL, sid, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7}}`)
	<-fx.slowDone
	eventually(t, "id 7 to be reusable after the cancel", func() bool {
		r := rpc(t, fx.ts.URL, sid, toolCall(7, "echo"))
		_ = readBody(t, r)
		return r.StatusCode == http.StatusOK
	})
}
