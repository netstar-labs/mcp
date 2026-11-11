package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// H4 (D-3): a per-UID connection cap alone must not make a server refuse
// every connection on a platform that cannot read peer credentials. It
// degrades to untracked there; an explicit credential policy still fails
// closed.
func TestAdmitWithoutCredsOnlyFailsClosedForAnExplicitPolicy(t *testing.T) {
	credErr := errors.New("no peer credentials")
	cases := []struct {
		name string
		opts UnixOptions
		want bool
	}{
		{"no policy", UnixOptions{}, true},
		{"per-uid cap only", UnixOptions{MaxConnsPerUID: 8}, true},
		{"require creds", UnixOptions{RequirePeerCred: true}, false},
		{"uid allow-list", UnixOptions{AllowUID: []uint32{1000}}, false},
		{"gid allow-list", UnixOptions{AllowGID: []uint32{1000}}, false},
	}
	srv := New(Options{Name: "t"})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			defer b.Close()
			defer a.Close()
			if got := srv.admit(a, nil, credErr, tc.opts); got != tc.want {
				t.Fatalf("admit = %v, want %v", got, tc.want)
			}
			if !tc.want {
				// A refused connection is closed.
				if _, err := b.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
					t.Fatalf("refused connection not closed: %v", err)
				}
			}
		})
	}
}

// The default mcpd configuration (a per-uid cap, no allow-list) must serve a
// connection on every platform, with or without readable peer credentials.
func TestServePerUIDCapAloneServes(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{MaxConnsPerUID: 8}, nil)
	h.client() // completes the initialize handshake, or fails the test
}

// H5 (B-2): "2.0" is the only jsonrpc version accepted on stdio and sockets, for
// every kind of message and inside batches, as it already is over HTTP.

// readErrorFrame reads frames until one is an error response and returns it.
func (c *testClient) readErrorFrame() wireMessage {
	c.t.Helper()
	for {
		wm := c.readRaw()
		if wm.Error != nil {
			return wm
		}
	}
}

func TestBadVersionNotificationIsRejectedAndDoesNotInitialize(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  map[string]any
	}{
		{"1.0", map[string]any{"jsonrpc": "1.0", "method": NotifyInitialized}},
		{"missing", map[string]any{"method": NotifyInitialized}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, c := newTestServer(t, func(s *Server) { echoTool(s, "echo") })
			c.request(MethodInitialize, InitializeParams{
				ProtocolVersion: LatestProtocolVersion,
				ClientInfo:      Implementation{Name: "c", Version: "1"},
			})
			c.send(tc.msg)
			e := c.readErrorFrame()
			if e.Error.Code != CodeInvalidRequest || string(e.ID) != "null" {
				t.Fatalf("reply = code %d id %s, want %d with a null id", e.Error.Code, e.ID, CodeInvalidRequest)
			}
			// The rejected notification must not have completed the handshake.
			if err := c.requestErr(MethodToolsList, nil); err.Code != CodeNotInitialized {
				t.Fatalf("tools/list after a rejected initialized: code %d, want %d", err.Code, CodeNotInitialized)
			}
		})
	}
}

func TestBadVersionRequestKeepsItsID(t *testing.T) {
	_, c := newTestServer(t, nil)
	c.send(map[string]any{"id": 41, "method": MethodPing}) // jsonrpc missing
	e := c.readErrorFrame()
	if e.Error.Code != CodeInvalidRequest || string(e.ID) != "41" {
		t.Fatalf("reply = code %d id %s, want %d with id 41", e.Error.Code, e.ID, CodeInvalidRequest)
	}
}

func TestBadVersionInsideABatch(t *testing.T) {
	_, c := newTestServer(t, nil)
	c.handshake()
	c.send([]map[string]any{
		{"jsonrpc": "2.0", "id": 1, "method": MethodPing},
		{"jsonrpc": "1.0", "id": 2, "method": MethodPing},
		{"jsonrpc": "1.0", "method": "notifications/whatever"},
	})
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var replies []wireMessage
	if err := json.Unmarshal(line, &replies); err != nil {
		t.Fatalf("batch reply is not an array: %s", line)
	}
	if len(replies) != 3 {
		t.Fatalf("got %d replies, want 3 (ok, error for id 2, error for the notification): %s", len(replies), line)
	}
	if replies[0].Error != nil || string(replies[0].ID) != "1" {
		t.Fatalf("member 1 = %+v, want a result for id 1", replies[0])
	}
	if replies[1].Error == nil || string(replies[1].ID) != "2" {
		t.Fatalf("member 2 = %+v, want an error for id 2", replies[1])
	}
	if replies[2].Error == nil || string(replies[2].ID) != "null" {
		t.Fatalf("member 3 = %+v, want an error with a null id", replies[2])
	}
}

func TestBadVersionResponseIsNotRouted(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "ask"}, func(ctx context.Context, sess *Session, _ json.RawMessage) (*CallToolResult, error) {
			ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			r, err := sess.Elicit(ctx, "who?", json.RawMessage(`{"type":"object"}`))
			if err != nil {
				return ToolText("unanswered"), nil
			}
			return ToolText("answered:%s", r.Action), nil
		})
	})
	c.handshakeWith(ClientCapabilities{Elicitation: &emptyCap{}})
	id := c.callTool("ask")
	req := c.serverRequest(MethodElicitationCreate)
	c.send(map[string]any{"jsonrpc": "1.0", "id": json.RawMessage(req.ID),
		"result": map[string]any{"action": "accept"}})
	// The rejection does not carry the response's id: an error addressed to it
	// could be taken for the answer to one of the peer's own requests.
	if e := c.readErrorFrame(); e.Error.Code != CodeInvalidRequest || string(e.ID) != "null" {
		t.Fatalf("rejection = code %d id %s, want %d with a null id", e.Error.Code, e.ID, CodeInvalidRequest)
	}
	if got := toolResultText(t, c.awaitID(id)); got != "unanswered" {
		t.Fatalf("tool saw %q, want the bad-version response ignored", got)
	}
}

// H1 (C-9): queued and running requests are bounded by the bytes they hold, per
// session, not only by their number.

func queueTool(gate chan struct{}) func(*Server) {
	return func(s *Server) {
		_ = s.AddTool(Tool{Name: "hold"}, func(ctx context.Context, _ *Session, _ json.RawMessage) (*CallToolResult, error) {
			select {
			case <-gate:
			case <-ctx.Done():
			}
			return ToolText("ok"), nil
		})
	}
}

// holdCall sends a tools/call for "hold" carrying about n bytes of arguments.
func (c *testClient) holdCall(n int) json.RawMessage {
	c.t.Helper()
	c.id++
	id := c.id
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": MethodToolsCall,
		"params": map[string]any{"name": "hold", "arguments": map[string]any{"pad": strings.Repeat("x", n)}}})
	return json.RawMessage(fmt.Sprintf("%d", id))
}

func TestQueuedBytesBudget(t *testing.T) {
	gate := make(chan struct{})
	h := startServer(t, Options{MaxConcurrentRequests: 1, MaxQueuedBytes: 100_000}, UnixOptions{}, queueTool(gate))
	c := h.client()

	id1 := c.holdCall(40_000) // runs
	id2 := c.holdCall(40_000) // queued behind it: 80k held, within budget
	id3 := c.holdCall(40_000) // would hold 120k: refused

	// The refusal is immediate, while the first two are still held.
	e := c.readErrorFrame()
	if e.Error.Code != CodeInternalError || !strings.Contains(e.Error.Message, "overloaded") || string(e.ID) != string(id3) {
		t.Fatalf("third request got id %s %+v, want an overloaded error for id %s", e.ID, e.Error, id3)
	}

	close(gate)
	for _, id := range []json.RawMessage{id1, id2} {
		if got := toolResultText(t, c.awaitID(id)); got != "ok" {
			t.Fatalf("request %s = %q", id, got)
		}
	}

	// What was held is released: the same load fits again.
	id4 := c.holdCall(40_000)
	if got := toolResultText(t, c.awaitID(id4)); got != "ok" {
		t.Fatalf("request after the queue drained = %q", got)
	}
}

// A single request larger than the budget still runs when nothing else is
// held, so a small budget never makes a large message impossible.
func TestQueuedBytesBudgetAdmitsALoneOversizedRequest(t *testing.T) {
	gate := make(chan struct{})
	close(gate)
	h := startServer(t, Options{MaxQueuedBytes: 1000}, UnixOptions{}, queueTool(gate))
	c := h.client()
	if got := toolResultText(t, c.awaitID(c.holdCall(50_000))); got != "ok" {
		t.Fatalf("lone oversized request = %q", got)
	}
}

// H2 (C-14): Broadcast is not held up by a peer that stopped reading, and drops
// only that peer.
func TestBroadcastDropsAPeerThatStoppedReading(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{}, nil) // WriteTimeout 0: nothing else bounds a write
	h.srv.broadcastGrace = 300 * time.Millisecond

	healthy := h.client()
	var got atomic.Int64
	go func() { // the healthy peer keeps reading
		for {
			if _, err := healthy.r.ReadBytes('\n'); err != nil {
				return
			}
			got.Add(1)
		}
	}()
	stalled := h.client() // never reads again

	eventually(t, "both sessions ready", func() bool {
		n := 0
		for _, s := range h.srv.Sessions() {
			if s.State() == StateReady {
				n++
			}
		}
		return n == 2
	})

	pad := strings.Repeat("x", 256<<10)
	done := make(chan int)
	go func() {
		sent := 0
		for ; sent < 400 && len(h.srv.Sessions()) == 2; sent++ {
			h.srv.Broadcast("notifications/test", map[string]string{"pad": pad})
		}
		done <- sent
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Broadcast is still blocked on the peer that stopped reading")
	}

	if n := len(h.srv.Sessions()); n != 1 {
		t.Fatalf("%d sessions left, want only the healthy one", n)
	}
	// The stalled peer's connection was closed by the server.
	_ = stalled.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1<<20)
	for {
		if _, err := stalled.conn.Read(buf); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("the stalled peer's connection is still open")
			}
			break
		}
	}
	if got.Load() == 0 {
		t.Fatal("the healthy peer received nothing")
	}
}

// H3 (C-8): HTTPOptions.MaxSessionsPerIdentity caps the live sessions one
// identity may hold, so one caller cannot fill MaxSessions.
func TestHTTPMaxSessionsPerIdentity(t *testing.T) {
	who := func(r *http.Request) string { return r.Header.Get("X-Caller") }
	fx := newHTTPFixture(t, HTTPOptions{Identify: who, MaxSessionsPerIdentity: 2})

	open := func(caller string) (string, int) {
		var extra []string
		if caller != "" {
			extra = []string{"X-Caller", caller}
		}
		resp := rpc(t, fx.ts.URL, "", initBody, extra...)
		_ = readBody(t, resp)
		return resp.Header.Get(headerSessionID), resp.StatusCode
	}

	sid1, st := open("alice")
	if st != 200 {
		t.Fatalf("alice #1: status %d", st)
	}
	if _, st = open("alice"); st != 200 {
		t.Fatalf("alice #2: status %d", st)
	}
	if _, st = open("alice"); st != http.StatusTooManyRequests {
		t.Fatalf("alice #3: status %d, want 429", st)
	}
	// Another identity is unaffected: alice did not take bob's room.
	if _, st = open("bob"); st != 200 {
		t.Fatalf("bob #1: status %d", st)
	}

	// Ending a session frees its slot, once (a second DELETE is a 404 and must
	// not free another).
	del := func(sid, caller string) int {
		req, _ := http.NewRequest(http.MethodDelete, fx.ts.URL, nil)
		req.Header.Set(headerSessionID, sid)
		req.Header.Set("X-Caller", caller)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if st := del(sid1, "alice"); st != http.StatusNoContent {
		t.Fatalf("DELETE status %d", st)
	}
	del(sid1, "alice")
	if _, st = open("alice"); st != 200 {
		t.Fatalf("alice after a DELETE: status %d, want 200", st)
	}
	if _, st = open("alice"); st != http.StatusTooManyRequests {
		t.Fatalf("alice over the cap again: status %d, want 429 (the count drifted)", st)
	}

	// No identity, no per-identity cap: the global one is the only bound.
	for i := 0; i < 4; i++ {
		if _, st = open(""); st != 200 {
			t.Fatalf("anonymous #%d: status %d", i, st)
		}
	}
}

// A session ending on its own (handshake timeout) also releases its slot.
func TestHTTPPerIdentitySlotReleasedOnExpiry(t *testing.T) {
	who := func(r *http.Request) string { return r.Header.Get("X-Caller") }
	fx := newHTTPFixture(t, HTTPOptions{Identify: who, MaxSessionsPerIdentity: 1, HandshakeTimeout: 60 * time.Millisecond})
	for i := 0; i < 3; i++ {
		eventually(t, "alice's slot to be free", func() bool {
			resp := rpc(t, fx.ts.URL, "", initBody, "X-Caller", "alice")
			_ = readBody(t, resp)
			return resp.StatusCode == 200
		})
	}
}

// wedge fills a session's outbound queue until its writer is blocked on a peer
// that does not read. A queue that merely reports full may still be draining
// into the socket buffer, so it keeps going until the queue is still full after
// a pause.
func wedge(t *testing.T, s *Session, frame []byte) {
	t.Helper()
	for i := 0; i < 5000; i++ {
		if err := s.offer(frame); err == nil {
			continue
		} else if !errors.Is(err, errQueueFull) {
			t.Fatalf("filling the queue: %v", err)
		}
		time.Sleep(40 * time.Millisecond)
		if errors.Is(s.offer(frame), errQueueFull) {
			return
		}
	}
	t.Fatal("the writer never blocked: the test cannot stall this peer")
}

// Sessions that cannot take a notification are waited on together, so one
// Broadcast costs one grace period however many peers have stopped reading.
func TestBroadcastWaitsOnStalledPeersTogether(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{}, nil)
	const grace = time.Second
	h.srv.broadcastGrace = grace
	a, b := h.client(), h.client()
	defer a.conn.Close()
	defer b.conn.Close()

	frame, err := encodeNotification("notifications/test", map[string]string{"pad": strings.Repeat("x", 256<<10)})
	if err != nil {
		t.Fatal(err)
	}
	sessions := h.srv.Sessions()
	if len(sessions) != 2 {
		t.Fatalf("%d sessions, want 2", len(sessions))
	}
	for _, s := range sessions {
		wedge(t, s, frame)
	}

	start := time.Now()
	h.srv.Broadcast("notifications/test", struct{}{})
	took := time.Since(start)
	if took < grace/2 {
		t.Fatalf("Broadcast returned in %v: the queues were not full, the test measured nothing", took)
	}
	if took > grace*17/10 {
		t.Fatalf("Broadcast took %v with two stalled peers, want about one grace period (%v)", took, grace)
	}
	// The registry drops a session when its ServeConn returns, just after Close.
	eventually(t, "both stalled peers to be closed", func() bool { return len(h.srv.Sessions()) == 0 })
}

// Everything a queued request retains counts against the budget, not only the
// members the dispatcher reads: a request that also carries a "result" or
// "error" member is still a request, and the Session keeps the whole message.
func TestQueuedBytesBudgetCountsUnreadMembers(t *testing.T) {
	for _, member := range []string{"result", "error"} {
		t.Run(member, func(t *testing.T) {
			gate := make(chan struct{})
			h := startServer(t, Options{MaxConcurrentRequests: 1, MaxQueuedBytes: 100_000}, UnixOptions{}, queueTool(gate))
			c := h.client()
			send := func() json.RawMessage {
				c.id++
				id := c.id
				c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": MethodToolsCall,
					"params": map[string]any{"name": "hold"},
					member:   map[string]any{"pad": strings.Repeat("x", 40_000)}})
				return json.RawMessage(fmt.Sprintf("%d", id))
			}
			send()
			send()
			id3 := send()
			e := c.readErrorFrame()
			if !strings.Contains(e.Error.Message, "overloaded") || string(e.ID) != string(id3) {
				t.Fatalf("third request got id %s %+v, want an overloaded error for id %s", e.ID, e.Error, id3)
			}
			close(gate)
		})
	}
}

// A refused initialize must leave the counts exactly as it found them, even if
// its handshake timer fires while it waits for the registry lock.
func TestHTTPRefusedInitializeDoesNotUnbalanceTheCounts(t *testing.T) {
	who := func(r *http.Request) string { return r.Header.Get("X-Caller") }
	fx := newHTTPFixture(t, HTTPOptions{Identify: who, MaxSessionsPerIdentity: 1,
		SessionTTL: time.Hour, HandshakeTimeout: 30 * time.Millisecond})

	// alice's one session is Ready, so the short handshake timer leaves it alone.
	resp := rpc(t, fx.ts.URL, "", initBody, "X-Caller", "alice")
	sid := resp.Header.Get(headerSessionID)
	_ = readBody(t, resp)
	rpc(t, fx.ts.URL, sid, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, "X-Caller", "alice")

	// A second initialize, delayed at the registry lock for longer than its own
	// handshake timer.
	fx.h.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequest(http.MethodPost, fx.ts.URL, strings.NewReader(initBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("X-Caller", "alice")
		if r, err := http.DefaultClient.Do(req); err == nil {
			_ = r.Body.Close()
		}
	}()
	time.Sleep(150 * time.Millisecond)
	fx.h.mu.Unlock()
	<-done

	// Let every timer that is going to fire do so, then compare the books.
	time.Sleep(200 * time.Millisecond)
	fx.h.mu.Lock()
	defer fx.h.mu.Unlock()
	live := 0
	for _, s := range fx.h.sessions {
		if s.owner == "alice" {
			live++
		}
	}
	if live != 1 || fx.h.owned["alice"] != 1 {
		t.Fatalf("alice: %d live sessions, owned = %d; want 1 and 1", live, fx.h.owned["alice"])
	}
}

// The rejection does not echo the peer's version string back: a notification
// has no reply to amplify otherwise, and the string is the peer's to make as
// long (and as expensive to quote) as the frame limit allows.
func TestBadVersionRejectionDoesNotEchoThePeersString(t *testing.T) {
	_, c := newTestServer(t, nil)
	c.handshake()
	c.send(map[string]any{"jsonrpc": strings.Repeat("\u0080", 100_000), "method": "notifications/x"})
	e := c.readErrorFrame()
	if e.Error.Code != CodeInvalidRequest {
		t.Fatalf("code = %d", e.Error.Code)
	}
	if n := len(e.Error.Message); n > 200 {
		t.Fatalf("rejection message is %d bytes for a 200000-byte version string", n)
	}
}

// A peer that is reading, however slowly, is not closed: the grace is for a
// peer that makes no progress, not a deadline on the whole wait. Here the
// notification queues behind a full queue and 64 replies blocked on it, which a
// slow reader needs far longer than the grace to drain.
func TestBroadcastWaitsOnAPeerThatIsReadingSlowly(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{}, nil)
	const grace = 150 * time.Millisecond
	h.srv.broadcastGrace = grace
	c := h.client()
	sess := h.srv.Sessions()[0]

	frame, err := encodeNotification("notifications/test", map[string]string{"pad": strings.Repeat("x", 32<<10)})
	if err != nil {
		t.Fatal(err)
	}
	wedge(t, sess, frame)
	for i := 0; i < 64; i++ { // replies from busy handlers, ahead of the broadcast
		go sess.sendRaw(frame)
	}
	time.Sleep(100 * time.Millisecond)

	go func() { // the peer wakes up and reads one frame's worth every 10ms
		buf := make([]byte, 32<<10)
		for {
			time.Sleep(10 * time.Millisecond)
			_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.conn.Read(buf); err != nil {
				return
			}
		}
	}()

	start := time.Now()
	h.srv.Broadcast("notifications/test", struct{}{})
	took := time.Since(start)
	if st := sess.State(); st != StateReady {
		t.Fatalf("a peer that was reading was closed (state %d) after %v", st, took)
	}
	if took < 2*grace {
		t.Fatalf("Broadcast returned in %v: it did not have to wait, so the test measured nothing", took)
	}
}

// logBuf is a log destination safe to read while a server is still logging.
type logBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Where the per-uid cap cannot be enforced, the server says so once, instead of
// degrading silently (or once per connection).
func TestPerUIDCapNotEnforcedIsLoggedOnce(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("peer credentials are readable on Linux, so the cap is enforced")
	}
	var lb logBuf
	h := startServer(t, Options{Logger: log.New(&lb, "", 0)}, UnixOptions{MaxConnsPerUID: 8}, nil)
	h.client()
	h.client()
	h.client()
	if n := strings.Count(lb.String(), "MaxConnsPerUID is not enforced"); n != 1 {
		t.Fatalf("logged %d times, want once; log:\n%s", n, lb.String())
	}
}

// A peer that makes progress too slowly to matter is closed all the same: the
// wait on one session has an overall bound, or a peer that drains a frame every
// few seconds could hold a Broadcast caller for as long as it likes.
func TestBroadcastGivesUpOnAPeerThatReadsTooSlowly(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{}, nil)
	h.srv.broadcastGrace = 100 * time.Millisecond
	h.srv.broadcastMaxWait = 500 * time.Millisecond
	c := h.client()
	sess := h.srv.Sessions()[0]

	frame, err := encodeNotification("notifications/test", map[string]string{"pad": strings.Repeat("x", 32<<10)})
	if err != nil {
		t.Fatal(err)
	}
	wedge(t, sess, frame)
	for i := 0; i < 64; i++ {
		go sess.sendRaw(frame)
	}
	time.Sleep(100 * time.Millisecond)
	go func() { // reading, so the writer progresses, but needs over a second to get through
		buf := make([]byte, 32<<10)
		for {
			time.Sleep(10 * time.Millisecond)
			_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.conn.Read(buf); err != nil {
				return
			}
		}
	}()

	start := time.Now()
	h.srv.Broadcast("notifications/test", struct{}{})
	took := time.Since(start)
	if st := sess.State(); st != StateClosed {
		t.Fatalf("a peer too slow to matter was kept (state %d) for %v", st, took)
	}
	if took > 1500*time.Millisecond {
		t.Fatalf("Broadcast waited %v for a session capped at 500ms", took)
	}
}
