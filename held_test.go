package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
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

// Sessions that cannot take a notification are waited on together, so one
// Broadcast costs one grace period however many peers have stopped reading.
func TestBroadcastWaitsOnStalledPeersTogether(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{}, nil)
	const grace = time.Second
	h.srv.broadcastGrace = grace
	a, b := h.client(), h.client()
	defer a.conn.Close()
	defer b.conn.Close()

	// Fill both sessions' outbound queues directly. A queue that merely reports
	// full may still be draining into the socket buffer, so keep going until it
	// is still full after a pause: the writer is then blocked on a peer that
	// does not read.
	frame, err := encodeNotification("notifications/test", map[string]string{"pad": strings.Repeat("x", 256<<10)})
	if err != nil {
		t.Fatal(err)
	}
	sessions := h.srv.Sessions()
	if len(sessions) != 2 {
		t.Fatalf("%d sessions, want 2", len(sessions))
	}
	for _, s := range sessions {
		stable := false
		for i := 0; i < 5000 && !stable; i++ {
			if err := s.offer(frame); err == nil {
				continue
			} else if !errors.Is(err, errQueueFull) {
				t.Fatalf("filling the queue: %v", err)
			}
			time.Sleep(40 * time.Millisecond)
			stable = errors.Is(s.offer(frame), errQueueFull)
		}
		if !stable {
			t.Fatal("the writer never blocked: the test cannot stall this peer")
		}
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
