package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// rawPeer dials a Client over a net.Pipe and returns the other end as a Framer,
// so a test plays the server by hand and sees exactly what the Client puts on the
// wire — which a real Server hides, because it answers correctly.
func rawPeer(t *testing.T, opts ClientOptions) (*Client, Framer) {
	t.Helper()
	a, b := net.Pipe()
	c := Dial(a, opts)
	fr := NewFramer(b, b, FramerOptions{Closer: b})
	t.Cleanup(func() { _ = c.Close(); _ = fr.Close() })
	return c, fr
}

func mustRead(t *testing.T, fr Framer) wireMessage {
	t.Helper()
	m, err := readFrame(fr)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// readFrame is mustRead without the t.Fatal, so a goroutine playing the server
// can use it.
func readFrame(fr Framer) (wireMessage, error) {
	done := make(chan struct{})
	var (
		raw []byte
		err error
	)
	go func() { raw, err = fr.ReadMessage(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return wireMessage{}, errors.New("timed out reading a frame from the client")
	}
	if err != nil {
		return wireMessage{}, fmt.Errorf("read frame: %w", err)
	}
	var m wireMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return wireMessage{}, fmt.Errorf("decode %q: %w", raw, err)
	}
	return m, nil
}

func mustWrite(t *testing.T, fr Framer, s string) {
	t.Helper()
	if err := fr.WriteMessage([]byte(s)); err != nil {
		t.Errorf("write %s: %v", s, err)
	}
}

// A server may ping its client at any time and the receiver must answer with an
// empty result (basic/utilities/ping). With no handler the client used to answer
// method-not-found, which a liveness-checking server reads as a dead peer.
func TestClientAnswersPing(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts ClientOptions
	}{
		{"nil handler", ClientOptions{}},
		{"handler rejecting everything", ClientOptions{OnRequest: func(context.Context, string, json.RawMessage) (any, *RPCError) {
			return nil, Errorf(CodeMethodNotFound, "no")
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, fr := rawPeer(t, tc.opts)
			mustWrite(t, fr, `{"jsonrpc":"2.0","id":7,"method":"ping"}`)
			m := mustRead(t, fr)
			if m.Error != nil || string(m.Result) != "{}" || string(m.ID) != "7" {
				t.Fatalf("ping answered with %+v (result %s), want id 7 and {}", m.Error, m.Result)
			}
		})
	}
}

// OnRequest runs off the read loop. While it blocks (an elicitation waits on a
// human), responses to the client's own calls must still be delivered.
func TestClientServerRequestDoesNotBlockReadLoop(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	c, fr := rawPeer(t, ClientOptions{OnRequest: func(context.Context, string, json.RawMessage) (any, *RPCError) {
		close(started)
		<-release
		return struct{}{}, nil
	}})
	defer close(release)

	go func() {
		call, err := readFrame(fr) // tools/call
		if err != nil {
			t.Error(err)
			return
		}
		mustWrite(t, fr, `{"jsonrpc":"2.0","id":99,"method":"elicitation/create","params":{}}`)
		<-started
		mustWrite(t, fr, `{"jsonrpc":"2.0","id":`+string(call.ID)+`,"result":{"content":[]}}`)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.CallTool(ctx, "x", nil); err != nil {
		t.Fatalf("a blocked OnRequest stalled the read loop: %v", err)
	}
}

// A handler may call back into the same Client; run inline on the read loop
// that would deadlock, because the reply it waits for is read by that loop.
func TestClientServerRequestHandlerMayCallBack(t *testing.T) {
	var c *Client
	c, fr := rawPeer(t, ClientOptions{OnRequest: func(ctx context.Context, _ string, _ json.RawMessage) (any, *RPCError) {
		if _, err := c.ListTools(ctx); err != nil {
			return nil, Errorf(CodeInternalError, "%v", err)
		}
		return struct{}{}, nil
	}})
	mustWrite(t, fr, `{"jsonrpc":"2.0","id":50,"method":"elicitation/create","params":{}}`)
	list := mustRead(t, fr) // the handler's own ListTools
	mustWrite(t, fr, `{"jsonrpc":"2.0","id":`+string(list.ID)+`,"result":{"tools":[]}}`)
	m := mustRead(t, fr)
	if string(m.ID) != "50" || m.Error != nil {
		t.Fatalf("handler that called back got %+v / id %s", m.Error, m.ID)
	}
}

// Cancelling the context of a pending call tells the server to stop (utilities/
// cancellation); initialize is the one request the spec forbids cancelling.
func TestClientCancelSendsNotification(t *testing.T) {
	c, fr := rawPeer(t, ClientOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := c.CallTool(ctx, "slow", nil); errc <- err }()
	call := mustRead(t, fr)
	cancel()
	if err := <-errc; err != context.Canceled {
		t.Fatalf("CallTool err = %v, want context.Canceled", err)
	}
	n := mustRead(t, fr)
	if n.Method != NotifyCancelled {
		t.Fatalf("after cancel the server saw %q, want %s", n.Method, NotifyCancelled)
	}
	var p CancelledParams
	_ = json.Unmarshal(n.Params, &p)
	if string(p.RequestID) != string(call.ID) {
		t.Fatalf("cancelled requestId %s, want %s", p.RequestID, call.ID)
	}
}

func TestClientDoesNotCancelInitialize(t *testing.T) {
	c, fr := rawPeer(t, ClientOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := c.Initialize(ctx); errc <- err }()
	mustRead(t, fr)
	cancel()
	<-errc
	// The next frame, if any, must not be a cancellation.
	_ = c.Close()
	if raw, err := fr.ReadMessage(); err == nil {
		t.Fatalf("client sent %s after initialize was cancelled; initialize must not be cancelled", raw)
	}
}

// A server cancelling its own request to the client (e.g. a timed-out
// elicitation) must reach the OnRequest handler's context, and the reply is
// suppressed.
func TestClientServerCancelReachesHandler(t *testing.T) {
	cancelled := make(chan struct{})
	started := make(chan struct{})
	_, fr := rawPeer(t, ClientOptions{OnRequest: func(ctx context.Context, _ string, _ json.RawMessage) (any, *RPCError) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, Errorf(CodeInternalError, "cancelled")
	}})
	mustWrite(t, fr, `{"jsonrpc":"2.0","id":5,"method":"elicitation/create","params":{}}`)
	<-started
	mustWrite(t, fr, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":5}}`)
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("notifications/cancelled did not reach the handler's context")
	}
	// Prove the reply was suppressed: the next frame the peer sees is the ping answer.
	mustWrite(t, fr, `{"jsonrpc":"2.0","id":6,"method":"ping"}`)
	if m := mustRead(t, fr); string(m.ID) != "6" {
		t.Fatalf("got a reply for id %s, want the ping (6); the cancelled request must not be answered", m.ID)
	}
}

// Close cancels the context of a handler that is still running, so it cannot
// outlive the connection.
func TestClientCloseCancelsHandlers(t *testing.T) {
	var sawCancel atomic.Bool
	done := make(chan struct{})
	started := make(chan struct{})
	c, fr := rawPeer(t, ClientOptions{OnRequest: func(ctx context.Context, _ string, _ json.RawMessage) (any, *RPCError) {
		close(started)
		<-ctx.Done()
		sawCancel.Store(true)
		close(done)
		return nil, Errorf(CodeInternalError, "closed")
	}})
	mustWrite(t, fr, `{"jsonrpc":"2.0","id":5,"method":"elicitation/create","params":{}}`)
	<-started
	_ = c.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close left a handler running")
	}
	if !sawCancel.Load() {
		t.Fatal("handler ended without a cancelled context")
	}
}

func TestClientListToolsFollowsCursor(t *testing.T) {
	c, fr := rawPeer(t, ClientOptions{})
	go func() {
		pages := []string{
			`{"tools":[{"name":"a","inputSchema":{}}],"nextCursor":"p2"}`,
			`{"tools":[{"name":"b","inputSchema":{}}],"nextCursor":"p3"}`,
			`{"tools":[{"name":"c","inputSchema":{}}]}`,
		}
		for i, page := range pages {
			m, err := readFrame(fr)
			if err != nil {
				t.Error(err)
				return
			}
			var p PaginatedParams
			_ = json.Unmarshal(m.Params, &p)
			if want := []string{"", "p2", "p3"}[i]; p.Cursor != want {
				t.Errorf("page %d requested with cursor %q, want %q", i, p.Cursor, want)
			}
			mustWrite(t, fr, `{"jsonrpc":"2.0","id":`+string(m.ID)+`,"result":`+page+`}`)
		}
	}()
	tools, err := c.ListTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 3 || tools[2].Name != "c" {
		t.Fatalf("got %d tools %+v, want a, b, c across three pages", len(tools), tools)
	}
}

// A server that keeps returning the same cursor must not loop the client forever.
func TestClientListToolsRefusesCursorLoop(t *testing.T) {
	c, fr := rawPeer(t, ClientOptions{})
	go func() {
		for {
			raw, err := fr.ReadMessage()
			if err != nil {
				return
			}
			var m wireMessage
			_ = json.Unmarshal(raw, &m)
			_ = fr.WriteMessage([]byte(`{"jsonrpc":"2.0","id":` + string(m.ID) + `,"result":{"tools":[],"nextCursor":"same"}}`))
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.ListTools(ctx)
	if err == nil || !strings.Contains(err.Error(), "cursor") {
		t.Fatalf("err = %v, want a repeated-cursor error", err)
	}
}

// The handshake result names a version; a version the client does not speak is
// an error, not a silent mismatch (lifecycle: the client SHOULD disconnect).
func TestClientRejectsUnsupportedVersion(t *testing.T) {
	c, fr := rawPeer(t, ClientOptions{})
	go func() {
		m, err := readFrame(fr)
		if err != nil {
			t.Error(err)
			return
		}
		mustWrite(t, fr, `{"jsonrpc":"2.0","id":`+string(m.ID)+`,"result":{"protocolVersion":"1999-01-01","capabilities":{},"serverInfo":{"name":"x","version":"0"}}}`)
	}()
	if _, err := c.Initialize(context.Background()); err == nil || !strings.Contains(err.Error(), "1999-01-01") {
		t.Fatalf("err = %v, want an unsupported-version error naming 1999-01-01", err)
	}
}
