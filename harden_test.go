package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSanitizeNeutralisesControlCharacters(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"line\nbreak", "line.break"},
		{"carriage\rreturn", "carriage.return"},
		{"tab\there", "tab.here"},
		{"nul\x00byte", "nul.byte"},
		{"del\x7f", "del."},
		{"unicode ✓ kept", "unicode ✓ kept"},
	}
	for _, tc := range cases {
		if got := sanitize(tc.in); got != tc.want {
			t.Fatalf("sanitize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	long := sanitize(strings.Repeat("a", maxLogFieldBytes*2))
	if len(long) > maxLogFieldBytes+3 {
		t.Fatalf("sanitize did not truncate: len = %d", len(long))
	}

	// Truncation must not split a multi-byte rune.
	multi := sanitize(strings.Repeat("é", maxLogFieldBytes))
	if !utf8.ValidString(strings.TrimSuffix(multi, "...")) {
		t.Fatal("truncation produced invalid UTF-8")
	}
}

// A client that embeds a newline in its name must not be able to write a
// forged line into the server's log.
func TestLogInjectionViaClientName(t *testing.T) {
	var logbuf syncBuffer
	h := startServer(t, Options{
		Logger:         log.New(&logbuf, "", 0),
		OnSessionStart: func(s *Session) {},
	}, UnixOptions{}, func(s *Server) { echoTool(s, "echo") })

	c := h.dial()
	c.request(MethodInitialize, InitializeParams{
		ProtocolVersion: LatestProtocolVersion,
		ClientInfo: Implementation{
			Name:    "evil\n2026/01/01 00:00:00 auth: access granted for root",
			Version: "1\r\ninjected",
		},
	})
	c.notify(NotifyInitialized, struct{}{})
	c.request(MethodToolsList, nil)

	info := h.srv.Sessions()[0].ClientInfo()
	if strings.ContainsAny(info.Name, "\r\n") || strings.ContainsAny(info.Version, "\r\n") {
		t.Fatalf("client identity retained control characters: %+v", info)
	}
	if !strings.Contains(info.Name, "evil") {
		t.Fatalf("sanitisation destroyed the value: %q", info.Name)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestOversizedIDIsRejected(t *testing.T) {
	_, c := newTestServer(t, nil)
	c.send(map[string]any{
		"jsonrpc": "2.0",
		"id":      strings.Repeat("A", maxIDBytes+1),
		"method":  MethodPing,
	})
	wm := c.readRaw()
	if wm.Error == nil || wm.Error.Code != CodeInvalidRequest {
		t.Fatalf("expected invalid request, got %+v", wm)
	}
}

func TestOversizedMethodIsRejected(t *testing.T) {
	_, c := newTestServer(t, nil)
	c.send(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  strings.Repeat("m", maxMethodBytes+1),
	})
	wm := c.readRaw()
	if wm.Error == nil || wm.Error.Code != CodeInvalidRequest {
		t.Fatalf("expected invalid request, got %+v", wm)
	}
}

func TestBatchRejectedUnlessEnabled(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{}, func(s *Server) { echoTool(s, "echo") })
	c := h.client()
	c.send([]any{map[string]any{"jsonrpc": "2.0", "id": 1, "method": MethodPing}})
	wm := c.readRaw()
	if wm.Error == nil || wm.Error.Code != CodeInvalidRequest {
		t.Fatalf("batching should be off by default, got %+v", wm)
	}
}

func TestBatchItemLimit(t *testing.T) {
	h := startServer(t, Options{AcceptBatch: true, MaxBatchItems: 4}, UnixOptions{},
		func(s *Server) { echoTool(s, "echo") })
	c := h.client()

	var batch []any
	for i := 0; i < 5; i++ {
		batch = append(batch, map[string]any{"jsonrpc": "2.0", "id": 100 + i, "method": MethodPing})
	}
	c.send(batch)
	wm := c.readRaw()
	if wm.Error == nil || wm.Error.Code != CodeInvalidRequest {
		t.Fatalf("expected batch rejection, got %+v", wm)
	}
	if !strings.Contains(wm.Error.Message, "4") {
		t.Fatalf("error should name the limit: %q", wm.Error.Message)
	}
}

func TestSubscriptionLimit(t *testing.T) {
	h := startServer(t, Options{MaxSubscriptions: 3}, UnixOptions{}, func(s *Server) {
		_ = s.AddResource(Resource{URI: "test://r", Name: "r"},
			func(ctx context.Context, sess *Session, uri string) ([]ResourceContents, error) {
				return nil, nil
			})
	})
	c := h.client()

	for i := 0; i < 3; i++ {
		c.request(MethodResourcesSubscribe, SubscribeParams{URI: fmt.Sprintf("test://r/%d", i)})
	}
	err := c.requestErr(MethodResourcesSubscribe, SubscribeParams{URI: "test://r/overflow"})
	if err.Code != CodeResourceLimit {
		t.Fatalf("code = %d, want %d", err.Code, CodeResourceLimit)
	}

	// Re-subscribing to an existing URI is not a new allocation and must
	// still be accepted at the limit.
	c.request(MethodResourcesSubscribe, SubscribeParams{URI: "test://r/0"})

	// Freeing one makes room again.
	c.request(MethodResourcesUnsubscribe, SubscribeParams{URI: "test://r/0"})
	c.request(MethodResourcesSubscribe, SubscribeParams{URI: "test://r/new"})
}

func TestOversizedSubscriptionURIRejected(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{}, func(s *Server) {
		_ = s.AddResource(Resource{URI: "test://r", Name: "r"},
			func(ctx context.Context, sess *Session, uri string) ([]ResourceContents, error) {
				return nil, nil
			})
	})
	c := h.client()
	err := c.requestErr(MethodResourcesSubscribe, SubscribeParams{
		URI: "test://" + strings.Repeat("x", maxSubscriptionURIBytes),
	})
	if err.Code != CodeInvalidParams {
		t.Fatalf("code = %d, want %d", err.Code, CodeInvalidParams)
	}
}

// A client that connects and then says nothing must be dropped rather than
// holding a goroutine, a buffer and a descriptor forever.
func TestIdleTimeoutDropsSilentConnection(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{IdleTimeout: 150 * time.Millisecond}, nil)
	c := h.dial()

	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := c.conn.Read(buf); err == nil {
		t.Fatal("connection stayed open past the idle timeout")
	}
}

func TestHandshakeTimeoutDropsUninitialisedSession(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{HandshakeTimeout: 150 * time.Millisecond}, nil)
	c := h.dial()

	// Ping is answered before initialize, so the connection is active but the
	// handshake is still incomplete: the timer must fire anyway.
	c.request(MethodPing, nil)

	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := c.conn.Read(buf); err == nil {
		t.Fatal("uninitialised session survived the handshake timeout")
	}
}

func TestMaxConnsPerUID(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("per-uid limits need SO_PEERCRED")
	}
	h := startServer(t, Options{}, UnixOptions{MaxConnsPerUID: 2}, nil)

	var live []net.Conn
	defer func() {
		for _, c := range live {
			_ = c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		conn, err := net.Dial("unix", h.path)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		live = append(live, conn)
	}

	// The third connection is accepted at the socket layer and then closed by
	// the gate, so the failure surfaces as EOF on first read.
	conn, err := net.Dial("unix", h.path)
	if err != nil {
		t.Fatalf("dial 3: %v", err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("third connection from the same uid was not rejected")
	}
}

func TestListenerRefusesSymlinkedPath(t *testing.T) {
	dir := shortTempDir(t)
	target := filepath.Join(dir, "real.sock")
	link := filepath.Join(dir, "link.sock")

	l, err := ListenUnix(target, UnixOptions{})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if _, err := ListenUnix(link, UnixOptions{ReplaceStale: true}); err == nil {
		t.Fatal("bound through a symlink")
	} else if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// The socket must never exist with permissions wider than requested, which is
// what creating it under a umask buys over chmod-after-bind.
func TestSocketNeverWiderThanRequested(t *testing.T) {
	for _, mode := range []os.FileMode{0o600, 0o660, 0o640} {
		dir := shortTempDir(t)
		path := filepath.Join(dir, "m.sock")
		l, err := ListenUnix(path, UnixOptions{Mode: mode})
		if err != nil {
			t.Fatalf("listen %o: %v", mode, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if got := info.Mode().Perm(); got&^mode != 0 {
			t.Fatalf("mode %o is wider than requested %o", got, mode)
		}
		_ = l.Close()
	}
}

// The framer reuses its read buffer between messages. Everything the session
// retains must therefore be a copy; this pipelines distinct payloads and
// checks that neither reply is contaminated by the other. Run under -race it
// also proves the buffer is not read by a handler goroutine after reuse.
func TestPipelinedFramesDoNotAlias(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "echo"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			time.Sleep(2 * time.Millisecond) // outlive the next ReadMessage
			var in struct {
				V string `json:"v"`
			}
			if err := json.Unmarshal(args, &in); err != nil {
				return ToolError("unmarshal: %v", err), nil
			}
			return ToolText("%s", in.V), nil
		})
	})
	c.handshake()

	const n = 12
	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		payload := strings.Repeat(string(rune('a'+i)), 200+i)
		id := fmt.Sprintf("%d", 1000+i)
		want[idKey(json.RawMessage(id))] = payload
		args, _ := json.Marshal(map[string]string{"v": payload})
		c.send(map[string]any{"jsonrpc": "2.0", "id": 1000 + i, "method": MethodToolsCall,
			"params": map[string]any{"name": "echo", "arguments": json.RawMessage(args)}})
	}

	for remaining := n; remaining > 0; {
		wm := c.readRaw()
		if wm.kind() != kindResponse {
			continue
		}
		expect, tracked := want[idKey(wm.ID)]
		if !tracked {
			continue
		}
		var res CallToolResult
		if err := json.Unmarshal(wm.Result, &res); err != nil {
			t.Fatal(err)
		}
		if len(res.Content) != 1 || res.Content[0].Text != expect {
			t.Fatalf("id %s: got %q, want %q", wm.ID, res.Content[0].Text, expect)
		}
		remaining--
	}
}

func TestHeaderFramingRejectsFlood(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < maxHeaderCount+5; i++ {
		fmt.Fprintf(&sb, "X-Pad-%d: filler\r\n", i)
	}
	f := NewFramer(strings.NewReader(sb.String()), io.Discard, FramerOptions{Mode: FramingHeaders})
	if _, err := f.ReadMessage(); err == nil {
		t.Fatal("unbounded header count accepted")
	}
}

func TestHeaderFramingRejectsEndlessHeaderLine(t *testing.T) {
	line := "X-Pad: " + strings.Repeat("z", maxHeaderLineBytes*2) + "\r\n\r\n"
	f := NewFramer(strings.NewReader(line), io.Discard, FramerOptions{Mode: FramingHeaders})
	if _, err := f.ReadMessage(); err == nil {
		t.Fatal("unbounded header line accepted")
	}
}

// Cached list pages must be dropped when the registry changes, or a client
// that listed once would never see a newly registered tool.
func TestListCacheInvalidatedOnRegistryChange(t *testing.T) {
	srv, c := newTestServer(t, func(s *Server) { echoTool(s, "a") })
	c.handshake()

	var first ListToolsResult
	if err := json.Unmarshal(c.request(MethodToolsList, nil), &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Tools) != 1 {
		t.Fatalf("tools = %+v", first.Tools)
	}

	echoTool(srv, "b")

	var second ListToolsResult
	if err := json.Unmarshal(c.request(MethodToolsList, nil), &second); err != nil {
		t.Fatal(err)
	}
	if len(second.Tools) != 2 {
		t.Fatalf("cache was not invalidated: %+v", second.Tools)
	}

	srv.RemoveTool("a")
	var third ListToolsResult
	if err := json.Unmarshal(c.request(MethodToolsList, nil), &third); err != nil {
		t.Fatal(err)
	}
	if len(third.Tools) != 1 || third.Tools[0].Name != "b" {
		t.Fatalf("removal not reflected: %+v", third.Tools)
	}
}

// Non-canonical offsets bypass the cache; they must still return the right
// window and must not poison the cached canonical pages.
func TestNonCanonicalCursorIsCorrectAndUncached(t *testing.T) {
	srv, c := newTestServer(t, func(s *Server) {
		for _, n := range []string{"a", "b", "c", "d", "e"} {
			echoTool(s, n)
		}
	})
	c.handshake()

	var odd ListToolsResult
	raw := c.request(MethodToolsList, PaginatedParams{Cursor: encodeCursor("tools", 1)})
	if err := json.Unmarshal(raw, &odd); err != nil {
		t.Fatal(err)
	}
	if len(odd.Tools) != 2 || odd.Tools[0].Name != "b" || odd.Tools[1].Name != "c" {
		t.Fatalf("offset 1 window = %+v", odd.Tools)
	}
	if srv.toolCache.pageCount() > 1 {
		t.Fatalf("non-canonical page was cached: %d entries", srv.toolCache.pageCount())
	}

	var first ListToolsResult
	if err := json.Unmarshal(c.request(MethodToolsList, nil), &first); err != nil {
		t.Fatal(err)
	}
	if first.Tools[0].Name != "a" || first.Tools[1].Name != "b" {
		t.Fatalf("canonical page corrupted: %+v", first.Tools)
	}
}

// Integer ids beyond float64's exact range must remain distinct. The float
// normalisation path collapses them, which is why integers take their own
// route.
func TestLargeIntegerIDsStayDistinct(t *testing.T) {
	a := idKey(json.RawMessage("9007199254740993"))
	b := idKey(json.RawMessage("9007199254740992"))
	if a == b {
		t.Fatalf("large ids collided: %s == %s", a, b)
	}
	if got := idKey(json.RawMessage("9007199254740993")); got != a {
		t.Fatalf("idKey is not deterministic: %s vs %s", got, a)
	}
}

// The outbound-call quota bounds how many server-to-client requests a session
// can have outstanding, so a client that accepts requests and never answers
// cannot grow the correlation map without limit.
func TestPendingCallLimit(t *testing.T) {
	blocked := make(chan error, 1)
	second := make(chan error, 1)

	h := startServer(t, Options{MaxPendingCalls: 1}, UnixOptions{}, func(s *Server) {
		_ = s.AddTool(Tool{Name: "hold"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			_, err := sess.ListRoots(ctx) // never answered by the client
			blocked <- err
			return ToolText("done"), nil
		})
		_ = s.AddTool(Tool{Name: "also"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			_, err := sess.ListRoots(ctx)
			second <- err
			return ToolText("done"), nil
		})
	})

	c := h.dial()
	c.send(map[string]any{"jsonrpc": "2.0", "id": 1, "method": MethodInitialize,
		"params": InitializeParams{
			ProtocolVersion: LatestProtocolVersion,
			ClientInfo:      Implementation{Name: "t", Version: "1"},
			Capabilities:    ClientCapabilities{Roots: &RootsCapability{}},
		}})
	c.awaitID(json.RawMessage("1"))
	c.notify(NotifyInitialized, struct{}{})

	// First call occupies the single pending slot. Waiting for the inbound
	// roots/list proves the slot is taken before the second call is issued.
	c.send(map[string]any{"jsonrpc": "2.0", "id": 10, "method": MethodToolsCall,
		"params": CallToolParams{Name: "hold"}})
	for {
		wm := c.readRaw()
		if wm.kind() == kindRequest && wm.Method == MethodRootsList {
			break
		}
	}

	c.send(map[string]any{"jsonrpc": "2.0", "id": 11, "method": MethodToolsCall,
		"params": CallToolParams{Name: "also"}})

	select {
	case err := <-second:
		if err == nil || !strings.Contains(err.Error(), "pending") {
			t.Fatalf("second outbound call error = %v, want a quota refusal", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second call was not refused")
	}
}
