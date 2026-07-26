package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
)

// testClient is a minimal line-framed JSON-RPC client, deliberately hand-rolled
// so the tests exercise the wire format rather than the server's own encoder.
type testClient struct {
	t    testing.TB
	conn net.Conn
	r    *bufio.Reader
	id   int
}

func (c *testClient) send(v any) {
	c.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		c.t.Fatalf("marshal: %v", err)
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.conn.Write(append(b, '\n')); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

func (c *testClient) request(method string, params any) json.RawMessage {
	c.t.Helper()
	c.id++
	id := c.id
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	return c.awaitID(json.RawMessage(fmt.Sprintf("%d", id)))
}

func (c *testClient) notify(method string, params any) {
	c.t.Helper()
	c.send(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (c *testClient) readRaw() wireMessage {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	var wm wireMessage
	if err := json.Unmarshal(line, &wm); err != nil {
		c.t.Fatalf("decode %q: %v", line, err)
	}
	return wm
}

// awaitID skips notifications and unrelated replies until the matching
// response arrives, then fails the test if it carried an error.
func (c *testClient) awaitID(id json.RawMessage) json.RawMessage {
	c.t.Helper()
	want := idKey(id)
	for {
		wm := c.readRaw()
		if wm.kind() != kindResponse || idKey(wm.ID) != want {
			continue
		}
		if wm.Error != nil {
			c.t.Fatalf("unexpected error reply: %v", wm.Error)
		}
		return wm.Result
	}
}

func (c *testClient) requestErr(method string, params any) *RPCError {
	c.t.Helper()
	c.id++
	id := c.id
	want := idKey(json.RawMessage(fmt.Sprintf("%d", id)))
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	for {
		wm := c.readRaw()
		if wm.kind() != kindResponse || idKey(wm.ID) != want {
			continue
		}
		if wm.Error == nil {
			c.t.Fatalf("expected an error reply for %s, got result %s", method, wm.Result)
		}
		return wm.Error
	}
}

func (c *testClient) handshake() InitializeResult {
	c.t.Helper()
	raw := c.request(MethodInitialize, InitializeParams{
		ProtocolVersion: LatestProtocolVersion,
		ClientInfo:      Implementation{Name: "test-client", Version: "0.1"},
		Capabilities: ClientCapabilities{
			Roots:    &RootsCapability{ListChanged: true},
			Sampling: &emptyCap{},
		},
	})
	var res InitializeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		c.t.Fatalf("initialize result: %v", err)
	}
	c.notify(NotifyInitialized, struct{}{})
	return res
}

// harness is one running server on a Unix socket, torn down by t.Cleanup.
type harness struct {
	t    testing.TB
	srv  *Server
	path string
}

// startServer is the single place tests and benchmarks build a live server.
// Zero-valued fields in o are filled with test-friendly defaults.
func startServer(t testing.TB, o Options, u UnixOptions, configure func(*Server)) *harness {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets not available")
	}
	if o.Name == "" {
		o.Name = "test"
	}
	if o.Version == "" {
		o.Version = "1.0.0"
	}
	if o.Logger == nil {
		o.Logger = log.New(io.Discard, "", 0)
	}

	srv := New(o)
	if configure != nil {
		configure(srv)
	}

	sock := filepath.Join(shortTempDir(t), "s.sock")
	if u.Mode == 0 {
		u.Mode = 0o600
	}
	u.ReplaceStale = true
	l, err := ListenUnix(sock, u)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = srv.Serve(ctx, l, u) }()

	h := &harness{t: t, srv: srv, path: sock}
	t.Cleanup(func() {
		cancel()
		_ = srv.Close()
		wg.Wait()
	})
	return h
}

// dial opens a connection without performing the handshake.
func (h *harness) dial() *testClient {
	h.t.Helper()
	conn, err := net.Dial("unix", h.path)
	if err != nil {
		h.t.Fatalf("dial: %v", err)
	}
	h.t.Cleanup(func() { _ = conn.Close() })
	return &testClient{t: h.t, conn: conn, r: bufio.NewReader(conn)}
}

// client dials and completes the handshake.
func (h *harness) client() *testClient {
	h.t.Helper()
	c := h.dial()
	c.handshake()
	return c
}

// newTestServer is the common case: default options, page size 2 so the
// pagination tests do not need a hundred entries, batching enabled.
func newTestServer(t testing.TB, configure func(*Server)) (*Server, *testClient) {
	t.Helper()
	h := startServer(t, Options{PageSize: 2, AcceptBatch: true}, UnixOptions{}, configure)
	return h.srv, h.dial()
}

func echoTool(srv *Server, name string) {
	_ = srv.AddTool(Tool{Name: name}, func(ctx context.Context, s *Session, args json.RawMessage) (*CallToolResult, error) {
		return ToolText("%s", string(args)), nil
	})
}

func TestSocketPermissions(t *testing.T) {
	dir := shortTempDir(t)
	sock := filepath.Join(dir, "perm.sock")
	l, err := ListenUnix(sock, UnixOptions{Mode: 0o600, ReplaceStale: true})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	info, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("socket mode = %o, want 600", got)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatal("created file is not a socket")
	}
}

func TestLiveSocketIsNotReclaimed(t *testing.T) {
	dir := shortTempDir(t)
	sock := filepath.Join(dir, "live.sock")
	l, err := ListenUnix(sock, UnixOptions{ReplaceStale: true})
	if err != nil {
		t.Fatalf("first listen: %v", err)
	}
	defer l.Close()

	if _, err := ListenUnix(sock, UnixOptions{ReplaceStale: true}); err == nil {
		t.Fatal("second listen succeeded; a live socket must never be hijacked")
	}
}

func TestHandshakeAndCapabilities(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) { echoTool(s, "echo") })
	res := c.handshake()

	if res.ProtocolVersion != LatestProtocolVersion {
		t.Fatalf("protocol = %q, want %q", res.ProtocolVersion, LatestProtocolVersion)
	}
	if res.ServerInfo.Name != "test" {
		t.Fatalf("serverInfo.name = %q", res.ServerInfo.Name)
	}
	if res.Capabilities.Tools == nil || !res.Capabilities.Tools.ListChanged {
		t.Fatal("tools capability not advertised")
	}
	if res.Capabilities.Resources != nil {
		t.Fatal("resources capability advertised with no resources registered")
	}
}

// A client that pipelines initialize and notifications/initialized without
// waiting must still end up in the ready state.
func TestPipelinedHandshake(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) { echoTool(s, "echo") })

	c.send(map[string]any{"jsonrpc": "2.0", "id": 1, "method": MethodInitialize,
		"params": InitializeParams{ProtocolVersion: LatestProtocolVersion,
			ClientInfo: Implementation{Name: "pipelined", Version: "1"}}})
	c.send(map[string]any{"jsonrpc": "2.0", "method": NotifyInitialized})
	c.send(map[string]any{"jsonrpc": "2.0", "id": 2, "method": MethodToolsList})
	c.id = 2

	c.awaitID(json.RawMessage("1"))
	raw := c.awaitID(json.RawMessage("2"))
	var res ListToolsResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Tools) != 1 {
		t.Fatalf("tools = %+v", res.Tools)
	}
}

func TestProtocolVersionFallback(t *testing.T) {
	_, c := newTestServer(t, nil)
	raw := c.request(MethodInitialize, InitializeParams{
		ProtocolVersion: "1999-01-01",
		ClientInfo:      Implementation{Name: "old", Version: "0"},
	})
	var res InitializeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.ProtocolVersion != LatestProtocolVersion {
		t.Fatalf("fallback = %q, want %q", res.ProtocolVersion, LatestProtocolVersion)
	}
}

func TestRequestsBeforeInitialize(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) { echoTool(s, "echo") })

	if err := c.requestErr(MethodToolsList, nil); err.Code != CodeNotInitialized {
		t.Fatalf("code = %d, want %d", err.Code, CodeNotInitialized)
	}
	// Ping must work in any state.
	c.request(MethodPing, nil)
}

func TestDoubleInitializeRejected(t *testing.T) {
	_, c := newTestServer(t, nil)
	c.handshake()
	if err := c.requestErr(MethodInitialize, InitializeParams{ProtocolVersion: LatestProtocolVersion}); err.Code != CodeInvalidRequest {
		t.Fatalf("code = %d, want %d", err.Code, CodeInvalidRequest)
	}
}

func TestToolCallAndStructuredOutput(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{
			Name:        "add",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}}}`),
		}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			var in struct{ A, B float64 }
			if err := json.Unmarshal(args, &in); err != nil {
				return ToolError("bad args"), nil
			}
			structured, _ := json.Marshal(map[string]float64{"sum": in.A + in.B})
			return &CallToolResult{
				Content:           []Content{TextContent(fmt.Sprintf("%g", in.A+in.B))},
				StructuredContent: structured,
			}, nil
		})
	})
	c.handshake()

	raw := c.request(MethodToolsCall, CallToolParams{
		Name:      "add",
		Arguments: json.RawMessage(`{"a":2,"b":40}`),
	})
	var res CallToolResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("unexpected tool error: %+v", res.Content)
	}
	if len(res.Content) != 1 || res.Content[0].Text != "42" {
		t.Fatalf("content = %+v", res.Content)
	}
	if string(res.StructuredContent) != `{"sum":42}` {
		t.Fatalf("structuredContent = %s", res.StructuredContent)
	}
}

// A handler returning a plain error must surface as an execution failure the
// model can read, not as a JSON-RPC protocol error.
func TestToolErrorIsNotProtocolError(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "boom"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			return nil, errors.New("upstream unavailable")
		})
	})
	c.handshake()

	raw := c.request(MethodToolsCall, CallToolParams{Name: "boom"})
	var res CallToolResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("isError not set")
	}
	if len(res.Content) == 0 || !strings.Contains(res.Content[0].Text, "upstream unavailable") {
		t.Fatalf("content = %+v", res.Content)
	}
}

// An *RPCError returned by a handler is a protocol fault and must not be
// laundered into a tool result.
func TestHandlerRPCErrorPropagates(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "denied"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			return nil, Errorf(CodeInvalidParams, "argument out of range")
		})
	})
	c.handshake()

	if err := c.requestErr(MethodToolsCall, CallToolParams{Name: "denied"}); err.Code != CodeInvalidParams {
		t.Fatalf("code = %d, want %d", err.Code, CodeInvalidParams)
	}
}

func TestPanicInHandlerIsContained(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "panic"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			panic("deliberate")
		})
	})
	c.handshake()

	if err := c.requestErr(MethodToolsCall, CallToolParams{Name: "panic"}); err.Code != CodeInternalError {
		t.Fatalf("code = %d, want %d", err.Code, CodeInternalError)
	}
	// The session must still be usable.
	c.request(MethodPing, nil)
}

func TestUnknownMethodAndTool(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) { echoTool(s, "echo") })
	c.handshake()

	if err := c.requestErr("does/not/exist", nil); err.Code != CodeMethodNotFound {
		t.Fatalf("method code = %d", err.Code)
	}
	if err := c.requestErr(MethodToolsCall, CallToolParams{Name: "nope"}); err.Code != CodeMethodNotFound {
		t.Fatalf("tool code = %d", err.Code)
	}
}

func TestPagination(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		for _, n := range []string{"a", "b", "c", "d", "e"} {
			echoTool(s, n)
		}
	})
	c.handshake()

	var seen []string
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("pagination did not terminate")
		}
		raw := c.request(MethodToolsList, PaginatedParams{Cursor: cursor})
		var res ListToolsResult
		if err := json.Unmarshal(raw, &res); err != nil {
			t.Fatal(err)
		}
		if len(res.Tools) > 2 {
			t.Fatalf("page size = %d, want <= 2", len(res.Tools))
		}
		for _, tool := range res.Tools {
			seen = append(seen, tool.Name)
		}
		if res.NextCursor == "" {
			break
		}
		cursor = res.NextCursor
	}
	if got := strings.Join(seen, ","); got != "a,b,c,d,e" {
		t.Fatalf("tools = %s", got)
	}

	// A cursor minted for one collection must not be honoured by another.
	if err := c.requestErr(MethodPromptsList, PaginatedParams{Cursor: encodeCursor("tools", 0)}); err.Code != CodeInvalidParams {
		t.Fatalf("cross-collection cursor accepted (code %d)", err.Code)
	}
}

func TestCancellationSuppressesReply(t *testing.T) {
	started := make(chan struct{})
	observed := make(chan error, 1)

	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "slow"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			close(started)
			<-ctx.Done()
			observed <- ctx.Err()
			return ToolText("finished anyway"), nil
		})
	})
	c.handshake()

	c.id++
	slowID := c.id
	c.send(map[string]any{"jsonrpc": "2.0", "id": slowID, "method": MethodToolsCall,
		"params": CallToolParams{Name: "slow"}})

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("tool never started")
	}

	c.notify(NotifyCancelled, CancelledParams{
		RequestID: json.RawMessage(fmt.Sprintf("%d", slowID)),
		Reason:    "user aborted",
	})

	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler context was never cancelled")
	}

	// Issue a ping; the cancelled request must never produce a reply.
	c.id++
	pingID := c.id
	c.send(map[string]any{"jsonrpc": "2.0", "id": pingID, "method": MethodPing})

	wantSlow := idKey(json.RawMessage(fmt.Sprintf("%d", slowID)))
	wantPing := idKey(json.RawMessage(fmt.Sprintf("%d", pingID)))
	for {
		wm := c.readRaw()
		if wm.kind() != kindResponse {
			continue
		}
		if idKey(wm.ID) == wantSlow {
			t.Fatal("server replied to a cancelled request")
		}
		if idKey(wm.ID) == wantPing {
			return
		}
	}
}

func TestResourcesAndTemplates(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddResource(Resource{URI: "test://fixed", Name: "fixed"},
			func(ctx context.Context, sess *Session, uri string) ([]ResourceContents, error) {
				return []ResourceContents{TextResource(uri, "text/plain", "fixed body")}, nil
			})
		_ = s.AddResourceTemplate(ResourceTemplate{URITemplate: "test://item/{id}", Name: "item"},
			func(ctx context.Context, sess *Session, uri string) ([]ResourceContents, error) {
				vars, ok := URITemplateVars("test://item/{id}", uri)
				if !ok {
					return nil, errors.New("no match")
				}
				return []ResourceContents{TextResource(uri, "text/plain", "item "+vars["id"])}, nil
			})
	})
	res := c.handshake()
	if res.Capabilities.Resources == nil || !res.Capabilities.Resources.Subscribe {
		t.Fatal("resources capability missing subscribe")
	}

	var read ReadResourceResult
	if err := json.Unmarshal(c.request(MethodResourcesRead, ReadResourceParams{URI: "test://fixed"}), &read); err != nil {
		t.Fatal(err)
	}
	if read.Contents[0].Text != "fixed body" {
		t.Fatalf("fixed read = %+v", read.Contents)
	}

	if err := json.Unmarshal(c.request(MethodResourcesRead, ReadResourceParams{URI: "test://item/42"}), &read); err != nil {
		t.Fatal(err)
	}
	if read.Contents[0].Text != "item 42" {
		t.Fatalf("template read = %+v", read.Contents)
	}

	if err := c.requestErr(MethodResourcesRead, ReadResourceParams{URI: "test://missing"}); err.Code != CodeResourceNotFound {
		t.Fatalf("code = %d, want %d", err.Code, CodeResourceNotFound)
	}
}

func TestSubscriptionRoutesUpdates(t *testing.T) {
	srv, c := newTestServer(t, func(s *Server) {
		_ = s.AddResource(Resource{URI: "test://watched", Name: "watched"},
			func(ctx context.Context, sess *Session, uri string) ([]ResourceContents, error) {
				return []ResourceContents{TextResource(uri, "text/plain", "v1")}, nil
			})
	})
	c.handshake()
	c.request(MethodResourcesSubscribe, SubscribeParams{URI: "test://watched"})

	// The subscription is recorded on the session; give the handler a moment
	// to complete before broadcasting.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(srv.Sessions()) == 1 && srv.Sessions()[0].isSubscribed("test://watched") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscription was never recorded")
		}
		time.Sleep(5 * time.Millisecond)
	}

	srv.NotifyResourceUpdated("test://watched")
	srv.NotifyResourceUpdated("test://ignored")

	for {
		wm := c.readRaw()
		if wm.kind() != kindNotification {
			continue
		}
		if wm.Method != NotifyResourceUpdated {
			t.Fatalf("unexpected notification %s", wm.Method)
		}
		var p ResourceUpdatedParams
		if err := json.Unmarshal(wm.Params, &p); err != nil {
			t.Fatal(err)
		}
		if p.URI != "test://watched" {
			t.Fatalf("update for unsubscribed uri %s", p.URI)
		}
		return
	}
}

func TestPromptRequiredArguments(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddPrompt(Prompt{
			Name:      "greet",
			Arguments: []PromptArgument{{Name: "who", Required: true}},
		}, func(ctx context.Context, sess *Session, args map[string]string) (*GetPromptResult, error) {
			return &GetPromptResult{Messages: []PromptMessage{
				{Role: "user", Content: TextContent("hello " + args["who"])},
			}}, nil
		})
	})
	c.handshake()

	if err := c.requestErr(MethodPromptsGet, GetPromptParams{Name: "greet"}); err.Code != CodeInvalidParams {
		t.Fatalf("code = %d", err.Code)
	}

	var res GetPromptResult
	raw := c.request(MethodPromptsGet, GetPromptParams{Name: "greet", Arguments: map[string]string{"who": "world"}})
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.Messages[0].Content.Text != "hello world" {
		t.Fatalf("messages = %+v", res.Messages)
	}
}

// The server initiates a request and the client answers it; this is the path
// used by sampling, roots and elicitation.
func TestServerInitiatedRequest(t *testing.T) {
	roots := make(chan []Root, 1)
	fail := make(chan error, 1)

	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "ask_roots"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			r, err := sess.ListRoots(ctx)
			if err != nil {
				fail <- err
				return nil, err
			}
			roots <- r
			return ToolText("ok"), nil
		})
	})
	c.handshake()

	c.id++
	callID := c.id
	c.send(map[string]any{"jsonrpc": "2.0", "id": callID, "method": MethodToolsCall,
		"params": CallToolParams{Name: "ask_roots"}})

	// Answer the inbound roots/list request.
	for {
		wm := c.readRaw()
		if wm.kind() == kindRequest && wm.Method == MethodRootsList {
			c.send(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(wm.ID),
				"result": ListRootsResult{Roots: []Root{{URI: "file:///work", Name: "work"}}}})
			break
		}
	}

	select {
	case got := <-roots:
		if len(got) != 1 || got[0].Name != "work" {
			t.Fatalf("roots = %+v", got)
		}
	case err := <-fail:
		t.Fatalf("ListRoots: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for roots")
	}

	c.awaitID(json.RawMessage(fmt.Sprintf("%d", callID)))
}

func TestProgressNotification(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "work"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			total := 2.0
			sess.ReportProgress(ctx, 1, &total, "halfway")
			return ToolText("done"), nil
		})
	})
	c.handshake()

	c.id++
	id := c.id
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": MethodToolsCall,
		"params": map[string]any{
			"name":  "work",
			"_meta": map[string]any{"progressToken": "tok-1"},
		}})

	var sawProgress bool
	want := idKey(json.RawMessage(fmt.Sprintf("%d", id)))
	for {
		wm := c.readRaw()
		if wm.kind() == kindNotification && wm.Method == NotifyProgress {
			var p ProgressParams
			if err := json.Unmarshal(wm.Params, &p); err != nil {
				t.Fatal(err)
			}
			if string(p.ProgressToken) != `"tok-1"` {
				t.Fatalf("progressToken = %s", p.ProgressToken)
			}
			sawProgress = true
		}
		if wm.kind() == kindResponse && idKey(wm.ID) == want {
			break
		}
	}
	if !sawProgress {
		t.Fatal("no progress notification received")
	}
}

func TestLoggingLevelGate(t *testing.T) {
	emit := make(chan struct{})
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "log"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			sess.Log(LogDebug, "test", "should be suppressed")
			sess.Log(LogError, "test", "should be delivered")
			close(emit)
			return ToolText("ok"), nil
		})
	})
	c.handshake()
	c.request(MethodLoggingSetLevel, SetLevelParams{Level: LogWarning})

	c.id++
	id := c.id
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": MethodToolsCall, "params": CallToolParams{Name: "log"}})

	want := idKey(json.RawMessage(fmt.Sprintf("%d", id)))
	var messages []string
	for {
		wm := c.readRaw()
		if wm.kind() == kindNotification && wm.Method == NotifyMessage {
			var p LoggingMessageParams
			if err := json.Unmarshal(wm.Params, &p); err != nil {
				t.Fatal(err)
			}
			messages = append(messages, p.Level+":"+string(p.Data))
		}
		if wm.kind() == kindResponse && idKey(wm.ID) == want {
			break
		}
	}
	if len(messages) != 1 || !strings.HasPrefix(messages[0], "error:") {
		t.Fatalf("messages = %v; debug should have been filtered", messages)
	}
}

func TestBatchRequest(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) { echoTool(s, "echo") })
	c.handshake()

	c.send([]any{
		map[string]any{"jsonrpc": "2.0", "id": 100, "method": MethodPing},
		map[string]any{"jsonrpc": "2.0", "method": NotifyProgress, "params": map[string]any{}},
		map[string]any{"jsonrpc": "2.0", "id": 101, "method": MethodToolsList},
	})

	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var batch []wireMessage
	if err := json.Unmarshal(line, &batch); err != nil {
		t.Fatalf("batch reply is not an array: %s", line)
	}
	if len(batch) != 2 {
		t.Fatalf("batch reply length = %d, want 2 (the notification gets no reply)", len(batch))
	}
}

func TestParseErrorAndInvalidRequest(t *testing.T) {
	_, c := newTestServer(t, nil)

	_, _ = c.conn.Write([]byte("{not json}\n"))
	wm := c.readRaw()
	if wm.Error == nil || wm.Error.Code != CodeParseError {
		t.Fatalf("expected parse error, got %+v", wm)
	}

	c.send(map[string]any{"jsonrpc": "2.0", "id": 7})
	wm = c.readRaw()
	if wm.Error == nil || wm.Error.Code != CodeInvalidRequest {
		t.Fatalf("expected invalid request, got %+v", wm)
	}
}

func TestMessageSizeLimit(t *testing.T) {
	f := NewFramer(strings.NewReader(strings.Repeat("x", 1024)+"\n"), io.Discard,
		FramerOptions{Mode: FramingLines, MaxMessageBytes: 64})
	if _, err := f.ReadMessage(); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("err = %v, want ErrMessageTooLarge", err)
	}
}

func TestHeaderFramingRoundTrip(t *testing.T) {
	var buf strings.Builder
	w := NewFramer(strings.NewReader(""), &buf, FramerOptions{Mode: FramingHeaders})
	if err := w.WriteMessage([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`)); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(buf.String(), "Content-Length: 36\r\n\r\n") {
		t.Fatalf("framing = %q", buf.String())
	}

	r := NewFramer(strings.NewReader(buf.String()), io.Discard, FramerOptions{Mode: FramingHeaders})
	msg, err := r.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != `{"jsonrpc":"2.0","id":1,"result":{}}` {
		t.Fatalf("round trip = %s", msg)
	}
}

func TestPeerCredentials(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SO_PEERCRED is Linux-only in this build")
	}
	ready := make(chan *PeerCred, 1)
	_, c := newTestServer(t, func(s *Server) {
		s.opts.OnSessionStart = func(sess *Session) { ready <- sess.Peer() }
	})
	c.handshake()

	select {
	case peer := <-ready:
		if peer == nil {
			t.Fatal("no peer credentials")
		}
		if peer.UID != uint32(os.Getuid()) {
			t.Fatalf("uid = %d, want %d", peer.UID, os.Getuid())
		}
		if peer.PID != int32(os.Getpid()) {
			t.Fatalf("pid = %d, want %d", peer.PID, os.Getpid())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSessionStart never fired")
	}
}

func TestPeerAllowList(t *testing.T) {
	opts := UnixOptions{AllowUID: []uint32{1000}}
	if peerAllowed(nil, opts) {
		t.Fatal("nil credentials must be denied when an allow-list is set")
	}
	if peerAllowed(&PeerCred{UID: 1001, GID: 1001}, opts) {
		t.Fatal("uid 1001 must be denied")
	}
	if !peerAllowed(&PeerCred{UID: 1000}, opts) {
		t.Fatal("uid 1000 must be allowed")
	}
	if !peerAllowed(&PeerCred{UID: 9}, UnixOptions{}) {
		t.Fatal("empty allow-list must permit everyone")
	}
}

func TestURITemplateMatching(t *testing.T) {
	cases := []struct {
		tmpl, uri string
		want      map[string]string
		ok        bool
	}{
		{"test://item/{id}", "test://item/42", map[string]string{"id": "42"}, true},
		{"test://item/{id}", "test://item/a/b", nil, false},
		{"file:///{+path}", "file:///a/b/c.txt", map[string]string{"path": "a/b/c.txt"}, true},
		{"test://{a}/x/{b}", "test://1/x/2", map[string]string{"a": "1", "b": "2"}, true},
		{"test://item/{id}", "test://item/", nil, false},
		{"test://item/{id}", "test://other/42", nil, false},
		{"test://item/{id}/log", "test://item/7/log", map[string]string{"id": "7"}, true},
		{"test://item/{id}", "test://item/a%2Fb", map[string]string{"id": "a/b"}, true},
		{"static://thing", "static://thing", map[string]string{}, true},
	}
	for _, tc := range cases {
		got, ok := URITemplateVars(tc.tmpl, tc.uri)
		if ok != tc.ok {
			t.Fatalf("%s vs %s: ok = %v, want %v", tc.tmpl, tc.uri, ok, tc.ok)
		}
		if !ok {
			continue
		}
		for k, v := range tc.want {
			if got[k] != v {
				t.Fatalf("%s vs %s: %s = %q, want %q", tc.tmpl, tc.uri, k, got[k], v)
			}
		}
	}
}

func TestIDKeyDiscriminatesTypes(t *testing.T) {
	if idKey(json.RawMessage(`7`)) == idKey(json.RawMessage(`"7"`)) {
		t.Fatal("numeric and string ids must not collide")
	}
	if idKey(json.RawMessage(`7`)) != idKey(json.RawMessage(`7.0`)) {
		t.Fatal("7 and 7.0 are the same JSON number")
	}
}

func TestConcurrentToolCalls(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "sleep"}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
			time.Sleep(50 * time.Millisecond)
			return ToolText("ok"), nil
		})
	})
	c.handshake()

	const n = 16
	ids := make(map[string]bool, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		c.id++
		ids[idKey(json.RawMessage(fmt.Sprintf("%d", c.id)))] = false
		c.send(map[string]any{"jsonrpc": "2.0", "id": c.id, "method": MethodToolsCall,
			"params": CallToolParams{Name: "sleep"}})
	}
	for remaining := n; remaining > 0; {
		wm := c.readRaw()
		if wm.kind() != kindResponse {
			continue
		}
		key := idKey(wm.ID)
		if done, tracked := ids[key]; tracked && !done {
			ids[key] = true
			remaining--
		}
	}
	// Serial execution would take at least 800ms.
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Fatalf("16 calls took %v; requests are not being handled concurrently", elapsed)
	}
}
