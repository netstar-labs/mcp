package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// benchServer starts a server on a Unix socket and returns a dial function, so
// each benchmark goroutine can hold its own session.
func benchServer(b *testing.B, configure func(*Server)) func() *testClient {
	b.Helper()
	srv := New(Options{
		Name:     "bench",
		Version:  "1.0.0",
		Logger:   log.New(io.Discard, "", 0),
		PageSize: 100,
	})
	if configure != nil {
		configure(srv)
	}

	sock := filepath.Join(b.TempDir(), "b.sock")
	l, err := ListenUnix(sock, UnixOptions{ReplaceStale: true})
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = srv.Serve(ctx, l, UnixOptions{}) }()

	var conns []net.Conn
	var mu sync.Mutex
	b.Cleanup(func() {
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		cancel()
		_ = srv.Close()
		wg.Wait()
	})

	return func() *testClient {
		conn, err := net.Dial("unix", sock)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		mu.Lock()
		conns = append(conns, conn)
		mu.Unlock()
		c := &testClient{t: b, conn: conn, r: bufio.NewReader(conn)}
		c.handshake()
		return c
	}
}

func benchEchoTool(s *Server) {
	_ = s.AddTool(Tool{
		Name:        "echo",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"v":{"type":"string"}}}`),
	}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
		return &CallToolResult{Content: []Content{{Type: ContentTypeText, Text: string(args)}}}, nil
	})
}

// BenchmarkToolCallRoundTrip measures one full request/response cycle across a
// real socket, which is the number that actually matters.
func BenchmarkToolCallRoundTrip(b *testing.B) {
	dial := benchServer(b, benchEchoTool)
	c := dial()
	params := CallToolParams{Name: "echo", Arguments: json.RawMessage(`{"v":"hello"}`)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.request(MethodToolsCall, params)
	}
}

// BenchmarkToolCallParallel runs independent sessions concurrently.
func BenchmarkToolCallParallel(b *testing.B) {
	dial := benchServer(b, benchEchoTool)
	params := CallToolParams{Name: "echo", Arguments: json.RawMessage(`{"v":"hello"}`)}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := dial()
		for pb.Next() {
			c.request(MethodToolsCall, params)
		}
	})
}

func BenchmarkPingRoundTrip(b *testing.B) {
	dial := benchServer(b, nil)
	c := dial()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.request(MethodPing, nil)
	}
}

// BenchmarkToolsList is the listing path with a realistic registry size.
func BenchmarkToolsList(b *testing.B) {
	dial := benchServer(b, func(s *Server) {
		for i := 0; i < 100; i++ {
			_ = s.AddTool(Tool{
				Name:        fmt.Sprintf("tool_%03d", i),
				Title:       "Tool",
				Description: "A tool that does a thing with some arguments and returns a result.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"number"}},"required":["a"]}`),
			}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
				return ToolText("ok"), nil
			})
		}
	})
	c := dial()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.request(MethodToolsList, nil)
	}
}

func BenchmarkMarshalResult(b *testing.B) {
	res := &CallToolResult{
		Content: []Content{
			{Type: ContentTypeText, Text: "a moderately sized tool result payload with some text in it"},
		},
		StructuredContent: json.RawMessage(`{"ok":true,"count":42}`),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshal(res); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIDKey(b *testing.B) {
	ids := []json.RawMessage{
		json.RawMessage(`1`),
		json.RawMessage(`4096`),
		json.RawMessage(`"req-0001"`),
		json.RawMessage(`9007199254740991`),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idKey(ids[i%len(ids)])
	}
}

func BenchmarkFramerWrite(b *testing.B) {
	payload := []byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"hello world"}]}}`)
	f := NewFramer(strings.NewReader(""), io.Discard, FramerOptions{Mode: FramingLines})
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.WriteMessage(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFramerRead(b *testing.B) {
	line := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"v":"hello"}}}` + "\n"
	stream := strings.Repeat(line, 512)
	b.ReportAllocs()
	b.SetBytes(int64(len(line)))
	b.ResetTimer()
	for i := 0; i < b.N; i += 512 {
		f := NewFramer(strings.NewReader(stream), io.Discard, FramerOptions{Mode: FramingLines})
		for j := 0; j < 512 && i+j < b.N; j++ {
			if _, err := f.ReadMessage(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkProgressTokenOf(b *testing.B) {
	withToken := json.RawMessage(`{"name":"echo","arguments":{"v":"x"},"_meta":{"progressToken":"tok-1"}}`)
	without := json.RawMessage(`{"name":"echo","arguments":{"v":"x"}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			_ = progressTokenOf(withToken)
		} else {
			_ = progressTokenOf(without)
		}
	}
}

// BenchmarkURITemplateMatches is the dispatcher path: does this URI belong to
// this template at all.
func BenchmarkURITemplateMatches(b *testing.B) {
	tmpl, err := CompileURITemplate("test://item/{id}/log")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !tmpl.Matches("test://item/12345/log") {
			b.Fatal("no match")
		}
	}
}

// BenchmarkURITemplateMatch is the handler path, which needs the bindings.
func BenchmarkURITemplateMatch(b *testing.B) {
	tmpl, err := CompileURITemplate("test://item/{id}/log")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := tmpl.Match("test://item/12345/log"); !ok {
			b.Fatal("no match")
		}
	}
}

// BenchmarkURITemplateCompileAndMatch is the convenience wrapper, for
// comparison: this is what registering the template once avoids.
func BenchmarkURITemplateCompileAndMatch(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := URITemplateVars("test://item/{id}/log", "test://item/12345/log"); !ok {
			b.Fatal("no match")
		}
	}
}

func BenchmarkSessionSetup(b *testing.B) {
	dial := benchServer(b, benchEchoTool)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := dial()
		_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
		_ = c.conn.Close()
	}
}

// newBareSession builds a Session with no live transport, for benchmarking
// handlers without socket and client-side costs in the measurement.
func newBareSession(b *testing.B, srv *Server) *Session {
	b.Helper()
	f := NewFramer(strings.NewReader(""), io.Discard, FramerOptions{})
	s := newSession(srv, f, "bench", nil)
	s.state.Store(StateReady)
	return s
}

func benchRegistry(n int) func(*Server) {
	return func(s *Server) {
		for i := 0; i < n; i++ {
			_ = s.AddTool(Tool{
				Name:        fmt.Sprintf("tool_%03d", i),
				Title:       "Tool",
				Description: "A tool that does a thing with some arguments and returns a result.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"number"}},"required":["a"]}`),
			}, func(ctx context.Context, sess *Session, args json.RawMessage) (*CallToolResult, error) {
				return ToolText("ok"), nil
			})
		}
	}
}

// BenchmarkToolsListServerSide isolates what the server does for one
// tools/list over a 100-tool registry, with the page cache warm.
func BenchmarkToolsListServerSide(b *testing.B) {
	srv := New(Options{Name: "bench", Logger: log.New(io.Discard, "", 0), PageSize: 100})
	benchRegistry(100)(srv)
	sess := newBareSession(b, srv)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, rerr := sess.handleToolsList(nil)
		if rerr != nil {
			b.Fatal(rerr)
		}
		if _, err := marshal(res); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkToolsListServerSideCold is the same call with the cache invalidated
// every iteration: the cost the cache is avoiding.
func BenchmarkToolsListServerSideCold(b *testing.B) {
	srv := New(Options{Name: "bench", Logger: log.New(io.Discard, "", 0), PageSize: 100})
	benchRegistry(100)(srv)
	sess := newBareSession(b, srv)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.gen.Add(1) // simulate a registry mutation
		res, rerr := sess.handleToolsList(nil)
		if rerr != nil {
			b.Fatal(rerr)
		}
		if _, err := marshal(res); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDispatchToolCall isolates server-side dispatch of one tools/call.
func BenchmarkDispatchToolCall(b *testing.B) {
	srv := New(Options{Name: "bench", Logger: log.New(io.Discard, "", 0)})
	benchEchoTool(srv)
	sess := newBareSession(b, srv)

	frame := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"v":"hello"}}}`)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wm wireMessage
		if err := json.Unmarshal(frame, &wm); err != nil {
			b.Fatal(err)
		}
		res, rerr := sess.execRequest(ctx, &wm)
		if rerr != nil {
			b.Fatal(rerr)
		}
		if _, err := marshal(res); err != nil {
			b.Fatal(err)
		}
	}
}
