package mcp

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

// testClientServer spins a Server with an echo tool on one end of a net.Pipe and
// returns a Client dialled to the other. Client and server are from the same
// library, so the round-trip proves the Client is wire-compatible with the
// Server (and, by construction, with aegis-mcp, which is an mcp.Server).
func testClientServer(t *testing.T) (*Client, context.CancelFunc) {
	t.Helper()
	srv := New(Options{Name: "test", Version: "0"})
	_ = srv.AddTool(
		Tool{Name: "echo", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, s *Session, args json.RawMessage) (*CallToolResult, error) {
			var a struct {
				Text string `json:"text"`
			}
			_ = json.Unmarshal(args, &a)
			if a.Text == "boom" {
				return ToolError("kaboom"), nil
			}
			return ToolText("echo:%s", a.Text), nil
		})
	c1, c2 := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = srv.ServeConn(ctx, NewFramer(c1, c1, FramerOptions{Mode: FramingLines, Closer: c1}), ConnConfig{Remote: "test"})
	}()
	client := Dial(c2, ClientOptions{Info: Implementation{Name: "c", Version: "0"}})
	return client, cancel
}

func TestClientRoundTrip(t *testing.T) {
	client, cancel := testClientServer(t)
	defer cancel()
	defer client.Close()
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()

	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	tools, err := client.ListTools(ctx)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "echo" {
		t.Fatalf("want [echo], got %+v", tools)
	}
	res, err := client.CallTool(ctx, "echo", map[string]any{"text": "hi"})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if len(res.Content) == 0 || !strings.Contains(res.Content[0].Text, "echo:hi") {
		t.Fatalf("want echo:hi, got %+v", res)
	}
}

func TestClientToolError(t *testing.T) {
	client, cancel := testClientServer(t)
	defer cancel()
	defer client.Close()
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	// A tool that ran and failed is a result with IsError, not a transport error.
	res, err := client.CallTool(ctx, "echo", map[string]any{"text": "boom"})
	if err != nil {
		t.Fatalf("tool-level error must not be an RPC error: %v", err)
	}
	if !res.IsError {
		t.Fatal("want IsError for a failed tool run")
	}
}

func TestClientUnknownTool(t *testing.T) {
	client, cancel := testClientServer(t)
	defer cancel()
	defer client.Close()
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CallTool(ctx, "nope", nil); err == nil {
		t.Fatal("unknown tool should surface as a JSON-RPC error")
	}
}
