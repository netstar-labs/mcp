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

// TestClientElicitationCapabilityMatchesHandler: the client advertises
// elicitation only when it has a handler that can answer one. A server checks
// the advertised capability before eliciting (Session.Elicit), so advertising it
// with a nil OnRequest makes that check pass and then answers the elicitation
// with method-not-found mid-call — the failure lands on the server's user, far
// from the client that mis-advertised. The round-trip asserts what the SERVER
// sees, which is the only thing that governs its behaviour.
func TestClientElicitationCapabilityMatchesHandler(t *testing.T) {
	// dial spins a server whose tool reports the client capabilities it observed.
	dial := func(t *testing.T, opts ClientOptions) (*Client, context.CancelFunc) {
		t.Helper()
		srv := New(Options{Name: "test", Version: "0"})
		_ = srv.AddTool(
			Tool{Name: "caps", InputSchema: json.RawMessage(`{"type":"object"}`)},
			func(ctx context.Context, s *Session, _ json.RawMessage) (*CallToolResult, error) {
				if s.ClientCapabilities().Elicitation != nil {
					return ToolText("elicitation"), nil
				}
				return ToolText("none"), nil
			})
		c1, c2 := net.Pipe()
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_ = srv.ServeConn(ctx, NewFramer(c1, c1, FramerOptions{Mode: FramingLines, Closer: c1}), ConnConfig{Remote: "test"})
		}()
		return Dial(c2, opts), cancel
	}

	observe := func(t *testing.T, opts ClientOptions) string {
		t.Helper()
		client, cancel := dial(t, opts)
		defer cancel()
		defer client.Close()
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if _, err := client.Initialize(ctx); err != nil {
			t.Fatalf("initialize: %v", err)
		}
		res, err := client.CallTool(ctx, "caps", nil)
		if err != nil {
			t.Fatalf("caps: %v", err)
		}
		if len(res.Content) == 0 {
			t.Fatal("caps returned no content")
		}
		return res.Content[0].Text
	}

	info := Implementation{Name: "c", Version: "0"}
	if got := observe(t, ClientOptions{Info: info}); got != "none" {
		t.Errorf("nil OnRequest advertised %q; a client that cannot answer must not advertise elicitation", got)
	}
	handler := func(context.Context, string, json.RawMessage) (any, *RPCError) { return map[string]any{}, nil }
	if got := observe(t, ClientOptions{Info: info, OnRequest: handler}); got != "elicitation" {
		t.Errorf("OnRequest set advertised %q, want elicitation", got)
	}
}
