package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// handshakeWith is handshake with chosen client capabilities.
func (c *testClient) handshakeWith(caps ClientCapabilities) {
	c.t.Helper()
	c.request(MethodInitialize, InitializeParams{
		ProtocolVersion: LatestProtocolVersion,
		ClientInfo:      Implementation{Name: "test-client", Version: "0.1"},
		Capabilities:    caps,
	})
	c.notify(NotifyInitialized, struct{}{})
}

// callTool sends tools/call without waiting, returning the id to await.
func (c *testClient) callTool(name string) json.RawMessage {
	c.t.Helper()
	c.id++
	id := c.id
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": MethodToolsCall, "params": CallToolParams{Name: name}})
	return json.RawMessage(fmt.Sprintf("%d", id))
}

// serverRequest reads until the server sends a request with the given method.
func (c *testClient) serverRequest(method string) wireMessage {
	c.t.Helper()
	for {
		wm := c.readRaw()
		if wm.kind() == kindRequest && wm.Method == method {
			return wm
		}
	}
}

func toolResultText(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var res CallToolResult
	if err := json.Unmarshal(raw, &res); err != nil || len(res.Content) == 0 {
		t.Fatalf("tool result %s: %v", raw, err)
	}
	return res.Content[0].Text
}

func TestElicitRoundTrip(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "ask"}, func(ctx context.Context, sess *Session, _ json.RawMessage) (*CallToolResult, error) {
			r, err := sess.Elicit(ctx, "who?", json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}}}`))
			if err != nil {
				return ToolError("%v", err), nil
			}
			return ToolText("%s/%s", r.Action, r.Content), nil
		})
	})
	c.handshakeWith(ClientCapabilities{Elicitation: &emptyCap{}})
	id := c.callTool("ask")
	req := c.serverRequest(MethodElicitationCreate)
	var p ElicitParams
	if err := json.Unmarshal(req.Params, &p); err != nil || p.Message != "who?" {
		t.Fatalf("elicitation params %s: %v", req.Params, err)
	}
	c.send(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(req.ID),
		"result": ElicitResult{Action: "accept", Content: json.RawMessage(`{"name":"ada"}`)}})
	if got := toolResultText(t, c.awaitID(id)); got != `accept/{"name":"ada"}` {
		t.Fatalf("tool saw %q", got)
	}
}

// The server must not ask a client that did not say it could answer.
func TestSamplingAndElicitationRefusedWithoutCapability(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "ask"}, func(ctx context.Context, sess *Session, _ json.RawMessage) (*CallToolResult, error) {
			_, e1 := sess.Elicit(ctx, "x", json.RawMessage(`{}`))
			_, e2 := sess.CreateMessage(ctx, &CreateMessageParams{})
			_, e3 := sess.ListRoots(ctx)
			return ToolText("%v|%v|%v", e1, e2, e3), nil
		})
	})
	c.handshakeWith(ClientCapabilities{})
	got := toolResultText(t, c.awaitID(c.callTool("ask")))
	for _, want := range []string{"elicitation", "sampling", "roots"} {
		if !strings.Contains(got, "does not support "+want) {
			t.Errorf("%q does not refuse %s", got, want)
		}
	}
}

func TestCreateMessageRoundTrip(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "think"}, func(ctx context.Context, sess *Session, _ json.RawMessage) (*CallToolResult, error) {
			r, err := sess.CreateMessage(ctx, &CreateMessageParams{MaxTokens: 5})
			if err != nil {
				return ToolError("%v", err), nil
			}
			return ToolText("%s:%s", r.Model, r.Role), nil
		})
	})
	c.handshakeWith(ClientCapabilities{Sampling: &emptyCap{}})
	id := c.callTool("think")
	req := c.serverRequest(MethodSamplingCreateMessage)
	c.send(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(req.ID),
		"result": map[string]any{"role": "assistant", "model": "m1", "content": map[string]any{"type": "text", "text": "hi"}}})
	if got := toolResultText(t, c.awaitID(id)); got != "m1:assistant" {
		t.Fatalf("tool saw %q", got)
	}
}

// A handler that stops waiting tells the client to stop too.
func TestCallCancelsOutboundRequestWhenContextEnds(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		_ = s.AddTool(Tool{Name: "impatient"}, func(ctx context.Context, sess *Session, _ json.RawMessage) (*CallToolResult, error) {
			ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			_, err := sess.Elicit(ctx, "x", json.RawMessage(`{}`))
			return ToolText("%v", err), nil
		})
	})
	c.handshakeWith(ClientCapabilities{Elicitation: &emptyCap{}})
	id := c.callTool("impatient")
	req := c.serverRequest(MethodElicitationCreate) // never answered
	var cancelled wireMessage
	for {
		cancelled = c.readRaw()
		if cancelled.Method == NotifyCancelled {
			break
		}
	}
	var p CancelledParams
	_ = json.Unmarshal(cancelled.Params, &p)
	if string(p.RequestID) != string(req.ID) {
		t.Fatalf("cancelled %s, want the elicitation %s", p.RequestID, req.ID)
	}
	if got := toolResultText(t, c.awaitID(id)); !strings.Contains(got, "deadline") {
		t.Fatalf("tool saw %q, want the deadline error", got)
	}
}

// Outstanding server→client requests are bounded per session.
func TestOutboundCallsAreBounded(t *testing.T) {
	h := startServer(t, Options{MaxPendingCalls: 2}, UnixOptions{}, func(s *Server) {
		_ = s.AddTool(Tool{Name: "flood"}, func(ctx context.Context, sess *Session, _ json.RawMessage) (*CallToolResult, error) {
			var wg sync.WaitGroup
			errs := make([]error, 4)
			cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			for i := range errs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, errs[i] = sess.Elicit(cctx, "x", json.RawMessage(`{}`))
				}()
			}
			wg.Wait()
			n := 0
			for _, e := range errs {
				if e != nil && strings.Contains(e.Error(), "outbound calls already pending") {
					n++
				}
			}
			return ToolText("%d", n), nil
		})
	})
	c := h.dial()
	c.handshakeWith(ClientCapabilities{Elicitation: &emptyCap{}})
	if got := toolResultText(t, c.awaitID(c.callTool("flood"))); got != "2" {
		t.Fatalf("%s of 4 concurrent elicitations were refused, want 2 (MaxPendingCalls is 2)", got)
	}
}
