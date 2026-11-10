package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestCompletion(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		s.SetCompleter(func(ctx context.Context, sess *Session, p *CompleteParams) ([]string, error) {
			switch p.Argument.Value {
			case "err":
				return nil, errors.New("backend down")
			case "none":
				return nil, nil
			}
			out := make([]string, 150)
			for i := range out {
				out[i] = fmt.Sprintf("%s-%d", p.Argument.Value, i)
			}
			return out, nil
		})
	})
	res := c.handshake()
	if res.Capabilities.Completions == nil {
		t.Fatal("a registered completer must advertise the completions capability")
	}
	complete := func(value string) (CompleteResult, *RPCError) {
		params := map[string]any{
			"ref":      map[string]any{"type": "ref/prompt", "name": "p"},
			"argument": map[string]any{"name": "a", "value": value},
		}
		c.id++
		id := c.id
		c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": MethodCompletionComplete, "params": params})
		for {
			wm := c.readRaw()
			if wm.kind() != kindResponse || string(wm.ID) != fmt.Sprint(id) {
				continue
			}
			var out CompleteResult
			if wm.Error == nil {
				if err := json.Unmarshal(wm.Result, &out); err != nil {
					t.Fatal(err)
				}
			}
			return out, wm.Error
		}
	}
	got, rerr := complete("x")
	if rerr != nil || len(got.Completion.Values) != maxCompletionValues || !got.Completion.HasMore || *got.Completion.Total != 150 {
		t.Fatalf("150 candidates: %d values, hasMore=%v, total=%v, err=%v; want %d, true, 150",
			len(got.Completion.Values), got.Completion.HasMore, got.Completion.Total, rerr, maxCompletionValues)
	}
	if got, rerr = complete("none"); rerr != nil || got.Completion.Values == nil || len(got.Completion.Values) != 0 {
		t.Fatalf("no candidates must encode as [], got %v, %v", got.Completion.Values, rerr)
	}
	if _, rerr = complete("err"); rerr == nil || rerr.Code != CodeInternalError {
		t.Fatalf("a failing completer must be an internal error, got %v", rerr)
	}
}

func TestCompletionWithoutCompleterIsMethodNotFound(t *testing.T) {
	_, c := newTestServer(t, nil)
	c.handshake()
	err := c.requestErr(MethodCompletionComplete, map[string]any{
		"ref": map[string]any{"type": "ref/prompt", "name": "p"}, "argument": map[string]any{"name": "a", "value": "x"}})
	if err.Code != CodeMethodNotFound {
		t.Fatalf("code %d, want %d", err.Code, CodeMethodNotFound)
	}
}

// resources/list, resources/templates/list and prompts/list page the same way
// tools/list does (page size 2 in the test server).
func TestResourceTemplatePromptListsPaginate(t *testing.T) {
	_, c := newTestServer(t, func(s *Server) {
		noRes := func(context.Context, *Session, string) ([]ResourceContents, error) { return nil, nil }
		for i := range 3 {
			_ = s.AddResource(Resource{URI: fmt.Sprintf("test://r%d", i), Name: fmt.Sprintf("r%d", i)}, noRes)
			_ = s.AddResourceTemplate(ResourceTemplate{URITemplate: fmt.Sprintf("test://t%d/{id}", i), Name: fmt.Sprintf("t%d", i)}, noRes)
			_ = s.AddPrompt(Prompt{Name: fmt.Sprintf("p%d", i)}, func(context.Context, *Session, map[string]string) (*GetPromptResult, error) {
				return &GetPromptResult{}, nil
			})
		}
	})
	c.handshake()

	walk := func(method string, count func(json.RawMessage) (int, string)) int {
		total, cursor := 0, ""
		for pages := 0; pages < 5; pages++ {
			n, next := count(c.request(method, PaginatedParams{Cursor: cursor}))
			total += n
			if next == "" {
				return total
			}
			cursor = next
		}
		t.Fatalf("%s: no final page", method)
		return 0
	}
	if n := walk(MethodResourcesList, func(r json.RawMessage) (int, string) {
		var out ListResourcesResult
		_ = json.Unmarshal(r, &out)
		return len(out.Resources), out.NextCursor
	}); n != 3 {
		t.Errorf("resources/list returned %d across pages, want 3", n)
	}
	if n := walk(MethodResourcesTemplates, func(r json.RawMessage) (int, string) {
		var out ListResourceTemplatesResult
		_ = json.Unmarshal(r, &out)
		return len(out.ResourceTemplates), out.NextCursor
	}); n != 3 {
		t.Errorf("resources/templates/list returned %d across pages, want 3", n)
	}
	if n := walk(MethodPromptsList, func(r json.RawMessage) (int, string) {
		var out ListPromptsResult
		_ = json.Unmarshal(r, &out)
		return len(out.Prompts), out.NextCursor
	}); n != 3 {
		t.Errorf("prompts/list returned %d across pages, want 3", n)
	}
}

// list_changed notifications reach every ready session.
func TestListChangedBroadcasts(t *testing.T) {
	srv, c := newTestServer(t, nil)
	c.handshake()
	// Broadcast skips sessions that have not finished initializing, and the
	// server processes notifications/initialized after handshake returns.
	eventually(t, "the session to be ready", func() bool {
		ss := srv.Sessions()
		return len(ss) == 1 && ss[0].State() == StateReady
	})
	for _, tc := range []struct {
		method string
		send   func()
	}{
		{NotifyToolsChanged, srv.NotifyToolListChanged},
		{NotifyResourcesChanged, srv.NotifyResourceListChanged},
		{NotifyPromptsChanged, srv.NotifyPromptListChanged},
		{"notifications/custom", func() { srv.Broadcast("notifications/custom", map[string]int{"n": 1}) }},
	} {
		tc.send()
		for {
			if wm := c.readRaw(); wm.Method == tc.method {
				break
			}
		}
	}
}
