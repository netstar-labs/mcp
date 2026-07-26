package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"runtime/debug"
	"strings"
	"syscall"
)

// maxStackBytes bounds a panic trace written to the log. A goroutine dump from
// a deeply nested handler can run to megabytes.
const maxStackBytes = 4096

// toRPCError converts a handler error into a protocol error. An explicit
// *RPCError from the handler is a deliberate protocol fault and passes
// through; anything else is wrapped with the supplied code.
func toRPCError(err error, code int, format string, args ...any) *RPCError {
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr
	}
	return Errorf(code, format, args...)
}

// execRequest dispatches one request. It returns either a result value to be
// marshalled or a JSON-RPC error, never both.
func (s *Session) execRequest(ctx context.Context, wm *wireMessage) (result any, rerr *RPCError) {
	// A panic in a handler must cost one request, not the daemon.
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			if len(stack) > maxStackBytes {
				stack = stack[:maxStackBytes]
			}
			s.server.opts.Logger.Printf("%s: panic in %s: %v\n%s",
				s.remote, sanitize(wm.Method), r, stack)
			result = nil
			rerr = Errorf(CodeInternalError, "internal error")
		}
	}()

	if wm.Method == MethodInitialize {
		return s.handleInitialize(wm.Params)
	}
	if wm.Method == MethodPing {
		// Ping is answered in any state; it is the liveness probe.
		return struct{}{}, nil
	}
	if s.State() != StateReady {
		return nil, Errorf(CodeNotInitialized,
			"session is %s: send initialize and notifications/initialized first", stateName(s.State()))
	}

	switch wm.Method {
	case MethodToolsList:
		return s.handleToolsList(wm.Params)
	case MethodToolsCall:
		return s.handleToolsCall(ctx, wm.Params)
	case MethodResourcesList:
		return s.handleResourcesList(wm.Params)
	case MethodResourcesTemplates:
		return s.handleTemplatesList(wm.Params)
	case MethodResourcesRead:
		return s.handleResourcesRead(ctx, wm.Params)
	case MethodResourcesSubscribe:
		return s.handleSubscribe(wm.Params, true)
	case MethodResourcesUnsubscribe:
		return s.handleSubscribe(wm.Params, false)
	case MethodPromptsList:
		return s.handlePromptsList(wm.Params)
	case MethodPromptsGet:
		return s.handlePromptsGet(ctx, wm.Params)
	case MethodLoggingSetLevel:
		return s.handleSetLevel(wm.Params)
	case MethodCompletionComplete:
		return s.handleComplete(ctx, wm.Params)
	default:
		return nil, Errorf(CodeMethodNotFound, "method not found: %s", sanitize(wm.Method))
	}
}

func (s *Session) handleInitialize(params json.RawMessage) (any, *RPCError) {
	if !s.state.CompareAndSwap(StateNew, StateInitializing) {
		return nil, Errorf(CodeInvalidRequest, "initialize has already been performed")
	}
	var p InitializeParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		s.state.Store(StateNew)
		return nil, rerr
	}

	negotiated := s.server.negotiate(p.ProtocolVersion)

	// Peer-supplied identity is sanitised at the boundary, once, so no logging
	// site downstream has to remember to do it.
	p.ClientInfo.Name = sanitize(p.ClientInfo.Name)
	p.ClientInfo.Title = sanitize(p.ClientInfo.Title)
	p.ClientInfo.Version = sanitize(p.ClientInfo.Version)

	s.mu.Lock()
	s.clientInfo = p.ClientInfo
	s.clientCaps = p.Capabilities
	s.version = negotiated
	s.mu.Unlock()

	return InitializeResult{
		ProtocolVersion: negotiated,
		Capabilities:    s.server.capabilities(),
		ServerInfo: Implementation{
			Name:    s.server.opts.Name,
			Title:   s.server.opts.Title,
			Version: s.server.opts.Version,
		},
		Instructions: s.server.opts.Instructions,
	}, nil
}

func (s *Session) handleToolsList(params json.RawMessage) (any, *RPCError) {
	var p PaginatedParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	srv := s.server
	return srv.listPage("tools", &srv.toolCache, p.Cursor,
		func() int { return len(srv.toolOrder) },
		func(lo, hi int, next string) any {
			out := ListToolsResult{Tools: make([]Tool, 0, hi-lo), NextCursor: next}
			for _, name := range srv.toolOrder[lo:hi] {
				out.Tools = append(out.Tools, srv.tools[name].def)
			}
			return out
		})
}

func (s *Session) handleResourcesList(params json.RawMessage) (any, *RPCError) {
	var p PaginatedParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	srv := s.server
	return srv.listPage("resources", &srv.resourceCache, p.Cursor,
		func() int { return len(srv.resourceOrder) },
		func(lo, hi int, next string) any {
			out := ListResourcesResult{Resources: make([]Resource, 0, hi-lo), NextCursor: next}
			for _, uri := range srv.resourceOrder[lo:hi] {
				out.Resources = append(out.Resources, srv.resources[uri].def)
			}
			return out
		})
}

func (s *Session) handleTemplatesList(params json.RawMessage) (any, *RPCError) {
	var p PaginatedParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	srv := s.server
	return srv.listPage("templates", &srv.templateCache, p.Cursor,
		func() int { return len(srv.templates) },
		func(lo, hi int, next string) any {
			out := ListResourceTemplatesResult{
				ResourceTemplates: make([]ResourceTemplate, 0, hi-lo),
				NextCursor:        next,
			}
			for _, t := range srv.templates[lo:hi] {
				out.ResourceTemplates = append(out.ResourceTemplates, t.def)
			}
			return out
		})
}

func (s *Session) handlePromptsList(params json.RawMessage) (any, *RPCError) {
	var p PaginatedParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	srv := s.server
	return srv.listPage("prompts", &srv.promptCache, p.Cursor,
		func() int { return len(srv.promptOrder) },
		func(lo, hi int, next string) any {
			out := ListPromptsResult{Prompts: make([]Prompt, 0, hi-lo), NextCursor: next}
			for _, name := range srv.promptOrder[lo:hi] {
				out.Prompts = append(out.Prompts, srv.prompts[name].def)
			}
			return out
		})
}

func (s *Session) handleToolsCall(ctx context.Context, params json.RawMessage) (any, *RPCError) {
	var p CallToolParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	if p.Name == "" {
		return nil, Errorf(CodeInvalidParams, "tool name is required")
	}

	s.server.mu.RLock()
	entry, ok := s.server.tools[p.Name]
	s.server.mu.RUnlock()
	if !ok {
		return nil, Errorf(CodeMethodNotFound, "unknown tool: %s", sanitize(p.Name))
	}

	args := p.Arguments
	if len(args) == 0 {
		args = emptyObject
	}

	res, err := entry.fn(ctx, s, args)
	if err != nil {
		// A returned error is treated as tool-execution failure and handed
		// back to the model, unless the handler deliberately returned an
		// *RPCError, which is a protocol-level fault.
		var rpcErr *RPCError
		if errors.As(err, &rpcErr) {
			return nil, rpcErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, Errorf(CodeInternalError, "tool %s: %v", sanitize(p.Name), err)
		}
		return &CallToolResult{Content: []Content{TextContent(err.Error())}, IsError: true}, nil
	}
	if res == nil {
		res = &CallToolResult{}
	}
	if res.Content == nil {
		// The field is required and must serialise as [], not null.
		res.Content = []Content{}
	}
	return res, nil
}

var emptyObject = json.RawMessage(`{}`)

func (s *Session) handleResourcesRead(ctx context.Context, params json.RawMessage) (any, *RPCError) {
	var p ReadResourceParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	if p.URI == "" {
		return nil, Errorf(CodeInvalidParams, "resource uri is required")
	}

	srv := s.server
	srv.mu.RLock()
	var fn ResourceHandler
	if entry, ok := srv.resources[p.URI]; ok {
		fn = entry.fn
	} else {
		for _, t := range srv.templates {
			// Matches avoids building the variable map, which the dispatcher
			// does not need; handlers extract it themselves.
			if t.tmpl.Matches(p.URI) {
				fn = t.fn
				break
			}
		}
	}
	srv.mu.RUnlock()

	if fn == nil {
		return nil, Errorf(CodeResourceNotFound, "resource not found: %s", sanitize(p.URI)).
			WithData(map[string]string{"uri": p.URI})
	}

	contents, err := fn(ctx, s, p.URI)
	if err != nil {
		return nil, toRPCError(err, CodeInternalError, "read %s: %v", sanitize(p.URI), err)
	}
	if contents == nil {
		contents = []ResourceContents{}
	}
	return ReadResourceResult{Contents: contents}, nil
}

func (s *Session) handleSubscribe(params json.RawMessage, add bool) (any, *RPCError) {
	var p SubscribeParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	if p.URI == "" {
		return nil, Errorf(CodeInvalidParams, "resource uri is required")
	}
	if len(p.URI) > maxSubscriptionURIBytes {
		return nil, Errorf(CodeInvalidParams, "uri exceeds %d bytes", maxSubscriptionURIBytes)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !add {
		delete(s.subs, p.URI)
		return struct{}{}, nil
	}
	if _, exists := s.subs[p.URI]; !exists && len(s.subs) >= s.server.opts.MaxSubscriptions {
		return nil, Errorf(CodeResourceLimit, "subscription limit of %d reached",
			s.server.opts.MaxSubscriptions)
	}
	s.subs[p.URI] = struct{}{}
	return struct{}{}, nil
}

func (s *Session) handlePromptsGet(ctx context.Context, params json.RawMessage) (any, *RPCError) {
	var p GetPromptParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	s.server.mu.RLock()
	entry, ok := s.server.prompts[p.Name]
	s.server.mu.RUnlock()
	if !ok {
		return nil, Errorf(CodeInvalidParams, "unknown prompt: %s", sanitize(p.Name))
	}
	for _, arg := range entry.def.Arguments {
		if arg.Required {
			if _, present := p.Arguments[arg.Name]; !present {
				return nil, Errorf(CodeInvalidParams, "missing required argument: %s", sanitize(arg.Name))
			}
		}
	}
	res, err := entry.fn(ctx, s, p.Arguments)
	if err != nil {
		return nil, toRPCError(err, CodeInternalError, "prompt %s: %v", sanitize(p.Name), err)
	}
	if res == nil {
		res = &GetPromptResult{}
	}
	if res.Messages == nil {
		res.Messages = []PromptMessage{}
	}
	return res, nil
}

func (s *Session) handleSetLevel(params json.RawMessage) (any, *RPCError) {
	var p SetLevelParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	if _, ok := logSeverity[p.Level]; !ok {
		return nil, Errorf(CodeInvalidParams, "unknown logging level: %s", sanitize(p.Level))
	}
	s.logLevel.Store(p.Level)
	return struct{}{}, nil
}

// maxCompletionValues is the page size the wire format specifies for
// completion results.
const maxCompletionValues = 100

func (s *Session) handleComplete(ctx context.Context, params json.RawMessage) (any, *RPCError) {
	s.server.mu.RLock()
	completer := s.server.completer
	s.server.mu.RUnlock()
	if completer == nil {
		return nil, Errorf(CodeMethodNotFound, "completions are not supported")
	}
	var p CompleteParams
	if rerr := unmarshalParams(params, &p); rerr != nil {
		return nil, rerr
	}
	values, err := completer(ctx, s, &p)
	if err != nil {
		return nil, toRPCError(err, CodeInternalError, "completion: %v", err)
	}
	total := len(values)
	hasMore := false
	if len(values) > maxCompletionValues {
		values = values[:maxCompletionValues]
		hasMore = true
	}
	if values == nil {
		values = []string{}
	}
	var out CompleteResult
	out.Completion.Values = values
	out.Completion.Total = &total
	out.Completion.HasMore = hasMore
	return out, nil
}

func (s *Session) handleNotification(ctx context.Context, wm *wireMessage) {
	defer func() {
		if r := recover(); r != nil {
			s.server.opts.Logger.Printf("%s: panic in notification %s: %v",
				s.remote, sanitize(wm.Method), r)
		}
	}()

	switch wm.Method {
	case NotifyInitialized:
		if s.state.CompareAndSwap(StateInitializing, StateReady) {
			if s.server.opts.OnSessionStart != nil {
				// Off the read path: a slow callback must not stall the
				// connection, and the state transition has already happened.
				go s.server.opts.OnSessionStart(s)
			}
		}

	case NotifyCancelled:
		var p CancelledParams
		if err := json.Unmarshal(wm.Params, &p); err != nil {
			return
		}
		if len(p.RequestID) > maxIDBytes {
			return
		}
		key := idKey(p.RequestID)
		s.mu.Lock()
		entry, ok := s.inflight[key]
		s.mu.Unlock()
		if ok {
			entry.cancelled.Store(true)
			entry.cancel()
		}

	case NotifyProgress:
		// Progress on a server-initiated request. Nothing to do by default.

	case NotifyRootsChanged:
		// The client's roots changed; handlers can re-fetch with ListRoots.

	default:
		s.server.opts.Logger.Printf("%s: ignoring unknown notification %s",
			s.remote, sanitize(wm.Method))
	}
	_ = ctx
}

// URITemplate is a compiled RFC 6570 template supporting simple ({var}) and
// reserved ({+var}) expansion, which is the subset MCP servers use.
// Compilation is done once at registration; matching allocates only when the
// caller asks for the bound variables.
type URITemplate struct {
	raw    string
	tokens []tmplToken
}

type tmplToken struct {
	lit      string
	name     string
	reserved bool
}

// CompileURITemplate parses a template. It reports an error for an unbalanced
// expression rather than silently failing to match at request time.
func CompileURITemplate(tmpl string) (*URITemplate, error) {
	t := &URITemplate{raw: tmpl}
	for i := 0; i < len(tmpl); {
		open := strings.IndexByte(tmpl[i:], '{')
		if open < 0 {
			t.tokens = append(t.tokens, tmplToken{lit: tmpl[i:]})
			break
		}
		if open > 0 {
			t.tokens = append(t.tokens, tmplToken{lit: tmpl[i : i+open]})
		}
		end := strings.IndexByte(tmpl[i+open:], '}')
		if end < 0 {
			return nil, fmt.Errorf("mcp: unbalanced '{' in URI template %q", tmpl)
		}
		expr := tmpl[i+open+1 : i+open+end]
		reserved := strings.HasPrefix(expr, "+") || strings.HasPrefix(expr, "#")
		name := strings.TrimLeft(expr, "+#./;?&")
		if name == "" {
			return nil, fmt.Errorf("mcp: empty variable in URI template %q", tmpl)
		}
		t.tokens = append(t.tokens, tmplToken{name: name, reserved: reserved})
		i += open + end + 1
	}
	return t, nil
}

func (t *URITemplate) String() string { return t.raw }

// Matches reports whether uri fits the template without building the variable
// map. This is the dispatcher's path.
func (t *URITemplate) Matches(uri string) bool {
	return t.walk(uri, nil)
}

// Match returns the bound variables, percent-decoded.
func (t *URITemplate) Match(uri string) (map[string]string, bool) {
	vars := make(map[string]string, len(t.tokens)/2+1)
	if !t.walk(uri, vars) {
		return nil, false
	}
	return vars, true
}

// walk is the shared matcher. When vars is nil it only reports success, which
// keeps the read path allocation-free.
func (t *URITemplate) walk(uri string, vars map[string]string) bool {
	pos := 0
	for i, tk := range t.tokens {
		if tk.name == "" {
			if !strings.HasPrefix(uri[pos:], tk.lit) {
				return false
			}
			pos += len(tk.lit)
			continue
		}
		// Variable: bounded by the next literal, or the end of the URI.
		var value string
		if i+1 < len(t.tokens) && t.tokens[i+1].name == "" {
			idx := strings.Index(uri[pos:], t.tokens[i+1].lit)
			if idx < 0 {
				return false
			}
			value = uri[pos : pos+idx]
		} else {
			value = uri[pos:]
		}
		if value == "" {
			return false
		}
		if !tk.reserved && strings.Contains(value, "/") {
			return false
		}
		if vars != nil {
			if decoded, err := url.PathUnescape(value); err == nil {
				vars[tk.name] = decoded
			} else {
				vars[tk.name] = value
			}
		}
		pos += len(value)
	}
	return pos == len(uri)
}

// URITemplateVars compiles and matches in one call. Prefer CompileURITemplate
// when the template is fixed, which it almost always is.
func URITemplateVars(tmpl, uri string) (map[string]string, bool) {
	compiled, err := CompileURITemplate(tmpl)
	if err != nil {
		return nil, false
	}
	return compiled.Match(uri)
}

// isBenignClose reports whether an I/O error is an ordinary disconnect rather
// than a fault worth logging.
func isBenignClose(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, context.Canceled) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Err != nil {
		return strings.Contains(opErr.Err.Error(), "use of closed network connection")
	}
	return false
}
