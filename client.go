package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync"
)

// Client limits. Both bound something the peer controls.
const (
	// maxClientServerRequests bounds server→client requests being handled at
	// once; a server that floods elicitations is answered "overloaded" rather
	// than getting a goroutine each.
	maxClientServerRequests = 64

	// maxListPages bounds how many pages ListTools follows, so a server that
	// never stops returning a cursor cannot hold the client forever.
	maxListPages = 1000
)

// Client is a minimal MCP client over a Framer. It performs the initialize
// handshake, lists and calls tools, and dispatches server→client requests
// (elicitation/create, sampling/createMessage, roots/list) to a handler. A
// background goroutine reads the stream and correlates responses by id; every
// public method is safe for concurrent use.
//
// It is deliberately small — enough for a consumer (a gateway, a tool that
// composes another) to drive a tool server, not a feature-complete client.
// Resources, prompts, sampling, and server notifications are out of scope; add
// them when a consumer needs them.
type Client struct {
	fr    Framer
	info  Implementation
	onReq ServerRequestHandler

	writeMu sync.Mutex // serialises WriteMessage across callers and the reader's responses

	mu       sync.Mutex
	nextID   int64
	pending  map[string]chan wireMessage
	handling map[string]context.CancelFunc // server→client requests being handled, by id key
	closed   bool
	err      error // terminal read error

	ctx    context.Context // cancelled when the connection ends; the parent of every handler's context
	cancel context.CancelFunc
	sem    chan struct{} // bounds concurrent server→client request handlers

	done     chan struct{}
	doneOnce sync.Once
}

// ServerRequestHandler answers a server→client request. Return a value to encode
// as the result, or an *RPCError. A nil handler answers every server request
// with method-not-found, which is correct for a client that advertises no
// client-side capabilities.
type ServerRequestHandler func(ctx context.Context, method string, params json.RawMessage) (any, *RPCError)

// ClientOptions configures Dial.
type ClientOptions struct {
	Info            Implementation       // client identity sent at initialize
	Framing         FramingMode          // defaults to FramingLines (MCP stdio / socket)
	MaxMessageBytes int                  // zero selects DefaultMaxMessageBytes
	OnRequest       ServerRequestHandler // handles server→client requests; nil ⇒ method-not-found, and the elicitation capability is not advertised

	// HTTPClient and Header apply to DialHTTP only. HTTPClient carries the TLS
	// configuration, proxy and RoundTripper (nil selects a client with no
	// timeout, which a stream needs); Header is sent on every request, which is
	// where a bearer token goes. Framing is ignored by DialHTTP.
	HTTPClient *http.Client
	Header     http.Header
}

// Dial starts a client over rwc — a socket, a stdio pipe pair, or an in-memory
// pipe. It launches the read loop immediately; call Initialize next, and Close
// when done. Close is the caller's responsibility even if Initialize fails.
func Dial(rwc io.ReadWriteCloser, opts ClientOptions) *Client {
	fr := NewFramer(rwc, rwc, FramerOptions{
		Mode:            opts.Framing,
		MaxMessageBytes: opts.MaxMessageBytes,
		Closer:          rwc,
	})
	return newClient(fr, opts)
}

// newClient builds a Client over any Framer and starts its read loop. Dial and
// DialHTTP differ only in the Framer.
func newClient(fr Framer, opts ClientOptions) *Client {
	c := &Client{
		fr:       fr,
		info:     opts.Info,
		onReq:    opts.OnRequest,
		pending:  make(map[string]chan wireMessage),
		handling: make(map[string]context.CancelFunc),
		sem:      make(chan struct{}, maxClientServerRequests),
		done:     make(chan struct{}),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	go c.readLoop()
	return c
}

// Initialize performs the MCP handshake and sends notifications/initialized,
// which the spec requires before any other request.
func (c *Client) Initialize(ctx context.Context) (InitializeResult, error) {
	var res InitializeResult
	// Advertise elicitation only when a handler can actually answer it. A server
	// checks the advertised capability before eliciting (Session.Elicit), so
	// claiming it with a nil handler makes that check pass and then answers the
	// elicitation with method-not-found mid-call — the failure surfaces on the
	// server's side, far from the client that mis-advertised.
	caps := ClientCapabilities{}
	if c.onReq != nil {
		caps.Elicitation = &emptyCap{}
	}
	raw, err := c.call(ctx, MethodInitialize, InitializeParams{
		ProtocolVersion: LatestProtocolVersion,
		Capabilities:    caps,
		ClientInfo:      c.info,
	})
	if err != nil {
		return res, err
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return res, err
	}
	if !slices.Contains(DefaultProtocolVersions, res.ProtocolVersion) {
		return res, fmt.Errorf("mcp: server chose protocol version %q, which this client does not speak", sanitize(res.ProtocolVersion))
	}
	if err := c.notify(NotifyInitialized, nil); err != nil {
		return res, err
	}
	return res, nil
}

// ListTools returns every tool the server lists, following nextCursor across
// pages. A server that repeats a cursor, or never stops paging, is an error.
func (c *Client) ListTools(ctx context.Context) ([]Tool, error) {
	var (
		tools  []Tool
		cursor string
		seen   = map[string]struct{}{}
	)
	for range maxListPages {
		raw, err := c.call(ctx, MethodToolsList, PaginatedParams{Cursor: cursor})
		if err != nil {
			return nil, err
		}
		var res ListToolsResult
		if err := json.Unmarshal(raw, &res); err != nil {
			return nil, err
		}
		tools = append(tools, res.Tools...)
		if res.NextCursor == "" {
			return tools, nil
		}
		if _, dup := seen[res.NextCursor]; dup {
			return nil, errors.New("mcp: server repeated a pagination cursor")
		}
		seen[res.NextCursor] = struct{}{}
		cursor = res.NextCursor
	}
	return nil, fmt.Errorf("mcp: server returned more than %d pages of tools", maxListPages)
}

// CallTool invokes name with args. A JSON-RPC error (unknown tool, malformed
// arguments) is returned as err; a tool that ran and failed is a non-nil result
// with IsError set.
func (c *Client) CallTool(ctx context.Context, name string, args map[string]any) (*CallToolResult, error) {
	var argsRaw json.RawMessage
	if args != nil {
		b, err := json.Marshal(args)
		if err != nil {
			return nil, err
		}
		argsRaw = b
	}
	raw, err := c.call(ctx, MethodToolsCall, CallToolParams{Name: name, Arguments: argsRaw})
	if err != nil {
		return nil, err
	}
	var res CallToolResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Close shuts the connection; pending and future calls fail.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.cancel()          // handlers still running see their context end
	return c.fr.Close() // unblocks the read loop, which closes c.done
}

// ---- internals ----

func (c *Client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	pb, err := marshal(params)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		// A connection that ended for a reason says so; only a plain Close, or a
		// clean EOF, is "closed".
		cause := c.err
		c.mu.Unlock()
		if cause != nil && !errors.Is(cause, io.EOF) {
			return nil, cause
		}
		return nil, errors.New("mcp: client closed")
	}
	c.nextID++
	idRaw := json.RawMessage(strconv.AppendInt(nil, c.nextID, 10))
	key := idKey(idRaw)
	ch := make(chan wireMessage, 1)
	c.pending[key] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
	}()

	req, err := marshal(outRequest{JSONRPC: JSONRPCVersion, ID: idRaw, Method: method, Params: pb})
	if err != nil {
		return nil, err
	}
	if err := c.write(req); err != nil {
		return nil, err
	}

	select {
	case m := <-ch:
		if m.Error != nil {
			return nil, m.Error
		}
		return m.Result, nil
	case <-ctx.Done():
		// Tell the server to stop working on it (utilities/cancellation), except
		// for initialize, which the spec forbids cancelling. Best effort, and off
		// this goroutine: a peer that has stopped reading would otherwise make a
		// cancelled call outlive its own context. The write ends when Close does.
		if method != MethodInitialize {
			go func() { _ = c.notify(NotifyCancelled, CancelledParams{RequestID: idRaw, Reason: ctx.Err().Error()}) }()
		}
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.readErr()
	}
}

func (c *Client) notify(method string, params any) error {
	pb, err := marshal(params)
	if err != nil {
		return err
	}
	raw, err := marshal(outNotification{JSONRPC: JSONRPCVersion, Method: method, Params: pb})
	if err != nil {
		return err
	}
	return c.write(raw)
}

func (c *Client) write(raw []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.fr.WriteMessage(raw)
}

func (c *Client) readLoop() {
	for {
		raw, err := c.fr.ReadMessage()
		if err != nil {
			c.fail(err)
			return
		}
		var m wireMessage
		if json.Unmarshal(raw, &m) != nil {
			continue // ignore a frame we cannot parse
		}
		switch m.kind() {
		case kindResponse:
			c.deliver(m)
		case kindRequest:
			c.dispatchServerRequest(m)
		case kindNotification:
			if m.Method == NotifyCancelled {
				c.cancelServerRequest(m.Params)
			}
		default:
			// invalid — nothing to correlate
		}
	}
}

func (c *Client) deliver(m wireMessage) {
	key := idKey(m.ID)
	c.mu.Lock()
	ch, ok := c.pending[key]
	c.mu.Unlock()
	if ok {
		select {
		case ch <- m:
		default: // buffered(1) and each id is used once, so this cannot lose a live answer
		}
	}
}

// dispatchServerRequest answers a server→client request off the read loop. A
// handler may block for as long as a human takes to answer an elicitation, and
// may call back into the Client; run inline, either would stall the loop that
// delivers the replies they wait for.
func (c *Client) dispatchServerRequest(m wireMessage) {
	if m.Method == MethodPing {
		c.respond(m.ID, struct{}{}, nil) // ping is answered by every peer, handler or not
		return
	}
	if c.onReq == nil {
		c.respond(m.ID, nil, Errorf(CodeMethodNotFound, "no handler for %s", sanitize(m.Method)))
		return
	}
	select {
	case c.sem <- struct{}{}:
	default:
		c.respond(m.ID, nil, Errorf(CodeInternalError, "client is overloaded; retry later"))
		return
	}
	ctx, cancel := context.WithCancel(c.ctx)
	key := idKey(m.ID)
	c.mu.Lock()
	c.handling[key] = cancel
	c.mu.Unlock()
	go func() {
		defer func() { <-c.sem }()
		result, rerr := c.onReq(ctx, m.Method, m.Params)
		c.mu.Lock()
		delete(c.handling, key)
		c.mu.Unlock()
		cancelled := ctx.Err() != nil
		cancel()
		if cancelled {
			return // the server cancelled it, or the connection ended: no reply is expected
		}
		c.respond(m.ID, result, rerr)
	}()
}

// cancelServerRequest ends the handler of a request the server cancelled.
func (c *Client) cancelServerRequest(params json.RawMessage) {
	var p CancelledParams
	if json.Unmarshal(params, &p) != nil || len(p.RequestID) == 0 {
		return
	}
	c.mu.Lock()
	cancel := c.handling[idKey(p.RequestID)]
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// respond writes the reply to a server→client request.
func (c *Client) respond(id json.RawMessage, result any, rerr *RPCError) {
	if rerr != nil {
		if raw, err := marshal(outError{JSONRPC: JSONRPCVersion, ID: id, Error: rerr}); err == nil {
			_ = c.write(raw)
		}
		return
	}
	rb, err := marshal(result)
	if err != nil {
		eb, _ := marshal(outError{JSONRPC: JSONRPCVersion, ID: id, Error: Errorf(CodeInternalError, "encode result: %v", err)})
		_ = c.write(eb)
		return
	}
	if raw, err := marshal(outResponse{JSONRPC: JSONRPCVersion, ID: id, Result: rb}); err == nil {
		_ = c.write(raw)
	}
}

func (c *Client) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.closed = true
	c.mu.Unlock()
	c.cancel()
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *Client) readErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil && !errors.Is(c.err, io.EOF) {
		return c.err
	}
	return errors.New("mcp: connection closed")
}
