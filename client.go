package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
)

// Client is a minimal MCP client over a Framer. It performs the initialize
// handshake, lists and calls tools, and dispatches server→client requests
// (elicitation/create, sampling/createMessage, roots/list) to a handler. A
// background goroutine reads the stream and correlates responses by id; every
// public method is safe for concurrent use.
//
// It is deliberately small — enough for a consumer (a gateway, a tool that
// composes another) to drive a local tool server, not a feature-complete client.
// Resources, prompts, sampling, and pagination past the first page are out of
// scope; add them when a consumer needs them.
type Client struct {
	fr    Framer
	info  Implementation
	onReq ServerRequestHandler

	writeMu sync.Mutex // serialises WriteMessage across callers and the reader's responses

	mu      sync.Mutex
	nextID  int64
	pending map[string]chan wireMessage
	closed  bool
	err     error // terminal read error

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
	c := &Client{
		fr:      fr,
		info:    opts.Info,
		onReq:   opts.OnRequest,
		pending: make(map[string]chan wireMessage),
		done:    make(chan struct{}),
	}
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
	_ = c.notify(NotifyInitialized, nil)
	return res, nil
}

// ListTools returns the server's first page of tools.
func (c *Client) ListTools(ctx context.Context) ([]Tool, error) {
	raw, err := c.call(ctx, MethodToolsList, PaginatedParams{})
	if err != nil {
		return nil, err
	}
	var res ListToolsResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return res.Tools, nil
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
		c.mu.Unlock()
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
			c.handleServerRequest(m)
		default:
			// notification / invalid — nothing to correlate
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

func (c *Client) handleServerRequest(m wireMessage) {
	var (
		result any
		rerr   *RPCError
	)
	if c.onReq != nil {
		result, rerr = c.onReq(context.Background(), m.Method, m.Params)
	} else {
		rerr = Errorf(CodeMethodNotFound, "no handler for %s", sanitize(m.Method))
	}
	if rerr != nil {
		if raw, err := marshal(outError{JSONRPC: JSONRPCVersion, ID: m.ID, Error: rerr}); err == nil {
			_ = c.write(raw)
		}
		return
	}
	rb, err := marshal(result)
	if err != nil {
		eb, _ := marshal(outError{JSONRPC: JSONRPCVersion, ID: m.ID, Error: Errorf(CodeInternalError, "encode result: %v", err)})
		_ = c.write(eb)
		return
	}
	if raw, err := marshal(outResponse{JSONRPC: JSONRPCVersion, ID: m.ID, Result: rb}); err == nil {
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
