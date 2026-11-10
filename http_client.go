package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ErrSessionExpired is the terminal error of a Client whose server no longer
// knows its session (HTTP 404 on a request that carried a session id). The
// session is gone for good: dial again.
var ErrSessionExpired = errors.New("mcp: server no longer knows this session")

const (
	// maxHTTPClientExchanges bounds requests in flight from one client. Past it
	// the writer waits, which is the back-pressure a caller sees as a slow call.
	maxHTTPClientExchanges = 64

	httpClientQueue = 64

	// notifyTimeout bounds the POST of a notification or reply, which the caller
	// waits for (see httpClientFramer).
	notifyTimeout = 30 * time.Second

	// maxHTTPErrorBody is how much of an error response's body is read for the
	// message. The rest is not needed and not trusted.
	maxHTTPErrorBody = 4096
)

// DialHTTP returns a Client that speaks to an MCP server over streamable HTTP at
// endpoint, the counterpart of Server.HTTPHandler. Call Initialize next, and
// Close when done; Close ends the server's session.
//
// ctx bounds the connection's whole life, not any one call: cancelling it
// closes the client. DialHTTP does no I/O, so its only errors are a bad
// endpoint. Authentication is the caller's: put credentials in
// ClientOptions.Header, or in the RoundTripper of ClientOptions.HTTPClient.
//
// A server request (OnRequest) arrives on the stream of the POST it relates to
// or, when the client has a handler, on a GET stream opened after initialize.
// The stream is reopened with backoff if it drops; a frame in flight when it
// drops is lost, as there is no resumption.
func DialHTTP(ctx context.Context, endpoint string, opts ClientOptions) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("mcp: endpoint: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("mcp: endpoint %q is not an http(s) URL", sanitize(endpoint))
	}
	hc := opts.HTTPClient
	if hc == nil {
		// No Timeout: it would cut every SSE stream. Per-call deadlines are the
		// context each call is given.
		hc = &http.Client{}
	}
	max := opts.MaxMessageBytes
	if max <= 0 {
		max = DefaultMaxMessageBytes
	}
	fctx, cancel := context.WithCancel(ctx)
	f := &httpClientFramer{
		url:        u.String(),
		hc:         hc,
		header:     opts.Header.Clone(),
		max:        max,
		wantStream: opts.OnRequest != nil,
		ctx:        fctx,
		cancel:     cancel,
		in:         make(chan []byte, httpClientQueue),
		sem:        make(chan struct{}, maxHTTPClientExchanges),
	}
	return newClient(f, opts), nil
}

// httpClientFramer presents streamable HTTP as a Framer so Client runs over it
// unchanged. A request's WriteMessage must return at once: Client holds a mutex
// around it, and a POST that waits for its response would serialise every
// concurrent call. So a request is sent on its own goroutine, and whatever comes
// back (a JSON body, or the frames of an SSE stream) is queued for ReadMessage.
//
// A notification or a reply is the opposite: it is sent before WriteMessage
// returns. Separate HTTP requests have no order, and the one that matters is
// notifications/initialized, which must reach the server before the request the
// client sends next. The server acknowledges these with a bare 202, so the wait
// is short.
type httpClientFramer struct {
	url        string
	hc         *http.Client
	header     http.Header
	max        int
	wantStream bool

	ctx    context.Context
	cancel context.CancelFunc
	in     chan []byte
	sem    chan struct{}
	wg     sync.WaitGroup

	mu       sync.Mutex
	closed   bool   // Close has begun: no goroutine may be added to wg after this
	sid      string // Mcp-Session-Id, set by the initialize response
	version  string // negotiated protocol version, sent on every later request
	initKey  string // idKey of the initialize request
	termErr  error  // why the connection ended, if not by Close
	gotInit  bool
	closing  sync.Once
	finished bool // the session is gone; Close need not DELETE it
}

// ReadMessage implements Framer.
func (f *httpClientFramer) ReadMessage() ([]byte, error) {
	select {
	case b := <-f.in:
		return b, nil
	case <-f.ctx.Done():
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.termErr != nil {
			return nil, f.termErr
		}
		return nil, io.EOF
	}
}

// WriteMessage implements Framer. It validates and dispatches; the exchange
// happens on its own goroutine.
func (f *httpClientFramer) WriteMessage(p []byte) error {
	var m wireMessage
	if err := json.Unmarshal(p, &m); err != nil {
		return fmt.Errorf("mcp: not a JSON-RPC message: %w", err)
	}
	kind := m.kind()
	if kind == kindInvalid {
		return errors.New("mcp: not a JSON-RPC request, notification or response")
	}
	select {
	case f.sem <- struct{}{}:
	case <-f.ctx.Done():
		return net.ErrClosed
	}
	body := bytes.Clone(p)

	f.mu.Lock()
	if f.closed || f.ctx.Err() != nil { // no new work once Close has begun, so wg.Wait cannot race an Add
		f.mu.Unlock()
		<-f.sem
		return net.ErrClosed
	}
	if kind == kindRequest && m.Method == MethodInitialize {
		f.initKey = idKey(m.ID)
	}
	f.wg.Add(1)
	f.mu.Unlock()

	run := func() {
		defer f.wg.Done()
		defer func() { <-f.sem }()
		f.exchange(kind, m.ID, m.Method, body)
	}
	if kind == kindRequest {
		go run()
	} else {
		run()
	}
	return nil
}

// Close implements Framer: end the server's session (best effort, bounded), then
// every goroutine this framer started.
func (f *httpClientFramer) Close() error {
	f.closing.Do(func() {
		f.mu.Lock()
		sid, gone := f.sid, f.finished
		f.mu.Unlock()
		if sid != "" && !gone && f.ctx.Err() == nil {
			f.deleteSession()
		}
		f.mu.Lock()
		f.closed = true // every wg.Add has happened under mu before this
		f.mu.Unlock()
		f.cancel()
		f.wg.Wait()
	})
	return nil
}

func (f *httpClientFramer) deleteSession() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, f.url, nil)
	if err != nil {
		return
	}
	f.decorate(req, false)
	if resp, err := f.hc.Do(req); err == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxHTTPErrorBody))
		_ = resp.Body.Close()
	}
}

// decorate sets the headers common to every request. The caller's headers go on
// first so they cannot displace the protocol's.
func (f *httpClientFramer) decorate(req *http.Request, initialize bool) {
	for k, vs := range f.header {
		req.Header[k] = append([]string(nil), vs...)
	}
	f.mu.Lock()
	sid, version := f.sid, f.version
	f.mu.Unlock()
	if sid != "" {
		req.Header.Set(headerSessionID, sid)
	}
	if version != "" && !initialize {
		req.Header.Set(headerProtocolVersion, version)
	}
}

// terminate ends the connection with err, once.
func (f *httpClientFramer) terminate(err error) {
	f.mu.Lock()
	if f.termErr == nil {
		f.termErr = err
	}
	f.finished = true
	f.mu.Unlock()
	f.cancel()
}

func (f *httpClientFramer) push(frame []byte) {
	select {
	case f.in <- frame:
	case <-f.ctx.Done():
	}
}

// failRequest answers a request the transport could not deliver an answer to, so
// the call returns an error now rather than at its deadline. Notifications and
// responses have no caller waiting.
func (f *httpClientFramer) failRequest(kind msgKind, id json.RawMessage, err error) {
	if kind != kindRequest || f.ctx.Err() != nil {
		return
	}
	f.push(marshalError(id, Errorf(CodeInternalError, "mcp: http: %s", sanitize(err.Error()))))
}

func (f *httpClientFramer) exchange(kind msgKind, id json.RawMessage, method string, body []byte) {
	ctx := f.ctx
	if kind != kindRequest {
		// The caller is waiting on this, holding the client's write lock: bound
		// it, so a server that never answers cannot freeze every later write.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, notifyTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(body))
	if err != nil {
		f.failRequest(kind, id, err)
		return
	}
	init := kind == kindRequest && method == MethodInitialize
	f.decorate(req, init)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")

	resp, err := f.hc.Do(req)
	if err != nil {
		f.failRequest(kind, id, err)
		return
	}
	defer resp.Body.Close()

	if init && resp.StatusCode/100 == 2 {
		if sid := resp.Header.Get(headerSessionID); sid != "" {
			if !validSessionID(sid) {
				f.failRequest(kind, id, errors.New("server sent an invalid "+headerSessionID))
				return
			}
			f.mu.Lock()
			f.sid = sid
			f.mu.Unlock()
		}
	}
	switch {
	case resp.StatusCode == http.StatusNotFound && f.hasSession():
		f.terminate(ErrSessionExpired)
	case resp.StatusCode/100 != 2:
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxHTTPErrorBody))
		f.failRequest(kind, id, fmt.Errorf("server answered %d: %s", resp.StatusCode, snippet))
	case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNoContent:
		if kind == kindRequest {
			f.failRequest(kind, id, fmt.Errorf("server answered %d to a request", resp.StatusCode))
		}
	case isSSE(resp.Header.Get("Content-Type")):
		f.readStream(resp.Body, kind, id)
	case isJSONContentType(resp.Header.Get("Content-Type")):
		raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(f.max)+1))
		switch {
		case err != nil:
			f.failRequest(kind, id, err)
		case len(raw) > f.max:
			f.failRequest(kind, id, ErrMessageTooLarge)
		default:
			f.deliver(bytes.TrimSpace(raw))
		}
	default:
		f.failRequest(kind, id, fmt.Errorf("unexpected Content-Type %q", sanitize(resp.Header.Get("Content-Type"))))
	}
}

func (f *httpClientFramer) hasSession() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sid != ""
}

// readStream reads SSE events until the stream ends. For a request's own stream
// it stops after that request's response, and reports a stream that ends without
// one.
func (f *httpClientFramer) readStream(body io.Reader, kind msgKind, id json.RawMessage) {
	rd := newSSEReader(body, f.max)
	want := ""
	if kind == kindRequest {
		want = idKey(id)
	}
	var streamErr error
	for {
		event, data, err := rd.next()
		if err != nil {
			streamErr = err
			break
		}
		if event != "" && event != "message" {
			continue
		}
		answered := f.deliver(data)
		if want != "" && answered == want {
			return
		}
	}
	if want != "" && f.ctx.Err() == nil {
		if errors.Is(streamErr, io.EOF) || errors.Is(streamErr, io.ErrUnexpectedEOF) {
			streamErr = errors.New("stream ended before the response")
		}
		f.failRequest(kindRequest, id, streamErr)
	}
}

// deliver queues one frame for ReadMessage, noting the handshake as it passes.
// It returns the idKey of the frame if it is a response, else "".
func (f *httpClientFramer) deliver(frame []byte) string {
	var m wireMessage
	if json.Unmarshal(frame, &m) != nil {
		return "" // not JSON-RPC; the Client would drop it too
	}
	frame = bytes.Clone(frame)
	key := ""
	if m.kind() == kindResponse {
		key = idKey(m.ID)
		f.mu.Lock()
		isInit := key == f.initKey && !f.gotInit
		f.mu.Unlock()
		if isInit && m.Error == nil {
			var res struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			if json.Unmarshal(m.Result, &res) == nil {
				f.mu.Lock()
				f.version, f.gotInit = res.ProtocolVersion, true
				f.mu.Unlock()
				f.startStream()
			}
		}
	}
	f.push(frame)
	return key
}

// startStream opens the GET stream, once, if the client can answer what arrives
// on it.
func (f *httpClientFramer) startStream() {
	if !f.wantStream {
		return
	}
	f.mu.Lock()
	if f.closed || f.ctx.Err() != nil {
		f.mu.Unlock()
		return
	}
	f.wg.Add(1)
	f.mu.Unlock()
	go func() {
		defer f.wg.Done()
		f.getLoop()
	}()
}

// getLoop holds the server→client stream open, reopening it after a drop. A
// server that does not offer one (405), or that rejects this client, ends it.
func (f *httpClientFramer) getLoop() {
	backoff := time.Second
	for f.ctx.Err() == nil {
		req, err := http.NewRequestWithContext(f.ctx, http.MethodGet, f.url, nil)
		if err != nil {
			return
		}
		f.decorate(req, false)
		req.Header.Set("Accept", "text/event-stream")
		resp, err := f.hc.Do(req)
		if err == nil {
			code := resp.StatusCode
			switch {
			case code == http.StatusNotFound && f.hasSession():
				_ = resp.Body.Close()
				f.terminate(ErrSessionExpired)
				return
			case code == http.StatusOK && isSSE(resp.Header.Get("Content-Type")):
				backoff = time.Second
				f.readStream(resp.Body, kindNotification, nil)
			case code >= 400 && code < 500:
				_ = resp.Body.Close()
				return // 405: no stream offered; 401/403/406: retrying will not help
			}
			_ = resp.Body.Close()
		}
		select {
		case <-time.After(backoff):
		case <-f.ctx.Done():
			return
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func isSSE(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && mt == "text/event-stream"
}

// validSessionID reports whether v is a legal Mcp-Session-Id: visible ASCII only
// (transports/streamable HTTP), and bounded so a hostile server cannot make the
// client echo megabytes of header on every request.
func validSessionID(v string) bool {
	if len(v) == 0 || len(v) > 256 {
		return false
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 0x21 || v[i] > 0x7e {
			return false
		}
	}
	return true
}
