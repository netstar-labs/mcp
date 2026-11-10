package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// errHTTPBusy is returned by a write that cannot get one of the client's exchange
// slots: a request when all are in use, a notification that waited notifyTimeout.
var errHTTPBusy = errors.New("mcp: too many requests in flight to this server")

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
// ClientOptions.Header, or in the RoundTripper of ClientOptions.HTTPClient. The
// default client follows no redirects (a redirect is reported as an error).
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
		// context each call is given. No redirects: a 307 or 308 replays the POST,
		// its body, the session id and every credential header the caller set to
		// wherever the response points, and net/http withholds only Authorization
		// and Cookie across hosts. A caller who wants redirects supplies a client.
		hc = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
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
		calls:      make(map[string]context.CancelFunc),
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
	calls    map[string]context.CancelFunc // in-flight request exchanges, by idKey, so a cancelled call can abort its POST
	closed   bool                          // Close has begun: no goroutine may be added to wg after this
	sid      string                        // Mcp-Session-Id, set by the initialize response
	version  string                        // negotiated protocol version, sent on every later request
	initKey  string                        // idKey of the initialize request
	termErr  error                         // why the connection ended, if not by Close
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
	if kind == kindNotification && m.Method == NotifyCancelled {
		f.abortCall(m.Params) // frees the slot whether or not the server honours the cancel
	}
	if kind == kindRequest {
		// A request never waits for a slot: past the limit the caller is told now,
		// not left behind a stalled server it cannot see.
		select {
		case f.sem <- struct{}{}:
		default:
			return errHTTPBusy
		}
	} else {
		t := time.NewTimer(notifyTimeout)
		defer t.Stop()
		select {
		case f.sem <- struct{}{}:
		case <-t.C:
			return errHTTPBusy
		case <-f.ctx.Done():
			return net.ErrClosed
		}
	}
	body := bytes.Clone(p)

	f.mu.Lock()
	if f.closed || f.ctx.Err() != nil { // no new work once Close has begun, so wg.Wait cannot race an Add
		f.mu.Unlock()
		<-f.sem
		return net.ErrClosed
	}
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if kind == kindRequest {
		if m.Method == MethodInitialize {
			f.initKey = idKey(m.ID)
		}
		ctx, cancel = context.WithCancel(f.ctx)
		f.calls[idKey(m.ID)] = cancel
	} else {
		// The caller is waiting on this, holding the client's write lock: bound
		// it, so a server that never answers cannot freeze every later write.
		ctx, cancel = context.WithTimeout(f.ctx, notifyTimeout)
	}
	f.wg.Add(1)
	f.mu.Unlock()

	run := func() error {
		defer f.wg.Done()
		defer func() { <-f.sem }()
		defer cancel()
		if kind == kindRequest {
			defer func() {
				f.mu.Lock()
				delete(f.calls, idKey(m.ID))
				f.mu.Unlock()
			}()
		}
		return f.exchange(ctx, kind, m.ID, m.Method, body)
	}
	if kind == kindRequest {
		go func() { _ = run() }()
		return nil
	}
	return run()
}

// abortCall ends the POST of a request the caller has cancelled. The caller is no
// longer listening, and the exchange would otherwise hold its slot for as long as
// the server cares to keep the POST open.
func (f *httpClientFramer) abortCall(params json.RawMessage) {
	var p CancelledParams
	if json.Unmarshal(params, &p) != nil || len(p.RequestID) == 0 {
		return
	}
	f.mu.Lock()
	cancel := f.calls[idKey(p.RequestID)]
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
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

// failRequest reports a message the transport could not complete. For a request it
// answers with an error response, so the call returns now rather than at its
// deadline, and returns nil; a notification or reply has no caller waiting on a
// response, but its writer is waiting on the write, so the error is returned to it.
func (f *httpClientFramer) failRequest(kind msgKind, id json.RawMessage, err error) error {
	if kind != kindRequest {
		return err
	}
	if f.ctx.Err() == nil {
		f.push(marshalError(id, Errorf(CodeInternalError, "mcp: http: %s", sanitize(err.Error()))))
	}
	return nil
}

func (f *httpClientFramer) exchange(ctx context.Context, kind msgKind, id json.RawMessage, method string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(body))
	if err != nil {
		return f.failRequest(kind, id, err)
	}
	init := kind == kindRequest && method == MethodInitialize
	f.decorate(req, init)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")

	resp, err := f.hc.Do(req)
	if err != nil {
		return f.failRequest(kind, id, err)
	}
	defer resp.Body.Close()

	if init && resp.StatusCode/100 == 2 {
		if sid := resp.Header.Get(headerSessionID); sid != "" {
			if !validSessionID(sid) {
				return f.failRequest(kind, id, errors.New("server sent an invalid "+headerSessionID))
			}
			f.mu.Lock()
			f.sid = sid
			f.mu.Unlock()
		}
	}
	switch {
	case resp.StatusCode == http.StatusNotFound && f.hasSession():
		f.terminate(ErrSessionExpired)
		if kind != kindRequest {
			return ErrSessionExpired
		}
	case resp.StatusCode/100 != 2:
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxHTTPErrorBody))
		return f.failRequest(kind, id, fmt.Errorf("server answered %d: %s", resp.StatusCode, snippet))
	case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNoContent:
		if kind == kindRequest {
			return f.failRequest(kind, id, fmt.Errorf("server answered %d to a request", resp.StatusCode))
		}
	case isMediaType(resp.Header.Get("Content-Type"), "text/event-stream"):
		f.readStream(resp.Body, kind, id)
	case isMediaType(resp.Header.Get("Content-Type"), "application/json"):
		raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(f.max)+1))
		switch {
		case err != nil:
			return f.failRequest(kind, id, err)
		case len(raw) > f.max:
			return f.failRequest(kind, id, ErrMessageTooLarge)
		}
		// A JSON reply to a request must be that request's response. A null-id
		// error (JSON-RPC 5: what a server sends for a request it cannot attribute),
		// a batch or another id would otherwise leave the call waiting for a reply
		// that has already come.
		if got := f.deliver(bytes.TrimSpace(raw)); kind == kindRequest && got != idKey(id) {
			return f.failRequest(kind, id, errors.New("the server's reply is not a response to the request"))
		}
	default:
		return f.failRequest(kind, id, fmt.Errorf("unexpected Content-Type %q", sanitize(resp.Header.Get("Content-Type"))))
	}
	return nil
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
			case code == http.StatusOK && isMediaType(resp.Header.Get("Content-Type"), "text/event-stream"):
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
