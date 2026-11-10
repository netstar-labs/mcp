package mcp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Defaults for HTTPOptions.
const (
	DefaultHTTPSessionTTL       = 30 * time.Minute
	DefaultHTTPHandshakeTimeout = 30 * time.Second
	DefaultHTTPMaxSessions      = 1024
	DefaultHTTPKeepAlive        = 25 * time.Second
	DefaultHTTPWriteTimeout     = 30 * time.Second
)

const (
	// httpInboundQueue is how many POST bodies may wait for the session's read
	// loop. A full queue answers 503 rather than blocking the POST.
	httpInboundQueue = 64

	// httpStreamQueue is how many server→client frames may wait on one stream.
	httpStreamQueue = 64

	// httpBacklogMax is how many server→client frames are held while no stream
	// is open to carry them; past it the oldest is dropped, and logged.
	httpBacklogMax = 64
)

// Header names as net/http canonicalises them.
const (
	headerSessionID       = "Mcp-Session-Id"
	headerProtocolVersion = "Mcp-Protocol-Version"
)

// HTTPOptions configures the streamable-HTTP transport. Frame and message limits
// come from the Server's Options.
//
// The transport is wire, not policy: it does not authenticate. Wrap the handler
// in whatever does (a token check, mTLS, a gateway), and use Identify to tell the
// transport who that wrapper decided the caller is.
type HTTPOptions struct {
	// AllowedOrigins is the set of browser origins (for example
	// "https://app.example.com") whose requests are accepted. A request with no
	// Origin header, which is every non-browser client, is always accepted. A
	// request with an Origin not listed is refused, so with the default of none
	// no browser page can reach the server: this is the DNS-rebinding guard the
	// MCP specification requires.
	AllowedOrigins []string

	// AllowedHosts, when non-empty, is the set of Host header values accepted
	// ("localhost:8080"). Set it when binding to loopback: a rebinding attack
	// reaches the server under the attacker's own hostname, and a same-origin
	// GET carries no Origin header to refuse it by.
	AllowedHosts []string

	// Identify, if set, is called on every request and returns the caller's
	// identity as decided by the authentication wrapper in front of the handler
	// (a token's subject, a certificate's common name). The identity at session
	// creation is stored, exposed to handlers as Session.Identity, and required
	// to match on every later request, so one authenticated caller cannot use a
	// session id that belongs to another. A mismatch looks like an unknown
	// session. An empty return is a valid identity ("anonymous").
	Identify func(*http.Request) string

	// SessionTTL is how long a session may sit with no request in flight and no
	// stream open before it is dropped. Default DefaultHTTPSessionTTL.
	SessionTTL time.Duration

	// HandshakeTimeout is how long a session may go without completing the
	// handshake (initialize, then notifications/initialized) before it is
	// dropped, so unauthenticated initialize requests cannot hold session slots
	// for SessionTTL. Default DefaultHTTPHandshakeTimeout.
	HandshakeTimeout time.Duration

	// MaxSessions bounds live sessions; initialize past it is answered 503.
	// Default DefaultHTTPMaxSessions.
	MaxSessions int

	// KeepAlive is the interval of SSE comments on an open stream, which keep
	// proxies from closing it and detect a client that has gone. Negative
	// disables. Default DefaultHTTPKeepAlive.
	KeepAlive time.Duration

	// WriteTimeout bounds one write to a stream. Default DefaultHTTPWriteTimeout.
	WriteTimeout time.Duration
}

// HTTPHandler returns an http.Handler that serves this Server over MCP's
// streamable-HTTP transport (as defined from revision 2025-03-26; the Server's
// usual version negotiation applies, so a 2024-11-05 client is served too): POST carries
// client→server messages, and responses come back as one JSON body or, when the
// server has something else to say first (an elicitation, a progress
// notification), as an SSE stream; GET opens a stream for server-initiated
// messages; DELETE ends the session. It shares the Server's tools, resources,
// prompts and limits with every other transport.
//
// Mount it on one path. It is unauthenticated: see HTTPOptions. It does not set
// connection timeouts, which belong to the http.Server that serves it; set at
// least ReadHeaderTimeout and ReadTimeout there, and not WriteTimeout, which
// would end every stream.
//
// What it does not do: resume a dropped stream (Last-Event-ID is ignored and a
// frame in flight when a stream drops is lost), batch (removed from the
// protocol in 2025-06-18), speak the deprecated 2024-11-05 HTTP+SSE transport,
// or share sessions between processes (a session lives in the memory of the
// process that created it, so a load balancer must pin a session to one node).
func (s *Server) HTTPHandler(opts HTTPOptions) http.Handler {
	if opts.SessionTTL <= 0 {
		opts.SessionTTL = DefaultHTTPSessionTTL
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = DefaultHTTPHandshakeTimeout
	}
	if opts.MaxSessions <= 0 {
		opts.MaxSessions = DefaultHTTPMaxSessions
	}
	if opts.KeepAlive == 0 {
		opts.KeepAlive = DefaultHTTPKeepAlive
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = DefaultHTTPWriteTimeout
	}
	opts.AllowedOrigins = slices.Clone(opts.AllowedOrigins)
	opts.AllowedHosts = slices.Clone(opts.AllowedHosts)
	return &httpHandler{srv: s, opts: opts, sessions: make(map[string]*httpSession)}
}

type httpHandler struct {
	srv  *Server
	opts HTTPOptions

	mu       sync.Mutex
	sessions map[string]*httpSession
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if !h.hostAllowed(r) {
		httpFail(w, http.StatusForbidden, CodeInvalidRequest, "host not allowed")
		return
	}
	if !h.originAllowed(r) {
		httpFail(w, http.StatusForbidden, CodeInvalidRequest, "origin not allowed")
		return
	}
	switch r.Method {
	case http.MethodPost:
		h.post(w, r)
	case http.MethodGet:
		h.get(w, r)
	case http.MethodDelete:
		h.delete(w, r)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		httpFail(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
	}
}

func (h *httpHandler) hostAllowed(r *http.Request) bool {
	if len(h.opts.AllowedHosts) == 0 {
		return true
	}
	return slices.ContainsFunc(h.opts.AllowedHosts, func(a string) bool { return strings.EqualFold(a, r.Host) })
}

func (h *httpHandler) originAllowed(r *http.Request) bool {
	o := r.Header.Get("Origin")
	if o == "" {
		return true
	}
	return slices.ContainsFunc(h.opts.AllowedOrigins, func(a string) bool { return strings.EqualFold(a, o) })
}

func (h *httpHandler) identity(r *http.Request) string {
	if h.opts.Identify == nil {
		return ""
	}
	return h.opts.Identify(r)
}

// httpFail answers with a JSON-RPC error body and an HTTP status.
func httpFail(w http.ResponseWriter, status, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(marshalError(nullLiteral, Errorf(code, "%s", msg)))
}

// ---- POST ----

func (h *httpHandler) post(w http.ResponseWriter, r *http.Request) {
	if !isMediaType(r.Header.Get("Content-Type"), "application/json") {
		httpFail(w, http.StatusUnsupportedMediaType, CodeInvalidRequest, "Content-Type must be application/json")
		return
	}
	// The client must be able to take either reply form (transports/streamable
	// HTTP): the server picks per request, after the handler has run.
	if accept := r.Header.Get("Accept"); !accepts(accept, "application/json") || !accepts(accept, "text/event-stream") {
		httpFail(w, http.StatusNotAcceptable, CodeInvalidRequest, "Accept must list application/json and text/event-stream")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(h.srv.opts.MaxMessageBytes)))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			httpFail(w, http.StatusRequestEntityTooLarge, CodeRequestTooLarge, "message exceeds the maximum size")
			return
		}
		httpFail(w, http.StatusBadRequest, CodeParseError, "cannot read request body")
		return
	}
	body = bytes.TrimSpace(body)
	if len(body) > 0 && body[0] == '[' {
		httpFail(w, http.StatusBadRequest, CodeInvalidRequest, "JSON-RPC batching is not supported over HTTP")
		return
	}
	var m wireMessage
	if err := json.Unmarshal(body, &m); err != nil {
		httpFail(w, http.StatusBadRequest, CodeParseError, "parse error")
		return
	}
	if rerr := m.validate(); rerr != nil {
		httpFail(w, http.StatusBadRequest, rerr.Code, rerr.Message)
		return
	}
	kind := m.kind()
	if m.JSONRPC != JSONRPCVersion || kind == kindInvalid {
		httpFail(w, http.StatusBadRequest, CodeInvalidRequest, "invalid request")
		return
	}

	var (
		s        *httpSession
		creating bool
	)
	if sid := r.Header.Get(headerSessionID); sid == "" {
		if kind != kindRequest || m.Method != MethodInitialize {
			httpFail(w, http.StatusBadRequest, CodeInvalidRequest, "missing "+headerSessionID+" header")
			return
		}
		var status int
		if s, status = h.create(r); s == nil {
			httpFail(w, status, CodeResourceLimit, "cannot create a session")
			return
		}
		creating = true
	} else if s = h.lookup(sid, r); s == nil {
		httpFail(w, http.StatusNotFound, CodeInvalidRequest, "unknown session")
		return
	} else if !h.versionOK(w, r, s) {
		return
	}
	// A session that fails before its initialize is queued is torn down, not
	// left to hold a slot until the handshake timeout.
	committed := false
	defer func() {
		if creating && !committed {
			s.terminate()
		}
	}()
	if !s.enter() {
		httpFail(w, http.StatusNotFound, CodeInvalidRequest, "unknown session")
		return
	}
	defer s.leave()

	switch kind {
	case kindNotification, kindResponse:
		if kind == kindNotification && m.Method == NotifyCancelled {
			s.cancelWaiter(m.Params)
		}
		if !h.enqueue(w, s, body) {
			return
		}
		w.WriteHeader(http.StatusAccepted)
	case kindRequest:
		wt, ok := s.register(m.ID)
		if !ok {
			httpFail(w, http.StatusBadRequest, CodeInvalidRequest, "a request with that id is already in flight")
			return
		}
		if !h.enqueue(w, s, body) {
			s.unregister(wt) // never reached the Session
			return
		}
		committed = true
		h.await(w, r, s, wt, creating)
		s.abandon(wt)
	}
}

// enqueue hands a body to the session's read loop, or answers the POST.
func (h *httpHandler) enqueue(w http.ResponseWriter, s *httpSession, body []byte) bool {
	// Closed is checked on its own first: with both cases ready a select picks at
	// random, and a notification to a dead session would sometimes be accepted.
	select {
	case <-s.closed:
		httpFail(w, http.StatusNotFound, CodeInvalidRequest, "unknown session")
		return false
	default:
	}
	select {
	case s.in <- body:
		return true
	default:
		w.Header().Set("Retry-After", "1")
		httpFail(w, http.StatusServiceUnavailable, CodeInternalError, "session is overloaded; retry later")
		return false
	}
}

// await answers a request POST: the response as a JSON body when it is the first
// thing the session has to say, otherwise as an SSE stream that carries whatever
// the server sent first and then the response. The choice is made lazily because
// a handler may elicit or report progress long after the request arrived.
func (h *httpHandler) await(w http.ResponseWriter, r *http.Request, s *httpSession, wt *httpWaiter, creating bool) {
	sw := newSSEWriter(w, h.opts.WriteTimeout)
	defer sw.finish()

	// The id is minted at initialize and echoed by the client from then on. It
	// goes out only with a reply that can carry it: an error status, or a failed
	// handshake, must not hand the client an id for a session that is gone.
	setID := func() {
		if creating {
			w.Header().Set(headerSessionID, s.id)
		}
	}
	var tick <-chan time.Time
	if h.opts.KeepAlive > 0 {
		t := time.NewTicker(h.opts.KeepAlive)
		defer t.Stop()
		tick = t.C
	}
	stream := func(frame []byte) bool {
		if !sw.started {
			setID()
			if err := sw.start(); err != nil {
				return false
			}
		}
		return sw.event(frame) == nil
	}

	for {
		select {
		case resp := <-wt.resp:
			// Everything the writer queued before the response is already in
			// wt.out (one writer, in order); deliver it first.
			for drained := false; !drained; {
				select {
				case f := <-wt.out:
					if !stream(f) {
						return
					}
				default:
					drained = true
				}
			}
			failed := creating && handshakeFailed(resp)
			if failed {
				s.terminate()
			}
			if sw.started {
				_ = sw.event(resp)
				return
			}
			sw.deadline()
			if !failed {
				setID()
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(resp)
			return
		case f := <-wt.out:
			if !stream(f) {
				return
			}
		case <-wt.cancelled:
			// The reply to a cancelled request is suppressed; end the POST. A
			// client that cancelled has stopped listening for it.
			if !sw.started {
				httpFail(w, http.StatusOK, CodeRequestCancelled, "request cancelled")
			}
			return
		case <-s.closed:
			if !sw.started {
				httpFail(w, endedStatus(creating), CodeInternalError, "session closed")
			}
			return
		case <-r.Context().Done():
			// A dropped connection is not a cancellation (the client sends
			// notifications/cancelled for that); the request keeps running and
			// its response is discarded when it arrives.
			return
		case <-tick:
			if sw.started && sw.comment() != nil {
				return
			}
		}
	}
}

// endedStatus is the HTTP status for a session that ended under an in-flight
// POST: 503 when it never finished starting (a shut-down server), 404 once the
// client had an id, which tells it to start a new session.
func endedStatus(creating bool) int {
	if creating {
		return http.StatusServiceUnavailable
	}
	return http.StatusNotFound
}

// handshakeFailed reports whether an initialize response is an error.
func handshakeFailed(resp []byte) bool {
	var m wireMessage
	return json.Unmarshal(resp, &m) != nil || m.Error != nil
}

// ---- GET ----

func (h *httpHandler) get(w http.ResponseWriter, r *http.Request) {
	if !accepts(r.Header.Get("Accept"), "text/event-stream") {
		httpFail(w, http.StatusNotAcceptable, CodeInvalidRequest, "Accept must list text/event-stream")
		return
	}
	sid := r.Header.Get(headerSessionID)
	if sid == "" {
		httpFail(w, http.StatusBadRequest, CodeInvalidRequest, "missing "+headerSessionID+" header")
		return
	}
	s := h.lookup(sid, r)
	if s == nil {
		httpFail(w, http.StatusNotFound, CodeInvalidRequest, "unknown session")
		return
	}
	if !h.versionOK(w, r, s) {
		return
	}
	if !s.enter() {
		httpFail(w, http.StatusNotFound, CodeInvalidRequest, "unknown session")
		return
	}
	defer s.leave()

	st := s.attach()
	defer s.detach(st)

	sw := newSSEWriter(w, h.opts.WriteTimeout)
	defer sw.finish()
	if err := sw.start(); err != nil {
		h.srv.opts.Logger.Printf("%s: stream: %v", s.remote, err)
		return
	}
	var tick <-chan time.Time
	if h.opts.KeepAlive > 0 {
		t := time.NewTicker(h.opts.KeepAlive)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case f := <-st.out:
			if sw.event(f) != nil {
				return
			}
		case <-st.kicked:
			return // a newer GET took over
		case <-s.closed:
			return
		case <-r.Context().Done():
			return
		case <-tick:
			if sw.comment() != nil {
				return
			}
		}
	}
}

// ---- DELETE ----

func (h *httpHandler) delete(w http.ResponseWriter, r *http.Request) {
	sid := r.Header.Get(headerSessionID)
	if sid == "" {
		httpFail(w, http.StatusBadRequest, CodeInvalidRequest, "missing "+headerSessionID+" header")
		return
	}
	s := h.lookup(sid, r)
	if s == nil {
		httpFail(w, http.StatusNotFound, CodeInvalidRequest, "unknown session")
		return
	}
	s.terminate()
	w.WriteHeader(http.StatusNoContent)
}

// versionOK enforces MCP-Protocol-Version once a session exists. A client that
// sends none is assumed to speak what it negotiated.
func (h *httpHandler) versionOK(w http.ResponseWriter, r *http.Request, s *httpSession) bool {
	v := r.Header.Get(headerProtocolVersion)
	if v == "" {
		return true
	}
	if !slices.Contains(h.srv.opts.ProtocolVersions, v) {
		httpFail(w, http.StatusBadRequest, CodeInvalidRequest, "unsupported protocol version")
		return false
	}
	if cur := s.protocolVersion(); cur != "" && cur != v {
		httpFail(w, http.StatusBadRequest, CodeInvalidRequest, "protocol version does not match the negotiated version")
		return false
	}
	return true
}

// ---- session registry ----

// create starts a session for an initialize request. The second result is the
// HTTP status to answer with when it returns nil.
func (h *httpHandler) create(r *http.Request) (*httpSession, int) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, http.StatusInternalServerError
	}
	s := &httpSession{
		h:       h,
		id:      hex.EncodeToString(raw[:]),
		owner:   h.identity(r),
		remote:  "http:" + sanitize(r.RemoteAddr),
		in:      make(chan []byte, httpInboundQueue),
		closed:  make(chan struct{}),
		waiters: make(map[string]*httpWaiter),
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.idle = time.AfterFunc(h.opts.HandshakeTimeout, s.expire)
	s.hs = time.AfterFunc(h.opts.HandshakeTimeout, s.handshakeExpired)

	h.mu.Lock()
	if len(h.sessions) >= h.opts.MaxSessions {
		h.mu.Unlock()
		s.idle.Stop()
		s.hs.Stop()
		cancel()
		return nil, http.StatusServiceUnavailable
	}
	h.sessions[s.id] = s
	h.mu.Unlock()

	go func() {
		err := h.srv.ServeConn(ctx, s, ConnConfig{
			Remote:    s.remote,
			Identity:  s.owner,
			OnSession: func(sess *Session) { s.setSession(sess) },
		})
		if err != nil && !isBenignClose(err) {
			h.srv.opts.Logger.Printf("%s: session ended: %v", s.remote, err)
		}
		s.Close() // the session ended on its own; release its slot
	}()
	return s, 0
}

// lookup finds a session by id for the identity making this request. An id that
// exists but belongs to another identity is indistinguishable from one that does
// not.
func (h *httpHandler) lookup(id string, r *http.Request) *httpSession {
	h.mu.Lock()
	s := h.sessions[id]
	h.mu.Unlock()
	if s == nil || s.owner != h.identity(r) {
		return nil
	}
	return s
}

// ---- one session ----

// httpSession is the Framer the Session reads and writes, bridging a
// request-scoped transport onto a connection-shaped one. Inbound, each POST body
// is queued for ReadMessage. Outbound, the Session's single writer calls
// WriteMessage with each frame, and the frame is routed: a response to the POST
// that is waiting for it, anything else to the GET stream if one is open, else to
// the oldest POST still waiting, else held (bounded) for the next GET.
//
// WriteMessage must never block, because it runs on the Session's one writer
// goroutine and every handler's reply queues behind it. Every route is a
// non-blocking send into a buffered channel that the HTTP handler goroutine
// drains, so a client that stops reading stalls its own stream and nothing else.
type httpSession struct {
	h      *httpHandler
	id     string
	owner  string
	remote string

	in     chan []byte
	closed chan struct{}
	cancel context.CancelFunc // ends the ServeConn context, and with it running handlers
	once   sync.Once

	mu      sync.Mutex
	sess    *Session
	waiters map[string]*httpWaiter
	seq     uint64
	get     *httpStream
	backlog [][]byte
	active  int // requests in flight plus open streams
	idle    *time.Timer
	hs      *time.Timer // the handshake deadline: unlike idle, an open stream does not hold it off
	lastUse time.Time   // when active last fell to zero
}

// httpWaiter is one POST request waiting for its response.
type httpWaiter struct {
	key       string
	seq       uint64
	resp      chan []byte   // the response; capacity 1, written once
	out       chan []byte   // server→client frames routed to this POST's stream
	cancelled chan struct{} // closed when the client cancels the request
	cancelOne sync.Once
	abandoned bool // the POST has gone; the id stays reserved until the Session answers it
}

// httpStream is the session's open GET stream.
type httpStream struct {
	out    chan []byte
	kicked chan struct{}
	once   sync.Once
}

func (s *httpSession) setSession(sess *Session) {
	s.mu.Lock()
	s.sess = sess
	s.mu.Unlock()
}

func (s *httpSession) protocolVersion() string {
	s.mu.Lock()
	sess := s.sess
	s.mu.Unlock()
	if sess == nil {
		return ""
	}
	return sess.ProtocolVersion()
}

// ReadMessage implements Framer: the next queued POST body.
func (s *httpSession) ReadMessage() ([]byte, error) {
	select {
	case b := <-s.in:
		return b, nil
	case <-s.closed:
		return nil, io.EOF
	}
}

// WriteMessage implements Framer. See the type comment: it never blocks.
func (s *httpSession) WriteMessage(p []byte) error {
	select {
	case <-s.closed:
		return net.ErrClosed
	default:
	}
	var m wireMessage
	if json.Unmarshal(p, &m) != nil {
		s.h.srv.opts.Logger.Printf("%s: dropping an outbound frame that is not JSON-RPC", s.remote)
		return nil
	}
	frame := bytes.Clone(p)
	switch m.kind() {
	case kindRequest, kindNotification:
		s.route(frame)
		return nil
	case kindResponse:
		s.mu.Lock()
		wt := s.waiters[idKey(m.ID)]
		if wt != nil {
			delete(s.waiters, wt.key)
		}
		s.mu.Unlock()
		if wt == nil {
			return nil // the POST that asked has gone; nothing to deliver it to
		}
		wt.resp <- frame // capacity 1, and the waiter is out of the map: one send, never blocks
	default:
		// A reply with a null id (the Session's answer to a frame it could not
		// parse) names no POST. The transport parses every body first, so this
		// is unreachable in practice; say so if it is not.
		s.h.srv.opts.Logger.Printf("%s: dropping an outbound frame with no id and no method", s.remote)
	}
	return nil
}

// route sends a server-initiated frame: to the GET stream, else the oldest
// waiting POST, else the backlog.
func (s *httpSession) route(frame []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.get; st != nil {
		select {
		case st.out <- frame:
			return
		default:
			// The stream's reader is not keeping up. End it rather than let it
			// stall delivery; the client reconnects and the frame waits in the
			// backlog for it.
			s.h.srv.opts.Logger.Printf("%s: GET stream is not draining; closing it", s.remote)
			st.kick()
			s.get = nil
		}
	}
	var oldest *httpWaiter
	for _, wt := range s.waiters {
		if wt.abandoned {
			continue // its POST is gone; a frame sent there is lost
		}
		if oldest == nil || wt.seq < oldest.seq {
			oldest = wt
		}
	}
	if oldest != nil {
		select {
		case oldest.out <- frame:
			return
		default:
		}
	}
	if len(s.backlog) >= httpBacklogMax {
		s.h.srv.opts.Logger.Printf("%s: no stream is open and %d frames are waiting; dropping the oldest", s.remote, httpBacklogMax)
		s.backlog = s.backlog[1:]
	}
	s.backlog = append(s.backlog, frame)
}

// Close implements Framer. It ends the session: idempotent, and the one place a
// session is released, whoever asks (DELETE, the idle timer, Server.Close, or
// the Session ending on its own).
func (s *httpSession) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.cancel()
		s.mu.Lock()
		if s.idle != nil {
			s.idle.Stop()
		}
		if s.hs != nil {
			s.hs.Stop()
		}
		if s.get != nil {
			s.get.kick()
		}
		s.mu.Unlock()
		s.h.mu.Lock()
		delete(s.h.sessions, s.id)
		s.h.mu.Unlock()
	})
	return nil
}

func (s *httpSession) terminate() { _ = s.Close() }

// enter marks a request or stream as active, which holds off the idle timer.
func (s *httpSession) enter() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
		return false
	default:
	}
	s.active++
	if s.idle != nil {
		s.idle.Stop()
	}
	return true
}

// leave ends one activity and, when none remain, arms the idle timer. Before the
// handshake completes the timer is the short one, so a flood of initialize
// requests cannot hold session slots for the full TTL.
func (s *httpSession) leave() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.active > 0 {
		return
	}
	select {
	case <-s.closed:
		return
	default:
	}
	s.lastUse = time.Now()
	s.idle.Reset(s.idleFor())
}

// idleFor is how long the session may now sit idle. Callers hold s.mu.
func (s *httpSession) idleFor() time.Duration {
	if s.sess != nil && s.sess.State() == StateReady {
		return s.h.opts.SessionTTL
	}
	return s.h.opts.HandshakeTimeout
}

// expire is the idle timer's callback. The timer was armed when the last activity
// ended, with the handshake timeout if the Session had not yet processed
// notifications/initialized, which can lag the POST that carried it. So it judges
// the session by what it is now: if it has not been idle for as long as it is now
// allowed, it is given the rest of that time.
func (s *httpSession) expire() {
	s.mu.Lock()
	if s.active > 0 {
		s.mu.Unlock()
		return
	}
	if idle, limit := time.Since(s.lastUse), s.idleFor(); !s.lastUse.IsZero() && idle < limit {
		s.idle.Reset(limit - idle)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.terminate()
}

// handshakeExpired drops a session that has not finished the handshake. It is
// separate from the idle timer because an open GET stream holds that one off, and a
// stream opened right after initialize must not let a session that never sends
// notifications/initialized keep its slot.
func (s *httpSession) handshakeExpired() {
	s.mu.Lock()
	ready := s.sess != nil && s.sess.State() == StateReady
	s.mu.Unlock()
	if !ready {
		s.terminate()
	}
}

// register adds a waiter for a request id; false when that id is already in
// flight on this session, which the Session's own in-flight table could not
// tell apart.
func (s *httpSession) register(id json.RawMessage) (*httpWaiter, bool) {
	key := idKey(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.waiters[key]; dup {
		return nil, false
	}
	s.seq++
	wt := &httpWaiter{
		key:       key,
		seq:       s.seq,
		resp:      make(chan []byte, 1),
		out:       make(chan []byte, httpStreamQueue),
		cancelled: make(chan struct{}),
	}
	s.waiters[key] = wt
	return wt, true
}

// unregister removes a waiter whose request never reached the Session.
func (s *httpSession) unregister(wt *httpWaiter) {
	s.mu.Lock()
	if s.waiters[wt.key] == wt {
		delete(s.waiters, wt.key)
	}
	s.mu.Unlock()
}

// abandon is called when a POST ends. If the Session has already answered, the
// waiter is gone from the table and there is nothing to do. If not (the client
// dropped the connection, and the request keeps running) the id stays reserved: a
// retry that reused it would otherwise be handed the first call's reply, and the
// Session's own in-flight table, which holds one entry per id, would be overwritten.
// The entry is removed when the Session answers, or when the client cancels.
func (s *httpSession) abandon(wt *httpWaiter) {
	s.mu.Lock()
	if s.waiters[wt.key] == wt {
		wt.abandoned = true
	}
	s.mu.Unlock()
}

// cancelWaiter ends the POST of a request the client has just cancelled. The
// Session suppresses the reply to a cancelled request, so without this the POST
// would wait for it until the connection dropped.
func (s *httpSession) cancelWaiter(params json.RawMessage) {
	var p CancelledParams
	if json.Unmarshal(params, &p) != nil || len(p.RequestID) == 0 {
		return
	}
	key := idKey(p.RequestID)
	s.mu.Lock()
	wt := s.waiters[key]
	if wt != nil {
		delete(s.waiters, key) // the Session suppresses a cancelled request's reply, so nothing else would
	}
	s.mu.Unlock()
	if wt != nil {
		wt.cancelOne.Do(func() { close(wt.cancelled) })
	}
}

// attach makes a new GET stream the session's stream, replacing any earlier one
// (a client reconnecting before the server noticed the old connection die), and
// hands it what was held while no stream was open.
func (s *httpSession) attach() *httpStream {
	st := &httpStream{out: make(chan []byte, httpStreamQueue), kicked: make(chan struct{})}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.get != nil {
		s.get.kick()
	}
	s.get = st
	n := 0
	for _, f := range s.backlog {
		select {
		case st.out <- f:
			n++
		default:
		}
	}
	s.backlog = slices.Delete(s.backlog, 0, n)
	return st
}

func (s *httpSession) detach(st *httpStream) {
	s.mu.Lock()
	if s.get == st {
		s.get = nil
	}
	s.mu.Unlock()
}

func (st *httpStream) kick() { st.once.Do(func() { close(st.kicked) }) }

// ---- header parsing ----

// isMediaType reports whether a Content-Type header names mediaType, ignoring
// parameters such as charset.
func isMediaType(header, mediaType string) bool {
	mt, _, err := mime.ParseMediaType(header)
	return err == nil && mt == mediaType
}

// accepts reports whether an Accept header admits a media type. An absent header
// admits everything (RFC 9110), and a range with q=0 admits nothing.
func accepts(header, mediaType string) bool {
	if strings.TrimSpace(header) == "" {
		return true
	}
	major, _, _ := strings.Cut(mediaType, "/")
	for _, part := range strings.Split(header, ",") {
		rng, params, _ := strings.Cut(part, ";")
		rng = strings.ToLower(strings.TrimSpace(rng))
		if rng != mediaType && rng != major+"/*" && rng != "*/*" {
			continue
		}
		if q, ok := qValue(params); ok && q <= 0 {
			continue
		}
		return true
	}
	return false
}

// qValue extracts q from the parameters of one media range.
func qValue(params string) (float64, bool) {
	for _, p := range strings.Split(params, ";") {
		k, v, ok := strings.Cut(p, "=")
		if ok && strings.EqualFold(strings.TrimSpace(k), "q") {
			q, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			return q, err == nil
		}
	}
	return 0, false
}
