package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// maxSubscriptionURIBytes bounds a URI retained in the per-session
// subscription set.
const maxSubscriptionURIBytes = 2048

// Session lifecycle states.
const (
	StateNew int32 = iota
	StateInitializing
	StateReady
	StateClosed
)

// State names, for diagnostics.
func stateName(v int32) string {
	switch v {
	case StateNew:
		return "new"
	case StateInitializing:
		return "initializing"
	case StateReady:
		return "ready"
	default:
		return "closed"
	}
}

// PeerCred are the credentials of the process on the other end of a Unix
// socket, where the kernel can supply them.
type PeerCred struct {
	PID int32
	UID uint32
	GID uint32
}

func (p *PeerCred) String() string {
	if p == nil {
		return "unknown"
	}
	return fmt.Sprintf("pid=%d uid=%d gid=%d", p.PID, p.UID, p.GID)
}

type inflightEntry struct {
	cancel    context.CancelFunc
	cancelled atomic.Bool
}

// Session is one client connection. Methods are safe for concurrent use.
type Session struct {
	server *Server
	framer Framer
	remote string
	peer   *PeerCred
	ident  string

	state atomic.Int32

	out        chan []byte
	done       chan struct{}
	quiesce    chan struct{}
	writerDone chan struct{}
	work       sync.WaitGroup
	closeOnce  sync.Once
	quiesceOne sync.Once
	closeErr   error

	sem      chan struct{}
	queued   atomic.Int32
	maxQueue int32

	mu         sync.Mutex
	clientInfo Implementation
	clientCaps ClientCapabilities
	version    string
	subs       map[string]struct{}
	inflight   map[string]*inflightEntry

	pendMu  sync.Mutex
	pending map[string]chan *wireMessage
	nextID  atomic.Int64

	logLevel atomic.Value // string
}

// State reports the session lifecycle state.
func (s *Session) State() int32 { return s.state.Load() }

// RemoteAddr is a printable identity for the peer.
func (s *Session) RemoteAddr() string { return s.remote }

// Peer returns the peer's Unix credentials, or nil when unavailable.
func (s *Session) Peer() *PeerCred { return s.peer }

// Identity is the caller identity the transport's wrapper vouched for (see
// HTTPOptions.Identify), or "" when none did. It is whatever the wrapper
// returned, verbatim: this package does not authenticate, it only carries the
// answer to the handlers that have to authorise by it.
func (s *Session) Identity() string { return s.ident }

// ClientInfo returns the implementation the client declared at initialize.
func (s *Session) ClientInfo() Implementation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientInfo
}

// ClientCapabilities returns what the client advertised.
func (s *Session) ClientCapabilities() ClientCapabilities {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientCaps
}

// ProtocolVersion is the negotiated revision.
func (s *Session) ProtocolVersion() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
}

// Server returns the owning Server.
func (s *Session) Server() *Server { return s.server }

func newSession(srv *Server, f Framer, remote string, peer *PeerCred) *Session {
	s := &Session{
		server:     srv,
		framer:     f,
		remote:     remote,
		peer:       peer,
		out:        make(chan []byte, 64),
		done:       make(chan struct{}),
		quiesce:    make(chan struct{}),
		writerDone: make(chan struct{}),
		sem:        make(chan struct{}, srv.opts.MaxConcurrentRequests),
		maxQueue:   int32(srv.opts.MaxConcurrentRequests) * 8,
		subs:       make(map[string]struct{}),
		inflight:   make(map[string]*inflightEntry),
		pending:    make(map[string]chan *wireMessage),
	}
	s.logLevel.Store(LogInfo)
	return s
}

// ConnConfig describes one accepted connection.
type ConnConfig struct {
	// Remote is a printable peer identity used in diagnostics.
	Remote string

	// Peer carries kernel-attested credentials, when available.
	Peer *PeerCred

	// Identity is a caller identity vouched for by something in front of this
	// package, exposed to handlers as Session.Identity.
	Identity string

	// OnSession, if set, runs once with the new Session before the read loop
	// starts. It is the hook transports use to arm a handshake timer.
	OnSession func(*Session)
}

// ServeConn runs the protocol over an established framer until the peer
// disconnects, the context is cancelled, or a fatal framing error occurs.
func (s *Server) ServeConn(ctx context.Context, f Framer, cfg ConnConfig) error {
	sess := newSession(s, f, cfg.Remote, cfg.Peer)
	sess.ident = cfg.Identity
	if cfg.OnSession != nil {
		cfg.OnSession(sess)
	}
	if err := s.addSession(sess); err != nil {
		_ = f.Close()
		return err
	}
	defer s.removeSession(sess)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); sess.writeLoop() }()

	// Propagate outer cancellation into the blocking read by closing the
	// underlying transport; there is no other way to interrupt a read.
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			_ = sess.Close()
		case <-stop:
		}
	}()

	err := sess.readLoop(ctx)

	// The peer has stopped sending, but handlers may still be running and
	// their replies may still be queued. Let both finish before the transport
	// is torn down, otherwise a client that half-closes (or a pipe that hits
	// EOF straight after the last request) silently loses its answers.
	sess.drain(sess.server.opts.DrainTimeout)

	close(stop)
	_ = sess.Close()
	wg.Wait()

	sess.state.Store(StateClosed)
	if s.opts.OnSessionEnd != nil {
		s.opts.OnSessionEnd(sess, err)
	}
	return err
}

func (s *Session) readLoop(ctx context.Context) error {
	for {
		frame, err := s.framer.ReadMessage()
		if err != nil {
			if isBenignClose(err) {
				return nil
			}
			if errors.Is(err, ErrMessageTooLarge) {
				// The stream cannot be resynchronised, so answer once with a
				// null-id error as a courtesy and then drop the connection.
				s.sendRaw(marshalError(nullLiteral,
					Errorf(CodeRequestTooLarge, "message exceeds %d bytes", s.server.opts.MaxMessageBytes)))
			}
			return err
		}
		s.handleFrame(ctx, frame)
	}
}

func (s *Session) handleFrame(ctx context.Context, frame []byte) {
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 {
		return
	}
	if trimmed[0] == '[' {
		s.handleBatch(ctx, trimmed)
		return
	}

	var wm wireMessage
	if err := json.Unmarshal(trimmed, &wm); err != nil {
		s.sendError(nullLiteral, Errorf(CodeParseError, "parse error: %v", err))
		return
	}
	if rerr := wm.validate(); rerr != nil {
		s.sendError(nullLiteral, rerr)
		return
	}
	s.route(ctx, &wm)
}

// handleBatch runs the members of a JSON-RPC batch sequentially on the read loop.
// Caution: because members run inline, a member handler that makes a server→client
// call (sampling/roots/elicitation) blocks the read loop until it completes — the
// same loop that must read the client's response — so a batched callback tool can
// wedge the connection. Batching is opt-in (Options.AcceptBatch) and exists only
// in the legacy 2025-03-26 revision; concurrent requests use the spawn path.
func (s *Session) handleBatch(ctx context.Context, frame []byte) {
	if !s.server.opts.AcceptBatch {
		s.sendError(nullLiteral, Errorf(CodeInvalidRequest,
			"JSON-RPC batching is not enabled; it exists only in protocol revision %s", ProtocolVersion20250326))
		return
	}
	var items []json.RawMessage
	if err := json.Unmarshal(frame, &items); err != nil {
		s.sendError(nullLiteral, Errorf(CodeParseError, "parse error: %v", err))
		return
	}
	if len(items) == 0 {
		s.sendError(nullLiteral, Errorf(CodeInvalidRequest, "empty batch"))
		return
	}
	// A batch is executed sequentially, so an unbounded one is a way to buy
	// arbitrary serial work with a single frame.
	if max := s.server.opts.MaxBatchItems; len(items) > max {
		s.sendError(nullLiteral, Errorf(CodeInvalidRequest, "batch exceeds %d members", max))
		return
	}

	// Batch members are executed sequentially so their replies can be
	// collected into a single array. This is legacy compatibility, not a hot
	// path worth parallelising.
	var replies []json.RawMessage
	for _, item := range items {
		var wm wireMessage
		if err := json.Unmarshal(item, &wm); err != nil {
			replies = append(replies, marshalError(nullLiteral, Errorf(CodeParseError, "parse error: %v", err)))
			continue
		}
		if rerr := wm.validate(); rerr != nil {
			replies = append(replies, marshalError(nullLiteral, rerr))
			continue
		}
		switch wm.kind() {
		case kindRequest:
			replies = append(replies, s.runRequestSync(ctx, &wm))
		case kindNotification:
			s.handleNotification(ctx, &wm)
		case kindResponse:
			s.routeResponse(&wm)
		default:
			replies = append(replies, marshalError(nullLiteral, Errorf(CodeInvalidRequest, "invalid request")))
		}
	}
	if len(replies) > 0 {
		s.sendRaw(mustMarshal(replies))
	}
}

func (s *Session) route(ctx context.Context, wm *wireMessage) {
	switch wm.kind() {
	case kindRequest:
		if wm.JSONRPC != JSONRPCVersion {
			s.sendError(wm.ID, Errorf(CodeInvalidRequest, "unsupported jsonrpc version %q", wm.JSONRPC))
			return
		}
		if wm.Method == MethodInitialize {
			// Run the handshake inline. A conformant client waits for the
			// initialize result before sending notifications/initialized, but
			// a pipelining one does not, and the notification must not be able
			// to overtake the request that it completes.
			if reply := s.runRequestSync(ctx, wm); reply != nil {
				s.sendRaw(reply)
			}
			return
		}
		s.spawnRequest(ctx, wm)
	case kindNotification:
		// Notifications are processed inline, in arrival order. Dispatching
		// them concurrently would let a request overtake the
		// notifications/initialized that authorises it, and would let a
		// cancellation be applied before the request it names is registered.
		s.handleNotification(ctx, wm)
	case kindResponse:
		s.routeResponse(wm)
	default:
		s.sendError(nullLiteral, Errorf(CodeInvalidRequest, "invalid request"))
	}
}

// spawnRequest runs a request concurrently. It registers the request as
// in-flight *synchronously on the read loop* before spawning the worker, so a
// pipelined notifications/cancelled can always find and cancel it — even while
// the request is still queued on the concurrency semaphore. Goroutines are
// admitted up to maxQueue and then rejected; actual work is capped by the
// semaphore, and keeping the read loop free is what lets a cancel through under
// load.
func (s *Session) spawnRequest(ctx context.Context, wm *wireMessage) {
	if s.queued.Add(1) > s.maxQueue {
		s.queued.Add(-1)
		s.sendError(wm.ID, Errorf(CodeInternalError, "server is overloaded; retry later"))
		return
	}
	reqCtx, entry, release := s.registerInflight(ctx, wm)
	s.work.Add(1)
	go func() {
		defer s.work.Done()
		defer s.queued.Add(-1)
		defer release()
		if reply := s.runRegistered(reqCtx, wm, entry); reply != nil {
			s.sendRaw(reply)
		}
	}()
}

// registerInflight creates a request's cancellable context and records it in
// s.inflight, returning the context, its entry, and a release func that cancels
// the context and removes the entry. Registering before the semaphore is what
// makes a cancellation reliable while the request is queued.
func (s *Session) registerInflight(ctx context.Context, wm *wireMessage) (context.Context, *inflightEntry, func()) {
	reqCtx, cancel := context.WithCancel(ctx)
	key := idKey(wm.ID)
	entry := &inflightEntry{cancel: cancel}
	s.mu.Lock()
	s.inflight[key] = entry
	s.mu.Unlock()
	return reqCtx, entry, func() {
		cancel()
		s.mu.Lock()
		delete(s.inflight, key)
		s.mu.Unlock()
	}
}

// runRequestSync registers a request and runs it inline — the initialize
// handshake and batch members, which execute on the read loop. Concurrent
// requests go through spawnRequest, which registers before spawning.
func (s *Session) runRequestSync(ctx context.Context, wm *wireMessage) json.RawMessage {
	reqCtx, entry, release := s.registerInflight(ctx, wm)
	defer release()
	return s.runRegistered(reqCtx, wm, entry)
}

// runRegistered runs an already-registered request: acquire a concurrency slot
// (or bail if cancelled or the session closed while queued), execute it, and
// return the encoded reply — nil if the reply must be suppressed because the
// request was cancelled.
func (s *Session) runRegistered(reqCtx context.Context, wm *wireMessage, entry *inflightEntry) json.RawMessage {
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-s.done:
		return nil
	case <-reqCtx.Done():
		return nil // cancelled or parent context done while queued
	}
	if entry.cancelled.Load() {
		return nil
	}

	reqCtx = withRequestInfo(reqCtx, &requestInfo{
		id:            wm.ID,
		method:        wm.Method,
		progressToken: progressTokenOf(wm.Params),
		session:       s,
	})

	result, rerr := s.execRequest(reqCtx, wm)

	// A cancelled request gets no reply: the client has already stopped
	// correlating that ID.
	if entry.cancelled.Load() {
		return nil
	}
	if rerr != nil {
		return marshalError(wm.ID, rerr)
	}
	encoded, err := marshal(result)
	if err != nil {
		return marshalError(wm.ID, Errorf(CodeInternalError, "cannot encode result: %v", err))
	}
	return mustMarshal(outResponse{JSONRPC: JSONRPCVersion, ID: wm.ID, Result: encoded})
}

func (s *Session) routeResponse(wm *wireMessage) {
	key := idKey(wm.ID)
	s.pendMu.Lock()
	ch, ok := s.pending[key]
	if ok {
		delete(s.pending, key)
	}
	s.pendMu.Unlock()
	if !ok {
		s.server.opts.Logger.Printf("%s: response for unknown id %s", s.remote, sanitize(string(wm.ID)))
		return
	}
	ch <- wm
}

// writeLoop is the single writer. Serialising here means handlers never
// interleave partial frames and the framer needs no cross-goroutine locking of
// its own.
func (s *Session) writeLoop() {
	defer close(s.writerDone)
	write := func(frame []byte) bool {
		if err := s.framer.WriteMessage(frame); err != nil {
			if !isBenignClose(err) {
				s.server.opts.Logger.Printf("%s: write: %v", s.remote, err)
			}
			_ = s.Close()
			return false
		}
		return true
	}
	for {
		select {
		case <-s.done:
			return
		case frame := <-s.out:
			if !write(frame) {
				return
			}
		case <-s.quiesce:
			// Ordered shutdown: flush what is already queued, then stop.
			for {
				select {
				case frame := <-s.out:
					if !write(frame) {
						return
					}
				default:
					return
				}
			}
		}
	}
}

// drain waits for in-flight handlers to finish and for their replies to reach
// the wire, bounded by d. It is a no-op once the session has been closed
// abruptly, since there is nowhere left to write.
func (s *Session) drain(d time.Duration) {
	if d <= 0 {
		d = DefaultDrainTimeout
	}
	finished := make(chan struct{})
	go func() {
		s.work.Wait()
		close(finished)
	}()

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-finished:
	case <-timer.C:
		s.server.opts.Logger.Printf("%s: drain timed out after %v", s.remote, d)
	case <-s.done:
		return
	}

	s.quiesceOne.Do(func() { close(s.quiesce) })
	select {
	case <-s.writerDone:
	case <-time.After(d):
	}
}

func (s *Session) sendRaw(frame []byte) {
	select {
	case s.out <- frame:
	case <-s.done:
	}
}

func (s *Session) sendError(id json.RawMessage, e *RPCError) {
	if len(id) == 0 {
		id = nullLiteral
	}
	s.sendRaw(marshalError(id, e))
}

// marshalError encodes a JSON-RPC error envelope — the one place the error
// wire-shape is spelled, so the several cold-path error sites can't drift.
func marshalError(id json.RawMessage, e *RPCError) json.RawMessage {
	return mustMarshal(outError{JSONRPC: JSONRPCVersion, ID: id, Error: e})
}

// Notify sends a notification to the client.
func (s *Session) Notify(method string, params any) error {
	encoded, err := encodeParams(params)
	if err != nil {
		return err
	}
	frame, err := marshal(outNotification{JSONRPC: JSONRPCVersion, Method: method, Params: encoded})
	if err != nil {
		return err
	}
	select {
	case s.out <- frame:
		return nil
	case <-s.done:
		return errors.New("mcp: session closed")
	}
}

// Call issues a server-to-client request and blocks for the reply. It is used
// for sampling/createMessage, roots/list and elicitation/create. result may be
// nil if the reply is not needed.
//
// Callers must check the corresponding client capability first; a client that
// did not advertise sampling is under no obligation to answer.
func (s *Session) Call(ctx context.Context, method string, params, result any) error {
	if s.State() == StateClosed {
		return errors.New("mcp: session closed")
	}
	id := s.nextID.Add(1)
	rawID := json.RawMessage(strconv.FormatInt(id, 10))
	key := idKey(rawID)

	ch := make(chan *wireMessage, 1)
	s.pendMu.Lock()
	if len(s.pending) >= s.server.opts.MaxPendingCalls {
		s.pendMu.Unlock()
		return fmt.Errorf("mcp: %d outbound calls already pending", s.server.opts.MaxPendingCalls)
	}
	s.pending[key] = ch
	s.pendMu.Unlock()

	cleanup := func() {
		s.pendMu.Lock()
		delete(s.pending, key)
		s.pendMu.Unlock()
	}

	encoded, err := encodeParams(params)
	if err != nil {
		cleanup()
		return err
	}
	frame, err := marshal(outRequest{JSONRPC: JSONRPCVersion, ID: rawID, Method: method, Params: encoded})
	if err != nil {
		cleanup()
		return err
	}

	select {
	case s.out <- frame:
	case <-s.done:
		cleanup()
		return errors.New("mcp: session closed")
	case <-ctx.Done():
		cleanup()
		return ctx.Err()
	}

	select {
	case reply := <-ch:
		if reply.Error != nil {
			return reply.Error
		}
		if result == nil {
			return nil
		}
		return json.Unmarshal(reply.Result, result)
	case <-ctx.Done():
		cleanup()
		// Tell the peer to stop working on it. Best effort.
		_ = s.Notify(NotifyCancelled, CancelledParams{RequestID: rawID, Reason: ctx.Err().Error()})
		return ctx.Err()
	case <-s.done:
		cleanup()
		return errors.New("mcp: session closed")
	}
}

// CreateMessage asks the client to run a model completion on the server's
// behalf.
func (s *Session) CreateMessage(ctx context.Context, p *CreateMessageParams) (*CreateMessageResult, error) {
	if s.ClientCapabilities().Sampling == nil {
		return nil, errors.New("mcp: client does not support sampling")
	}
	var out CreateMessageResult
	if err := s.Call(ctx, MethodSamplingCreateMessage, p, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListRoots asks the client for its filesystem roots.
func (s *Session) ListRoots(ctx context.Context) ([]Root, error) {
	if s.ClientCapabilities().Roots == nil {
		return nil, errors.New("mcp: client does not support roots")
	}
	var out ListRootsResult
	if err := s.Call(ctx, MethodRootsList, struct{}{}, &out); err != nil {
		return nil, err
	}
	return out.Roots, nil
}

// Elicit asks the client to collect structured input from the user.
func (s *Session) Elicit(ctx context.Context, message string, schema json.RawMessage) (*ElicitResult, error) {
	if s.ClientCapabilities().Elicitation == nil {
		return nil, errors.New("mcp: client does not support elicitation")
	}
	var out ElicitResult
	if err := s.Call(ctx, MethodElicitationCreate, ElicitParams{Message: message, RequestedSchema: schema}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Log emits a logging notification if level passes the client's threshold.
func (s *Session) Log(level, logger string, data any) {
	threshold, _ := s.logLevel.Load().(string)
	if logSeverity[level] < logSeverity[threshold] {
		return
	}
	encoded, err := marshal(data)
	if err != nil {
		return
	}
	_ = s.Notify(NotifyMessage, LoggingMessageParams{Level: level, Logger: logger, Data: encoded})
}

// ReportProgress emits a progress notification for the request carried by ctx.
// It is a no-op when the client did not supply a progress token, so handlers
// can call it unconditionally.
func (s *Session) ReportProgress(ctx context.Context, progress float64, total *float64, message string) {
	info := requestInfoFrom(ctx)
	if info == nil || len(info.progressToken) == 0 {
		return
	}
	_ = s.Notify(NotifyProgress, ProgressParams{
		ProgressToken: info.progressToken,
		Progress:      progress,
		Total:         total,
		Message:       message,
	})
}

func (s *Session) isSubscribed(uri string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.subs[uri]
	return ok
}

// Close terminates the session. It is idempotent.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		s.state.Store(StateClosed)
		close(s.done)
		s.closeErr = s.framer.Close()
	})
	return s.closeErr
}

func mustMarshal(v any) json.RawMessage {
	b, err := marshal(v)
	if err != nil {
		// Only reachable if an RPCError's Data was set to something
		// unmarshalable, which WithData already prevents.
		return json.RawMessage(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal encode failure"}}`)
	}
	return b
}

// requestInfo travels in the handler context.
type requestInfo struct {
	id            json.RawMessage
	method        string
	progressToken json.RawMessage
	session       *Session
}

var metaKey = []byte(`"_meta"`)

type requestInfoKey struct{}

func withRequestInfo(ctx context.Context, ri *requestInfo) context.Context {
	return context.WithValue(ctx, requestInfoKey{}, ri)
}

func requestInfoFrom(ctx context.Context) *requestInfo {
	ri, _ := ctx.Value(requestInfoKey{}).(*requestInfo)
	return ri
}

// SessionFromContext recovers the Session inside a handler that was handed only
// a context.
func SessionFromContext(ctx context.Context) *Session {
	if ri := requestInfoFrom(ctx); ri != nil {
		return ri.session
	}
	return nil
}

// progressTokenOf extracts params._meta.progressToken without disturbing the
// rest of the params.
func progressTokenOf(params json.RawMessage) json.RawMessage {
	// Almost no request carries a progress token, and a second full decode of
	// every params object to discover that is pure waste. A substring probe
	// cannot produce a false negative: the key must appear literally.
	if len(params) == 0 || !bytes.Contains(params, metaKey) {
		return nil
	}
	var env struct {
		Meta struct {
			ProgressToken json.RawMessage `json:"progressToken"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(params, &env); err != nil {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(env.Meta.ProgressToken), nullLiteral) {
		return nil
	}
	return env.Meta.ProgressToken
}
