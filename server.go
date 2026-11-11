package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults.
const (
	DefaultMaxMessageBytes       = 8 << 20 // 8 MiB
	DefaultMaxConcurrentRequests = 64
	DefaultPageSize              = 100
	DefaultDrainTimeout          = 5 * time.Second
	DefaultMaxSubscriptions      = 1024
	DefaultMaxBatchItems         = 64
	DefaultMaxPendingCalls       = 256

	// defaultBroadcastGrace is how long a fan-out lets a session whose
	// outbound queue is full go without its writer making progress before
	// closing it.
	defaultBroadcastGrace = 5 * time.Second
	// defaultBroadcastMaxWait is the most a fan-out waits on one session even
	// if its writer keeps making progress: a peer that drains a frame every few
	// seconds is as good as stalled, and would otherwise hold the caller for as
	// long as the replies queued ahead of the notification take.
	defaultBroadcastMaxWait = 30 * time.Second
)

func sprintf(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// Handler signatures. Every handler receives the Session so it can call back
// into the client (sampling, roots, elicitation, progress, logging) and the
// context so it can observe cancellation.
type (
	ToolHandler     func(ctx context.Context, s *Session, args json.RawMessage) (*CallToolResult, error)
	ResourceHandler func(ctx context.Context, s *Session, uri string) ([]ResourceContents, error)
	PromptHandler   func(ctx context.Context, s *Session, args map[string]string) (*GetPromptResult, error)
	Completer       func(ctx context.Context, s *Session, p *CompleteParams) ([]string, error)
)

type toolEntry struct {
	def Tool
	fn  ToolHandler
}

type resourceEntry struct {
	def Resource
	fn  ResourceHandler
}

type templateEntry struct {
	def  ResourceTemplate
	tmpl *URITemplate
	fn   ResourceHandler
}

type promptEntry struct {
	def Prompt
	fn  PromptHandler
}

// Options configures a Server.
type Options struct {
	// Name and Version identify this server to clients. Name is required.
	Name    string
	Title   string
	Version string

	// Instructions is optional free text describing how to use the server;
	// clients may surface it to the model as a system-prompt fragment.
	Instructions string

	// ProtocolVersions is the negotiation set, newest first.
	// Defaults to DefaultProtocolVersions.
	ProtocolVersions []string

	// Logger receives diagnostics. It must never write to stdout when the
	// stdio transport is in use; the default writes to stderr.
	Logger *log.Logger

	// MaxMessageBytes caps inbound frame size. Default DefaultMaxMessageBytes.
	MaxMessageBytes int

	// BufferSize is the per-direction I/O buffer for each connection.
	// Default DefaultBufferSize.
	BufferSize int

	// MaxConcurrentRequests bounds in-flight handlers per session.
	// Default DefaultMaxConcurrentRequests.
	MaxConcurrentRequests int

	// MaxQueuedBytes bounds, per session, the summed frame length of requests
	// that are queued or running. It is an admission limit on frame bytes, not
	// a bound on memory: a handler's decoded copy of its arguments, replies
	// waiting in the outbound queue, the frame the read loop is handling and,
	// over HTTP, request bodies waiting for the read loop are all outside it.
	// A request that would
	// exceed it is refused with the same "overloaded" error as one past the
	// count limit, except that a request is always admitted when the session
	// holds nothing else, so a small value never makes a large message
	// impossible. MaxConcurrentRequests*8 requests may queue, so without a byte
	// bound one session could hold that many MaxMessageBytes messages.
	// Default 8 * MaxMessageBytes.
	MaxQueuedBytes int

	// MaxSubscriptions bounds resource subscriptions per session, so a client
	// cannot grow the server's memory by subscribing in a loop.
	// Default DefaultMaxSubscriptions.
	MaxSubscriptions int

	// MaxBatchItems bounds the number of members in a JSON-RPC batch.
	// Default DefaultMaxBatchItems.
	MaxBatchItems int

	// MaxPendingCalls bounds outstanding server-to-client requests per
	// session. Default DefaultMaxPendingCalls.
	MaxPendingCalls int

	// PageSize is the number of entries returned per list request.
	PageSize int

	// DrainTimeout bounds how long a closing session waits for in-flight
	// handlers to finish and for queued replies to be flushed.
	// Default DefaultDrainTimeout.
	DrainTimeout time.Duration

	// AcceptBatch controls whether top-level JSON arrays are accepted.
	// Batching existed only in protocol revision 2025-03-26; it is rejected by
	// default and never emitted.
	AcceptBatch bool

	// OnSessionStart, if set, runs after a session completes initialization.
	OnSessionStart func(*Session)

	// OnSessionEnd, if set, runs when a session terminates.
	OnSessionEnd func(*Session, error)
}

// listCache holds the encoded form of canonical list pages. Encoding a
// hundred-tool registry on every tools/list is the single most expensive thing
// a well-behaved client can ask a server to do repeatedly.
type listCache struct {
	mu    sync.Mutex
	gen   uint64
	pages map[string][]byte
}

func (c *listCache) get(gen uint64, key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != gen {
		return nil, false
	}
	b, ok := c.pages[key]
	return b, ok
}

// pageCount reports how many pages are cached, for tests and diagnostics.
func (c *listCache) pageCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pages)
}

func (c *listCache) put(gen uint64, key string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != gen || c.pages == nil {
		c.gen = gen
		c.pages = make(map[string][]byte)
	}
	c.pages[key] = b
}

// Server holds the registry and spawns a Session per connection. A single
// Server is safe for concurrent use and can back stdio and any number of
// socket connections simultaneously.
type Server struct {
	opts Options

	// gen increments on every registry mutation and invalidates the caches.
	gen atomic.Uint64

	mu            sync.RWMutex
	tools         map[string]*toolEntry
	toolOrder     []string
	resources     map[string]*resourceEntry
	resourceOrder []string
	templates     []*templateEntry
	prompts       map[string]*promptEntry
	promptOrder   []string
	completer     Completer

	toolCache     listCache
	resourceCache listCache
	templateCache listCache
	promptCache   listCache

	// broadcastGrace is how long a fan-out lets a session whose outbound queue
	// is full go without its writer making progress before closing it. A field
	// so tests can shorten it.
	broadcastGrace time.Duration
	// broadcastMaxWait caps how long a fan-out waits on one session that is
	// making progress, however slowly. A field so tests can shorten it.
	broadcastMaxWait time.Duration

	sessMu   sync.Mutex
	sessions map[*Session]struct{}
	closed   bool
}

// New creates a Server. It panics only on a missing Name, which is a
// programming error rather than a runtime condition.
func New(opts Options) *Server {
	if opts.Name == "" {
		panic("mcp: Options.Name is required")
	}
	if opts.Version == "" {
		opts.Version = "0.0.0"
	}
	if len(opts.ProtocolVersions) == 0 {
		opts.ProtocolVersions = DefaultProtocolVersions
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, "mcp: ", log.LstdFlags|log.Lmsgprefix)
	}
	setDefault(&opts.MaxMessageBytes, DefaultMaxMessageBytes)
	setDefault(&opts.BufferSize, DefaultBufferSize)
	setDefault(&opts.MaxConcurrentRequests, DefaultMaxConcurrentRequests)
	setDefault(&opts.MaxQueuedBytes, 8*opts.MaxMessageBytes)
	setDefault(&opts.MaxSubscriptions, DefaultMaxSubscriptions)
	setDefault(&opts.MaxBatchItems, DefaultMaxBatchItems)
	setDefault(&opts.MaxPendingCalls, DefaultMaxPendingCalls)
	setDefault(&opts.PageSize, DefaultPageSize)
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = DefaultDrainTimeout
	}
	return &Server{
		opts:             opts,
		broadcastGrace:   defaultBroadcastGrace,
		broadcastMaxWait: defaultBroadcastMaxWait,
		tools:            make(map[string]*toolEntry),
		resources:        make(map[string]*resourceEntry),
		prompts:          make(map[string]*promptEntry),
		sessions:         make(map[*Session]struct{}),
	}
}

func setDefault(p *int, def int) {
	if *p <= 0 {
		*p = def
	}
}

var errDuplicate = errors.New("mcp: duplicate registration")

// AddTool registers a tool. An empty InputSchema is replaced with a
// permissive object schema, since the field is required on the wire.
func (s *Server) AddTool(t Tool, fn ToolHandler) error {
	if t.Name == "" {
		return errors.New("mcp: tool name is required")
	}
	if fn == nil {
		return errors.New("mcp: tool handler is required")
	}
	if len(t.InputSchema) == 0 {
		t.InputSchema = json.RawMessage(`{"type":"object"}`)
	}
	if !json.Valid(t.InputSchema) {
		return fmt.Errorf("mcp: tool %q: input schema is not valid JSON", t.Name)
	}
	if len(t.OutputSchema) > 0 && !json.Valid(t.OutputSchema) {
		return fmt.Errorf("mcp: tool %q: output schema is not valid JSON", t.Name)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.gen.Add(1)

	if _, exists := s.tools[t.Name]; exists {
		return fmt.Errorf("%w: tool %q", errDuplicate, t.Name)
	}
	s.tools[t.Name] = &toolEntry{def: t, fn: fn}
	s.toolOrder = insertSorted(s.toolOrder, t.Name)
	return nil
}

// RemoveTool unregisters a tool. Callers should follow with
// NotifyToolListChanged.
func (s *Server) RemoveTool(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tools[name]; !ok {
		return false
	}
	delete(s.tools, name)
	s.toolOrder = removeString(s.toolOrder, name)
	s.gen.Add(1)
	return true
}

// AddResource registers a resource at a fixed URI.
func (s *Server) AddResource(r Resource, fn ResourceHandler) error {
	if r.URI == "" {
		return errors.New("mcp: resource URI is required")
	}
	if r.Name == "" {
		r.Name = r.URI
	}
	if fn == nil {
		return errors.New("mcp: resource handler is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.gen.Add(1)

	if _, exists := s.resources[r.URI]; exists {
		return fmt.Errorf("%w: resource %q", errDuplicate, r.URI)
	}
	s.resources[r.URI] = &resourceEntry{def: r, fn: fn}
	s.resourceOrder = insertSorted(s.resourceOrder, r.URI)
	return nil
}

// AddResourceTemplate registers a URI template. The template is compiled once
// here rather than re-parsed on every read. On read, templates are tried in
// registration order after exact-URI resources fail to match.
func (s *Server) AddResourceTemplate(t ResourceTemplate, fn ResourceHandler) error {
	if t.URITemplate == "" {
		return errors.New("mcp: URI template is required")
	}
	if fn == nil {
		return errors.New("mcp: resource handler is required")
	}
	compiled, err := CompileURITemplate(t.URITemplate)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.gen.Add(1)

	s.templates = append(s.templates, &templateEntry{def: t, tmpl: compiled, fn: fn})
	return nil
}

// AddPrompt registers a prompt template.
func (s *Server) AddPrompt(p Prompt, fn PromptHandler) error {
	if p.Name == "" {
		return errors.New("mcp: prompt name is required")
	}
	if fn == nil {
		return errors.New("mcp: prompt handler is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.gen.Add(1)

	if _, exists := s.prompts[p.Name]; exists {
		return fmt.Errorf("%w: prompt %q", errDuplicate, p.Name)
	}
	s.prompts[p.Name] = &promptEntry{def: p, fn: fn}
	s.promptOrder = insertSorted(s.promptOrder, p.Name)
	return nil
}

// SetCompleter installs an argument-completion handler and advertises the
// completions capability.
func (s *Server) SetCompleter(c Completer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completer = c
	s.gen.Add(1)
}

// capabilities reflects what is registered right now. Advertising is derived
// rather than declared so the two cannot drift apart.
func (s *Server) capabilities() ServerCapabilities {
	s.mu.RLock()
	defer s.mu.RUnlock()

	caps := ServerCapabilities{Logging: &emptyCap{}}
	if len(s.tools) > 0 {
		caps.Tools = &ToolsCapability{ListChanged: true}
	}
	if len(s.resources) > 0 || len(s.templates) > 0 {
		caps.Resources = &ResourcesCapability{Subscribe: true, ListChanged: true}
	}
	if len(s.prompts) > 0 {
		caps.Prompts = &PromptsCapability{ListChanged: true}
	}
	if s.completer != nil {
		caps.Completions = &emptyCap{}
	}
	return caps
}

func (s *Server) negotiate(requested string) string {
	if slices.Contains(s.opts.ProtocolVersions, requested) {
		return requested
	}
	return s.opts.ProtocolVersions[0]
}

// Broadcast sends a notification to every session that has completed
// initialization. Errors are logged, not returned. A session whose outbound
// queue is full is waited on, together with any others in that state, and is
// closed if its writer makes no progress for a grace period (5s; a peer that
// stalls after one more write is closed within twice that) or if the wait has
// lasted 30s in all. A peer that has stopped reading therefore loses its
// connection rather than back-pressuring everyone, and so does one that reads too
// slowly to matter, which would otherwise hold the caller for as long as the
// replies queued ahead of the frame take to drain; a client that reconnects
// refreshes what the notification would have told it. A peer that reads at a
// useful rate is waited on and not closed, which can take up to the 30s, even with
// UnixOptions.WriteTimeout unset. A single write of one very large frame that
// outlasts the grace counts as no progress. Session.Notify, by contrast, still
// blocks on a full queue, because ordering with replies matters to its callers.
func (s *Server) Broadcast(method string, params any) {
	var ready []*Session
	for _, sess := range s.Sessions() {
		if sess.State() == StateReady {
			ready = append(ready, sess)
		}
	}
	s.fanout(ready, method, params, func(sess *Session, err error) {
		s.opts.Logger.Printf("broadcast %s to %s: %v", method, sess.RemoteAddr(), err)
	})
}

// fanout delivers one notification to each of sessions. A session whose
// queue has room gets it at once; those whose queue is full are waited on
// together, and closed if their writer makes no progress for broadcastGrace.
func (s *Server) fanout(sessions []*Session, method string, params any, logErr func(*Session, error)) {
	frame, err := encodeNotification(method, params)
	if err != nil {
		s.opts.Logger.Printf("notify %s: %v", sanitize(method), err)
		return
	}
	var slow []*Session
	for _, sess := range sessions {
		switch err := sess.offer(frame); {
		case err == nil:
		case errors.Is(err, errQueueFull):
			slow = append(slow, sess)
		default:
			logErr(sess, err)
		}
	}
	if len(slow) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, sess := range slow {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sess.offerWithin(frame, s.broadcastGrace, s.broadcastMaxWait); err != nil {
				logErr(sess, err)
			}
		}()
	}
	wg.Wait()
}

func (s *Server) NotifyToolListChanged()     { s.Broadcast(NotifyToolsChanged, struct{}{}) }
func (s *Server) NotifyResourceListChanged() { s.Broadcast(NotifyResourcesChanged, struct{}{}) }
func (s *Server) NotifyPromptListChanged()   { s.Broadcast(NotifyPromptsChanged, struct{}{}) }

// NotifyResourceUpdated informs only those sessions that subscribed to the URI,
// with the same handling of a session that stopped reading as Broadcast.
func (s *Server) NotifyResourceUpdated(uri string) {
	var subscribed []*Session
	for _, sess := range s.Sessions() {
		if sess.State() == StateReady && sess.isSubscribed(uri) {
			subscribed = append(subscribed, sess)
		}
	}
	s.fanout(subscribed, NotifyResourceUpdated, ResourceUpdatedParams{URI: uri}, func(_ *Session, err error) {
		s.opts.Logger.Printf("notify update %s: %v", sanitize(uri), err)
	})
}

// Sessions returns a snapshot of the live sessions.
func (s *Server) Sessions() []*Session {
	s.sessMu.Lock()
	defer s.sessMu.Unlock()
	out := make([]*Session, 0, len(s.sessions))
	for sess := range s.sessions {
		out = append(out, sess)
	}
	return out
}

func (s *Server) addSession(sess *Session) error {
	s.sessMu.Lock()
	defer s.sessMu.Unlock()
	if s.closed {
		return errors.New("mcp: server is shut down")
	}
	s.sessions[sess] = struct{}{}
	return nil
}

func (s *Server) removeSession(sess *Session) {
	s.sessMu.Lock()
	delete(s.sessions, sess)
	s.sessMu.Unlock()
}

// Close terminates every live session and refuses new ones. Listeners are
// owned by the caller of Serve and are shut down by cancelling its context.
func (s *Server) Close() error {
	s.sessMu.Lock()
	s.closed = true
	sessions := make([]*Session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.sessMu.Unlock()

	var errs []error
	for _, sess := range sessions {
		if err := sess.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Pagination. Cursors are opaque to the client by contract; here they encode a
// base64 offset scoped by a list name so a cursor cannot be replayed against a
// different collection.
func encodeCursor(scope string, offset int) string {
	var buf [32]byte
	b := append(buf[:0], scope...)
	b = append(b, ':')
	b = strconv.AppendInt(b, int64(offset), 10)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(scope, cursor string) (int, *RPCError) {
	if cursor == "" {
		return 0, nil
	}
	if len(cursor) > 64 {
		return 0, Errorf(CodeInvalidParams, "invalid cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, Errorf(CodeInvalidParams, "invalid cursor")
	}
	gotScope, off, ok := strings.Cut(string(raw), ":")
	if !ok || gotScope != scope {
		return 0, Errorf(CodeInvalidParams, "invalid cursor")
	}
	n, err := strconv.Atoi(off)
	if err != nil || n < 0 {
		return 0, Errorf(CodeInvalidParams, "invalid cursor")
	}
	return n, nil
}

// listPage is the shared body of every list handler: decode the cursor, serve
// from cache when the page is a canonical one, otherwise build, encode and
// cache it.
//
// Only in-range offsets that fall on a page boundary are cached, and the cache
// is keyed by the canonical encoding of the *clamped* offset — never the raw
// cursor. So a client cannot grow the cache past one entry per real page: forged
// out-of-range offsets clamp to a non-boundary and are not cached, and
// non-canonical encodings of the same offset (leading zeros, +0, -0) all collapse
// to one key.
func (s *Server) listPage(scope string, cache *listCache, cursor string,
	count func() int, build func(lo, hi int, next string) any) (any, *RPCError) {

	off, rerr := decodeCursor(scope, cursor)
	if rerr != nil {
		return nil, rerr
	}
	gen := s.gen.Load()

	s.mu.RLock()
	n := count()
	if off > n { // clamp before deciding cacheability, so out-of-range can't be "canonical"
		off = n
	}
	canonical := off%s.opts.PageSize == 0
	var key string
	if canonical {
		key = encodeCursor(scope, off) // canonical key from the clamped offset
		if encoded, ok := cache.get(gen, key); ok {
			s.mu.RUnlock()
			return json.RawMessage(encoded), nil
		}
	}
	hi := off + s.opts.PageSize
	if hi > n {
		hi = n
	}
	var next string
	if hi < n {
		next = encodeCursor(scope, hi)
	}
	result := build(off, hi, next)
	s.mu.RUnlock()

	if !canonical {
		return result, nil
	}
	encoded, err := marshal(result)
	if err != nil {
		return nil, Errorf(CodeInternalError, "encode %s list: %v", scope, err)
	}
	cache.put(gen, key, encoded)
	return json.RawMessage(encoded), nil
}

func insertSorted(list []string, v string) []string {
	i, _ := slices.BinarySearch(list, v) // callers dedup, so the found-bool is moot
	return slices.Insert(list, i, v)
}

func removeString(list []string, v string) []string {
	if i, ok := slices.BinarySearch(list, v); ok {
		return slices.Delete(list, i, i+1)
	}
	return list
}
