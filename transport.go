package mcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ServeStdio runs a single session over stdin/stdout using line framing, which
// is what the MCP stdio transport specifies.
//
// Nothing else in the process may write to stdout for the lifetime of this
// call: a stray fmt.Println corrupts the frame stream. The default logger
// already writes to stderr.
func (s *Server) ServeStdio(ctx context.Context) error {
	// Only stdin is closed on shutdown: that is what unblocks the pending
	// read. Closing stdout would discard buffered output and is left to the
	// process exit path.
	f := NewFramer(os.Stdin, os.Stdout, FramerOptions{
		Mode:            FramingLines,
		MaxMessageBytes: s.opts.MaxMessageBytes,
		BufferSize:      s.opts.BufferSize,
		Closer:          &rwCloser{closers: []io.Closer{os.Stdin}},
	})
	return s.ServeConn(ctx, f, ConnConfig{Remote: "stdio"})
}

// UnixOptions configures the Unix-domain listener.
type UnixOptions struct {
	// Mode is the permission bitmask applied to the socket file.
	// Defaults to 0600 (owner only). Ignored for abstract sockets.
	Mode fs.FileMode

	// Group, if non-empty, is the group name or GID that will own the socket;
	// pair it with Mode 0660 to grant a specific group access.
	Group string

	// ReplaceStale removes an existing socket file when no process is
	// listening on it. A live socket is never removed: that would silently
	// hijack a running instance.
	ReplaceStale bool

	// Framing selects the wire framing. Defaults to FramingLines.
	Framing FramingMode

	// AllowUID and AllowGID, when non-empty, restrict which peers may connect.
	// Enforced from kernel-supplied credentials (SO_PEERCRED), which the client
	// cannot forge. This is the only meaningful authentication available on an
	// abstract socket, which has no filesystem permissions at all.
	AllowUID []uint32
	AllowGID []uint32

	// RequirePeerCred rejects connections whose credentials cannot be read.
	// Leave false on platforms without SO_PEERCRED support.
	RequirePeerCred bool

	// MaxConns bounds concurrent connections. Zero means unlimited.
	MaxConns int

	// MaxConnsPerUID bounds concurrent connections from any single peer uid,
	// so one local account cannot exhaust the whole connection budget.
	// Requires readable peer credentials. Zero means unlimited.
	MaxConnsPerUID int

	// HandshakeTimeout drops connections that have not completed the
	// initialize handshake in time. Zero disables the check.
	HandshakeTimeout time.Duration

	// IdleTimeout drops connections with no inbound frame for this long.
	// Zero disables the check.
	IdleTimeout time.Duration

	// WriteTimeout bounds a single write. Without it, a peer that stops
	// reading blocks the session writer indefinitely and back-pressures every
	// handler behind it. Zero disables the check.
	WriteTimeout time.Duration
}

// ListenUnix creates a Unix-domain listener at path with the requested
// ownership and permissions. A path beginning with '@' selects the Linux
// abstract namespace, in which case no filesystem operations are performed.
//
// The socket is created under a umask so that it never exists with wider
// permissions than requested, not even momentarily, and the parent directory
// is created 0700. Ownership and mode are verified after bind.
func ListenUnix(path string, opts UnixOptions) (net.Listener, error) {
	abstract := strings.HasPrefix(path, "@")
	mode := opts.Mode
	if mode == 0 {
		mode = 0o600
	}

	if abstract {
		l, err := net.Listen("unix", path)
		if err != nil {
			return nil, fmt.Errorf("mcp: listen %s: %w", path, err)
		}
		return l, nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mcp: create socket directory: %w", err)
	}
	if err := clearStaleSocket(path, opts.ReplaceStale); err != nil {
		return nil, err
	}

	var l net.Listener
	err := withUmask(mode, func() error {
		var err error
		l, err = net.Listen("unix", path)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("mcp: listen %s: %w", path, err)
	}

	if err := finalizeSocket(path, mode, opts.Group); err != nil {
		_ = l.Close()
		return nil, err
	}
	// Go's UnixListener unlinks the socket file on Close by default, which is
	// what we want for a daemon that owns its own path.
	if ul, ok := l.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(true)
	}
	return l, nil
}

// finalizeSocket applies group ownership and then verifies that what sits at
// the path is the socket we created, with the permissions we asked for.
func finalizeSocket(path string, mode fs.FileMode, group string) error {
	if group != "" {
		gid, err := lookupGID(group)
		if err != nil {
			return err
		}
		if err := os.Chown(path, -1, gid); err != nil {
			return fmt.Errorf("mcp: chown socket: %w", err)
		}
	}
	// Belt and braces: umask cannot add bits, but a restrictive inherited
	// umask can remove them, and this catches a path swapped underneath us.
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("mcp: stat socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("mcp: %s is not a socket after bind", path)
	}
	if got := info.Mode().Perm(); got&^mode != 0 {
		return fmt.Errorf("mcp: socket mode %o is wider than the requested %o", got, mode)
	}
	return nil
}

// clearStaleSocket removes a leftover socket file, but only after proving that
// nothing is listening on it.
func clearStaleSocket(path string, replace bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("mcp: stat %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("mcp: %s is a symlink; refusing to bind through it", path)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("mcp: %s exists and is not a socket", path)
	}
	if !replace {
		return fmt.Errorf("mcp: %s already exists (set UnixOptions.ReplaceStale to reclaim it)", path)
	}
	conn, err := net.DialTimeout("unix", path, 250*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("mcp: %s is in use by a live listener", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("mcp: remove stale socket: %w", err)
	}
	return nil
}

// ListenSystemd returns listeners passed in by systemd socket activation
// (LISTEN_FDS / LISTEN_PID), or nil when the process was not socket-activated.
func ListenSystemd() ([]net.Listener, error) {
	if pidStr := os.Getenv("LISTEN_PID"); pidStr != "" {
		pid, err := strconv.Atoi(pidStr)
		if err != nil || pid != os.Getpid() {
			return nil, nil
		}
	}
	n, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || n <= 0 {
		return nil, nil
	}
	const firstFD = 3
	listeners := make([]net.Listener, 0, n)
	for i := 0; i < n; i++ {
		f := os.NewFile(uintptr(firstFD+i), fmt.Sprintf("systemd-socket-%d", i))
		l, err := net.FileListener(f)
		_ = f.Close() // FileListener dups the descriptor
		if err != nil {
			return nil, fmt.Errorf("mcp: adopt systemd fd %d: %w", firstFD+i, err)
		}
		listeners = append(listeners, l)
	}
	return listeners, nil
}

// connGate bounds total connections and per-uid connections.
type connGate struct {
	mu     sync.Mutex
	total  int
	perUID map[uint32]int
	maxAll int
	maxUID int
}

func newConnGate(maxAll, maxUID int) *connGate {
	return &connGate{perUID: make(map[uint32]int), maxAll: maxAll, maxUID: maxUID}
}

func (g *connGate) acquire(p *PeerCred) (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.maxAll > 0 && g.total >= g.maxAll {
		return nil, false
	}
	var uid uint32
	tracked := g.maxUID > 0 && p != nil
	if tracked {
		uid = p.UID
		if g.perUID[uid] >= g.maxUID {
			return nil, false
		}
		g.perUID[uid]++
	}
	g.total++

	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.total--
		if tracked {
			if g.perUID[uid]--; g.perUID[uid] <= 0 {
				delete(g.perUID, uid)
			}
		}
	}, true
}

// Serve accepts connections until the listener fails or ctx is cancelled. It
// returns after every in-flight session has finished, so a caller can rely on
// it for graceful shutdown.
func (s *Server) Serve(ctx context.Context, l net.Listener, opts UnixOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	var (
		wg      sync.WaitGroup
		backoff time.Duration
	)
	gate := newConnGate(opts.MaxConns, opts.MaxConnsPerUID)

	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || isBenignClose(err) {
				break
			}
			// Transient accept errors (EMFILE, ECONNABORTED) should not kill
			// the daemon; back off and retry.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() || isTemporary(err) {
				backoff = nextBackoff(backoff)
				s.opts.Logger.Printf("accept: %v; retrying in %v", err, backoff)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
				}
				continue
			}
			wg.Wait()
			return err
		}
		backoff = 0

		peer, credErr := peerCredentials(conn)
		if !s.admit(conn, peer, credErr, opts) {
			continue
		}
		release, ok := gate.acquire(peer)
		if !ok {
			s.opts.Logger.Printf("rejecting connection from %s: connection limit reached", peer)
			_ = conn.Close()
			continue
		}

		wg.Add(1)
		go func(c net.Conn, p *PeerCred) {
			defer wg.Done()
			defer release()
			s.serveAccepted(ctx, c, p, opts)
		}(conn, peer)
	}

	wg.Wait()
	return nil
}

// admit applies credential policy before any protocol byte is read.
func (s *Server) admit(c net.Conn, peer *PeerCred, credErr error, opts UnixOptions) bool {
	needCred := opts.RequirePeerCred || opts.MaxConnsPerUID > 0 ||
		len(opts.AllowUID) > 0 || len(opts.AllowGID) > 0
	if credErr != nil && needCred {
		s.opts.Logger.Printf("rejecting connection: peer credentials unavailable: %v", credErr)
		_ = c.Close()
		return false
	}
	if !peerAllowed(peer, opts) {
		s.opts.Logger.Printf("rejecting connection from %s: credentials not permitted", peer)
		_ = c.Close()
		return false
	}
	return true
}

func (s *Server) serveAccepted(ctx context.Context, c net.Conn, peer *PeerCred, opts UnixOptions) {
	remote := describeConn(c, peer)

	// Deadlines are enforced at the conn, below the framer, so a stalled peer
	// cannot pin a goroutine, a buffer and a file descriptor indefinitely.
	wrapped := &deadlineConn{Conn: c, idle: opts.IdleTimeout, write: opts.WriteTimeout}
	if opts.HandshakeTimeout > 0 && opts.IdleTimeout == 0 {
		// Give the handshake its own read bound even when no idle timeout is
		// configured.
		wrapped.idle = opts.HandshakeTimeout
	}

	framer := NewFramer(wrapped, wrapped, FramerOptions{
		Mode:            opts.Framing,
		MaxMessageBytes: s.opts.MaxMessageBytes,
		BufferSize:      s.opts.BufferSize,
		Closer:          c,
	})

	cfg := ConnConfig{Remote: remote, Peer: peer}
	if opts.HandshakeTimeout > 0 {
		cfg.OnSession = func(sess *Session) {
			time.AfterFunc(opts.HandshakeTimeout, func() {
				if sess.State() != StateReady {
					s.opts.Logger.Printf("%s: handshake timeout", remote)
					_ = sess.Close()
				}
			})
		}
	}

	if err := s.ServeConn(ctx, framer, cfg); err != nil && !isBenignClose(err) {
		s.opts.Logger.Printf("%s: session ended: %v", remote, err)
	}
}

// deadlineConn applies a rolling read deadline and a per-write deadline.
type deadlineConn struct {
	net.Conn
	idle  time.Duration
	write time.Duration
}

func (c *deadlineConn) Read(p []byte) (int, error) {
	if c.idle > 0 {
		if err := c.Conn.SetReadDeadline(time.Now().Add(c.idle)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Read(p)
}

func (c *deadlineConn) Write(p []byte) (int, error) {
	if c.write > 0 {
		if err := c.Conn.SetWriteDeadline(time.Now().Add(c.write)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Write(p)
}

func peerAllowed(p *PeerCred, opts UnixOptions) bool {
	if len(opts.AllowUID) == 0 && len(opts.AllowGID) == 0 {
		return true
	}
	if p == nil {
		return false
	}
	return slices.Contains(opts.AllowUID, p.UID) || slices.Contains(opts.AllowGID, p.GID)
}

func describeConn(c net.Conn, p *PeerCred) string {
	addr := "unix"
	if c.RemoteAddr() != nil {
		if s := c.RemoteAddr().String(); s != "" && s != "@" {
			addr = sanitize(s)
		}
	}
	if p != nil {
		return addr + " [" + p.String() + "]"
	}
	return addr
}

func nextBackoff(d time.Duration) time.Duration {
	switch {
	case d == 0:
		return 5 * time.Millisecond
	case d >= time.Second:
		return time.Second
	default:
		return d * 2
	}
}

func isTemporary(err error) bool {
	type temporary interface{ Temporary() bool }
	var t temporary
	if errors.As(err, &t) {
		return t.Temporary()
	}
	return false
}

// ListenAndServeUnix is the one-call form: bind, serve, and clean up.
func (s *Server) ListenAndServeUnix(ctx context.Context, path string, opts UnixOptions) error {
	l, err := ListenUnix(path, opts)
	if err != nil {
		return err
	}
	defer l.Close()
	return s.Serve(ctx, l, opts)
}

// lookupGID resolves a group name or numeric GID.
func lookupGID(group string) (int, error) {
	if gid, err := strconv.Atoi(group); err == nil {
		return gid, nil
	}
	g, err := user.LookupGroup(group)
	if err != nil {
		return 0, fmt.Errorf("mcp: lookup group %q: %w", group, err)
	}
	gid, err := strconv.Atoi(g.Gid)
	if err != nil {
		return 0, fmt.Errorf("mcp: group %q has non-numeric gid %q", group, g.Gid)
	}
	return gid, nil
}
