package mcp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// ErrMessageTooLarge is returned when an inbound frame exceeds the configured
// limit. It is fatal to the session: the stream cannot be resynchronised
// because the remainder of the oversized frame is indistinguishable from the
// next message.
var ErrMessageTooLarge = errors.New("mcp: message exceeds maximum size")

// Header-framing limits. A peer that never sends the terminating blank line
// would otherwise hold a connection open while the header map grows.
const (
	maxHeaderCount     = 32
	maxHeaderLineBytes = 4096
)

// DefaultBufferSize is the per-direction I/O buffer for one connection. It is
// deliberately small: two 64 KiB buffers per session is 128 MiB at a thousand
// sessions, which is a memory-exhaustion vector wearing a performance
// costume. Frames larger than the buffer are read incrementally.
const DefaultBufferSize = 4096

// headerBodyChunk is how much of a header-framed body is read, and the buffer
// grown for it, at a time.
const headerBodyChunk = 64 << 10

// Framer reads and writes complete protocol frames over a byte stream.
// A Framer is not safe for concurrent writers; the session serialises all
// writes through a single goroutine.
type Framer interface {
	ReadMessage() ([]byte, error)
	WriteMessage(p []byte) error
	Close() error
}

// FramingMode selects the wire framing.
type FramingMode int

const (
	// FramingLines is newline-delimited JSON: one compact JSON document per
	// line. This is what the MCP stdio transport specifies and what current
	// clients speak over sockets too.
	FramingLines FramingMode = iota

	// FramingHeaders is LSP-style "Content-Length: n\r\n\r\n<body>". Not part
	// of MCP, but useful when tunnelling through something that mangles line
	// boundaries.
	FramingHeaders
)

// FramerOptions configures a Framer.
type FramerOptions struct {
	Mode            FramingMode
	MaxMessageBytes int       // zero selects DefaultMaxMessageBytes
	BufferSize      int       // zero selects DefaultBufferSize
	Closer          io.Closer // optional; closed by Framer.Close
}

type streamFramer struct {
	mode   FramingMode
	r      *bufio.Reader
	w      *bufio.Writer
	closer io.Closer
	max    int

	// scratch is reused across ReadMessage calls. The returned slice is only
	// valid until the next call, which is safe because the session decodes
	// each frame before reading the next, and encoding/json copies every
	// json.RawMessage it produces rather than aliasing the input.
	scratch []byte

	wmu sync.Mutex
}

// NewFramer wraps a reader/writer pair.
func NewFramer(r io.Reader, w io.Writer, opts FramerOptions) Framer {
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = DefaultBufferSize
	}
	return &streamFramer{
		mode:   opts.Mode,
		r:      bufio.NewReaderSize(r, opts.BufferSize),
		w:      bufio.NewWriterSize(w, opts.BufferSize),
		closer: opts.Closer,
		max:    opts.MaxMessageBytes,
	}
}

func (f *streamFramer) ReadMessage() ([]byte, error) {
	if f.mode == FramingHeaders {
		return f.readHeaderFramed()
	}
	return f.readLine()
}

// dropOversizedScratch releases the read buffer once it has grown past the pool
// cap, so a single large frame doesn't pin memory for the session lifetime. The
// just-produced line/body slice keeps its own backing array, so clearing
// f.scratch here is safe.
func (f *streamFramer) dropOversizedScratch() {
	if cap(f.scratch) > maxPooledBuffer {
		f.scratch = nil
	}
}

// readLine accumulates until LF, enforcing the size cap as it goes rather than
// after the fact, so a hostile peer cannot force an unbounded allocation.
func (f *streamFramer) readLine() ([]byte, error) {
	for {
		buf := f.scratch[:0]
		for {
			chunk, err := f.r.ReadSlice('\n')
			if len(buf)+len(chunk) > f.max {
				return nil, ErrMessageTooLarge
			}
			buf = append(buf, chunk...)
			if err == nil {
				break
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				continue // partial line, keep reading
			}
			if errors.Is(err, io.EOF) && len(bytes.TrimSpace(buf)) > 0 {
				break // final line without trailing newline
			}
			f.scratch = buf
			return nil, err
		}
		f.scratch = buf
		f.dropOversizedScratch() // don't pin a huge buffer for the session after one big frame

		line := bytes.TrimRight(buf, "\r\n")
		if len(bytes.TrimSpace(line)) == 0 {
			continue // tolerate blank keepalive lines
		}
		return line, nil
	}
}

func (f *streamFramer) readHeaderFramed() ([]byte, error) {
	length := -1
	for count := 0; ; count++ {
		if count > maxHeaderCount {
			return nil, fmt.Errorf("mcp: more than %d headers", maxHeaderCount)
		}
		line, err := f.readHeaderLine()
		if err != nil {
			return nil, err
		}
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("mcp: malformed header %q", sanitize(line))
		}
		if strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			if length >= 0 {
				return nil, errors.New("mcp: duplicate Content-Length header")
			}
			n, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || n < 0 {
				return nil, fmt.Errorf("mcp: bad Content-Length %q", sanitize(value))
			}
			length = n
		}
	}
	if length < 0 {
		return nil, errors.New("mcp: missing Content-Length header")
	}
	if length > f.max {
		return nil, ErrMessageTooLarge
	}
	// Grow as the body arrives. Allocating the declared length first would let a
	// peer that sends a header and then nothing hold f.max bytes for 27 bytes of
	// input; the line reader already follows the bytes received, and so does this.
	body := f.scratch[:0]
	for len(body) < length {
		n := min(length-len(body), headerBodyChunk)
		body = slices.Grow(body, n)
		m, err := io.ReadFull(f.r, body[len(body):len(body)+n])
		body = body[:len(body)+m]
		if err != nil {
			f.scratch = body[:0]
			return nil, err
		}
	}
	f.scratch = body
	f.dropOversizedScratch() // body keeps its backing; release the oversized scratch
	return body, nil
}

// readHeaderLine reads one CRLF-terminated header, bounded in length so a peer
// cannot stream an endless header.
func (f *streamFramer) readHeaderLine() (string, error) {
	var sb strings.Builder
	for {
		chunk, err := f.r.ReadSlice('\n')
		if sb.Len()+len(chunk) > maxHeaderLineBytes {
			return "", fmt.Errorf("mcp: header line exceeds %d bytes", maxHeaderLineBytes)
		}
		sb.Write(chunk)
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return "", err
	}
	return strings.TrimRight(sb.String(), "\r\n"), nil
}

func (f *streamFramer) WriteMessage(p []byte) error {
	f.wmu.Lock()
	defer f.wmu.Unlock()

	if f.mode == FramingHeaders {
		if _, err := fmt.Fprintf(f.w, "Content-Length: %d\r\n\r\n", len(p)); err != nil {
			return err
		}
		if _, err := f.w.Write(p); err != nil {
			return err
		}
		return f.w.Flush()
	}

	// Line framing cannot carry an embedded newline. json.Marshal escapes them
	// inside strings and emits compact output, so this is a guard against a
	// caller handing us pre-formatted (indented) JSON, not a routine path.
	if bytes.ContainsAny(p, "\r\n") {
		var compact bytes.Buffer
		if err := jsonCompact(&compact, p); err != nil {
			return fmt.Errorf("mcp: frame contains a newline and is not valid JSON: %w", err)
		}
		p = compact.Bytes()
		if bytes.ContainsAny(p, "\r\n") {
			return errors.New("mcp: frame contains an unescapable newline")
		}
	}
	if _, err := f.w.Write(p); err != nil {
		return err
	}
	if err := f.w.WriteByte('\n'); err != nil {
		return err
	}
	return f.w.Flush()
}

func (f *streamFramer) Close() error {
	// A writer blocked in Write holds wmu, and closing the connection is the only
	// thing that ends that write, so Close must not wait for the lock: flush what
	// can be flushed without waiting, then close.
	if f.wmu.TryLock() {
		_ = f.w.Flush()
		f.wmu.Unlock()
	}
	if f.closer != nil {
		return f.closer.Close()
	}
	return nil
}

// rwCloser closes a set of descriptors exactly once.
type rwCloser struct {
	closers []io.Closer
	once    sync.Once
	err     error
}

func (c *rwCloser) Close() error {
	c.once.Do(func() {
		for _, cl := range c.closers {
			if err := cl.Close(); err != nil && c.err == nil {
				c.err = err
			}
		}
	})
	return c.err
}
