package mcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

// sseLineSlack is what a line may exceed the data cap by: the "data: " field name
// and a terminator. The data cap itself is enforced on the joined event.
const sseLineSlack = 64

// errSSEEventTooLarge is returned when one event's data exceeds the limit. Like
// ErrMessageTooLarge it ends the stream: the rest of the event cannot be told
// from the next.
var errSSEEventTooLarge = errors.New("mcp: SSE event exceeds maximum size")

// sseReader decodes a text/event-stream body into events. It implements what
// MCP's streamable-HTTP transport uses: "data" fields joined by newlines, an
// optional "event" name, comment lines ignored, an event dispatched on a blank
// line. "id" and "retry" are read and ignored (there is no resumption), and a
// bare CR is not a line ending (every sender this package talks to uses LF or
// CRLF).
//
// The size cap is enforced as bytes arrive, so a hostile or broken server
// cannot force an unbounded allocation by never sending a newline.
type sseReader struct {
	r   *bufio.Reader
	max int
}

func newSSEReader(r io.Reader, max int) *sseReader {
	return &sseReader{r: bufio.NewReaderSize(r, DefaultBufferSize), max: max}
}

// next returns the next event with data. An event with no data field is
// skipped, as the SSE specification says. The returned data is a fresh slice.
func (s *sseReader) next() (event string, data []byte, err error) {
	var (
		buf     []byte
		hasData bool
		name    string
		line    []byte
	)
	for {
		line, err = s.readLine(len(buf))
		if err != nil {
			// A stream that ends mid-event discards the partial event, per the
			// SSE specification.
			return "", nil, err
		}
		if len(line) == 0 { // blank line: dispatch
			if hasData {
				return name, buf, nil
			}
			name = ""
			continue
		}
		if line[0] == ':' { // comment (keep-alive)
			continue
		}
		field, value, _ := bytes.Cut(line, []byte{':'})
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(field) {
		case "data":
			if hasData {
				buf = append(buf, '\n')
			}
			if len(buf)+len(value) > s.max {
				return "", nil, errSSEEventTooLarge
			}
			buf = append(buf, value...)
			hasData = true
		case "event":
			name = string(value)
		}
	}
}

// readLine returns one line without its terminator. used is how much of the
// current event is already buffered, so the cap covers the event, not the line.
func (s *sseReader) readLine(used int) ([]byte, error) {
	var out []byte
	for {
		chunk, err := s.r.ReadSlice('\n')
		if used+len(out)+len(chunk) > s.max+sseLineSlack {
			return nil, errSSEEventTooLarge
		}
		out = append(out, chunk...)
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return nil, err // EOF with a partial line is a truncated stream
	}
	out = bytes.TrimSuffix(out, []byte{'\n'})
	return bytes.TrimSuffix(out, []byte{'\r'}), nil
}

// sseWriter writes a text/event-stream response with a deadline on every write.
// The deadline matters because a handler goroutine blocked writing to a client
// that stopped reading would otherwise hold its stream (and whatever it is
// waiting for) until the TCP connection dies.
type sseWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	timeout time.Duration
	started bool
}

func newSSEWriter(w http.ResponseWriter, timeout time.Duration) *sseWriter {
	return &sseWriter{w: w, rc: http.NewResponseController(w), timeout: timeout}
}

// start sends the headers. Anything the caller wants in them goes in before.
func (s *sseWriter) start() error {
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("X-Accel-Buffering", "no") // reverse proxies must not hold the stream back
	s.started = true
	s.w.WriteHeader(http.StatusOK)
	return s.flush()
}

// event writes one JSON-RPC frame as a "message" event.
func (s *sseWriter) event(frame []byte) error {
	// SSE is line-oriented. Frames this package marshals are compact, but a
	// handler may return a pretty-printed json.RawMessage; compact it rather than
	// emit a newline that would end the event early.
	if bytes.ContainsAny(frame, "\r\n") {
		var b bytes.Buffer
		if err := json.Compact(&b, frame); err != nil {
			return err
		}
		frame = b.Bytes()
	}
	buf := make([]byte, 0, len(frame)+32)
	buf = append(buf, "event: message\ndata: "...)
	buf = append(buf, frame...)
	buf = append(buf, '\n', '\n')
	return s.write(buf)
}

// comment writes a keep-alive. Proxies close idle streams; a failed write is how
// a dead client is noticed.
func (s *sseWriter) comment() error { return s.write([]byte(": keep-alive\n\n")) }

func (s *sseWriter) write(p []byte) error {
	s.deadline()
	if _, err := s.w.Write(p); err != nil {
		return err
	}
	return s.flush()
}

func (s *sseWriter) flush() error {
	s.deadline()
	return s.rc.Flush()
}

func (s *sseWriter) deadline() {
	if s.timeout > 0 {
		_ = s.rc.SetWriteDeadline(time.Now().Add(s.timeout)) // ErrNotSupported just means no deadline
	}
}

// finish clears the write deadline. net/http only resets it per request when the
// server has a WriteTimeout; without this a stale deadline would fail the next
// request on a kept-alive connection.
func (s *sseWriter) finish() { _ = s.rc.SetWriteDeadline(time.Time{}) }
