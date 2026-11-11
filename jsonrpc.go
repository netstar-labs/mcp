package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// JSONRPCVersion is the only version accepted on the wire.
const JSONRPCVersion = "2.0"

// Standard JSON-RPC 2.0 error codes plus the MCP-reserved range.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603

	// Implementation-defined server errors (-32000..-32099).
	CodeNotInitialized   = -32002
	CodeRequestTooLarge  = -32003
	CodeResourceNotFound = -32004 // used by resources/read per MCP convention
	CodeResourceLimit    = -32005 // a per-session quota was exhausted

	// CodeRequestCancelled is the code a transport answers with for a request
	// the peer cancelled before it completed (the LSP value, which MCP follows).
	CodeRequestCancelled = -32800
)

// Structural limits applied to every inbound message. These are deliberately
// not tunable: no legitimate peer approaches them, and making them adjustable
// would only create a way to misconfigure the parser.
const (
	// maxIDBytes bounds the JSON-RPC id. Ids become map keys, so an unbounded
	// id is an unbounded allocation under peer control.
	maxIDBytes = 512

	// maxMethodBytes bounds the method name. Real names are under 40 bytes.
	maxMethodBytes = 128

	// maxLogFieldBytes bounds any peer-controlled string reaching a log line.
	maxLogFieldBytes = 256
)

// RPCError is the JSON-RPC error object.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("jsonrpc error %d: %s", e.Code, e.Message)
}

// Errorf builds an RPCError with a formatted message.
func Errorf(code int, format string, args ...any) *RPCError {
	return &RPCError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// WithData attaches an arbitrary JSON payload to the error. Marshal failures
// are swallowed: an error object must never itself fail to serialise.
func (e *RPCError) WithData(v any) *RPCError {
	if b, err := marshal(v); err == nil {
		e.Data = slices.Clone(b)
	}
	return e
}

// wireMessage is the permissive decode target. Every inbound frame is decoded
// into this shape first and then classified; the distinction between an absent
// "id" and a literal null "id" is preserved, which JSON-RPC requires.
type wireMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error"`
}

type msgKind int

const (
	kindInvalid msgKind = iota
	kindRequest
	kindNotification
	kindResponse
)

func (m *wireMessage) kind() msgKind {
	hasID := len(m.ID) > 0 && !bytes.Equal(m.ID, nullLiteral)
	switch {
	case m.Method != "" && hasID:
		return kindRequest
	case m.Method != "" && !hasID:
		return kindNotification
	case m.Method == "" && hasID && (m.Result != nil || m.Error != nil):
		return kindResponse
	default:
		return kindInvalid
	}
}

// validate enforces the structural limits. It runs before the message reaches
// any handler, registry lookup or map key.
func (m *wireMessage) validate() *RPCError {
	if len(m.ID) > maxIDBytes {
		return Errorf(CodeInvalidRequest, "id exceeds %d bytes", maxIDBytes)
	}
	if len(m.Method) > maxMethodBytes {
		return Errorf(CodeInvalidRequest, "method name exceeds %d bytes", maxMethodBytes)
	}
	return nil
}

// mayAnswer reports whether an error reply to this message is permitted. Only
// a request has someone waiting on it; anything shaped like a response (an
// error or result, with or without an id) must never be answered, because the
// error a server sends for an unreadable request is itself such a message and
// two servers answering each other's errors would never stop. A notification
// has no recipient for a reply either.
func (m *wireMessage) mayAnswer() bool {
	switch m.kind() {
	case kindRequest:
		return true
	case kindInvalid:
		return m.Method == "" && m.Result == nil && m.Error == nil
	}
	return false
}

// checkVersion rejects a message whose "jsonrpc" member is not "2.0", whatever
// its kind. It returns the id the error reply carries: the request's own, so
// the caller can match it, and null for everything else, which has none. The
// error does not quote the peer's string, which has no length bound but the
// frame's.
func (m *wireMessage) checkVersion() (json.RawMessage, *RPCError) {
	if m.JSONRPC == JSONRPCVersion {
		return nil, nil
	}
	id := json.RawMessage(nullLiteral)
	if m.kind() == kindRequest {
		id = m.ID
	}
	return id, Errorf(CodeInvalidRequest, "jsonrpc must be %q", JSONRPCVersion)
}

var nullLiteral = []byte("null")

// Outbound shapes. These are separate types so that required members are
// always emitted: a response must carry "result" or "error", never neither and
// never both.
type outRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type outNotification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type outResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
}

type outError struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Error   *RPCError       `json:"error"`
}

// idKey normalises a request ID into a map key. JSON-RPC allows strings and
// numbers; 7 and "7" are distinct IDs, so the discriminator is retained.
//
// Integers are formatted through AppendInt rather than the float path, which
// keeps large ids exact and keeps the common case at one allocation.
// Non-integral numbers fall through to float normalisation so that 7, 7.0 and
// 7e0 all collide, as a correlating peer expects.
func idKey(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}

	if trimmed[0] == '"' {
		// A string without escapes can be sliced directly; only escaped forms
		// need the decoder.
		if len(trimmed) >= 2 && trimmed[len(trimmed)-1] == '"' && !bytes.ContainsRune(trimmed, '\\') {
			var b strings.Builder
			b.Grow(len(trimmed))
			b.WriteString("s:")
			b.Write(trimmed[1 : len(trimmed)-1])
			return b.String()
		}
		var s string
		if err := json.Unmarshal(trimmed, &s); err == nil {
			return "s:" + s
		}
		return "r:" + string(trimmed)
	}

	var buf [32]byte
	ts := string(trimmed)
	if n, err := strconv.ParseInt(ts, 10, 64); err == nil {
		return string(strconv.AppendInt(append(buf[:0], 'n', ':'), n, 10))
	} else if errors.Is(err, strconv.ErrRange) {
		// A syntactically-integer id beyond int64 range: key it by its exact
		// digits. The float path below would collapse distinct large integers
		// (2^63 and 2^63+1 share one float64), cross-wiring their correlation.
		return "n:" + ts
	}
	if f, err := strconv.ParseFloat(ts, 64); err == nil {
		// Integral floats normalise to the integer form so 7 and 7.0 agree.
		// Only in-range values are converted: out of range, the result of int64(f)
		// is implementation-defined, and 2^63 aliased 2^63-1 on arm64 but not amd64.
		const two63 = 1 << 63
		if f >= -two63 && f < two63 {
			if n := int64(f); float64(n) == f {
				return string(strconv.AppendInt(append(buf[:0], 'n', ':'), n, 10))
			}
		}
		return string(strconv.AppendFloat(append(buf[:0], 'n', ':'), f, 'g', -1, 64))
	}
	return "r:" + ts
}

// encBuf pairs a buffer with the encoder bound to it, so neither is
// reallocated per message.
type encBuf struct {
	buf bytes.Buffer
	enc *json.Encoder
}

var encPool = sync.Pool{
	New: func() any {
		e := new(encBuf)
		e.enc = json.NewEncoder(&e.buf)
		e.enc.SetEscapeHTML(false)
		return e
	},
}

// maxPooledBuffer caps what goes back into the pool, so one huge message does
// not pin megabytes for the process lifetime.
const maxPooledBuffer = 1 << 20

// marshal encodes without HTML escaping. encoding/json escapes <, > and & by
// default, which is pointless noise in a protocol payload and mangles URIs
// carried in resource links.
//
// Only the encode path returns a freshly allocated slice (and returns its
// scratch buffer to the pool). The two fast paths do NOT allocate: a nil/empty
// value returns the shared nullLiteral, and a json.RawMessage is passed through
// unchanged — so callers must treat the result as read-only (mutating it would
// corrupt nullLiteral or the caller's own buffer).
func marshal(v any) ([]byte, error) {
	if v == nil {
		return nullLiteral, nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		if len(raw) == 0 {
			return nullLiteral, nil
		}
		return raw, nil
	}

	e := encPool.Get().(*encBuf)
	e.buf.Reset()
	err := e.enc.Encode(v)
	if err != nil {
		encPool.Put(e)
		return nil, err
	}
	// Encode appends a newline; the line framer forbids embedded newlines.
	b := bytes.TrimRight(e.buf.Bytes(), "\n")
	out := make([]byte, len(b))
	copy(out, b)

	if e.buf.Cap() <= maxPooledBuffer {
		encPool.Put(e)
	}
	return out, nil
}

// encodeParams marshals the params of an outgoing request or notification, and
// maps "no params" (nil, which marshals to null) to an absent member: null is not a
// legal params value, and the Session, the Client and the HTTP client must agree on
// the shape they put on the wire.
func encodeParams(v any) (json.RawMessage, error) {
	b, err := marshal(v)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(b, nullLiteral) {
		return nil, nil
	}
	return b, nil
}

// unmarshalParams decodes request params, tolerating an absent params member.
// A null or missing params is treated as an empty object so that handlers with
// all-optional arguments do not have to special-case it.
func unmarshalParams(raw json.RawMessage, dst any) *RPCError {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), nullLiteral) {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return Errorf(CodeInvalidParams, "invalid params: %v", err)
	}
	return nil
}

// jsonCompact is a thin alias kept here so framing.go need not import
// encoding/json for a single call.
func jsonCompact(dst *bytes.Buffer, src []byte) error { return json.Compact(dst, src) }

// sanitize makes a peer-controlled string safe to place in a log line.
// Control characters — CR and LF above all — are replaced, and the result is
// truncated on a rune boundary. Without this, a client that names itself
// "x\n2026/01/01 mcpd auth: access granted" forges audit records.
func sanitize(s string) string {
	if len(s) > maxLogFieldBytes {
		s = s[:maxLogFieldBytes]
		for len(s) > 0 && !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
		s += "..."
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return strings.Map(func(r rune) rune {
				if r < 0x20 || r == 0x7f {
					return '.'
				}
				return r
			}, s)
		}
	}
	return s
}
