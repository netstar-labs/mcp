package mcp

import (
	"bytes"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

// A1 C-10: a read deadline that exists only to bound the handshake must not
// outlive it. With IdleTimeout zero ("disabled") a Ready session was still dropped
// after HandshakeTimeout of silence.
func TestReadySessionSurvivesHandshakeTimeoutWhenIdleIsDisabled(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{HandshakeTimeout: 300 * time.Millisecond}, nil)
	c := h.client()
	time.Sleep(900 * time.Millisecond) // three handshake timeouts
	c.request(MethodPing, struct{}{})  // fails the test if the connection was dropped
}

// ...and the handshake itself is still bounded: a peer that connects and never
// speaks is dropped.
func TestHandshakeTimeoutStillDropsASilentPeer(t *testing.T) {
	h := startServer(t, Options{}, UnixOptions{HandshakeTimeout: 200 * time.Millisecond}, nil)
	c := h.dial()
	_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.r.ReadByte(); err == nil {
		t.Fatal("read a byte from a server that should have dropped a silent peer")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("the silent peer was not dropped within 3s of a 200ms handshake timeout")
	}
}

// A1 C-2: Close must not wait for a blocked write. The write is blocked because
// the peer stopped reading, and closing the connection is the only thing that ends
// it, so a Close that first takes the write lock never returns.
func TestFramerCloseDoesNotWaitForABlockedWrite(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close() // never read: net.Pipe writes block until the other end reads
	f := NewFramer(a, a, FramerOptions{Closer: a, BufferSize: 16})
	wrote := make(chan error, 1)
	go func() { wrote <- f.WriteMessage(bytes.Repeat([]byte("x"), 1<<16)) }()
	time.Sleep(100 * time.Millisecond) // let the writer block holding the write lock

	closed := make(chan struct{})
	go func() { _ = f.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked behind a write that only Close can end")
	}
	select {
	case err := <-wrote:
		if err == nil {
			t.Fatal("the blocked write reported success after the connection was closed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("closing the connection did not end the blocked write")
	}
}

// A1 C-13: header framing must allocate as the body arrives, as line framing does,
// not the declared length up front. 32 peers that each send a header declaring an
// 8 MiB body and then nothing must not cost 256 MiB.
func TestHeaderFramingAllocatesAsBytesArrive(t *testing.T) {
	const peers = 32
	hdr := "Content-Length: 8388608\r\n\r\n"
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range peers {
		f := NewFramer(strings.NewReader(hdr), &bytes.Buffer{}, FramerOptions{Mode: FramingHeaders})
		if _, err := f.ReadMessage(); err == nil {
			t.Fatal("a truncated body was accepted")
		}
	}
	runtime.ReadMemStats(&after)
	if got := after.TotalAlloc - before.TotalAlloc; got > 32<<20 {
		t.Fatalf("%d peers sending 27 bytes each cost %d MiB; allocation must follow the bytes received", peers, got>>20)
	}
}

// ...while a genuine large body still round-trips.
func TestHeaderFramingLargeBodyRoundTrip(t *testing.T) {
	body := strings.Repeat("a", 3<<20)
	var wire bytes.Buffer
	if err := NewFramer(strings.NewReader(""), &wire, FramerOptions{Mode: FramingHeaders}).WriteMessage([]byte(`"` + body + `"`)); err != nil {
		t.Fatal(err)
	}
	got, err := NewFramer(&wire, &bytes.Buffer{}, FramerOptions{Mode: FramingHeaders}).ReadMessage()
	if err != nil || len(got) != len(body)+2 {
		t.Fatalf("read %d bytes, err %v; want %d", len(got), err, len(body)+2)
	}
}

// A1 C-16: converting an out-of-range float to int64 is implementation-defined, so
// 2^63 aliased 2^63-1 on arm64 and not on amd64.
func TestIDKeyOutOfRangeFloatIsDistinctOnEveryArchitecture(t *testing.T) {
	a, b := idKey([]byte(`9223372036854775807`)), idKey([]byte(`9223372036854775808.0`))
	if a == b {
		t.Fatalf("ids 2^63-1 and 2^63.0 share the key %q", a)
	}
	// In-range integral floats still agree with the integer form.
	if idKey([]byte(`7.0`)) != idKey([]byte(`7`)) || idKey([]byte(`-9223372036854775808.0`)) != idKey([]byte(`-9223372036854775808`)) {
		t.Fatal("integral floats no longer normalise to the integer key")
	}
}
