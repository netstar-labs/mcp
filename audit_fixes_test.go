package mcp

import (
	"encoding/base64"
	"io"
	"strings"
	"testing"
)

// TestListCacheBoundedAgainstForgedCursors is the regression for the cache-growth
// DoS: forged out-of-range offsets and non-canonical encodings of the same offset
// must not each mint a permanent cache entry. Before the fix, `canonical` was
// decided from the pre-clamp offset and the cache was keyed by the raw cursor, so
// this grew unbounded.
func TestListCacheBoundedAgainstForgedCursors(t *testing.T) {
	s := New(Options{Name: "t", PageSize: 2})
	cache := &listCache{}
	count := func() int { return 1 } // a one-item registry
	build := func(lo, hi int, next string) any {
		return map[string]any{"lo": lo, "hi": hi, "next": next}
	}
	call := func(cursor string) {
		if _, rerr := s.listPage("tools", cache, cursor, count, build); rerr != nil {
			// decode errors are fine (some forged strings are simply invalid); the
			// point is the cache must not grow.
			return
		}
	}
	forge := func(raw string) string { return base64.RawURLEncoding.EncodeToString([]byte(raw)) }

	// Out-of-range but page-aligned offsets — each would have been "canonical".
	for i := 1; i <= 500; i++ {
		call(encodeCursor("tools", i*2)) // 2,4,6,… all > n=1
	}
	// Non-canonical encodings that all decode to offset 0.
	for _, raw := range []string{"tools:00", "tools:+0", "tools:000000", "tools:0"} {
		call(forge(raw))
	}
	// The legitimate canonical pages.
	call("")                       // offset 0
	call(encodeCursor("tools", 0)) // offset 0 again

	if n := cache.pageCount(); n > 2 {
		t.Fatalf("cache grew to %d entries under forged cursors; want <= 2 (bounded to real pages)", n)
	}
}

// TestIDKeyLargeIntegersStayDistinct is the regression for the correlation-key
// collision: integer ids beyond int64 range must not collapse through float64.
func TestIDKeyLargeIntegersStayDistinct(t *testing.T) {
	a := idKey([]byte("9223372036854775808")) // 2^63
	b := idKey([]byte("9223372036854775809")) // 2^63 + 1
	if a == b {
		t.Fatalf("distinct large-integer ids collided: both -> %q", a)
	}
	// And the normalisation the fix must preserve:
	if idKey([]byte("7")) != idKey([]byte("7.0")) {
		t.Error("7 and 7.0 should share a key")
	}
	if idKey([]byte("42")) == idKey([]byte("43")) {
		t.Error("42 and 43 must differ")
	}
}

// TestDuplicateContentLengthRejected is the regression for accepting a second
// Content-Length header (last-wins). It must now be an error.
func TestDuplicateContentLengthRejected(t *testing.T) {
	in := "Content-Length: 3\r\nContent-Length: 3\r\n\r\nabc"
	f := NewFramer(strings.NewReader(in), io.Discard, FramerOptions{Mode: FramingHeaders})
	if _, err := f.ReadMessage(); err == nil {
		t.Fatal("duplicate Content-Length must be rejected")
	}
}
