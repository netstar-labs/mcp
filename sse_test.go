package mcp

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSSEReaderParsesEvents(t *testing.T) {
	in := "" +
		": keep-alive\n\n" + // comment only: no event
		"event: message\ndata: {\"a\":1}\n\n" +
		"data: line one\ndata: line two\n\n" + // data lines join with a newline
		"data:no-space\n\n" + // the space after the colon is optional
		"event: other\ndata: x\n\n" + // a named event is still returned, named
		"id: 7\nretry: 100\ndata: y\n\n" + // id and retry are read and ignored
		"event: orphan\n\n" + // no data: not an event
		"data: crlf\r\n\r\n" +
		"data: unterminated" // a partial event at EOF is discarded
	rd := newSSEReader(strings.NewReader(in), 1024)
	for i, want := range []struct{ event, data string }{
		{"message", `{"a":1}`},
		{"", "line one\nline two"},
		{"", "no-space"},
		{"other", "x"},
		{"", "y"},
		{"", "crlf"},
	} {
		ev, data, err := rd.next()
		if err != nil || ev != want.event || string(data) != want.data {
			t.Fatalf("event %d = (%q, %q, %v), want (%q, %q)", i, ev, data, err, want.event, want.data)
		}
	}
	if _, _, err := rd.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last event: %v, want EOF (the partial event is discarded)", err)
	}
}

// The cap is on the event, not the line, and is enforced as bytes arrive.
func TestSSEReaderCapsEventSize(t *testing.T) {
	const max = 32
	exact := "data: " + strings.Repeat("a", max) + "\n\n"
	over := "data: " + strings.Repeat("a", max+1) + "\n\n"
	split := "data: " + strings.Repeat("a", 20) + "\ndata: " + strings.Repeat("b", 20) + "\n\n" // 41 joined

	if _, data, err := newSSEReader(strings.NewReader(exact), max).next(); err != nil || len(data) != max {
		t.Fatalf("an event of exactly the cap: %d bytes, %v", len(data), err)
	}
	for name, in := range map[string]string{"one long line": over, "many short lines": split} {
		if _, _, err := newSSEReader(strings.NewReader(in), max).next(); !errors.Is(err, errSSEEventTooLarge) {
			t.Errorf("%s: err = %v, want errSSEEventTooLarge", name, err)
		}
	}
	// A line that never ends must be refused without reading it all.
	endless := io.MultiReader(strings.NewReader("data: "), strings.NewReader(strings.Repeat("x", 1<<20)))
	if _, _, err := newSSEReader(endless, max).next(); !errors.Is(err, errSSEEventTooLarge) {
		t.Errorf("an endless line: err = %v, want errSSEEventTooLarge", err)
	}
}

// A handler may return a pretty-printed json.RawMessage; a newline inside an SSE
// event would end it early and turn the rest into a different event.
func TestSSEWriterCompactsFramesWithNewlines(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := newSSEWriter(rec, 0)
	if err := sw.start(); err != nil {
		t.Fatal(err)
	}
	if err := sw.event([]byte("{\n  \"jsonrpc\": \"2.0\",\r\n  \"id\": 1\n}")); err != nil {
		t.Fatal(err)
	}
	if err := sw.comment(); err != nil {
		t.Fatal(err)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type %q", ct)
	}
	body := rec.Body.String()
	if want := "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1}\n\n: keep-alive\n\n"; body != want {
		t.Fatalf("body %q, want %q", body, want)
	}
	// And what the writer wrote, the reader reads back.
	ev, data, err := newSSEReader(strings.NewReader(body), 1024).next()
	if err != nil || ev != "message" || string(data) != `{"jsonrpc":"2.0","id":1}` {
		t.Fatalf("round trip = (%q, %q, %v)", ev, data, err)
	}
	if err := sw.event([]byte("{not json\n")); err == nil {
		t.Fatal("a frame with a newline that is not JSON cannot be compacted and must be refused")
	}
}
