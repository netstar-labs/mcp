package mcp

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Every function below sits on a trust boundary: bytes a peer controls. The
// property each fuzz target states is the same: never panic, never allocate past
// the bound, always terminate.

func FuzzReadFrameLines(f *testing.F) {
	for _, s := range []string{"{}\n", "", "\n\n", `{"a":1}` + "\r\n", strings.Repeat("x", 300), "no newline"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		fr := NewFramer(bytes.NewReader(in), &bytes.Buffer{}, FramerOptions{MaxMessageBytes: 128, BufferSize: 16})
		for range len(in) + 2 { // each call consumes input or ends
			msg, err := fr.ReadMessage()
			if err != nil {
				return
			}
			if len(msg) > 128 {
				t.Fatalf("frame of %d bytes past the 128 cap", len(msg))
			}
		}
	})
}

func FuzzReadFrameHeaders(f *testing.F) {
	for _, s := range []string{
		"Content-Length: 2\r\n\r\n{}",
		"Content-Length: 99999999999999999999\r\n\r\n",
		"Content-Length: 2\r\nContent-Length: 3\r\n\r\n{}",
		"Content-Length: -1\r\n\r\n",
		strings.Repeat("A: b\r\n", 40) + "\r\n",
		"",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		fr := NewFramer(bytes.NewReader(in), &bytes.Buffer{}, FramerOptions{Mode: FramingHeaders, MaxMessageBytes: 128, BufferSize: 16})
		for range len(in) + 2 {
			msg, err := fr.ReadMessage()
			if err != nil {
				return
			}
			if len(msg) > 128 {
				t.Fatalf("frame of %d bytes past the 128 cap", len(msg))
			}
		}
	})
}

func FuzzSSEReader(f *testing.F) {
	for _, s := range []string{
		"data: {}\n\n",
		"event: message\ndata: a\ndata: b\n\n",
		": keep-alive\n\n",
		"data:x",
		"data: " + strings.Repeat("y", 200) + "\n\n",
		"\n\n\n",
		"data: a\r\n\r\n",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		rd := newSSEReader(bytes.NewReader(in), 100)
		for range len(in) + 2 {
			_, data, err := rd.next()
			if err != nil {
				return
			}
			if len(data) > 100 {
				t.Fatalf("event data of %d bytes past the 100 cap", len(data))
			}
		}
	})
}

func FuzzAccepts(f *testing.F) {
	for _, s := range []string{"", "*/*", "application/json, text/event-stream", "text/*;q=0", "a;q=;;,,", ";", "application/json;q=0.5"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, header string) {
		_ = accepts(header, "application/json")
		_ = accepts(header, "text/event-stream")
	})
}

func FuzzIDKey(f *testing.F) {
	for _, s := range []string{`1`, `"a"`, `7.0`, `1e400`, `9007199254740993`, `"\ud800"`, `-0`, `null`, ``, `{}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		a := idKey(raw)
		if b := idKey(raw); a != b {
			t.Fatalf("idKey is not deterministic: %q vs %q", a, b)
		}
	})
}

func FuzzURITemplate(f *testing.F) {
	for _, s := range [][2]string{{"file:///{path}", "file:///a/b"}, {"x://{+rest}", "x://a/b/c"}, {"{a}{b}", "xy"}, {"", ""}, {"{", "{"}} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, tmpl, uri string) {
		tt, err := CompileURITemplate(tmpl)
		if err != nil {
			return
		}
		vars, ok := tt.Match(uri)
		if ok != tt.Matches(uri) {
			t.Fatalf("Match and Matches disagree on %q against %q", tmpl, uri)
		}
		_ = vars
	})
}

// FuzzHTTPPost throws arbitrary bodies and header values at the transport. It
// must answer every one with a status and never panic or hang.
func FuzzHTTPPost(f *testing.F) {
	srv := New(Options{Name: "fuzz", MaxMessageBytes: 4096, Logger: log.New(io.Discard, "", 0)})
	echoTool(srv, "echo")
	h := srv.HTTPHandler(HTTPOptions{KeepAlive: -1, MaxSessions: 8})
	f.Cleanup(func() { _ = srv.Close() })

	for _, s := range []string{
		initBody,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"ping"}`,
		`[{"jsonrpc":"2.0","id":2,"method":"ping"}]`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo"}}`,
		`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}`,
		`{"jsonrpc":"2.0","id":3,"result":{}}`,
		`{`, ``, `null`, `"x"`,
	} {
		f.Add([]byte(s), "", "")
	}
	f.Fuzz(func(t *testing.T, body []byte, sid, version string) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)).WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if strings.ContainsAny(sid, "\r\n") || strings.ContainsAny(version, "\r\n") {
			return // not a header value a real client could send
		}
		if sid != "" {
			req.Header.Set(headerSessionID, sid)
		}
		if version != "" {
			req.Header.Set(headerProtocolVersion, version)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code < 200 || rec.Code > 599 {
			t.Fatalf("status %d", rec.Code)
		}
	})
}
