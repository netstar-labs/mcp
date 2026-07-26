package mcp

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

// TestContentConstructors exercises the public Content/ResourceContents helpers —
// the shapes a tool/resource handler returns — and checks their wire encoding.
func TestContentConstructors(t *testing.T) {
	raw := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	b64 := base64.StdEncoding.EncodeToString(raw)

	if c := TextContent("hello"); c.Type != ContentTypeText || c.Text != "hello" {
		t.Errorf("TextContent = %+v", c)
	}
	if c := ImageContent(raw, "image/png"); c.Type != ContentTypeImage || c.Data != b64 || c.MIMEType != "image/png" {
		t.Errorf("ImageContent = %+v", c)
	}
	if c := AudioContent(raw, "audio/wav"); c.Type != ContentTypeAudio || c.Data != b64 || c.MIMEType != "audio/wav" {
		t.Errorf("AudioContent = %+v", c)
	}
	if c := ResourceLink("file:///x", "x", "text/plain"); c.Type != ContentTypeResourceLink || c.URI != "file:///x" || c.Name != "x" {
		t.Errorf("ResourceLink = %+v", c)
	}
	blob := BlobResource("file:///b", "application/octet-stream", raw)
	if blob.URI != "file:///b" || blob.Blob != b64 || blob.MIMEType != "application/octet-stream" {
		t.Errorf("BlobResource = %+v", blob)
	}
	if c := EmbeddedResource(blob); c.Type != ContentTypeResource || c.Resource == nil || c.Resource.Blob != b64 {
		t.Errorf("EmbeddedResource = %+v", c)
	}

	// Content must round-trip through JSON (it's the wire form).
	for _, c := range []Content{
		TextContent("hi"), ImageContent(raw, "image/png"), AudioContent(raw, "audio/wav"),
		ResourceLink("file:///x", "x", "text/plain"), EmbeddedResource(blob),
	} {
		b, err := json.Marshal(c)
		if err != nil {
			t.Fatalf("marshal %v: %v", c.Type, err)
		}
		var got Content
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("unmarshal %v: %v", c.Type, err)
		}
		if got.Type != c.Type {
			t.Errorf("round-trip type: got %q want %q", got.Type, c.Type)
		}
	}
}

func TestSessionFromContext(t *testing.T) {
	// A bare context has no session.
	if s := SessionFromContext(t.Context()); s != nil {
		t.Errorf("SessionFromContext(bare) = %p, want nil", s)
	}
}
