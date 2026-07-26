package mcp

import (
	"os"
	"testing"
)

// shortTempDir is t.TempDir() with a short base path. A unix socket path must fit
// the platform's sun_path limit (~104 bytes on macOS, 108 on Linux), which the
// default per-test TMPDIR on macOS (`/var/folders/…/TestLongName…/001`) can
// exceed. Tests that bind a socket use this instead of t.TempDir().
func shortTempDir(t testing.TB) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "mcp")
	if err != nil {
		return t.TempDir() // fall back where /tmp isn't available; fine if TMPDIR is short
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}
