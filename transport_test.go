package mcp

import "testing"

// TestListenSystemdNoActivation covers the no-activation paths of the systemd
// socket-adoption entrypoint: absent, zero, or wrong-PID LISTEN_* must yield no
// listeners and no error (the server then falls back to its own listeners).
func TestListenSystemdNoActivation(t *testing.T) {
	cases := []struct{ pid, fds string }{
		{"", ""},     // not activated
		{"", "0"},    // activated with zero fds
		{"", "junk"}, // malformed count
		{"1", "1"},   // LISTEN_PID for another process (pid 1 != us)
	}
	for _, c := range cases {
		t.Setenv("LISTEN_PID", c.pid)
		t.Setenv("LISTEN_FDS", c.fds)
		ls, err := ListenSystemd()
		if err != nil || ls != nil {
			t.Errorf("ListenSystemd(pid=%q,fds=%q) = %v, %v; want nil, nil", c.pid, c.fds, ls, err)
		}
	}
}
