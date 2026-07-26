//go:build !windows

package mcp

import (
	"io/fs"
	"sync"
	"syscall"
)

// umaskMu serialises the umask window. umask is a process-wide property, so
// any file created concurrently by another goroutine during this window is
// also affected. The window is a few microseconds around bind(2), and the
// alternative — create-then-chmod — leaves the socket world-accessible for the
// same duration, which is the thing worth avoiding.
var umaskMu sync.Mutex

// withUmask runs fn with the process umask set so that a file created with
// mode 0777 lands with exactly the requested permission bits.
func withUmask(mode fs.FileMode, fn func() error) error {
	umaskMu.Lock()
	defer umaskMu.Unlock()

	old := syscall.Umask(int(^mode & 0o777))
	defer syscall.Umask(old)
	return fn()
}
