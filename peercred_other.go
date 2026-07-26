//go:build !linux

package mcp

import (
	"errors"
	"net"
)

// peerCredentials is unavailable off Linux: SO_PEERCRED is a Linux facility and a
// standard-library-only build has no portable equivalent (macOS/BSD would need
// golang.org/x/sys). Callers get an error, so UID/GID admission — which needs
// kernel-attested credentials — fails closed (see Server.admit); socket-file
// permissions and umask remain the access control on these platforms.
func peerCredentials(net.Conn) (*PeerCred, error) {
	return nil, errors.New("mcp: peer credentials unsupported on this platform (Linux only)")
}
