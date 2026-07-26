//go:build linux

package mcp

import (
	"errors"
	"net"
	"syscall"
)

// peerCredentials reads the kernel-attested identity of the connecting
// process. The values come from the kernel at connect() time, not from the
// peer, so they cannot be spoofed by the client. Note that the PID is
// advisory for authorisation purposes: it can be recycled after the peer
// exits, so authorise on UID/GID and treat PID as an audit hint only.
func peerCredentials(c net.Conn) (*PeerCred, error) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return nil, errors.New("mcp: peer credentials require a unix socket")
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return nil, err
	}
	var (
		cred    *syscall.Ucred
		credErr error
	)
	ctlErr := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if ctlErr != nil {
		return nil, ctlErr
	}
	if credErr != nil {
		return nil, credErr
	}
	return &PeerCred{PID: cred.Pid, UID: cred.Uid, GID: cred.Gid}, nil
}
