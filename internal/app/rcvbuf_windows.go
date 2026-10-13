//go:build windows

package app

import (
	"net"
	"syscall"
)

// readBufferSize reads back the socket's actual receive buffer. See the doc
// comment on the non-Windows variant for why the EFFECTIVE size is the one that
// matters rather than the requested one.
//
// Windows exists here only so the lifecycle is testable and buildable on the
// development host; the deployment target is AIX/ppc64.
func readBufferSize(c *net.UDPConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var (
		size   int
		optErr error
	)
	if err := raw.Control(func(fd uintptr) {
		size, optErr = syscall.GetsockoptInt(
			syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	}); err != nil {
		return 0, err
	}
	return size, optErr
}
