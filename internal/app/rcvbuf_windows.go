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
	return sockoptInt(c, func(fd uintptr) (int, error) {
		return syscall.GetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
}
