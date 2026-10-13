//go:build !windows

package app

import (
	"net"
	"syscall"
)

// readBufferSize reads back the socket's ACTUAL receive buffer.
//
// This matters because the kernel silently caps the requested size at
// udp_recvspace: the requested value is a wish, and only the effective one
// bounds Q0's slack -- 640 KB / 516 B is about 1,270 datagrams, about 0.53 s at
// 2,400/s, which is the arithmetic NFR4's budget is derived from. An operator
// who reads the requested number and believes it has the wrong model of how
// much slack the process has, and ASM-4 already warns the real figure may be 4x
// smaller if AIX charges mbuf clusters rather than payload bytes.
//
// Done with the stdlib syscall package through SyscallConn, not with
// golang.org/x/sys: Vol 4 section 6.7 excludes that module deliberately because
// its AIX surface is thin. Linux reports double what was requested (it adds its
// own bookkeeping allowance); AIX does not. The value is reported as the kernel
// gives it rather than halved, because halving it on one platform and not the
// other is how a figure stops being comparable to netstat.
func readBufferSize(c *net.UDPConn) (int, error) {
	return sockoptInt(c, func(fd uintptr) (int, error) {
		return syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
}
