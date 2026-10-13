package app

import "net"

// sockoptInt reads one integer socket option, running get on the connection's
// file descriptor under Control.
//
// The boilerplate lives here, untagged, so each per-platform readBufferSize
// holds only the expression that genuinely differs between them: the type the
// platform's syscall package wants the fd as. Before this, the SyscallConn
// error path, the (size, optErr) pair and the Control wrapper were duplicated
// per platform -- so every fix to the error handling had to be made twice, and
// the copy that only exists for the development host is the one that gets
// forgotten.
func sockoptInt(c *net.UDPConn, get func(fd uintptr) (int, error)) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var (
		v      int
		optErr error
	)
	if err := raw.Control(func(fd uintptr) {
		v, optErr = get(fd)
	}); err != nil {
		return 0, err
	}
	return v, optErr
}
