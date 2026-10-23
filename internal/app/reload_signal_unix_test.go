//go:build !windows

package app

import (
	"os"
	"syscall"
	"testing"
)

// hupSupported says whether this platform can deliver a SIGHUP to a process.
const hupSupported = true

// sendHUP delivers a real SIGHUP to this test process. The
// caller must already hold a signal.Notify for it, or the default disposition
// ends the test binary.
func sendHUP(t *testing.T) {
	t.Helper()
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatalf("kill -HUP self: %v", err)
	}
}
