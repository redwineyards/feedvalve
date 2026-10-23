//go:build windows

package app

import "testing"

// hupSupported is false: Windows has no SIGHUP to deliver to a process, so the
// real-signal smoke test skips with that reason and every other test drives the
// reload function directly.
const hupSupported = false

func sendHUP(t *testing.T) {}
