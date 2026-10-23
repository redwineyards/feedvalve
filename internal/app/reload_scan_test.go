package app_test

import (
	"strings"
	"testing"
)

// TestG_BLD_3_ScanCoversTheReloadDriver is the non-vacuity half of G-BLD-3 for
// the SIGHUP path.
//
// SETUP: the scan's own file list. ASSERTION: it parsed internal/app/reload.go,
// and that file calls neither the process-exit function nor a fatal logger.
// WOULD HAVE CAUGHT: a move of the reload driver out of the scanned tree, or an
// exit on a "cannot reload" branch that would skip the drain.
func TestG_BLD_3_ScanCoversTheReloadDriver(t *testing.T) {
	seen := false
	for _, f := range scanTree(t) {
		if f.rel != "internal/app/reload.go" {
			continue
		}
		seen = true
		for _, c := range f.calls {
			if (c.pkg == "os" && c.sel == "Exit") || strings.HasPrefix(c.sel, "Fatal") {
				t.Errorf("%s: %s.%s in the reload driver", c.pos, c.pkg, c.sel)
			}
		}
	}
	if !seen {
		t.Error("the G-BLD-3 scan did not parse internal/app/reload.go")
	}
}
