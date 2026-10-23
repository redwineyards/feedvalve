package app

import (
	"testing"
	"time"
)

// TestTheGuardEvaluationIntervalFollowsAReload is the pickup test for
// ordering.guardEvalIntervalSec (Vol 4 section 2 pickup table: a reload that
// changes a timer's period takes effect within one OLD period, because the
// sleeping goroutine is still on it -- documented, not engineered around).
//
// SETUP: a graph from a file with the default 5 s evaluation interval.
// STIMULUS: the file is edited to 7 s and reload runs.
// ASSERTION: guardInterval, which runStats calls to re-arm the guard timer
// after every evaluation, returns 5 s before and 7 s after. A test override
// still wins (it is the seam the 50 ms lifecycle test relies on).
// WOULD HAVE CAUGHT: the re-arm reading the startup configuration, so the
// edited interval is ignored until a restart.
// NON-VACUITY: the 5 s read before the reload.
func TestTheGuardEvaluationIntervalFollowsAReload(t *testing.T) {
	fx := newReloadFix(t, false)
	g := fx.g
	if got := g.guardInterval(); got != 5*time.Second {
		t.Fatalf("guardInterval before the reload = %v, want 5s", got)
	}
	fx.set("ordering", "guardEvalIntervalSec", 7)
	if got := g.reload(); got != reloadApplied {
		t.Fatalf("reload = %v\n%s", got, fx.log())
	}
	if got := g.guardInterval(); got != 7*time.Second {
		t.Errorf("guardInterval after the reload = %v, want 7s", got)
	}
	g.guardEvalOverride = 50 * time.Millisecond
	if got := g.guardInterval(); got != 50*time.Millisecond {
		t.Errorf("guardInterval with the test override = %v, want 50ms", got)
	}
}
