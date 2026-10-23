//go:build feedvalve_assert

package mirror

import (
	"strings"
	"testing"
	"time"
)

// TestG_ASSERT_1_ShardHalfSecondGoroutinePanics is gate G-ASSERT-1's shard
// half (V4:1763), under -tags feedvalve_assert.
//
// SETUP: the test goroutine holds the shard's loop body (owner.enter), as a
// Run parked in select does.
//
// STIMULUS: a second goroutine calls Run on the same shard.
//
// ASSERTION: it panics with the G-ASSERT-1 message before touching anything.
//
// Non-vacuity: once the first owner leaves, Run on another goroutine runs and
// returns nil through the drain, no panic.
//
// WOULD HAVE CAUGHT: two goroutines driving one shard's state -- the
// ownership violation the lock-free design rests on never happening (Vol 0
// section 9.1), on a target with no race detector.
func TestG_ASSERT_1_ShardHalfSecondGoroutinePanics(t *testing.T) {
	r := newRunRig(t, rigOpts{})
	runOn := func() (panicked any, err error) {
		out := make(chan struct{})
		go func() {
			defer close(out)
			defer func() { panicked = recover() }()
			err = r.s.Run()
		}()
		select {
		case <-out:
		case <-time.After(5 * time.Second):
			t.Fatal("Run neither panicked nor returned: the ownership assertion is not armed, or blocks")
		}
		return panicked, err
	}

	// in is closed first, so a Run that skipped the assertion returns nil
	// through the drain -- a clean failure below -- rather than parking.
	r.closeIn()
	r.s.owner.enter(r.s.ID)
	p, _ := runOn()
	if s, _ := p.(string); !strings.Contains(s, "G-ASSERT-1") {
		t.Fatalf("second goroutine: recovered %v, want the G-ASSERT-1 panic", p)
	}
	r.s.owner.exit()

	if p, err := runOn(); p != nil || err != nil {
		t.Fatalf("non-vacuity: a sole owner's Run panicked %v, returned %v", p, err)
	}
	r.done <- nil // Run returned; the rig's cleanup waits on done
}
