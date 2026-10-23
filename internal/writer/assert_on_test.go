//go:build feedvalve_assert

package writer

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestG_ASSERT_1_WriterHalfSecondGoroutinePanics is gate G-ASSERT-1's writer
// half, under -tags feedvalve_assert.
//
// SETUP: a pool whose batchQ is already closed, and a writer whose loop body
// the test goroutine holds (owner.enter), as a loop parked in select does.
//
// STIMULUS: a second goroutine runs the loop of the same writer.
//
// ASSERTION: it panics with the G-ASSERT-1 message before touching anything.
//
// Non-vacuity: once the first owner leaves, the loop on another goroutine runs
// to completion (batchQ is closed) and returns without a panic.
//
// WOULD HAVE CAUGHT: two goroutines driving one writer's session, statement
// cache and binders -- single-goroutine state that nothing else protects.
func TestG_ASSERT_1_WriterHalfSecondGoroutinePanics(t *testing.T) {
	r := newPoolRig(t, rigOpts{})
	close(r.batchQ)
	r.p.batchQ = r.batchQ
	r.p.completion = nil
	w := &writer{p: r.p, id: 0, quit: make(chan struct{}), par: r.p.params()}

	runOn := func() (panicked any) {
		out := make(chan struct{})
		go func() {
			defer close(out)
			defer func() { panicked = recover() }()
			r.p.loop(context.Background(), w)
		}()
		select {
		case <-out:
		case <-time.After(5 * time.Second):
			t.Fatal("the loop neither panicked nor returned: the ownership assertion is not armed, or blocks")
		}
		return panicked
	}

	w.owner.enter(w.id)
	p := runOn()
	if s, _ := p.(string); !strings.Contains(s, "G-ASSERT-1") {
		t.Fatalf("second goroutine: recovered %v, want the G-ASSERT-1 panic", p)
	}
	w.owner.exit()

	if p := runOn(); p != nil {
		t.Fatalf("non-vacuity: a sole owner's loop panicked: %v", p)
	}
}
