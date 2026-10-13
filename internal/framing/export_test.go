package framing

import (
	"time"

	"feedvalve/internal/model"
)

// Test-only helpers. They live in a _test.go file so they are absent from the
// production build: nothing outside Drain may dequeue, because Drain is where
// the three correctness premises of the wakeup live, and a second dequeue path
// in the shipped binary would be an invitation to bypass them.

// recvForTest dequeues one datagram without the spin, the park or the drain
// obligation. It is the bare index algebra, for tests that want to step the
// ring one slot at a time and for the benchmark, which must not allocate a
// closure per iteration.
//
// It deliberately does NOT take the single-consumer guard: callers are
// single-goroutine tests, and taking it would stop a test from using recv and
// Drain in the same function.
func (c *Consumer) recvForTest() (model.Datagram, bool) {
	r := c.r
	h := r.head.Load()
	if h == r.tail.Load() {
		return model.Datagram{}, false
	}
	d := r.slots[h&r.mask]
	r.head.Store(h + 1)
	return d, true
}

// wakePendingForTest reports whether a wake token is buffered. Used to assert
// that the capacity-1 channel coalesces rather than accumulating, and that
// Close deposits one.
func (c *Consumer) wakePendingForTest() bool { return len(c.r.wake) > 0 }

// drainWakeForTest removes a buffered token, so a test can set up a precise
// park state.
func (c *Consumer) drainWakeForTest() {
	select {
	case <-c.r.wake:
	default:
	}
}

func nowForTest() time.Time { return time.Now() }
