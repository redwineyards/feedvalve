package framing

import (
	"errors"
	"testing"
	"time"

	"feedvalve/internal/model"
)

// withFakeSince replaces the clock for the duration of a test, so budget
// expiry is exercised without sleeping for one.
func withFakeSince(t *testing.T, fn func(time.Time) time.Duration) *int {
	t.Helper()
	calls := 0
	old := since
	since = func(t0 time.Time) time.Duration {
		calls++
		return fn(t0)
	}
	t.Cleanup(func() { since = old })
	return &calls
}

// TestPublishWithinFastPathReadsNoClock is Vol 1 section 10.5 property 3,
// applied to the publish instead of the shard send.
//
// At the occupancy Vol 0 section 9.5.6 expects -- "the receiver's budget should
// be almost unused", because publishing is a 1 KB copy and an atomic store --
// essentially every publish takes the fast path. A time.Since there would add
// tens of thousands of clock reads a second to measure a quantity that is
// always zero, and would make the benchmark meaningless as well.
func TestPublishWithinFastPathReadsNoClock(t *testing.T) {
	calls := withFakeSince(t, func(time.Time) time.Duration { return 0 })

	p, _ := newTestRing(t, 8)
	for i := range 8 {
		d := dg(i)
		if err := p.PublishWithin(&d, time.Now(), 50*time.Millisecond); err != nil {
			t.Fatalf("item %d: %v", i, err)
		}
	}
	if *calls != 0 {
		t.Errorf("the clock was read %d times on the fast path, want 0", *calls)
	}
}

// TestPublishWithinExpiresWhenTheRingStaysFull.
//
// Expiry means "QD is full AND the decoder is not draining -- the decoder is
// wedged or starved" (Vol 0 section 9.5.6). It is fatal to the process by req
// section 10.2, since FeedValve discards no unit anywhere -- but it is returned
// as an error here, never acted on, because os.Exit would skip the defers that
// close the database.
func TestPublishWithinExpiresWhenTheRingStaysFull(t *testing.T) {
	const budget = 50 * time.Millisecond
	withFakeSince(t, func(time.Time) time.Duration { return budget }) // already spent

	p, _ := newTestRing(t, 2)
	for i := range 2 {
		d := dg(i)
		if !p.TryPublish(&d) {
			t.Fatalf("TryPublish refused item %d into an empty ring", i)
		}
	}

	over := dg(99)
	err := p.PublishWithin(&over, time.Now(), budget)
	if !errors.Is(err, model.ErrStallBudgetExpired) {
		t.Errorf("PublishWithin into a full ring = %v, want "+
			"model.ErrStallBudgetExpired. There is no drop path: req section 10.2 "+
			"permits exactly one response to a critical-path queue overflowing, and "+
			"it is to stop the process", err)
	}
	if got := p.Fulls(); got == 0 {
		t.Error("Fulls() is zero after a full-ring publish; qd_send_blocked would " +
			"report nothing and section 9.5.6's decoder signal would be lost")
	}
}

// TestPublishWithinSucceedsOnceSpaceAppears proves the retry loop actually
// retries rather than failing on the first full observation.
func TestPublishWithinSucceedsOnceSpaceAppears(t *testing.T) {
	const budget = 50 * time.Millisecond
	p, c := newTestRing(t, 1)

	first := dg(1)
	if !p.TryPublish(&first) {
		t.Fatal("TryPublish refused into an empty capacity-1 ring")
	}

	// The clock advances only a little per read, so the loop gets several
	// attempts before the budget runs out. The consumer frees the slot on the
	// third read.
	reads := 0
	withFakeSince(t, func(time.Time) time.Duration {
		reads++
		if reads == 3 {
			if _, ok := c.recvForTest(); !ok {
				t.Error("the ring was unexpectedly empty")
			}
		}
		return time.Duration(reads) * time.Millisecond
	})

	second := dg(2)
	if err := p.PublishWithin(&second, time.Now(), budget); err != nil {
		t.Errorf("PublishWithin = %v, want nil once a slot was freed", err)
	}
}

func TestPublishWithinReportsClosed(t *testing.T) {
	p, _ := newTestRing(t, 4)
	p.Close()
	d := dg(1)
	if err := p.PublishWithin(&d, time.Now(), time.Second); !errors.Is(err, ErrClosed) {
		t.Errorf("PublishWithin after Close = %v, want ErrClosed", err)
	}
}

// TestPublishWithinUsesTheCallersStartTime pins Vol 1 section 10.5 property 1:
// ONE budget per read-loop iteration, shared across every unit the datagram
// fans out. "A per-unit deadline would let a single datagram stall for
// 35 x 50 ms and satisfy its own rule the whole way down."
//
// So t0 is the caller's and PublishWithin must not substitute its own: a
// publish that reset the clock would be exactly that per-unit deadline.
func TestPublishWithinUsesTheCallersStartTime(t *testing.T) {
	var seen []time.Time
	withFakeSince(t, func(t0 time.Time) time.Duration {
		seen = append(seen, t0)
		return time.Hour // expire immediately
	})

	p, _ := newTestRing(t, 1)
	fill := dg(0)
	p.TryPublish(&fill)

	t0 := time.Now().Add(-time.Minute) // a deliberately old origin
	d := dg(1)
	if err := p.PublishWithin(&d, t0, time.Second); !errors.Is(err, model.ErrStallBudgetExpired) {
		t.Fatalf("PublishWithin = %v, want expiry", err)
	}
	if len(seen) == 0 {
		t.Fatal("the clock was never read on the full path")
	}
	if !seen[0].Equal(t0) {
		t.Errorf("elapsed time was measured from %v, want the caller's %v", seen[0], t0)
	}
}
