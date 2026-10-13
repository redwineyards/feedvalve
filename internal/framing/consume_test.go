package framing

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/model"
)

// waitFor spins until cond holds or the deadline passes, yielding rather than
// sleeping.
//
// Every concurrency test here waits on an observable -- Parks(), a delivery
// channel -- instead of sleeping for a guessed interval. A sleep long enough to
// be reliable makes the suite slow, and a sleep short enough to be fast makes
// it flaky; neither actually proves the edge was reached.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// TestConsumerParksWhenTheRingIsEmpty proves the park happens at all.
//
// Without this, every other concurrency test in this file could pass while the
// consumer busy-spun forever -- which would be correct, and would burn 25 % of
// the partition's entitled capacity, the thing Vol 0 section 9.5.4 rejects
// busy-waiting over.
func TestConsumerParksWhenTheRingIsEmpty(t *testing.T) {
	p, c := newTestRing(t, 4)

	done := make(chan error, 1)
	go func() { done <- c.Drain(func(*model.Datagram) error { return nil }) }()

	waitFor(t, "the consumer to park on an empty ring", func() bool { return c.Parks() > 0 })

	p.Close()
	if err := <-done; err != nil {
		t.Errorf("Drain = %v, want nil", err)
	}
}

// TestPublishWakesAConsumerThatHasAlreadyParked is the liveness test for the
// commonest real sequence: the feed goes quiet, the consumer drains to empty and
// parks, and the next datagram arrives ~400 microseconds later. If that publish
// does not wake the consumer, the datagram waits for the one after it.
//
// BE CLEAR ABOUT WHAT THIS DOES NOT PROVE. It is not a gate against the
// conditional send of Vol 0 section 9.5.4, and it was written believing it was.
// Mutation testing settled it: reintroducing the broken conditional form and
// running this test, it still passes. The reason is structural -- waiting for
// Parks() to rise is the only way to know the consumer reached the park, and
// that same wait guarantees the consumer's head store is visible, so the
// producer reads a FRESH head and the conditional send fires correctly. The
// stale-head interleaving needs the producer's load to race the consumer's
// store, which cannot be forced from a test without instrumenting the publish
// path itself.
//
// TestTheWakeSendIsUnconditional in ring_source_test.go is the gate that
// actually holds that property, and it is a source scan for exactly this reason.
// This test is still worth having: it proves the park and wake path works at
// all, which no source scan can.
func TestPublishWakesAConsumerThatHasAlreadyParked(t *testing.T) {
	p, c := newTestRing(t, 2)

	delivered := make(chan int64, 4)
	done := make(chan error, 1)
	go func() {
		done <- c.Drain(func(d *model.Datagram) error {
			delivered <- d.RecvMono
			return nil
		})
	}()

	first := dg(0)
	if !p.TryPublish(&first) {
		t.Fatal("TryPublish refused the first item")
	}
	select {
	case got := <-delivered:
		if got != 0 {
			t.Fatalf("first delivery = %d, want 0", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first datagram was never delivered")
	}

	// The consumer has stored head for item 0. Wait for it to park, which is
	// the state in which the stale-head read would lose the next wakeup.
	waitFor(t, "the consumer to park after draining", func() bool { return c.Parks() > 0 })

	second := dg(1)
	if !p.TryPublish(&second) {
		t.Fatal("TryPublish refused the second item")
	}

	select {
	case got := <-delivered:
		if got != 1 {
			t.Errorf("second delivery = %d, want 1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("THE SECOND DATAGRAM WAS NEVER DELIVERED.\n" +
			"A publish into a ring whose consumer had already parked did not wake it, " +
			"so the park and wake path is broken outright -- every datagram after a " +
			"quiet moment would wait for the one behind it. Check that TryPublish " +
			"stores tail BEFORE it sends (announce, then signal) and that the wake " +
			"channel has capacity 1.")
	}

	p.Close()
	if err := <-done; err != nil {
		t.Errorf("Drain = %v, want nil", err)
	}
}

// TestSpuriousWakeIsHarmless. The unconditional send means tokens arrive for
// publishes the consumer was already awake for, and Close deposits one too. The
// park must treat a token as a hint and re-read the ring -- level plus edge --
// so a spurious token costs one wasted pass and nothing else.
func TestSpuriousWakeIsHarmless(t *testing.T) {
	p, c := newTestRing(t, 4)

	// Deposit a token with the ring empty: a park must not treat it as data.
	select {
	case c.r.wake <- struct{}{}:
	default:
		t.Fatal("could not seed a token into an empty cap-1 channel")
	}
	if !c.wakePendingForTest() {
		t.Fatal("the seeded token is not buffered")
	}

	done := make(chan error, 1)
	n := 0
	go func() {
		done <- c.Drain(func(*model.Datagram) error { n++; return nil })
	}()

	waitFor(t, "the consumer to consume the spurious token and park", func() bool {
		return c.Parks() > 0
	})
	if n != 0 {
		t.Errorf("the callback ran %d times on an empty ring", n)
	}

	p.Close()
	if err := <-done; err != nil {
		t.Errorf("Drain = %v", err)
	}
}

// TestWakeChannelCoalesces pins the capacity-1 buffer. Many publishes into a
// ring with no consumer must leave exactly one token, not a backlog -- the
// channel is an edge signal, and a buffer that grew would make the consumer
// take one wasted pass per publish.
func TestWakeChannelCoalesces(t *testing.T) {
	p, c := newTestRing(t, 8)
	for i := range 8 {
		d := dg(i)
		if !p.TryPublish(&d) {
			t.Fatalf("TryPublish refused item %d", i)
		}
	}
	if got := len(c.r.wake); got != 1 {
		t.Errorf("wake channel holds %d tokens after 8 publishes, want 1. The "+
			"capacity-1 buffer is the LEVEL half of level-plus-edge and must coalesce", got)
	}
}

// TestCloseWakesAParkedConsumer is the shutdown half of announce-then-signal.
// A Close that only set the flag would leave the consumer parked forever, and
// the process would hang at shutdown rather than drain.
func TestCloseWakesAParkedConsumer(t *testing.T) {
	p, c := newTestRing(t, 4)

	done := make(chan error, 1)
	go func() { done <- c.Drain(func(*model.Datagram) error { return nil }) }()
	waitFor(t, "the consumer to park", func() bool { return c.Parks() > 0 })

	// Remove the token so Close must deposit its own; otherwise a leftover
	// token could wake the consumer and hide a Close that never signals.
	c.drainWakeForTest()

	p.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Drain = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wake the parked consumer: the process would hang at " +
			"shutdown instead of draining (Vol 0 section 9.5.7)")
	}
}

// TestClosePublishedRaceLosesNothingMidStream.
//
// The producer publishes in a tight loop while the main goroutine closes. The
// correct assertion is a strict PREFIX with no internal gaps: the one item
// whose closed-check read false just before Close's store may or may not be
// accepted, which is a documented window on a contract-violating path. What
// must never happen is a gap in the middle, which would be real loss.
func TestClosePublishedRaceLosesNothingMidStream(t *testing.T) {
	p, c := newTestRing(t, 64)

	var published atomic.Int64
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := 0; ; i++ {
			d := dg(i)
			// Retry the SAME item until it lands. Advancing i on a full ring
			// would skip datagrams in the test itself and then blame the ring.
			for !p.TryPublish(&d) {
				if p.Closed() {
					return
				}
				runtime.Gosched()
			}
			published.Store(int64(i) + 1)
		}
	}()

	// progress is read by this goroutine while the consumer appends, so the
	// slice itself must not be: an atomic counter is the only thing safe to
	// poll. Reading len(consumed) here would be a data race in the TEST, which
	// -race would report against the ring.
	var progress atomic.Int64
	var consumed []int64
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- c.Drain(func(d *model.Datagram) error {
			consumed = append(consumed, d.RecvMono)
			progress.Add(1)
			return nil
		})
	}()

	waitFor(t, "the stream to get going", func() bool { return progress.Load() > 100 })
	p.Close()
	<-producerDone
	if err := <-consumerDone; err != nil {
		t.Fatalf("Drain = %v", err)
	}

	for i, got := range consumed {
		if got != int64(i) {
			t.Fatalf("consumed[%d] = %d, want %d: a gap mid-stream is real loss, not "+
				"the documented one-item window at close", i, got, i)
		}
	}
	if int64(len(consumed)) > published.Load() {
		t.Errorf("consumed %d items but only %d were published",
			len(consumed), published.Load())
	}
	if len(consumed) < 100 {
		t.Errorf("only %d items made it through; the test did not exercise anything",
			len(consumed))
	}
}

// TestASecondConsumerIsRejected is Vol 0 section 9.5.5 enforced at runtime.
func TestASecondConsumerIsRejected(t *testing.T) {
	p, c := newTestRing(t, 4)

	done := make(chan error, 1)
	go func() { done <- c.Drain(func(*model.Datagram) error { return nil }) }()
	waitFor(t, "the first consumer to start", func() bool { return c.Parks() > 0 })

	if err := c.Drain(func(*model.Datagram) error { return nil }); !errors.Is(err, ErrMultipleConsumers) {
		t.Errorf("a second Drain returned %v, want ErrMultipleConsumers", err)
	}

	p.Close()
	if err := <-done; err != nil {
		t.Errorf("the first Drain = %v, want nil", err)
	}
}

// TestDrainStartedInALoopRejectsAllButOne is the shape Vol 0 section 9.5.5
// actually fears: "a source scan that fails the build if the consumer is
// started in a loop". The scan belongs in P1-10 with the other gates; this is
// the runtime guard, which is the part that cannot be refactored away.
func TestDrainStartedInALoopRejectsAllButOne(t *testing.T) {
	p, c := newTestRing(t, 16)

	const consumers = 4
	var wg sync.WaitGroup
	var rejections atomic.Int64
	errs := make(chan error, consumers)
	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Drain(func(*model.Datagram) error { return nil })
			if errors.Is(err, ErrMultipleConsumers) {
				rejections.Add(1)
			}
			errs <- err
		}()
	}

	// Wait for every loser to have been REJECTED before closing, not merely for
	// the winner to park. Each goroutine calls Drain exactly once and the only
	// outcomes are win or reject, so three rejections proves all four have
	// called. Closing earlier would let the winner return, release the guard,
	// and allow the stragglers to drain cleanly in sequence -- which is legal
	// behaviour and would make this test assert nothing.
	waitFor(t, "every other consumer to be rejected", func() bool {
		return rejections.Load() == consumers-1
	})
	p.Close()
	wg.Wait()
	close(errs)

	rejected, clean := 0, 0
	for err := range errs {
		switch {
		case errors.Is(err, ErrMultipleConsumers):
			rejected++
		case err == nil:
			clean++
		default:
			t.Errorf("unexpected Drain error: %v", err)
		}
	}
	if clean != 1 {
		t.Errorf("%d consumers drained cleanly, want exactly 1. A second consumer "+
			"reorders datagrams relative to arrival, which silently demotes the "+
			"whole ordering story to the auto-disabling sequence guard "+
			"(Vol 0 section 9.5.5)", clean)
	}
	if rejected != consumers-1 {
		t.Errorf("%d consumers were rejected, want %d", rejected, consumers-1)
	}
}

// TestDrainCanBeCalledAgainAfterItReturns. The guard is released on return, or
// a decoder that restarted -- and every sequential test in this file -- would
// be locked out.
func TestDrainCanBeCalledAgainAfterItReturns(t *testing.T) {
	p, c := newTestRing(t, 4)
	d := dg(1)
	p.TryPublish(&d)
	p.Close()

	for attempt := range 2 {
		if err := c.Drain(func(*model.Datagram) error { return nil }); err != nil {
			t.Errorf("attempt %d: Drain = %v, want nil", attempt, err)
		}
	}
}

// TestParkAndWakeUnderOneProcessor is a genuine liveness gate on the spin
// bound, not a formality: with a single P, an unbounded spin never yields and
// the consumer would hang forever instead of parking. It is also the closest
// this host gets to the entitled-capacity pressure of the real partition.
func TestParkAndWakeUnderOneProcessor(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)

	p, c := newTestRing(t, 2)
	delivered := make(chan int64, 16)
	done := make(chan error, 1)
	go func() {
		done <- c.Drain(func(d *model.Datagram) error {
			delivered <- d.RecvMono
			return nil
		})
	}()

	for i := range 8 {
		d := dg(i)
		for !p.TryPublish(&d) {
			runtime.Gosched() // ring full with one P; let the consumer run
		}
		select {
		case got := <-delivered:
			if got != int64(i) {
				t.Fatalf("delivery %d = %d", i, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("item %d never arrived with GOMAXPROCS=1; the spin must be "+
				"bounded and the consumer must actually park", i)
		}
	}

	p.Close()
	if err := <-done; err != nil {
		t.Errorf("Drain = %v", err)
	}
}
