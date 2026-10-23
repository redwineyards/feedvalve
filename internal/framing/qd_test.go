package framing

import (
	"errors"
	"testing"
	"time"

	"feedvalve/internal/model"
)

// datagramN is a datagram whose bytes and receipt time both identify it, so a
// test can tell which one arrived and that it arrived intact.
func datagramN(i int) model.Datagram {
	var d model.Datagram
	d.RecvMono = int64(i)
	d.N = 3
	d.Buf[0], d.Buf[1], d.Buf[2] = byte(i), byte(i>>8), 0xA5
	return d
}

func newQD(t *testing.T, capacity int) (*Producer, *Consumer) {
	t.Helper()
	p, c, err := NewQD(capacity)
	if err != nil {
		t.Fatalf("NewQD(%d): %v", capacity, err)
	}
	return p, c
}

// drainAll collects every datagram Drain delivers, by value.
func drainAll(t *testing.T, c *Consumer) []model.Datagram {
	t.Helper()
	var got []model.Datagram
	if err := c.Drain(func(d *model.Datagram) error {
		got = append(got, *d)
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	return got
}

func TestNewQDRejectsACapacityBelowOne(t *testing.T) {
	for _, n := range []int{0, -1} {
		if _, _, err := NewQD(n); !errors.Is(err, ErrCapacity) {
			t.Errorf("NewQD(%d) = %v, want ErrCapacity", n, err)
		}
	}
}

// TestQDIsFIFOAndDrainsEverythingAfterClose is the property the decoder's
// ordering rests on (ADR-007 mechanism 1) and the drain obligation of Vol 0
// section 9.5.7 together: every datagram published before Close is delivered,
// intact, in publish order, and only then does Drain return.
func TestQDIsFIFOAndDrainsEverythingAfterClose(t *testing.T) {
	const n = 100
	p, c := newQD(t, n)
	for i := range n {
		d := datagramN(i)
		if !p.TryPublish(&d) {
			t.Fatalf("publish %d refused with room for %d", i, n)
		}
	}
	if got := c.Len(); got != n {
		t.Fatalf("Len = %d, want %d", got, n)
	}
	p.Close()

	got := drainAll(t, c)
	if len(got) != n {
		t.Fatalf("drained %d, want %d", len(got), n)
	}
	for i, d := range got {
		if want := datagramN(i); d != want {
			t.Fatalf("datagram %d = %+v..., want %+v...", i, d.Bytes(), want.Bytes())
		}
	}
}

func TestTryPublishRefusesWhenFull(t *testing.T) {
	p, c := newQD(t, 2)
	d := datagramN(1)
	for i := range 2 {
		if !p.TryPublish(&d) {
			t.Fatalf("publish %d into a capacity-2 QD was refused", i)
		}
	}
	if p.TryPublish(&d) {
		t.Fatal("a third publish into a full capacity-2 QD was accepted")
	}
	if got := c.Len(); got != 2 {
		t.Errorf("Len = %d, want 2", got)
	}
}

// TestPublishAfterCloseIsAnErrorNotAPanic: a send on a closed channel panics,
// and Close is idempotent, so both are guarded.
func TestPublishAfterCloseIsAnErrorNotAPanic(t *testing.T) {
	p, c := newQD(t, 4)
	p.Close()
	p.Close()
	d := datagramN(1)
	if p.TryPublish(&d) {
		t.Error("TryPublish after Close returned true")
	}
	if err := p.PublishWithin(&d, time.Now(), time.Second); !errors.Is(err, ErrClosed) {
		t.Errorf("PublishWithin after Close = %v, want ErrClosed", err)
	}
	if got := drainAll(t, c); len(got) != 0 {
		t.Errorf("an empty closed QD drained %d datagrams", len(got))
	}
}

func TestDrainReturnsTheCallbackErrorAndStops(t *testing.T) {
	p, c := newQD(t, 4)
	for i := range 3 {
		d := datagramN(i)
		p.TryPublish(&d)
	}
	stop := errors.New("decoder failed")
	calls := 0
	err := c.Drain(func(*model.Datagram) error {
		calls++
		return stop
	})
	if !errors.Is(err, stop) || calls != 1 {
		t.Errorf("Drain = %v after %d calls, want the callback's error after 1", err, calls)
	}
	if got := c.Len(); got != 2 {
		t.Errorf("Len = %d after the error, want the 2 undelivered datagrams left", got)
	}
}

// TestASecondConsumerIsRejected: Vol 0 section 9.5.5, exactly one consumer. A
// second concurrent Drain is refused rather than allowed to reorder delivery;
// a later Drain, after the first returned, is fine.
func TestASecondConsumerIsRejected(t *testing.T) {
	p, c := newQD(t, 4)
	entered := make(chan struct{})
	release := make(chan struct{})
	first := make(chan error, 1)
	d := datagramN(1)
	p.TryPublish(&d)
	go func() {
		first <- c.Drain(func(*model.Datagram) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	if err := c.Drain(func(*model.Datagram) error { return nil }); !errors.Is(err, ErrMultipleConsumers) {
		t.Errorf("a concurrent second Drain = %v, want ErrMultipleConsumers", err)
	}
	close(release)
	p.Close()
	if err := <-first; err != nil {
		t.Fatalf("first Drain = %v", err)
	}
	if err := c.Drain(func(*model.Datagram) error { return nil }); err != nil {
		t.Errorf("a Drain after the first returned = %v, want nil", err)
	}
}

// TestPublishWithinExpiresWhenQDStaysFull: the budget is real, and expiry is
// the sentinel the receiver turns into a terminal condition.
func TestPublishWithinExpiresWhenQDStaysFull(t *testing.T) {
	p, _ := newQD(t, 1)
	d := datagramN(1)
	p.TryPublish(&d)

	const budget = 20 * time.Millisecond
	t0 := time.Now()
	err := p.PublishWithin(&d, t0, budget)
	if !errors.Is(err, model.ErrStallBudgetExpired) {
		t.Fatalf("PublishWithin on a full QD = %v, want ErrStallBudgetExpired", err)
	}
	if took := time.Since(t0); took < budget {
		t.Errorf("expired after %v, before the %v budget", took, budget)
	}
}

// TestPublishWithinUsesTheCallersStartTime: the budget is per read-loop
// iteration (Vol 1 section 10.5 property 1), so a t0 already older than the
// budget expires at once instead of granting a fresh wait.
func TestPublishWithinUsesTheCallersStartTime(t *testing.T) {
	p, _ := newQD(t, 1)
	d := datagramN(1)
	p.TryPublish(&d)

	start := time.Now()
	err := p.PublishWithin(&d, start.Add(-time.Second), 50*time.Millisecond)
	if !errors.Is(err, model.ErrStallBudgetExpired) {
		t.Fatalf("PublishWithin = %v, want ErrStallBudgetExpired", err)
	}
	if took := time.Since(start); took > 25*time.Millisecond {
		t.Errorf("waited %v for a budget that was already spent", took)
	}
}

func TestPublishWithinSucceedsOnceSpaceAppears(t *testing.T) {
	p, c := newQD(t, 1)
	first, second := datagramN(1), datagramN(2)
	p.TryPublish(&first)

	done := make(chan error, 1)
	go func() { done <- p.PublishWithin(&second, time.Now(), 5*time.Second) }()

	got := make(chan []model.Datagram, 1)
	go func() {
		var seen []model.Datagram
		_ = c.Drain(func(d *model.Datagram) error {
			seen = append(seen, *d)
			return nil
		})
		got <- seen
	}()
	if err := <-done; err != nil {
		t.Fatalf("PublishWithin = %v once the decoder made room", err)
	}
	p.Close()
	if seen := <-got; len(seen) != 2 || seen[0] != first || seen[1] != second {
		t.Errorf("drained %d datagrams, want first then second", len(seen))
	}
}

// TestTryPublishAndDrainAllocateNothing: QD is on the receive path G-ALLOC-1
// holds at zero allocations.
func TestTryPublishAndDrainAllocateNothing(t *testing.T) {
	p, c := newQD(t, 1)
	d := datagramN(1)
	allocs := testing.AllocsPerRun(100, func() {
		p.TryPublish(&d)
		<-c.q.ch
	})
	if allocs != 0 {
		t.Errorf("a publish and receive allocate %.1f times, want 0", allocs)
	}
}
