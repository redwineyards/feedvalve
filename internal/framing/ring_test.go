package framing

import (
	"errors"
	"testing"
	"unsafe"

	"feedvalve/internal/model"
)

// newTestRing builds a ring and fails the test if construction did.
func newTestRing(t *testing.T, capacity int) (*Producer, *Consumer) {
	t.Helper()
	p, c, err := NewRing(capacity)
	if err != nil {
		t.Fatalf("NewRing(%d): %v", capacity, err)
	}
	return p, c
}

// dg builds a datagram stamped with seq, at three positions spread across the
// 1040-byte element.
//
// Three positions and not one, because a single field at offset 0 would not
// notice a partially published slot. If tail.Store ever became a plain
// assignment, a consumer could see a slot whose first field was written and
// whose last was not -- and checking RecvMono alone would pass. This samples
// tearing rather than proving its absence, which is the honest claim.
func dg(seq int) model.Datagram {
	var d model.Datagram
	d.RecvMono = int64(seq)
	d.N = int32(seq & 0x3FF)
	d.Buf[0] = byte(seq)
	d.Buf[len(d.Buf)-1] = byte(seq >> 8)
	return d
}

// checkSeq verifies a datagram carries seq at all three positions.
func checkSeq(d *model.Datagram, seq int) (string, bool) {
	switch {
	case d.RecvMono != int64(seq):
		return "RecvMono", false
	case d.N != int32(seq&0x3FF):
		return "N", false
	case d.Buf[0] != byte(seq):
		return "Buf[0]", false
	case d.Buf[len(d.Buf)-1] != byte(seq>>8):
		return "Buf[1023]", false
	}
	return "", true
}

func TestNewRingRejectsNonPowerOfTwoCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1, 3, 1000, 5000, 8191} {
		if _, _, err := NewRing(capacity); !errors.Is(err, ErrCapacityNotPowerOfTwo) {
			t.Errorf("NewRing(%d) error = %v, want ErrCapacityNotPowerOfTwo. The ring "+
				"wraps by masking, and a non-power-of-two does not waste space -- it "+
				"silently corrupts the index and starts returning the wrong datagram "+
				"(Vol 0 section 9.5.3)", capacity, err)
		}
	}
}

// TestNewRingAcceptsCapacityOneDeliberately records a divergence from config.
//
// config enforces 1024-65536 on runtime.datagramQueueCapacity (rule V-RANGE);
// the ring's own precondition is only power-of-two and at least 1. Duplicating
// the 1024 floor here would force every full-ring and park/wake test to be an
// 8192-item affair. Capacity 1 is in fact the sharpest test of those edges,
// since every publish fills the ring and every consume empties it.
func TestNewRingAcceptsCapacityOneDeliberately(t *testing.T) {
	for _, capacity := range []int{1, 2, 4, 8192} {
		p, _, err := NewRing(capacity)
		if err != nil {
			t.Errorf("NewRing(%d): %v", capacity, err)
			continue
		}
		if got := p.Cap(); got != capacity {
			t.Errorf("Cap() = %d, want %d", got, capacity)
		}
	}
}

// TestHeadAndTailAreOneCacheLineApart is the runtime companion to the build
// gate at the bottom of ring.go.
//
// False sharing has no correctness symptom and there is no profiler on the AIX
// target (Vol 4 section 6.7), so this would never be found by observation. The
// build gate is the real defence; this test exists so the failure says what it
// means.
func TestHeadAndTailAreOneCacheLineApart(t *testing.T) {
	gap := unsafe.Offsetof(ring{}.tail) - unsafe.Offsetof(ring{}.head)
	if gap != cacheLine {
		t.Errorf("tail is %d bytes after head, want %d. The producer and the consumer "+
			"would write the same cache line on every operation, and Vol 0 section "+
			"9.5.3 says false sharing then costs more than the lock this structure "+
			"exists to avoid. Note the ppc64 line is 128, not 64.", gap, cacheLine)
	}
}

func TestPublishThenConsumePreservesTheDatagram(t *testing.T) {
	p, c := newTestRing(t, 4)
	in := dg(12345)
	if !p.TryPublish(&in) {
		t.Fatal("TryPublish into an empty ring returned false")
	}
	if got := p.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}

	p.Close()
	var got []int
	err := c.Drain(func(d *model.Datagram) error {
		if where, ok := checkSeq(d, 12345); !ok {
			t.Errorf("%s did not survive the round trip", where)
		}
		got = append(got, int(d.RecvMono))
		return nil
	})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("drained %d datagrams, want 1", len(got))
	}
}

// TestRingFillsToCapacityAndRefusesMore pins the full test. Single-goroutine,
// so there is no scheduling involved and a failure is unambiguous.
func TestRingFillsToCapacityAndRefusesMore(t *testing.T) {
	const capacity = 4
	p, c := newTestRing(t, capacity)

	for i := range capacity {
		d := dg(i)
		if !p.TryPublish(&d) {
			t.Fatalf("TryPublish refused item %d in a ring of capacity %d", i, capacity)
		}
	}
	if got := p.Len(); got != capacity {
		t.Errorf("Len() = %d, want %d", got, capacity)
	}

	over := dg(99)
	if p.TryPublish(&over) {
		t.Error("a full ring accepted another datagram; the index would wrap onto a " +
			"slot the consumer has not read and the datagram in it would be lost")
	}
	if got := p.Fulls(); got != 1 {
		t.Errorf("Fulls() = %d, want 1: the receiver feeds this into qd_send_blocked, "+
			"which Vol 0 section 9.5.6 reads as a signal about the DECODER", got)
	}

	// One slot freed means exactly one more accepted.
	p.Close()
	drained := 0
	_ = c.Drain(func(d *model.Datagram) error {
		drained++
		return errStopDrain // stop after one
	})
	if drained != 1 {
		t.Fatalf("drained %d, want 1", drained)
	}
	if p.TryPublish(&over) {
		t.Error("publish after Close succeeded")
	}
}

var errStopDrain = errors.New("stop")

// TestIndicesWrapPastCapacity exercises the mask. Four times round the ring, so
// head and tail both pass the wrap point repeatedly while staying monotonic.
//
// Monotonic uint64 indices are what keep the full/empty test unambiguous: at
// 2,400 datagrams/s a uint64 wraps in about 7x10^8 years, so tail - head is
// exact. Storing MASKED indices in the atomics instead would make tail == head
// mean either empty or full, and that is the bug this test would catch.
func TestIndicesWrapPastCapacity(t *testing.T) {
	const capacity = 4
	p, c := newTestRing(t, capacity)

	seq := 0
	want := 0
	for round := range 4 * capacity {
		// Fill, then drain, so the indices advance past the wrap boundary.
		for range capacity {
			d := dg(seq)
			if !p.TryPublish(&d) {
				t.Fatalf("round %d: TryPublish refused seq %d", round, seq)
			}
			seq++
		}
		if got := p.Len(); got != capacity {
			t.Fatalf("round %d: Len() = %d, want %d", round, got, capacity)
		}
		for range capacity {
			d, ok := c.recvForTest()
			if !ok {
				t.Fatalf("round %d: ring empty with %d expected", round, capacity)
			}
			if where, ok := checkSeq(&d, want); !ok {
				t.Fatalf("round %d: %s wrong at seq %d (got RecvMono %d)",
					round, where, want, d.RecvMono)
			}
			want++
		}
		if got := p.Len(); got != 0 {
			t.Fatalf("round %d: Len() = %d after draining, want 0", round, got)
		}
	}
	if want != seq {
		t.Errorf("published %d, consumed %d", seq, want)
	}
}

// TestDrainDeliversEverythingPublishedBeforeClose is Vol 0 section 9.5.7's
// obligation, and the reason it is stated as an obligation at all: "QD must be
// DRAINED, not abandoned, or the no-loss property has a hole at shutdown
// precisely where it is easiest to overlook."
//
// Zero concurrency: fill the ring completely, close it, then drain. A Drain that
// checked closed before emptiness would return immediately and lose every one.
func TestDrainDeliversEverythingPublishedBeforeClose(t *testing.T) {
	const capacity = 8
	p, c := newTestRing(t, capacity)
	for i := range capacity {
		d := dg(i)
		if !p.TryPublish(&d) {
			t.Fatalf("TryPublish refused item %d", i)
		}
	}
	p.Close()

	want := 0
	err := c.Drain(func(d *model.Datagram) error {
		if where, ok := checkSeq(d, want); !ok {
			t.Errorf("%s wrong at seq %d", where, want)
		}
		want++
		return nil
	})
	if err != nil {
		t.Fatalf("Drain returned %v, want nil for a clean close", err)
	}
	if want != capacity {
		t.Errorf("drained %d of %d datagrams. Closing must not discard what the "+
			"receiver already published (Vol 0 section 9.5.7)", want, capacity)
	}
}

func TestPublishAfterCloseIsAnErrorNotAPanic(t *testing.T) {
	p, _ := newTestRing(t, 4)
	p.Close()

	d := dg(1)
	if p.TryPublish(&d) {
		t.Error("TryPublish succeeded after Close")
	}
	if !p.Closed() {
		t.Error("Closed() reports false after Close")
	}
	if err := p.PublishWithin(&d, nowForTest(), 0); !errors.Is(err, ErrClosed) {
		t.Errorf("PublishWithin after Close = %v, want ErrClosed. Publishing after "+
			"close is a contract violation, and it must be visible and harmless "+
			"rather than a panic on the receive goroutine", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	p, c := newTestRing(t, 2)
	p.Close()
	p.Close()
	p.Close()
	if err := c.Drain(func(*model.Datagram) error { return nil }); err != nil {
		t.Errorf("Drain after repeated Close = %v, want nil", err)
	}
}

func TestEmptyClosedRingDrainsImmediately(t *testing.T) {
	p, c := newTestRing(t, 4)
	p.Close()
	n := 0
	if err := c.Drain(func(*model.Datagram) error { n++; return nil }); err != nil {
		t.Errorf("Drain = %v, want nil", err)
	}
	if n != 0 {
		t.Errorf("delivered %d datagrams from an empty ring", n)
	}
}

// TestDrainReturnsTheCallbackError, and does NOT drain the remainder.
//
// The asymmetry is deliberate and worth pinning: drain-to-empty is an
// obligation on the clean shutdown path, whereas an error out of the decoder is
// a fatal path already heading for exit, where abandoning the remainder is
// correct.
func TestDrainReturnsTheCallbackErrorAndStops(t *testing.T) {
	p, c := newTestRing(t, 8)
	for i := range 5 {
		d := dg(i)
		p.TryPublish(&d)
	}
	p.Close()

	sentinel := errors.New("decoder failed")
	seen := 0
	err := c.Drain(func(*model.Datagram) error {
		seen++
		if seen == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("Drain = %v, want the callback's error", err)
	}
	if seen != 2 {
		t.Errorf("callback ran %d times, want 2: Drain must stop at the error", seen)
	}
	if got := c.Len(); got != 3 {
		t.Errorf("Len() = %d, want 3 left undrained", got)
	}
}
