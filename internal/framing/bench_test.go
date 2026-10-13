package framing

import (
	"testing"
	"unsafe"

	"feedvalve/internal/model"
)

// The card requires "a benchmark shows zero allocations in steady state".
// allocs/op is the number that matters, not ns/op -- the convention
// internal/obs/instrument_test.go established for the first benchmarks in this
// repository. The ring is one allocation at construction and none afterwards.

// BenchmarkPublishConsume is the steady state and the one that answers the
// card: one publish and one consume per iteration on a small ring.
//
// A small ring is the honest choice, because it means every iteration empties
// the ring and therefore every publish attempts the wake send. That is exactly
// the cost the unconditional send in TryPublish adds, so this benchmark prices
// the decision rather than hiding it.
//
// recvForTest rather than Drain, because Drain takes a callback and calling it
// inside the loop would allocate a closure per iteration and measure that
// instead.
func BenchmarkPublishConsume(b *testing.B) {
	p, c := newBenchRing(b, 4)
	d := dg(1)

	b.ReportAllocs()
	b.SetBytes(int64(unsafe.Sizeof(d)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !p.TryPublish(&d) {
			b.Fatal("ring full in a one-in-one-out benchmark")
		}
		if _, ok := c.recvForTest(); !ok {
			b.Fatal("ring empty immediately after a publish")
		}
	}
}

// BenchmarkTryPublish isolates the publish: the 1040-byte copy into the slot
// plus the tail store plus the wake send. Vol 0 section 9.5.6 describes it as
// "a 1 KB copy and an atomic store", and the copy should dominate -- which is
// why this reports bytes per second.
//
// The periodic drain is excluded from the timing rather than amortised into it,
// because otherwise this would measure the consumer too. The ring stays modest:
// an earlier version sized it to hold every iteration, which allocated a
// gigabyte and reported it as 280 B/op.
func BenchmarkTryPublish(b *testing.B) {
	const capacity = 4096
	p, c := newBenchRing(b, capacity)
	d := dg(1)

	b.ReportAllocs()
	b.SetBytes(int64(unsafe.Sizeof(d)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if p.TryPublish(&d) {
			continue
		}
		b.StopTimer()
		for {
			if _, ok := c.recvForTest(); !ok {
				break
			}
		}
		b.StartTimer()
		if !p.TryPublish(&d) {
			b.Fatal("publish failed into a freshly drained ring")
		}
	}
}

// BenchmarkConsume isolates the consumer's index algebra and the slot copy out
// -- the half BenchmarkPublishConsume measures jointly with the publish.
//
// There is deliberately no benchmark of Drain itself. Drain blocks until the
// ring is closed, so timing it needs either a concurrent producer (which
// measures the handoff and the scheduler, not the consumer) or a prefill sized
// to b.N (which needs a ring sized to b.N). An earlier attempt did the fill and
// the drain outside the b.N loop and reported 0.0009 ns/op, which is what a
// benchmark that measures nothing looks like. The cost Drain adds over this is
// one callback dispatch.
func BenchmarkConsume(b *testing.B) {
	const capacity = 4096
	p, c := newBenchRing(b, capacity)
	d := dg(1)

	b.ReportAllocs()
	b.SetBytes(int64(unsafe.Sizeof(d)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := c.recvForTest(); ok {
			continue
		}
		b.StopTimer()
		for range capacity {
			if !p.TryPublish(&d) {
				break
			}
		}
		b.StartTimer()
		if _, ok := c.recvForTest(); !ok {
			b.Fatal("consume failed from a freshly filled ring")
		}
	}
}

// BenchmarkDrainCallback measures the whole consumer path including Drain's
// callback dispatch, over a ring that is prefilled and closed so Drain returns
// rather than parking. b.N bounds the items, and the fill is not timed.
func BenchmarkDrainCallback(b *testing.B) {
	const capacity = 4096
	d := dg(1)

	b.ReportAllocs()
	b.SetBytes(int64(unsafe.Sizeof(d)))
	b.ResetTimer()

	delivered := 0
	for delivered < b.N {
		b.StopTimer()
		p, c := newBenchRing(b, capacity)
		want := 0
		for want < capacity && delivered+want < b.N {
			if !p.TryPublish(&d) {
				break
			}
			want++
		}
		p.Close()
		b.StartTimer()

		got := 0
		if err := c.Drain(func(*model.Datagram) error { got++; return nil }); err != nil {
			b.Fatalf("Drain: %v", err)
		}
		if got != want {
			b.Fatalf("drained %d, filled %d", got, want)
		}
		delivered += got
		if want == 0 {
			break
		}
	}
}

func newBenchRing(b *testing.B, capacity int) (*Producer, *Consumer) {
	b.Helper()
	p, c, err := NewRing(capacity)
	if err != nil {
		b.Fatalf("NewRing(%d): %v", capacity, err)
	}
	return p, c
}
