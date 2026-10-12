package framing

import (
	"runtime"
	"testing"

	"feedvalve/internal/model"
)

// TestQDPropertyStrictFIFO is the card's central requirement: nothing lost,
// nothing duplicated, strict FIFO across millions of items.
//
// ONE comparison per item proves all three. The producer stamps a dense,
// strictly increasing sequence; the consumer asserts each delivery equals its
// own strictly increasing counter. A lost item makes the next delivery too
// large, a duplicate makes it too small, and a reordering makes it one or the
// other. No map and no bitset, so verification is O(1) per item and allocates
// nothing -- which matters when the item count is in the millions.
//
// Run it under -race AND plain. The two find different things: -race finds a
// plain store where an atomic belongs, or the slot-lifetime bug if the copy in
// Drain is ever removed; the FIFO assertion finds everything that is wrong
// about the index algebra, and is the only one of the two that runs on the AIX
// target, which has no race detector at all (req section 4).
func TestQDPropertyStrictFIFO(t *testing.T) {
	items := 2_000_000
	if testing.Short() {
		items = 100_000
	}

	for _, tc := range []struct {
		name string
		// capacity trades which edge gets exercised: a tiny ring fills
		// constantly, a large one is empty constantly.
		capacity int
		// yield makes the producer give up its slot between publishes, so the
		// consumer drains to empty and parks instead of always finding data.
		yield bool
		// wantFulls requires the full-ring edge to have been reached.
		wantFulls bool
	}{
		{name: "producer_fast_tiny_ring", capacity: 4, wantFulls: true},
		{name: "consumer_fast_large_ring", capacity: 1024, yield: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, c := newTestRing(t, tc.capacity)

			// First mismatch only. A t.Errorf per item would produce millions
			// of lines and obscure the one that matters.
			type mismatch struct {
				at        int
				where     string
				gotRecv   int64
				wantRecv  int64
				truncated bool
			}
			var bad mismatch
			delivered := 0

			done := make(chan error, 1)
			go func() {
				done <- c.Drain(func(d *model.Datagram) error {
					if bad.where == "" {
						if where, ok := checkSeq(d, delivered); !ok {
							bad = mismatch{
								at: delivered, where: where,
								gotRecv: d.RecvMono, wantRecv: int64(delivered),
							}
						}
					}
					delivered++
					return nil
				})
			}()

			for i := range items {
				d := dg(i)
				for !p.TryPublish(&d) {
					runtime.Gosched()
				}
				if tc.yield {
					runtime.Gosched()
				}
			}
			p.Close()
			if err := <-done; err != nil {
				t.Fatalf("Drain = %v, want nil", err)
			}

			if bad.where != "" {
				t.Errorf("item %d: %s wrong (RecvMono got %d, want %d).\n"+
					"A delivery out of sequence means one of three things, all of "+
					"which this single assertion catches: a datagram was lost (the "+
					"next one reads too high), delivered twice (too low), or "+
					"reordered. Strict FIFO over a dense sequence is the whole "+
					"no-loss/no-duplication property.",
					bad.at, bad.where, bad.gotRecv, bad.wantRecv)
			}
			if delivered != items {
				t.Errorf("delivered %d of %d datagrams", delivered, items)
			}

			// EDGE COVERAGE. Without these the test could pass millions of
			// items having never filled the ring and never parked -- proving
			// the index algebra and nothing about the wakeup, which is where
			// the real risk is.
			if c.Parks() == 0 {
				t.Errorf("the consumer never parked, so this run exercised no part "+
					"of the wakeup path. %d items at capacity %d", items, tc.capacity)
			}
			if tc.wantFulls && p.Fulls() == 0 {
				t.Errorf("the ring never filled at capacity %d, so the full-ring edge "+
					"was not exercised", tc.capacity)
			}
			t.Logf("%d items, capacity %d: %d parks, %d full encounters",
				items, tc.capacity, c.Parks(), p.Fulls())
		})
	}
}

// TestQDPropertyAtCapacityOne is the degenerate ring, and the harshest wakeup
// test available: every publish fills it and every consume empties it, so the
// producer and consumer hand off through the park and wake path on essentially
// every single item.
func TestQDPropertyAtCapacityOne(t *testing.T) {
	items := 200_000
	if testing.Short() {
		items = 20_000
	}
	p, c := newTestRing(t, 1)

	delivered := 0
	firstBad := -1
	done := make(chan error, 1)
	go func() {
		done <- c.Drain(func(d *model.Datagram) error {
			if firstBad < 0 && d.RecvMono != int64(delivered) {
				firstBad = delivered
			}
			delivered++
			return nil
		})
	}()

	for i := range items {
		d := dg(i)
		for !p.TryPublish(&d) {
			runtime.Gosched()
		}
	}
	p.Close()
	if err := <-done; err != nil {
		t.Fatalf("Drain = %v", err)
	}

	if firstBad >= 0 {
		t.Errorf("first out-of-sequence delivery at item %d", firstBad)
	}
	if delivered != items {
		t.Errorf("delivered %d of %d", delivered, items)
	}
	if c.Parks() == 0 || p.Fulls() == 0 {
		t.Errorf("a capacity-1 ring should hit both edges constantly: %d parks, "+
			"%d full encounters", c.Parks(), p.Fulls())
	}
}
