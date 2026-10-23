package app

import "testing"

// TestNextRecvMonoIsStrictlyIncreasing pins the receipt clock's one property:
// a reading equal to or behind the previous one is bumped past it, and a
// reading ahead of it is kept as read. Without the bump, two datagrams read in
// the same timer tick tie, and the ordering guard drops the second as a
// re-delivery (phase3-tasks seam 9).
func TestNextRecvMonoIsStrictlyIncreasing(t *testing.T) {
	for _, c := range []struct{ now, last, want int64 }{
		{100, 0, 100},   // first datagram
		{150, 100, 150}, // clock advanced: kept as read
		{100, 100, 101}, // same tick: bumped
		{90, 100, 101},  // never goes backwards
		{0, 0, 1},       // a zero reading still moves past the zero last
	} {
		if got := nextRecvMono(c.now, c.last); got != c.want {
			t.Errorf("nextRecvMono(%d, %d) = %d, want %d", c.now, c.last, got, c.want)
		}
	}

	// A run of identical readings stays strictly increasing.
	last := int64(0)
	for i := 0; i < 1000; i++ {
		next := nextRecvMono(500, last)
		if next <= last {
			t.Fatalf("step %d: %d does not follow %d", i, next, last)
		}
		last = next
	}
}
