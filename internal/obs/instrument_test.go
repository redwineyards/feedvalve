package obs

import (
	"sync"
	"testing"
)

func TestCounterAccumulates(t *testing.T) {
	var c Counter
	c.Add(1)
	c.Add(41)
	if got := c.Load(); got != 42 {
		t.Errorf("Load = %d, want 42", got)
	}
}

// TestMaxKeepsTheLargerValue pins the one property that distinguishes shape M
// from a plain store: two observations must leave the LARGER behind, not the
// later one. A Store would pass every other test in this file.
func TestMaxKeepsTheLargerValue(t *testing.T) {
	var m Max
	m.Observe(100)
	m.Observe(7)
	if got := m.Load(); got != 100 {
		t.Errorf("Load = %d, want 100: a lower observation must not lower the mark", got)
	}
	m.Observe(101)
	if got := m.Load(); got != 101 {
		t.Errorf("Load = %d, want 101", got)
	}
}

// TestMaxResetsAtEmission is why these names must end _max or _max_ns. A mark
// that is never reset answers "the worst this process ever saw", which after a
// week of uptime is a number from last Tuesday; the question an operator asks
// is how close to the cliff they are now.
func TestMaxResetsAtEmission(t *testing.T) {
	var m Max
	m.Observe(500)
	if got := m.LoadAndReset(); got != 500 {
		t.Fatalf("LoadAndReset = %d, want 500", got)
	}
	if got := m.Load(); got != 0 {
		t.Errorf("after LoadAndReset the mark is %d, want 0. Vol 0 section 8.1: the "+
			"published value is the maximum in the INTERVAL", got)
	}
}

func TestGaugeStoresAndAdds(t *testing.T) {
	var g Gauge
	g.Store(9)
	g.Add(-2)
	if got := g.Load(); got != 7 {
		t.Errorf("Load = %d, want 7", got)
	}
}

// TestGaugeGoesNegativeRatherThanWrapping pins the choice of int64 over
// uint64. An occupancy decremented once too often is a defect worth seeing; as
// an unsigned value it would read as 18 quintillion and look like corruption
// somewhere else entirely.
func TestGaugeGoesNegativeRatherThanWrapping(t *testing.T) {
	var g Gauge
	g.Add(-1)
	if got := g.Load(); got != -1 {
		t.Errorf("Load = %d, want -1", got)
	}
}

// TestMaxUnderConcurrentObserveKeepsTheMaximum exercises the CAS loop. There
// is no race detector on the target platform (req section 4) and none on this
// host either, so the assertion has to be on the OUTCOME rather than on the
// detector: whatever interleaving occurs, the mark must equal the largest value
// any goroutine observed.
func TestMaxUnderConcurrentObserveKeepsTheMaximum(t *testing.T) {
	var m Max
	const goroutines, each = 8, 1000

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := range uint64(each) {
				m.Observe(base*each + i)
			}
		}(uint64(g))
	}
	wg.Wait()

	want := uint64(goroutines-1)*each + each - 1
	if got := m.Load(); got != want {
		t.Errorf("Load = %d, want %d. A lost CAS must retry against the re-read "+
			"value, or the larger observation is dropped", got, want)
	}
}

// The task card's "done when" requires a benchmark showing ZERO allocations on
// the counter and max paths. These are the first benchmarks in the repository.
//
// The number that matters is allocs/op, not ns/op: Vol 0 section 8.4's MUST is
// about allocation on the receive goroutine, and ADR-006 exists because a
// per-event allocation at 10,000 messages/s is what turns instrumentation into
// a garbage source.

func BenchmarkCounterAdd(b *testing.B) {
	var c Counter
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Add(1)
	}
}

func BenchmarkMaxObserve(b *testing.B) {
	var m Max
	b.ReportAllocs()
	// Rising values, so every iteration takes the CAS path rather than the
	// cheap early return -- benchmarking the branch that does work.
	for i := 0; i < b.N; i++ {
		m.Observe(uint64(i))
	}
}

func BenchmarkMaxObserveBelowMark(b *testing.B) {
	var m Max
	m.Observe(1 << 62)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Observe(1)
	}
}

func BenchmarkGaugeStore(b *testing.B) {
	var g Gauge
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		g.Store(int64(i))
	}
}
