package obs

import "sync/atomic"

// The three instrument shapes of Vol 0 section 8.1, and no others. There are no
// percentiles and no histograms here: a p99 needs a reservoir or a bucketed
// histogram, which is precisely the instrumentation req section 8 defers.
//
// All three are addressable values living inside the registry's slabs, never
// copied out. atomic.Uint64 and atomic.Int64 carry a noCopy field, so go vet
// reports a copy as a lock copy -- which is the diagnostic we want, because a
// copied counter increments a value nothing ever emits.

// Counter is shape C: a monotonic total, added to by the goroutine that owns
// the event, with no lock and no channel.
//
// Emission publishes the cumulative total AND the delta since the last
// emission. That is not redundancy. There is no metrics system downstream to
// differentiate a counter by hand (Vol 4 section 6.7), so a total alone would
// leave every rate question to arithmetic at 03:00.
type Counter struct{ v atomic.Uint64 }

// Add increments the counter. One atomic add, no allocation; this is what the
// receive path calls at 10,000 packets/s.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Load reads the cumulative total.
func (c *Counter) Load() uint64 { return c.v.Load() }

// Max is shape M: a compare-and-swap-if-greater high-water mark, reset to zero
// at emission, so the published value is the maximum observed during the
// interval.
//
// The reset is the whole point and it is why these names MUST end _max or
// _max_ns (section 8.1, Vol 4 section 10.1). A high-water mark that is never
// reset answers "what is the worst this process has ever seen", which after a
// week of uptime is a number from last Tuesday. The question an operator asks
// is "how close to the cliff are we now".
type Max struct{ v atomic.Uint64 }

// Observe records n if it exceeds the current mark.
//
// The common case is a value below the mark, which costs one atomic load and
// no write at all. The loop retries only when a concurrent Observe won the
// race, and it re-reads rather than assuming: two goroutines both raising the
// mark must leave the larger value behind, not the later one.
func (m *Max) Observe(n uint64) {
	for {
		cur := m.v.Load()
		if n <= cur {
			return
		}
		if m.v.CompareAndSwap(cur, n) {
			return
		}
	}
}

// LoadAndReset returns the interval maximum and resets the mark to zero. Called
// only by the stats goroutine, once per emission.
func (m *Max) LoadAndReset() uint64 { return m.v.Swap(0) }

// Load reads the mark without resetting it. For tests and for the health line,
// which must not consume an interval maximum that the home group line is also
// going to publish.
func (m *Max) Load() uint64 { return m.v.Load() }

// Gauge is shape G: a value published by the goroutine that OWNS the state it
// describes, and read by the stats goroutine.
//
// This shape exists because of a race, not for symmetry. HLD section 10.1 says
// "two instrument shapes, and only two", then lists shard_depth, dirty_depth,
// cohort_depth, rows_inflight, writers_busy, circuit_state and session_state
// and calls them "sampled at emission". dirty_depth and cohort_depth are len()
// of a shard-owned slice: the stats goroutine cannot read them without a data
// race, and this platform has no race detector (req section 4), so the race
// would never be caught where it runs. Shape G resolves it -- the owner Stores
// at a point it already visits, one extra store per epoch rather than per unit,
// and the stats goroutine reads the atomic.
//
// batchq_depth is the exception that proves the rule: len(chan) is race-free,
// so it genuinely is sampled at emission.
//
// Signed, because Add(-1) on an occupancy that has been decremented once too
// often must read as negative rather than wrapping to 18 quintillion. A gauge
// that goes negative is a defect worth seeing, not one worth hiding.
type Gauge struct{ v atomic.Int64 }

// Store publishes a value the owner recomputed, such as a ring length.
func (g *Gauge) Store(n int64) { g.v.Store(n) }

// Add adjusts an occupancy the owner increments and decrements, such as
// writers_busy.
func (g *Gauge) Add(d int64) { g.v.Add(d) }

// Load samples the gauge.
func (g *Gauge) Load() int64 { return g.v.Load() }
