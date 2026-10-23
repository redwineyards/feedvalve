package obs

import (
	"strconv"
	"sync/atomic"
	"time"
)

// Reg is the instrument registry: the whole ledger, resolved, and the
// resolution surface every subsystem receives.
//
// Vol 0 section 8.4 sketches a Registry INTERFACE, as "the small interface HLD
// section 10.1 says deferred instrumentation attaches behind". It is not
// declared: it had exactly one implementation and no test double, which
// CLAUDE.md rules out, and every caller ended up type-asserting back to *Reg
// for the accessors it did not carry. The day a second backend (a histogram, a
// scrape endpoint) exists, extract the interface from this type's method set.
//
// The KEY is an ID, not a name, a deliberate departure from section 8.4's
// sketch. Its own MUST says the accessors are "never called from the receive,
// shard or writer path -- a per-event map lookup and a []Label allocation on the
// receive goroutine is exactly the cost ADR-006 exists to avoid". A name-keyed
// accessor can only honour that MUST by convention; an ID-keyed one honours it
// by construction, and a mistyped instrument becomes a compile error rather
// than a startup panic.
//
// Every accessor is startup-only and panics after Freeze (gate G-OBS-1).
// Callers resolve once and cache the returned pointer in their own struct; the
// hot path is Counter.Add, which is one atomic instruction.
//
// Reg owns no goroutine and holds no context, and that is a scope boundary
// rather than an omission. Vol 4 section 6.1 puts the 60-second ticker on the
// stats goroutine, which app starts at startup step 15; obs owns Emit and the
// stats goroutine calls it. There is nothing here to start or stop.
type Reg struct {
	dims [numDims]resolvedDim

	// base is each instrument's offset into the slab for its shape. Runtime
	// rather than constant, because {k}'s arity comes from runtime.shards --
	// which is what makes the ID constants instrument IDENTITIES rather than
	// byte offsets, the right factoring regardless.
	base [numInstruments]uint32

	// Three slabs, because Counter and Max are atomic.Uint64 while Gauge is
	// atomic.Int64. Allocated once, never grown: a slab that could grow would
	// move a pointer a shard goroutine is holding.
	counters []Counter
	maxes    []Max
	gauges   []Gauge

	unknownCodes codeTable
	oracleCodes  codeTable

	frozen      atomic.Bool
	intervalSec atomic.Uint64

	// started is the monotonic origin for uptime_seconds. time.Time, so that
	// time.Since reads the monotonic clock -- Vol 4 section 10.2 forbids
	// subtracting wall-clock readings, because an NTP step would otherwise
	// make every elapsed measurement wrong and can yield a negative duration.
	started time.Time

	// Emission state, owned exclusively by the goroutine that calls Emit.
	// None of it is atomic and none of it needs to be: Emit has exactly one
	// caller, the stats goroutine, and Vol 0 section 9.1 makes that goroutine
	// the sole owner of "the previous-emission snapshot of every counter".
	//
	// curC, curM, curG and deltaC hold the current snapshot, which is what the
	// render pass reads -- never the instruments themselves. curC and curG
	// double as the previous-emission snapshot: each is read for the delta or
	// the movement comparison immediately before it is overwritten, so a
	// separate prev copy would hold the same value at every observable instant.
	curC   []uint64
	curM   []uint64
	curG   []int64
	deltaC []uint64
	buf    []byte

	// sinceEmit counts intervals since each group last appeared, for Vol 4
	// section 6.1 rule 3. A count rather than a sequence difference, so there
	// is no wraparound to reason about.
	sinceEmit [numGroups]uint32

	// prefix is the pre-rendered "name=" or "name{labels}=" for every slot of
	// every instrument, built in Freeze so emission never formats a label.
	// Indexed the same way the slabs are, per shape.
	prefixC []string
	prefixM []string
	prefixG []string

	buckets [numClasses]bucket
}

// New allocates the whole ledger against a resolved configuration.
//
// This is startup step 9 of Vol 4 section 1.4, and its position there is the
// point: instruments are resolved once, before any goroutine exists, so a
// collision is a startup error rather than a silent double-count.
func New(o Options) *Reg {
	r := &Reg{
		dims:         resolveDims(o),
		unknownCodes: newCodeTable(unknownTranscodeSlots),
		oracleCodes:  newCodeTable(oracleErrorSlots + 1),
		started:      time.Now(),
	}
	r.intervalSec.Store(60)

	var nc, nm, ng uint32
	for id := range numInstruments {
		d := &r.dims[ledger[id].dim]
		n := d.size * d.stride
		switch ledger[id].shape {
		case shapeCounter:
			r.base[id] = nc
			nc += n
		case shapeMax:
			r.base[id] = nm
			nm += n
		case shapeGauge:
			r.base[id] = ng
			ng += n
		}
	}

	r.counters = make([]Counter, nc)
	r.maxes = make([]Max, nm)
	r.gauges = make([]Gauge, ng)
	return r
}

// SetIntervalSec records the emission interval for the interval_s= field.
//
// The interval is reloadable (stats.intervalSec), and the lifecycle goroutine
// that applies a reload is the owner of this value. Atomic because the reload
// lands on the lifecycle goroutine while Emit reads it on the stats goroutine.
func (r *Reg) SetIntervalSec(n int) {
	if n > 0 {
		r.intervalSec.Store(uint64(n))
	}
}

// Freeze ends resolution. After it, every accessor panics (gate G-OBS-1) and
// Emit becomes legal.
//
// It also pre-renders every slot's label prefix and sizes the emission buffer,
// which is what makes Emit allocation-free (gate G-OBS-2). Both are possible
// only once no further instrument can appear, so the two halves of this method
// are the same decision.
//
// app calls it at the end of startup step 9, before the first goroutine starts.
func (r *Reg) Freeze() {
	if r.frozen.Load() {
		return
	}

	r.prefixC = r.renderPrefixes(shapeCounter, len(r.counters))
	r.prefixM = r.renderPrefixes(shapeMax, len(r.maxes))
	r.prefixG = r.renderPrefixes(shapeGauge, len(r.gauges))

	r.curC = make([]uint64, len(r.counters))
	r.deltaC = make([]uint64, len(r.counters))
	r.curM = make([]uint64, len(r.maxes))
	r.curG = make([]int64, len(r.gauges))
	r.buf = make([]byte, 0, r.maxLineLen())

	// Nothing has been emitted yet, so every group is due. Starting at the
	// forced threshold makes the FIRST emission a full snapshot of all fifteen
	// groups -- a complete startup baseline -- and it staggers the forced lines
	// after that according to real activity, rather than having all fifteen
	// collide on interval ten.
	for g := range r.sinceEmit {
		r.sinceEmit[g] = forcedEmitEvery
	}

	r.frozen.Store(true)
}

// Frozen reports whether resolution has ended. For tests and for app's own
// ordering assertions.
func (r *Reg) Frozen() bool { return r.frozen.Load() }

// renderPrefixes builds the "name=" / "name{label}=" text for every slot of
// every instrument of one shape.
//
// A dynamic dimension's slots are left empty: their label is a code that
// nobody has seen yet, so emission renders those in three pieces instead.
func (r *Reg) renderPrefixes(s shape, n int) []string {
	out := make([]string, n)
	for id := range numInstruments {
		if ledger[id].shape != s {
			continue
		}
		d := &r.dims[ledger[id].dim]
		if d.kind == dimDynamic {
			continue
		}
		for i := range d.size {
			out[r.base[id]+i*d.stride] = ledger[id].name + d.labels[i] + "="
		}
	}
	return out
}

// maxLineLen bounds the longest possible group line, so the emission buffer is
// allocated once and append can never reallocate it.
//
// Computed as the whole ledger rather than per group, and generously: this runs
// once, and a buffer twice the size it needs is irrelevant next to being wrong
// about it. 26 bytes per value covers "=" plus a 20-digit uint64 plus "(+)" and
// a second number.
func (r *Reg) maxLineLen() int {
	const perValue = 48
	total := 128 // the line prefix: timestamp, level, gen, interval, group
	for _, p := range r.prefixC {
		total += len(p) + perValue
	}
	for _, p := range r.prefixM {
		total += len(p) + perValue
	}
	for _, p := range r.prefixG {
		total += len(p) + perValue
	}
	// The dynamic dimensions have no prefix strings but still emit; budget
	// their slots at a generous fixed width.
	total += int(r.dims[dimUnknownCode].size+r.dims[dimOracleCode].size) * (64 + perValue)
	return total
}

// --- resolution ------------------------------------------------------------
//
// Nine accessors rather than one variadic, so that no call site allocates a
// []int and no index is positional guesswork. Each validates the shape, the
// dimension and the range, and each panics rather than returning an error: all
// of them run at startup step 9, where a mistake is a programming error that
// must stop the process before it holds a resource, and none of them has a
// caller that could do anything useful with an error.

func (r *Reg) Counter(id ID) *Counter { return &r.counters[r.resolve(id, shapeCounter, 0, 0, 0)] }
func (r *Reg) CounterAt(id ID, i int) *Counter {
	return &r.counters[r.resolve(id, shapeCounter, 1, i, 0)]
}
func (r *Reg) CounterAt2(id ID, i, j int) *Counter {
	return &r.counters[r.resolve(id, shapeCounter, 2, i, j)]
}

func (r *Reg) Max(id ID) *Max              { return &r.maxes[r.resolve(id, shapeMax, 0, 0, 0)] }
func (r *Reg) MaxAt(id ID, i int) *Max     { return &r.maxes[r.resolve(id, shapeMax, 1, i, 0)] }
func (r *Reg) Gauge(id ID) *Gauge          { return &r.gauges[r.resolve(id, shapeGauge, 0, 0, 0)] }
func (r *Reg) GaugeAt(id ID, i int) *Gauge { return &r.gauges[r.resolve(id, shapeGauge, 1, i, 0)] }
func (r *Reg) GaugeAt2(id ID, i, j int) *Gauge {
	return &r.gauges[r.resolve(id, shapeGauge, 2, i, j)]
}

// UnknownTranscode resolves the counter for one unknown transaction code,
// claiming a slot in the dynamic table.
//
// The second return is false when the table is full, and the caller MUST then
// increment unknown_transcode_other instead. Returning the fallback rather than
// silently aggregating keeps the two cases distinguishable in the output, which
// is the whole point of the overflow bucket existing as a named instrument.
//
// Unlike the other accessors this one is legal after Freeze, and it has to be:
// an unknown code is by definition not knowable at startup. It is the single
// exception to G-OBS-1, and it is safe because it allocates nothing and
// resolves through a CAS rather than a registration.
func (r *Reg) UnknownTranscode(code int) (*Counter, bool) {
	slot, ok := r.unknownCodes.slot(int32(code))
	if !ok {
		// Indexed directly rather than through Counter, which would route
		// into resolve and panic on the frozen check. This accessor is the
		// documented exception to G-OBS-1 and the fallback has to be too --
		// otherwise a full table turns a logged inventory gap into a crash on
		// the receive goroutine, which is the opposite of what the overflow
		// bucket is for.
		return &r.counters[r.base[IDUnknownTranscodeOther]], false
	}
	return &r.counters[r.base[IDUnknownTranscode]+slot], true
}

// UnconfiguredTranscode resolves the counter for a code this build CAN decode
// but the configuration did not ask for (ADR-011). Same dynamic table as
// UnknownTranscode, so a code occupies one slot across both.
//
// Legal after Freeze for the same reason: which codes are catalogued but
// unconfigured is a fact about one deployment, and resolving it through a CAS
// allocates nothing. On a full table the count lands in the shared overflow
// bucket and ok is false, so the caller can say so in the log -- the
// alternative, dropping the count, would make a saturated table look like an
// absence of events.
func (r *Reg) UnconfiguredTranscode(code int) (*Counter, bool) {
	slot, ok := r.unknownCodes.slot(int32(code))
	if !ok {
		return &r.counters[r.base[IDUnknownTranscodeOther]], false
	}
	return &r.counters[r.base[IDUnconfiguredTranscode]+slot], true
}

// UnknownTranscodeSlot returns the dynamic table's INDEX for a code, claiming
// a slot if the code is new.
//
// The index, not the counter, because the gap detector's per-transcode state
// is indexed by it: Vol 1 section 7.7 requires that scope to reuse "the index
// the counter layer already computed for this code -- no second lookup, no
// map", and a detector-local table would be a second answer to a question
// this one already answers.
//
// ok is false when the table is full, in which case the caller shares one
// aggregate slot and says so -- the same contract as UnknownTranscode above,
// and legal after Freeze for the same reason: a CAS, no allocation, and a code
// that is by definition not knowable at startup.
func (r *Reg) UnknownTranscodeSlot(code int) (int, bool) {
	slot, ok := r.unknownCodes.slot(int32(code))
	if !ok {
		return -1, false
	}
	return int(slot), true
}

// OracleError resolves the counter for one ORA code, claiming a slot. On a
// full table it returns the reserved overflow slot, which renders
// oracle_error{other} -- so unlike unknown_transcode there is no separate
// instrument to fall back to, and the caller needs no second branch.
func (r *Reg) OracleError(code int) *Counter {
	base := r.base[IDOracleError]
	slot, ok := r.oracleCodes.slot(int32(code))
	if !ok {
		return &r.counters[base+r.dims[dimOracleCode].size-1]
	}
	return &r.counters[base+slot]
}

// resolve validates one lookup and returns the slab offset.
//
// labels is how many indices the caller supplied, which is checked against the
// dimension's own rank. That check is why there are separate At and At2 methods
// instead of one variadic: passing one index to a {k,destination} instrument
// would otherwise land on shard 0's first destination and look plausible.
func (r *Reg) resolve(id ID, want shape, labels, i, j int) uint32 {
	if r.frozen.Load() {
		panic("obs: " + ledger[id].name + " resolved after Freeze. Gate G-OBS-1: " +
			"instruments are resolved ONCE at startup, into fields the hot path holds " +
			"directly (Vol 0 section 8.4). Resolve it in startup step 9 and cache the " +
			"pointer; a per-event lookup on the receive goroutine is the cost ADR-006 " +
			"exists to avoid")
	}
	if id >= numInstruments {
		panic("obs: instrument id " + strconv.Itoa(int(id)) + " is out of range")
	}
	row := &ledger[id]
	if row.shape != want {
		panic("obs: " + row.name + " is shape " + row.shape.String() + ", not " +
			want.String() + ". The ledger fixes the shape (Vol 0 section 8.2); a counter " +
			"read as a gauge would publish a cumulative total where an operator reads a " +
			"current value")
	}

	d := &r.dims[row.dim]
	rank := 0
	switch {
	case row.dim == dimNone:
		rank = 0
	case d.inner > 1:
		rank = 2
	default:
		rank = 1
	}
	if labels != rank {
		panic("obs: " + row.name + " is labelled " + d.brace + ", which takes " +
			strconv.Itoa(rank) + " index/indices, but " + strconv.Itoa(labels) +
			" were given. Use the matching accessor; a {k,destination} instrument given " +
			"one index lands on destination zero and looks plausible")
	}

	off := uint32(0)
	switch rank {
	case 1:
		if i < 0 || uint32(i) >= d.size {
			panic(outOfRange(row.name, d.brace, i, d.size))
		}
		off = uint32(i)
	case 2:
		outer := d.size / d.inner
		if i < 0 || uint32(i) >= outer {
			panic(outOfRange(row.name, d.brace, i, outer))
		}
		if j < 0 || uint32(j) >= d.inner {
			panic(outOfRange(row.name, d.brace, j, d.inner))
		}
		off = uint32(i)*d.inner + uint32(j)
	}
	return r.base[id] + off*d.stride
}

func outOfRange(name, brace string, got int, size uint32) string {
	return "obs: " + name + brace + " index " + strconv.Itoa(got) +
		" is outside 0.." + strconv.Itoa(int(size)-1)
}
