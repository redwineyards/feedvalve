package obs

import (
	"io"
	"strconv"
	"time"
)

// timeLayout is the emission timestamp, matching log.LstdFlags|log.Lmicroseconds
// -- which is what poc/logger.go gives appLogger and what this process's other
// output therefore carries.
//
// The two must stay byte-identical. The stats line cannot go through
// *log.Logger, because Printf allocates and gate G-OBS-2 forbids it, so this
// format is hand-built and the identity is asserted by a test rather than
// guaranteed by sharing code. If they ever drift by one character, interleaving
// a WARN with a trend by grep stops working, which is the only correlation tool
// Vol 4 section 6.7 leaves an operator.
const timeLayout = "2006-01-02 15:04:05.000000"

// Emit writes one emission: the health line, then every group line that has
// something to say.
//
// ONE event, not fifteen. All lines share one timestamp and one gen=, so a grep
// reassembles them into a single consistent snapshot (Vol 4 section 6.1, delta
// D4-6). That is why the snapshot is taken in a separate first pass over every
// instrument before any byte is rendered: it is also what makes it impossible
// for the health projection to consume a value its home group still needs. An
// instrument on the health line is emitted twice, and if each render site read
// the instrument itself, the second counter delta would come out zero and the
// second LoadAndReset of a max would come out zero -- silently, and only for
// the handful of instruments on the health line.
//
// Emit allocates nothing. Every buffer is sized in Freeze, the timestamp is
// appended in place, and values go through strconv.Append* (gate G-OBS-2).
//
// It never calls back into its caller. Gauges that are genuinely sampled at
// emission -- batchq_depth, which is len(chan) and race-free, and
// writers_configured -- are Stored by their owner immediately before Emit is
// called. A callback would put a closure and an interface call on the one path
// that has to stay allocation-free.
func (r *Reg) Emit(w io.Writer) error {
	if !r.frozen.Load() {
		panic("obs: Emit before Freeze. The emission buffers and label prefixes are " +
			"allocated by Freeze, which is also what makes Emit allocation-free " +
			"(gate G-OBS-2)")
	}

	// uptime_seconds from the monotonic clock. Vol 4 section 10.2 MUST: never
	// a difference of two wall-clock readings, which an NTP step makes wrong
	// and can drive negative.
	r.gauges[r.base[IDUptimeSeconds]].Store(int64(time.Since(r.started) / time.Second))

	dirty := r.snapshot()
	now := time.Now()
	gen := uint64(r.curG[r.base[IDConfigGeneration]])
	interval := r.intervalSec.Load()

	// Rule 1: the health line is always emitted, and first. An operator who
	// reads one line reads that one (Vol 4 section 6.1, section 6.6 Q1-Q3).
	if err := r.writeHealth(w, now, gen, interval); err != nil {
		return err
	}

	for g := range numGroups {
		r.sinceEmit[g]++
		// Rule 2: emit when something moved. Rule 3: emit anyway every tenth
		// interval, so a trend never has an unbounded gap and a value that has
		// been zero all night is still visible as zero.
		if !dirty[g] && r.sinceEmit[g] < forcedEmitEvery {
			continue
		}
		r.sinceEmit[g] = 0
		if err := r.writeGroup(w, g, now, gen, interval); err != nil {
			return err
		}
	}
	return nil
}

// snapshot is pass one: read every instrument exactly once, compute deltas,
// and report which groups moved.
//
// This is the only place an instrument is read. Maxes are consumed here with
// LoadAndReset, which is the interval semantics of shape M and the reason the
// read cannot be deferred to the render pass.
func (r *Reg) snapshot() [numGroups]bool {
	var dirty [numGroups]bool

	for id := range numInstruments {
		row := &ledger[id]
		d := &r.dims[row.dim]
		g := row.group

		for i := range d.size {
			off := r.base[id] + i*d.stride
			switch row.shape {
			case shapeCounter:
				// curC still holds the previous emission's value until it is
				// overwritten, which is what the delta is measured against.
				v := r.counters[off].Load()
				delta := v - r.curC[off]
				r.curC[off] = v
				r.deltaC[off] = delta
				if delta != 0 {
					dirty[g] = true
				}
			case shapeMax:
				v := r.maxes[off].LoadAndReset()
				r.curM[off] = v
				if v != 0 {
					dirty[g] = true
				}
			case shapeGauge:
				v := r.gauges[off].Load()
				if v != r.curG[off] {
					dirty[g] = true
				}
				r.curG[off] = v
			}
		}
	}
	return dirty
}

// linePrefix starts a line: timestamp, level, the stats marker, the generation,
// the interval and the group.
func (r *Reg) linePrefix(g string, now time.Time, gen, interval uint64) {
	r.buf = r.buf[:0]
	r.buf = now.AppendFormat(r.buf, timeLayout)
	r.buf = append(r.buf, " INFO stats gen="...)
	r.buf = strconv.AppendUint(r.buf, gen, 10)
	r.buf = append(r.buf, " interval_s="...)
	r.buf = strconv.AppendUint(r.buf, interval, 10)
	r.buf = append(r.buf, " group="...)
	r.buf = append(r.buf, g...)
}

// writeHealth renders the sixteenth, projected group: the dozen values Vol 4
// section 6.6's first three questions read.
//
// Every member is rendered whether it moved or not. The line's purpose is to be
// readable unconditionally, so suppressing a quiet value from it would defeat
// the one rule that says a line is always present.
func (r *Reg) writeHealth(w io.Writer, now time.Time, gen, interval uint64) error {
	r.linePrefix("health", now, gen, interval)
	for _, id := range healthLine {
		r.appendInstrument(id, true)
	}
	r.buf = append(r.buf, '\n')
	_, err := w.Write(r.buf)
	return err
}

func (r *Reg) writeGroup(w io.Writer, g Group, now time.Time, gen, interval uint64) error {
	r.linePrefix(groupNames[g], now, gen, interval)
	for id := range numInstruments {
		if ledger[id].group == g {
			r.appendInstrument(id, false)
		}
	}
	r.buf = append(r.buf, '\n')
	_, err := w.Write(r.buf)
	return err
}

// appendInstrument renders every slot of one instrument from the snapshot.
//
// force makes a sparse slot print anyway; the health line uses it so its
// membership is fixed rather than conditional.
func (r *Reg) appendInstrument(id ID, force bool) {
	row := &ledger[id]
	d := &r.dims[row.dim]

	for i := range d.size {
		off := r.base[id] + i*d.stride

		// Sparsity: a high-cardinality slot prints only when it carries data.
		// Without this, the three {destination,column} instruments alone would
		// put 576 values on the schema_fit line -- about 18 KB, worse than the
		// single line the group split exists to avoid.
		if !force && d.kind != dimDense && !r.slotHasData(row.shape, off) {
			continue
		}
		// A dynamic slot nobody has occupied has no label to print at all.
		if d.kind == dimDynamic && !r.dynamicOccupied(id, i) {
			continue
		}

		r.buf = append(r.buf, ' ')
		if d.kind == dimDynamic {
			r.appendDynamicPrefix(id, i)
		} else {
			switch row.shape {
			case shapeCounter:
				r.buf = append(r.buf, r.prefixC[off]...)
			case shapeMax:
				r.buf = append(r.buf, r.prefixM[off]...)
			case shapeGauge:
				r.buf = append(r.buf, r.prefixG[off]...)
			}
		}

		switch row.shape {
		case shapeCounter:
			// Cumulative total AND delta. There is nothing downstream to
			// differentiate a counter by hand (Vol 4 section 6.7).
			r.buf = strconv.AppendUint(r.buf, r.curC[off], 10)
			r.buf = append(r.buf, "(+"...)
			r.buf = strconv.AppendUint(r.buf, r.deltaC[off], 10)
			r.buf = append(r.buf, ')')
		case shapeMax:
			// The interval maximum, already consumed and reset by the
			// snapshot. No delta: the value IS the interval.
			r.buf = strconv.AppendUint(r.buf, r.curM[off], 10)
		case shapeGauge:
			r.buf = strconv.AppendInt(r.buf, r.curG[off], 10)
		}
	}
}

func (r *Reg) slotHasData(s shape, off uint32) bool {
	switch s {
	case shapeCounter:
		return r.curC[off] != 0
	case shapeMax:
		return r.curM[off] != 0
	case shapeGauge:
		return r.curG[off] != 0
	}
	return false
}

// dynamicOccupied reports whether a dynamic slot has been claimed. The Oracle
// table's last slot is the reserved overflow bucket, which is always
// renderable.
func (r *Reg) dynamicOccupied(id ID, i uint32) bool {
	switch ledger[id].dim {
	case dimUnknownCode:
		return r.unknownCodes.at(i) != 0
	case dimOracleCode:
		if i == r.dims[dimOracleCode].size-1 {
			return true
		}
		return r.oracleCodes.at(i) != 0
	}
	return false
}

// appendDynamicPrefix renders "name{code}=" for a dynamic slot, in three
// pieces rather than from a pre-built string.
//
// Building the string when the slot is claimed would put an allocation on the
// receive goroutine at exactly the moment a new transaction code appears --
// which, per F2, is a moment that happens in production. Appending the digits
// here costs nothing and happens once a minute.
func (r *Reg) appendDynamicPrefix(id ID, i uint32) {
	r.buf = append(r.buf, ledger[id].name...)
	r.buf = append(r.buf, '{')
	switch ledger[id].dim {
	case dimUnknownCode:
		r.buf = strconv.AppendInt(r.buf, int64(r.unknownCodes.at(i)), 10)
	case dimOracleCode:
		if i == r.dims[dimOracleCode].size-1 {
			// Named rather than numbered, so the overflow bucket reads the
			// same way unknown_transcode_other does and no ORA number is
			// invented for it.
			r.buf = append(r.buf, "other"...)
		} else {
			r.buf = strconv.AppendInt(r.buf, int64(r.oracleCodes.at(i)), 10)
		}
	}
	r.buf = append(r.buf, "}="...)
}
