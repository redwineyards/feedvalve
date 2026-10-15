package app

import (
	"strconv"
	"strings"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// since is time.Since behind a variable so budget expiry is testable without
// sleeping. Tests replace it; nothing else may. The same seam framing/budget.go
// opens for the enqueue budget, for the same reason.
//
// It is time.Since over a monotonic t0 and NEVER a difference of two wall-clock
// readings (Vol 1 section 10.5 property 2). An NTP step between the two
// readings can produce a NEGATIVE duration, which as a remaining-budget
// computation hands the timer a value near 2^64 and as a uint64 counter
// argument is catastrophic.
var since = time.Since

// route is Vol 1 section 10.5's send discipline: stamp each unit with what only
// the receiver knows, pick its shard, and deliver it -- fast if the shard
// channel has room, against the datagram's budget if it does not, fatally if
// the budget runs out.
//
// Called from SubPacket, OUTSIDE the panic-recovery boundary (ADR-L10). That
// placement is the whole reason decodeSubPacket is a separate function: a
// recover() between budget expiry and the lifecycle would eat the one condition
// the design says must stop the process, and the process would keep running
// with a saturated queue, silently.
//
// Returns model.ErrStallBudgetExpired and nothing else. Every other outcome on
// this path is a successful delivery, because there is no drop path.
func (r *receiver) route(units []model.UpdateUnit, idx, count int) error {
	n := len(units)
	if n == 0 {
		// A normal outcome, not a reject (Vol 1 section 10.1): 7200 emits zero
		// units for a BookType outside {1,2}, and the two header-only codes
		// emit zero always.
		return nil
	}
	r.in.unitsEmitted.Add(uint64(n))
	r.in.unitsPerPacket.Observe(uint64(n))

	// Read once per message rather than per unit. All three come from the
	// snapshot loaded at the top of this datagram, so every unit of this
	// datagram is routed and timed against ONE configuration -- which is what
	// stops a reload changing the deadline between the first unit and the
	// thirty-fifth (loadRuntime).
	cfg := r.rt.Cfg
	shards, general := cfg.Runtime.Shards, cfg.Runtime.GeneralShard
	budget := cfg.StallBudget()

	for i := range units {
		u := &units[i]

		// The two fields decode deliberately does not set, because neither
		// belongs to the decode (internal/decode/unit.go).
		//
		// RecvMono is the DATAGRAM's receipt clock, read once by the receiver
		// at Vol 1 section 3.4 step 4 and stamped on every unit the datagram
		// produces: 2,400 clock reads a second instead of 20,000. The tiebreak
		// property survives because units within one datagram are ordered by
		// their arrival into the shard channel, which is FIFO with a single
		// producer -- a shared RecvMono inside one datagram cannot invert them
		// (section 10.3).
		u.RecvMono = r.recvMono
		if u.General() && u.LogTime <= 0 {
			// Section 10.3: the flag is set when NEITHER ordering component is
			// usable. For a token-scoped unit the primary component is Seq,
			// which a BCAST_HEADER always carries; for a general unit it is
			// LogTime (Vol 2 section 5.3's guard uses a plain comparison on it,
			// never Seq), so a general unit with no LogTime has nothing for the
			// guard to compare and the guard is bypassed for it.
			//
			// "Implausible" is bounded at zero and below, and NOT by an upper
			// bound against the current date: LogTime's epoch anchor is OQ-W2,
			// still open, and rejecting a timestamp using an assumption we have
			// not confirmed would discard good data on a guess.
			u.Flags |= model.FlagOrderingUnusable
		}

		k := ShardOf(u, shards, general)
		if err := r.send(k, u, i, n, idx, count, budget); err != nil {
			return err
		}
		r.unitsInDatagram++
	}
	return nil
}

// send delivers one unit to shard k.
//
// THE FAST PATH READS NO CLOCK AND ARMS NO TIMER, and that is Vol 1 section
// 10.5 property 3 rather than an optimisation: at the shard occupancy HLD
// section 6.6 computes -- 0.08% busy -- essentially every send takes the
// default arm, so a time.Since here would add 20,000 clock reads a second to
// measure a quantity that is zero.
//
// The unit is sent BY VALUE, which copies 360 bytes into the channel buffer.
// That copy is the transfer (ADR-006): fanoutBuf is reused by the very next
// sub-packet, so a pointer would hand the shard a window onto bytes that are
// about to become another token's.
func (r *receiver) send(k model.ShardID, u *model.UpdateUnit,
	i, n, idx, count int, budget time.Duration) error {

	ch := r.g.shardCh[k]
	select {
	case ch <- *u:
		return nil
	default:
	}

	// Blocked. Against a shard the design computes as 0.08% busy, reaching this
	// line at all is a defect signal rather than a load signal -- it is Vol 4
	// F24, the only early warning there is before F9 stops the process.
	r.in.shardSendBlocked[k].Add(1)

	used := since(r.t0)
	if used >= budget {
		// The budget was already gone when this unit was offered: an earlier
		// unit of this same datagram consumed it. Checked BEFORE arming the
		// timer, because Reset with a negative duration fires immediately and
		// would reach the same place by a more confusing route.
		return r.stallFatal(k, u, i, n, idx, count, budget, used)
	}

	// ONE REUSABLE TIMER, owned by this goroutine (property 4). go.mod fixes
	// the module at go 1.23.3, and from go1.23 timer channels are unbuffered
	// and a reset timer never delivers a stale value -- so Reset needs no drain
	// dance. That is a real dependency on the go directive: a downgrade of that
	// line would reintroduce a stale-tick bug that fires only under load.
	r.stallTimer.Reset(budget - used)
	select {
	case ch <- *u:
		r.stallTimer.Stop()
		r.in.stallBudgetUsedMaxNs.Observe(nonNegative(since(r.t0)))
		return nil
	case <-r.stallTimer.C:
		return r.stallFatal(k, u, i, n, idx, count, budget, since(r.t0))
	}
}

// nonNegative clamps a duration before it becomes a uint64 counter argument.
//
// since() is monotonic, so this cannot fire. It is here because the cost is one
// comparison on a path that has already blocked for milliseconds, and the
// failure it prevents is a max gauge pinned at 1.8e19 nanoseconds for the rest
// of the session -- which would destroy the one measurement an operator uses to
// judge the distance to the fatal cliff.
func nonNegative(d time.Duration) uint64 {
	if d < 0 {
		return 0
	}
	return uint64(d)
}

// stallFatal is Vol 1 section 10.6: expiry is fatal, there is no drop path.
//
// req section 10.2 permits exactly one response to a critical-path queue
// overflowing, and Vol 0 section 9.2 states the consequence in one line --
// "there is none". FeedValve discards no unit anywhere.
//
// Two things happen and BOTH are load-bearing. Fatal is what wakes Graph.wait,
// which selects on the terminal channel and not on decoderDone; the returned
// error is what stops the QD drain so the decoder goroutine finishes. Neither
// is os.Exit, which would skip every defer -- and this process has shards owing
// a final flush and an Oracle pool to close (gate G-BLD-3).
//
// The line carries section 10.6's whole table, because HLD section 10.2
// requires a terminal message to be diagnosable from the log alone, with no
// live process to query. shard_loop_max_ns{k} is the one field of that table
// missing here: it is the shard's own instrument and arrives with Phase 3.
func (r *receiver) stallFatal(k model.ShardID, u *model.UpdateUnit,
	i, n, idx, count int, budget, used time.Duration) error {

	// Allocating freely: this runs once, on the way to stopping the process.
	// The argument that keeps the receive path allocation-free does not apply
	// to a path that has no next iteration.
	detail := "shard=" + strconv.Itoa(int(k)) +
		" depth=" + strconv.Itoa(len(r.g.shardCh[k])) +
		" cap=" + strconv.Itoa(cap(r.g.shardCh[k])) +
		" code=" + strconv.Itoa(int(u.Code)) +
		" token=" + strconv.FormatInt(int64(u.Token), 10) +
		" unit_index=" + strconv.Itoa(i) + "/" + strconv.Itoa(n) +
		" units_in_datagram=" + strconv.Itoa(r.unitsInDatagram+1) +
		" subpacket=" + strconv.Itoa(idx+1) + "/" + strconv.Itoa(count) +
		" datagram_bytes=" + strconv.Itoa(r.datagramBytes) +
		" budget_ms=" + strconv.FormatInt(budget.Milliseconds(), 10) +
		" used_ns=" + strconv.FormatInt(int64(used), 10) +
		" depths=" + r.g.shardDepths()

	// Not rate-limited and not a WARN. At 0.08% steady-state occupancy a full
	// shard queue held for the whole budget means something is structurally
	// wrong -- a shard blocked where the design says it never blocks, or a load
	// an order of magnitude past req section 3.3. There is no lever that makes
	// this acceptable, which is exactly why it stops the process instead of
	// shedding the oldest unit: Q1 carries heterogeneous units for every token
	// and every transcode routed to that shard, so the oldest queued unit may
	// be the only carrier of a different destination's columns.
	r.g.deps.Log.Errorf(obs.ClassSessionAnomaly,
		"shard queue stall budget expired; nothing is discarded, the process stops: %s", detail)

	r.g.Fatal(FatalQ1Overflow, detail)
	return model.ErrStallBudgetExpired
}

// shardDepths renders every shard's queue depth, not just the stalled one.
//
// One shard stuck against three idle is a different fault from all four full
// (section 10.6): the first points at that shard's own loop, the second at
// something upstream of all of them. The distinction is free here and
// unrecoverable later.
func (g *Graph) shardDepths() string {
	var b strings.Builder
	for k := range g.shardCh {
		if k > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(k))
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(len(g.shardCh[k])))
	}
	return b.String()
}
