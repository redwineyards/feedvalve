package app

import (
	"encoding/binary"
	"errors"
	"flag"
	"math/bits"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/model"
)

// stressFor is G-RACE-3's run length. The gate's own figure is 60 s (V4:1707);
// the default is a short run so go test ./... stays quick, and the full one is
//
//	go test -race -tags feedvalve_assert -timeout 1h ./internal/app \
//	    -run TestG_RACE_3 -feedvalve.stress 60s
var stressFor = flag.Duration("feedvalve.stress", 2*time.Second,
	"how long G-RACE-3's scheduler-stress harness drives the pipeline (the gate's figure is 60s)")

// stressShards is the four shards of the gate: GOMAXPROCS is set to twice it.
const stressShards = 4

// mbpRecLTPOff and mbpRecNetChangeOff are last_traded_price and
// net_change_indicator within an INTERACTIVE_ONLY_MBP_DATA record (bible Part
// V's offset column, as the decoder's comments carry it).
const (
	mbpRecLTPOff       = 12
	mbpRecNetChangeOff = 16
)

// errStressBatch is the stress writer's batch-level failure: Err set, every row
// left Pending, which the shard reads as Failed (V2:82).
var errStressBatch = errors.New("stress writer: batch failed")

// jitterSpins is how many runtime.Gosched calls take about 100 us here,
// measured by calibrateJitter before any harness goroutine starts (so the
// write happens-before every read) and read only after.
//
// The 0-100 us bound of V4:1707 is therefore NOMINAL: calibrated on an idle
// scheduler. Under the run, a Gosched may hand the P to another runnable
// goroutine, so a draw of N calls can take well over N times the calibrated
// cost. That is a deliberate deviation -- the alternative, a sleep or a spin
// on the clock, cannot express 0-100 us at all where the monotonic clock
// moves once per timer tick (Windows), and a longer delay only widens the
// interleavings the gate is after. It is not capped by time.Since for the
// same reason: on such a clock the cap would be a whole tick.
var jitterSpins int

// calibrateJitter times a long run of Gosched calls: one call is far below
// the clock's resolution on some hosts (Windows' monotonic clock moves in
// timer ticks), but the average over many is not.
func calibrateJitter() {
	const n = 100_000
	start := time.Now()
	for range n {
		runtime.Gosched()
	}
	per := max(time.Since(start)/n, 1)
	jitterSpins = max(int(100*time.Microsecond/per), 1)
}

// jitter is G-RACE-3's injected delay: uniform over 0-100 us, as a random
// number of Gosched calls rather than a sleep or a clock-polling spin -- a
// sleep is rounded up to the timer's resolution, and so is a spin on a clock
// that only moves once a tick -- and every call is itself a scheduling point.
func jitter(rng *rand.Rand) {
	for range rng.Intn(jitterSpins + 1) {
		runtime.Gosched()
	}
}

// rowID names one mirror row across the whole pipeline.
type rowID struct {
	shard model.ShardID
	d     model.DestID
	row   model.RowKey
}

// rowRecord is what the stress writer's "database" holds for a row: the image
// as committed, column by column; the union of committed masks; the digest of
// the last committed batch row; and whether its last verdict was Rejected.
type rowRecord struct {
	img      [model.ImageSize]byte
	written  model.ColMask
	digest   uint64
	rejected bool
}

// stressWriter stands in for Phase 4's pool. It obeys the shard side of the
// boundary -- it writes only Outcome, ErrCode and Err, and never touches a
// batch after returning it -- and adjudicates every row (B-V3): mostly
// Committed, 5% Failed, 1% of batches failed whole, and the poison rows
// Rejected 20% of the time. It records what a database would hold, every
// time a row was outstanding in two batches at once (G-INFL-1), and every time
// a row whose last verdict was Rejected was offered again (G-ROW-1).
type stressWriter struct {
	g      *Graph
	poison map[model.RowKey]bool
	stop   chan struct{}
	wg     sync.WaitGroup

	mu                                       sync.Mutex
	out                                      map[rowID]bool
	rows                                     map[rowID]*rowRecord
	dup                                      int
	reoffered                                int // never reset: a quarantined row must never come back
	committed, failed, rejected, batchFailed int
}

func startStressWriters(t *testing.T, g *Graph, n int, poison map[model.RowKey]bool) *stressWriter {
	w := &stressWriter{g: g, poison: poison, stop: make(chan struct{}),
		out: map[rowID]bool{}, rows: map[rowID]*rowRecord{}}
	w.wg.Add(n)
	for i := range n {
		go w.serve(rand.New(rand.NewSource(int64(1000 + i))))
	}
	t.Cleanup(w.close) // a Fatalf before the explicit close must not leak them
	return w
}

// close stops the writers and waits for them. Called once Run has returned, so
// every batch is already home, and again by Cleanup: idempotent.
func (w *stressWriter) close() {
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	w.wg.Wait()
}

func (w *stressWriter) serve(rng *rand.Rand) {
	defer w.wg.Done()
	for {
		select {
		case <-w.stop:
			return
		case b := <-w.g.batchQ:
			jitter(rng) // batchQ -> writer
			w.take(b)
			w.adjudicate(rng, b)
			w.record(b)
			jitter(rng) // writer -> completionCh
			w.g.completionCh[b.Shard] <- b
		}
	}
}

// each calls f for every active row of b with its destination, which BatchRow
// does not carry: it is known only from the segment.
func each(b *model.Batch, f func(d model.DestID, r *model.BatchRow)) {
	for d := range model.MaxDestinations {
		for j := b.DestStart[d]; j < b.DestStart[d]+b.DestCount[d]; j++ {
			f(model.DestID(d), &b.Rows[j])
		}
	}
}

// take marks b's rows outstanding, counting any already outstanding.
func (w *stressWriter) take(b *model.Batch) {
	w.mu.Lock()
	defer w.mu.Unlock()
	each(b, func(d model.DestID, r *model.BatchRow) {
		id := rowID{b.Shard, d, r.Row}
		if w.out[id] {
			w.dup++
		}
		w.out[id] = true
		if rec := w.rows[id]; rec != nil && rec.rejected {
			w.reoffered++
		}
	})
}

// adjudicate writes the verdicts: the writer's three fields and nothing else.
func (w *stressWriter) adjudicate(rng *rand.Rand, b *model.Batch) {
	if rng.Intn(100) == 0 {
		b.Err = errStressBatch
		return
	}
	for i := range b.Active() {
		r := &b.Rows[i]
		switch {
		case w.poison[r.Row] && rng.Intn(5) == 0:
			r.Outcome, r.ErrCode = model.OutcomeRejected, 1438
		case rng.Intn(20) == 0:
			r.Outcome, r.ErrCode = model.OutcomeFailed, 3113
		default:
			r.Outcome, r.ErrCode = model.OutcomeCommitted, 0
		}
	}
}

// record applies the verdicts to the database and releases the rows, before
// the batch goes back: once it is on completionCh the shard may re-batch them.
func (w *stressWriter) record(b *model.Batch) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if b.Err != nil {
		w.batchFailed++
	}
	each(b, func(d model.DestID, r *model.BatchRow) {
		id := rowID{b.Shard, d, r.Row}
		delete(w.out, id)
		rec := w.rows[id]
		if rec == nil {
			rec = &rowRecord{}
			w.rows[id] = rec
		}
		switch {
		case b.Err != nil || r.Outcome == model.OutcomeFailed:
			w.failed++
		case r.Outcome == model.OutcomeRejected:
			w.rejected++
			rec.rejected = true
		case r.Outcome == model.OutcomeCommitted:
			w.committed++
			dst := w.g.deps.Dests.ByID(d)
			for m := r.Mask; m != 0; m &= m - 1 {
				col := dst.Column(uint8(bits.TrailingZeros64(uint64(m))))
				copy(rec.img[col.Offset:col.End()], r.Image[col.Offset:col.End()])
			}
			rec.written |= r.Mask
			rec.digest = r.Digest
			rec.rejected = false
		}
	})
}

// TestG_RACE_3_TheSchedulerStressHarness is gate G-RACE-3 (V4:1707).
//
// SETUP: the whole pipeline -- a loopback UDP socket, framing, the decoder,
// four live shards, and four stress writers in place of Phase 4's pool --
// with GOMAXPROCS at 2 x shards, flush.windowMs 5 and flush.maxBatchSize 4 so
// epochs and batches turn over constantly. A randomised 0-100 us delay at
// every channel boundary the test can reach: socket send, QD -> decoder
// (through Deps.Decode), batchQ -> writer and writer -> Q2c (in the writer).
// Decoder -> Q1 is NOT reachable: the fan-out's sends happen inside
// decodeDatagram, behind the seam, and a delay after it is only more
// QD -> decoder delay for the next datagram. Sixty-four tokens, sixteen per
// shard; one per shard is a poison row the writer sometimes rejects. The
// ordering guard is forced "on": G-ORD-2's auto-disable is not this gate, and
// tripping it would turn seam 9's ties into misleading G-ORD-1 failures.
//
// STIMULUS: datagrams of two 7208 records each, every record carrying a new
// last_traded_price, for -feedvalve.stress (default 2 s, the gate's 60 s on
// request), then SIGTERM and the drain. Every datagram has the same LogTime and
// BCSeqNo, so the receipt clock alone orders a row's contributions (the G-ORD-1
// shape).
//
// ASSERTION, over the accumulated state once Run has returned:
//   - G-ORD-1: every row holds the last value the decoder saw for it, and
//     nothing was superseded (V4:1623), and the receipt clock never tied --
//     a tie would discard a distinct datagram as a re-delivery, which
//     nextRecvMono rules out (phase3-tasks seam 9, closed);
//   - G-MASK-1: every non-quarantined row's database image equals its mirror
//     image over every column ever written -- no column re-dirtied in flight
//     was lost -- and its ColMask is 0;
//   - G-INFL-1: no row was ever outstanding in two batches at once;
//   - G-ROW-1: per row, Written is the union of committed masks and Digest the
//     last committed batch row's; a quarantined row is exactly one whose last
//     verdict was Rejected and is a poison row; and no row whose last verdict
//     was Rejected was ever offered to a writer again. (That a rejection or a
//     failure changes neither Written nor Digest is the mirror's unit-level
//     G-ROW-1 tests' to prove: every contribution here carries all sixteen
//     columns, so the commits mask it.)
//   - every datagram the receiver counted reached the decoder; what the
//     kernel dropped before it is logged;
//   - the INV-STATE walk (Dirty && InFlight never, ReDirty => InFlight) and
//     quiescence: nothing Dirty, InFlight or ReDirty, every Free list full;
//   - zero inflight_desync, batch_row_unreported, blob_len_mismatch and
//     drain_abandoned_rows; Run returns nil.
//
// Non-vacuity: committed, failed, rejected and batch-failed verdicts all
// occurred, rows were re-dirtied in flight and requeued for retry, a row was
// quarantined, and rows were checked.
//
// WOULD HAVE CAUGHT: any unsynchronised sharing on the data path under -race
// (the whole point: the production GOMAXPROCS of 4 is the least interleaved
// setting shipped); under -tags feedvalve_assert, two goroutines entering one
// shard (G-ASSERT-1's TryLock). Without -race, the interleaving defects whose
// symptom is state: ColMask cleared at completion (HLD R14), a row batched
// twice, a Failed row dropped instead of requeued, a quarantined row queued
// again, and units applied out of arrival order.
func TestG_RACE_3_TheSchedulerStressHarness(t *testing.T) {
	prev := runtime.GOMAXPROCS(2 * stressShards)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	calibrateJitter()

	// The decoder goroutine's state: the seam's receiver, its jitter source,
	// and the arrival record. Written only on the decoder goroutine; read
	// after Run has joined it -- except decoded, which the test polls.
	var (
		g       *Graph
		rcv     *receiver
		drng    = rand.New(rand.NewSource(1))
		expect  = map[uint32]uint32{} // token -> the last value the decoder saw
		seenAt  = map[uint32]int64{}  // token -> that datagram's RecvMono
		ties    uint64                // a token re-seen on an equal receipt clock
		decoded atomic.Int64
	)
	decode := func(d *model.Datagram) error {
		jitter(drng) // QD -> decoder
		if rcv == nil {
			rcv = newReceiver(g)
		}
		if err := rcv.decodeDatagram(d); err != nil {
			return err
		}
		p := d.Buf[6:d.N] // the one sub-packet's payload, after cNetId, iNoPackets, iCompLen
		for rec := range int(binary.BigEndian.Uint16(p[mbpAbsNoOfRecords:])) {
			base := mbpAbsRecord0 + rec*mbpAbsStride
			tok := binary.BigEndian.Uint32(p[base+mbpRecTokenOff:])
			// The guard's step 8: an equal receipt clock is a re-delivery.
			if d.RecvMono > seenAt[tok] {
				expect[tok] = binary.BigEndian.Uint32(p[base+mbpRecLTPOff:])
				seenAt[tok] = d.RecvMono
			} else {
				ties++
			}
		}
		decoded.Add(1)
		return nil
	}
	g, buf := testGraphWith(t, "127.0.0.1:0", func(c *config.Config) {
		configureTranscodes(t, c, allocGateCode)
		c.Runtime.Shards = stressShards
		c.Flush.WindowMs = 5
		c.Flush.MaxBatchSize = 4
		c.Ordering.Guard = "on"
	}, decode)

	shardToks := tokensPerShard(stressShards, 16)
	var toks []uint32
	poison := map[model.RowKey]bool{}
	for _, ts := range shardToks {
		toks = append(toks, ts...)
		poison[model.SingleKey(ts[0])] = true
	}

	cancel, done := runGraph(t, g)
	w := startStressWriters(t, g, stressShards, poison)

	// The socket: one sender, two distinct tokens per datagram (two records
	// of one token in one datagram would share a receipt clock), every record
	// a new value.
	conn, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialling the listener: %v", err)
	}
	defer conn.Close()
	srng := rand.New(rand.NewSource(2))
	env := make([]byte, 4+2+mbpAbsPayloadLen)
	env[0] = 8                              // cNetId
	binary.BigEndian.PutUint16(env[2:4], 1) // iNoPackets
	var seq uint32                          // iCompLen at env[4:6] stays 0: uncompressed
	for end := time.Now().Add(*stressFor); time.Now().Before(end); {
		i := srng.Intn(len(toks))
		j := (i + 1 + srng.Intn(len(toks)-1)) % len(toks)
		p := mbpPayloadTokens(toks[i], toks[j])
		for rec := range 2 {
			seq++
			base := mbpAbsRecord0 + rec*mbpAbsStride
			binary.BigEndian.PutUint32(p[base+mbpRecLTPOff:], seq)
			p[base+mbpRecNetChangeOff] = '+'
		}
		copy(env[6:], p)
		if _, err := conn.Write(env); err != nil {
			t.Fatalf("writing a datagram: %v", err)
		}
		jitter(srng) // socket
	}

	// Let the decoder catch up before SIGTERM. The jittered decoder is the
	// slowest stage, so a backlog sits in QD and the socket buffer; drained
	// after t_shutdown it can outlast flush.drainTimeoutMs, and a drain
	// deadline is a designed outcome (ADR-L37) that would end the shards with
	// batches out -- not the quiescence this gate asserts on.
	for last, deadline := int64(-1), time.Now().Add(time.Minute); ; {
		n := decoded.Load()
		if n == last {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the decoder was still decoding a minute after the sender stopped (%d)", n)
		}
		last = n
		time.Sleep(250 * time.Millisecond)
	}

	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run = %v, want nil for a requested shutdown\n%s", err, buf.String())
	}
	w.close()

	in := g.inst()
	sent, received := uint64(seq/2), in.datagramsReceived.Load()
	t.Logf("G-RACE-3: %v, jitter 0-%d Gosched calls (nominal 0-100 us); datagrams sent=%d received=%d "+
		"decoded=%d, dropped by the socket before the receiver=%d, qd_send_blocked=%d; verdicts "+
		"committed=%d failed=%d rejected=%d batch-failed=%d; redirty=%d receipt-clock ties=%d",
		*stressFor, jitterSpins, sent, received, decoded.Load(), sent-min(sent, received),
		in.qdSendBlocked.Load(), w.committed, w.failed, w.rejected, w.batchFailed,
		in.mirrorShards[0].RedirtyTotal.Load(), ties)

	// QD has no drop path (req section 10.2), so every datagram the receiver
	// counted must have reached the decoder; a stall there cannot pass for a
	// slow decoder. Loss before the receiver is the kernel's, and logged.
	if n := uint64(decoded.Load()); n != received {
		t.Errorf("datagrams_received = %d but the decoder saw %d: datagrams were lost between "+
			"the receiver and the decoder, where nothing may be discarded", received, n)
	}

	// ---- the counters ----
	var desync, unreported, retried, epochs uint64
	for k := range in.mirrorShards {
		desync += in.mirrorShards[k].InflightDesync.Load()
		unreported += in.mirrorShards[k].BatchRowUnreported.Load()
		retried += in.mirrorShards[k].RetryRequeued.Load()
		epochs += in.mirrorShards[k].EpochOpened.Load()
	}
	if desync != 0 || unreported != 0 {
		t.Errorf("inflight_desync = %d, batch_row_unreported = %d; want 0 and 0", desync, unreported)
	}
	if w.dup != 0 {
		t.Errorf("G-INFL-1: a row was outstanding in two batches at once %d times", w.dup)
	}
	if n := abandoned(g); n != 0 {
		t.Errorf("drain_abandoned_rows = %d, want 0 with working writers\n%s", n, buf.String())
	}
	if n := in.mirrorShards[0].UnitsSupersededOutOfOrder.Load(); n != 0 {
		t.Errorf("G-ORD-1: units_superseded_out_of_order = %d, want 0: a unit was judged out of "+
			"order on a feed whose order is its arrival", n)
	}
	if ties != 0 {
		t.Errorf("seam 9 regressed: the receipt clock tied %d times; nextRecvMono must make RecvMono "+
			"strictly increasing, or each tie discards a distinct datagram as a re-delivery", ties)
	}
	if w.reoffered != 0 {
		t.Errorf("G-ROW-1: a row whose last verdict was Rejected was offered to a writer again %d "+
			"times; a quarantined row stays off the rings until a release", w.reoffered)
	}
	for i, c := range in.mirrorTranscode.BlobLenMismatch {
		if n := c.Load(); n != 0 {
			t.Errorf("blob_len_mismatch[%d] = %d, want 0", i, n)
		}
	}

	// ---- the walk ----
	chunk := int32(g.deps.Cfg.Runtime.ArenaChunkRows)
	m := g.deps.Cfg.Runtime.MaxInFlightPerShard
	ltpCol := g.deps.Dests.ByID(0).Column(0) // last_traded_price
	checked, quarantined := 0, 0
	for k, s := range g.shards {
		if len(s.Free) != m {
			t.Errorf("shard %d: Free holds %d of %d batches at quiescence", k, len(s.Free), m)
		}
		for d := range model.DestID(model.MaxDestinations) {
			dst := g.deps.Dests.ByID(d)
			for row, slot := range s.Index[d] {
				e := &s.Arena[d][slot/chunk][slot%chunk]
				if (e.Dirty && e.InFlight) || (e.ReDirty && !e.InFlight) {
					t.Errorf("INV-STATE: shard %d dest %d row %d: Dirty %v InFlight %v ReDirty %v",
						k, d, row, e.Dirty, e.InFlight, e.ReDirty)
				}
				if e.Dirty || e.InFlight || e.ReDirty {
					t.Errorf("shard %d dest %d row %d not quiescent: Dirty %v InFlight %v ReDirty %v",
						k, d, row, e.Dirty, e.InFlight, e.ReDirty)
					continue
				}
				rec := w.rows[rowID{model.ShardID(k), d, row}]
				if rec == nil {
					t.Errorf("shard %d dest %d row %d never reached a writer", k, d, row)
					continue
				}
				if e.Written != rec.written || e.Digest != rec.digest {
					t.Errorf("G-ROW-1: shard %d row %d: Written %#x Digest %#x, the commits say %#x %#x",
						k, row, uint64(e.Written), e.Digest, uint64(rec.written), rec.digest)
				}
				if e.Quarantined {
					quarantined++
					if !rec.rejected || !poison[row] {
						t.Errorf("G-ROW-1: shard %d row %d is quarantined; last verdict Rejected %v, "+
							"a poison row %v -- want true, true", k, row, rec.rejected, poison[row])
					}
					continue
				}
				if rec.rejected {
					t.Errorf("G-ROW-1: shard %d row %d was last Rejected but is not quarantined", k, row)
				}
				if e.ColMask != 0 {
					t.Errorf("shard %d row %d: ColMask %#x at quiescence", k, row, uint64(e.ColMask))
				}
				for mk := e.Written; mk != 0; mk &= mk - 1 {
					col := dst.Column(uint8(bits.TrailingZeros64(uint64(mk))))
					if string(rec.img[col.Offset:col.End()]) != string(e.Image[col.Offset:col.End()]) {
						t.Errorf("G-MASK-1: shard %d row %d column %s: the database holds %x, the "+
							"mirror %x -- a column dirtied in flight was lost", k, row, col.Name(),
							rec.img[col.Offset:col.End()], e.Image[col.Offset:col.End()])
					}
				}
				if d == 0 {
					got := binary.BigEndian.Uint32(e.Image[ltpCol.Offset:])
					if want, ok := expect[uint32(row)]; !ok || got != want || !e.Written.Has(0) {
						t.Errorf("G-ORD-1: shard %d row %d holds last_traded_price %d (written %v); "+
							"the last value the decoder saw was %d (seen %v)",
							k, row, got, e.Written.Has(0), want, ok)
					}
				}
				checked++
			}
		}
	}
	if rows := len(g.shards[0].Index[0]) + len(g.shards[1].Index[0]) +
		len(g.shards[2].Index[0]) + len(g.shards[3].Index[0]); rows != len(expect) {
		t.Errorf("the shards hold %d rows; the decoder saw %d tokens", rows, len(expect))
	}

	// ---- non-vacuity ----
	for _, c := range []struct {
		what string
		n    uint64
	}{
		{"datagrams decoded", uint64(decoded.Load())},
		{"epochs opened", epochs},
		{"rows committed", uint64(w.committed)},
		{"rows failed", uint64(w.failed)},
		{"rows rejected", uint64(w.rejected)},
		{"batches failed whole", uint64(w.batchFailed)},
		{"rows re-dirtied in flight", in.mirrorShards[0].RedirtyTotal.Load()},
		{"rows requeued for retry", retried},
		{"rows quarantined at the end", uint64(quarantined)},
		{"rows checked against the database", uint64(checked)},
	} {
		if c.n == 0 {
			t.Errorf("non-vacuity: no %s, so part of the gate measured nothing", c.what)
		}
	}
}
