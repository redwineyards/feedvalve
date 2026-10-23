package mirror

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// evalParams is ordering.* at its defaults (Vol 0 section 7.2) under mode.
func evalParams(mode GuardMode) *Params {
	return &Params{Guard: mode, GuardSuppressionRatio: 0.5, GuardMinSamples: 100}
}

// evalRig is a one-code GuardTable with its gauge, snapshot and logger: the
// evaluator alone, the counters driven by hand.
type evalRig struct {
	tab   *GuardTable
	gauge []*obs.Gauge
	snap  *GuardSnapshot
	buf   bytes.Buffer
	log   *obs.AppLogger
}

func newEvalRig() *evalRig {
	r := &evalRig{tab: newTestGuardTable(7200), gauge: []*obs.Gauge{{}}}
	r.snap = NewGuardSnapshot(r.tab, r.gauge)
	r.log = obs.NewAppLogger(&r.buf, obs.LevelInfo, 1000, nil)
	return r
}

// add counts offered comparisons, superseded of them superseded.
func (r *evalRig) add(offered, superseded uint64) {
	r.tab.entries[0].offered.Add(offered)
	r.tab.entries[0].superseded.Add(superseded)
}

func (r *evalRig) eval(p *Params) { EvaluateGuards(r.tab, p, r.snap, r.log) }

func (r *evalRig) off() bool { return r.tab.entries[0].off.Load() }

// disabledLines is how many guard-disabled ERRORs were logged.
func (r *evalRig) disabledLines() int {
	return strings.Count(r.buf.String(), "ordering guard disabled")
}

// deadSeqBase is the dead transcode's first BCSeqNo. Every later one counts
// DOWN from it, so each reads as older at section 5.3 step 6.
const deadSeqBase = int32(1 << 20)

// ---- G-ORD-2 ----

// TestG_ORD_2_OnlyTheDeadSequenceTranscodeDisables is gate G-ORD-2, through
// the real merge and guard.
//
// The gate (V4:1624) names a CONSTANT BCSeqNo. Under section 5.3 step 8 that
// alone does not freeze anything: every unit has its own advancing RecvMono
// (B-V1b), so equal BCSeqNo and LogTime fall through to step 8 and Apply. The
// premise check below pins that, and the dead field here is one that reads as
// OLDER at step 6 -- a descending BCSeqNo -- which is the fault the gate is
// about (section 6 erratum to V4:1624 / HLD section 6.5).
//
// SETUP: destination MD_TOKEN_QUOTE, two contributors, one receipt clock
// advancing for every unit of both. 7208 is healthy: an ascending BCSeqNo on
// row A, three units a round. 7200's sequence is dead: after its first unit,
// every BCSeqNo is lower than the one before, LogTime constant, on row B, one
// unit a round, each carrying a new value -- so every comparison after its
// first is superseded at step 6. 150 rounds: past guardMinSamples (100) for
// both, at a mix whose GLOBAL ratio is about 0.25, below
// guardSuppressionRatio (0.5).
//
// STIMULUS: one EvaluateGuards; then one more 7200 unit for row B.
//
// ASSERTION: only 7200's guard is off; ordering_guard_disabled is 1 at 7200's
// position and 0 at every other; one ERROR, naming 7200; row B, frozen at its
// first value before the evaluation, takes the new one after it. Non-vacuity:
// the global ratio really is below the threshold and 7200's own is above it,
// and row B really was frozen.
//
// WOULD HAVE CAUGHT: a global ratio, which never fires for a minority
// transcode with a dead sequence field and, when it does fire, switches the
// guard off for the well-behaved ones too (HLD section 6.5).
func TestG_ORD_2_OnlyTheDeadSequenceTranscodeDisables(t *testing.T) {
	// Premise: a constant BCSeqNo with an advancing RecvMono is applied.
	if v := compareOrdering(seenEntry(0, ord{7, 100, 5}), 0, testUnit(7200, ord{7, 100, 6}, false)); v != verdictApply {
		t.Fatalf("premise: constant BCSeqNo and LogTime with a newer RecvMono = %d, want Apply (step 8)", v)
	}

	s, _ := newMergeShard(t, GuardAuto)
	const rowA, rowB = model.RowKey(1), model.RowKey(2)
	slot08, slot00 := slotOf(t, s, tokenQuote, 7208), slotOf(t, s, tokenQuote, 7200)
	var mono int64 // the receipt clock: one reading per unit, B-V1b
	seq08, seq00 := int32(0), deadSeqBase+1
	dead := func(v uint32) {
		seq00--
		mono++
		u := model.UpdateUnit{Code: 7200, Seq: seq00, LogTime: 100, RecvMono: mono}
		put(t, s, &u, tokenQuote, rowB, slot00, fv{0, be32(v)})
		s.merge(u)
	}
	for round := range 150 {
		for range 3 {
			seq08++
			mono++
			u := model.UpdateUnit{Code: 7208, Seq: seq08, LogTime: 100, RecvMono: mono}
			put(t, s, &u, tokenQuote, rowA, slot08, fv{0, be32(uint32(seq08))})
			s.merge(u)
		}
		dead(uint32(round + 1))
	}

	o00, s00 := guardCounters(s, 7200)
	o08, s08 := guardCounters(s, 7208)
	global := float64(s00+s08) / float64(o00+o08)
	if o00 < 100 || o08 < 100 || global >= 0.5 || float64(s00)/float64(o00) <= 0.5 {
		t.Fatalf("non-vacuity: 7200 %d/%d, 7208 %d/%d, global ratio %.3f; want both past 100 offered, "+
			"7200's own ratio above 0.5 and the global one below it", s00, o00, s08, o08, global)
	}
	if got := ltp(&rowEntry(s, tokenQuote, rowB).Image); got != 1 {
		t.Fatalf("non-vacuity: row B = %d before the evaluation, want 1 -- the dead transcode's rows were not frozen", got)
	}

	var buf bytes.Buffer
	disabled := s.inst.Transcode.GuardDisabled
	EvaluateGuards(s.guards, evalParams(GuardAuto), NewGuardSnapshot(s.guards, disabled),
		obs.NewAppLogger(&buf, obs.LevelInfo, 1000, nil))

	if !s.guards.lookup(7200).off.Load() || s.guards.lookup(7208).off.Load() {
		t.Fatalf("off: 7200 %v, 7208 %v; want only 7200 disabled",
			s.guards.lookup(7200).off.Load(), s.guards.lookup(7208).off.Load())
	}
	for i, g := range disabled {
		want := int64(0)
		if s.guards.entries[i].code == 7200 {
			want = 1
		}
		if got := g.Load(); got != want {
			t.Fatalf("ordering_guard_disabled{%d} = %d, want %d", s.guards.entries[i].code, got, want)
		}
	}
	if log := buf.String(); strings.Count(log, "ordering guard disabled") != 1 || !strings.Contains(log, "transcode=7200 ") {
		t.Fatalf("want exactly one guard-disabled ERROR, for 7200; got:\n%s", log)
	}

	dead(9999)
	if got := ltp(&rowEntry(s, tokenQuote, rowB).Image); got != 9999 {
		t.Fatalf("row B = %d after the disable, want 9999: the dead transcode's rows did not resume updating", got)
	}
}

// ---- adversarial ----

// TestGuardEvalBelowMinSamplesNeverDisables: after one real (healthy)
// evaluation has advanced the snapshot, every comparison in the next window
// superseded, but one short of the floor, across many evaluations with no new
// samples.
//
// WOULD HAVE CAUGHT: a floor on the lifetime total rather than the window's
// delta, which judges every window after the first on a handful of samples.
func TestGuardEvalBelowMinSamplesNeverDisables(t *testing.T) {
	r := newEvalRig()
	r.add(100, 0)
	r.eval(evalParams(GuardAuto))
	if r.off() || r.snap.offered[0] != 100 {
		t.Fatalf("setup: after a healthy evaluation off=%v snapshot offered=%d; want on and 100", r.off(), r.snap.offered[0])
	}
	r.add(99, 99)
	for range 10 {
		r.eval(evalParams(GuardAuto))
	}
	if r.off() || r.gauge[0].Load() != 0 || r.disabledLines() != 0 {
		t.Fatal("99 offered comparisons in the window disabled the guard; the floor is 100")
	}
	r.add(1, 1) // non-vacuity: the 100th sample is enough
	r.eval(evalParams(GuardAuto))
	if !r.off() || r.gauge[0].Load() != 1 || r.disabledLines() != 1 {
		t.Fatal("non-vacuity: 100 of 100 superseded did not disable the guard")
	}
}

// TestGuardEvalRatioIsPerWindow: a long healthy history followed by a window
// in which every comparison is superseded. The window's ratio is 1.0; the
// lifetime ratio is far below the threshold.
//
// WOULD HAVE CAUGHT: a lifetime ratio, under which a transcode healthy for
// hours that then loses its sequence field is diluted and never disabled, its
// rows frozen for the rest of the session.
func TestGuardEvalRatioIsPerWindow(t *testing.T) {
	r := newEvalRig()
	r.add(100000, 0)
	r.eval(evalParams(GuardAuto))
	if r.off() {
		t.Fatal("setup: a healthy window disabled the guard")
	}
	r.add(100, 100)
	o, s := r.tab.entries[0].offered.Load(), r.tab.entries[0].superseded.Load()
	if lifetime := float64(s) / float64(o); lifetime > 0.5 {
		t.Fatalf("non-vacuity: lifetime ratio %.4f, want it at or below the 0.5 threshold", lifetime)
	}
	r.eval(evalParams(GuardAuto))
	if !r.off() || r.gauge[0].Load() != 1 {
		t.Fatal("a window of 100 of 100 superseded after a healthy history did not disable the guard")
	}
}

// TestGuardEvalRatioAtTheThresholdDoesNotDisable: the comparison is strictly
// greater-than (V2:1358), so a ratio equal to the threshold leaves the guard
// on -- and a configured threshold of 1.0, which validate accepts, can never
// fire.
func TestGuardEvalRatioAtTheThresholdDoesNotDisable(t *testing.T) {
	r := newEvalRig()
	r.add(100, 50)
	r.eval(evalParams(GuardAuto))
	if r.off() {
		t.Fatal("ratio 0.5 against a 0.5 threshold disabled the guard; the test is strictly greater-than")
	}

	all := newEvalRig()
	p := evalParams(GuardAuto)
	p.GuardSuppressionRatio = 1.0
	all.add(1000, 1000)
	all.eval(p)
	if all.off() {
		t.Fatal("ratio 1.0 against a 1.0 threshold disabled the guard")
	}

	r.add(100, 51) // non-vacuity: just over the threshold does fire
	r.eval(evalParams(GuardAuto))
	if !r.off() {
		t.Fatal("non-vacuity: 51 of 100 superseded did not disable the guard")
	}
}

// TestGuardEvalWindowSkewCannotReachOne: the evaluator's two loads are not one
// snapshot, so every comparison finishing between them lands its offered in
// the previous window and its superseded in this one. Nothing bounds how many,
// so Δsuperseded can exceed Δoffered by any amount. The ratio is clamped to 1,
// so a 1.0 threshold stays unreachable.
func TestGuardEvalWindowSkewCannotReachOne(t *testing.T) {
	r := newEvalRig()
	p := evalParams(GuardAuto)
	p.GuardSuppressionRatio = 1.0
	first := evalParams(GuardOn)
	first.GuardMinSamples = 1
	r.add(4, 0) // offers read by the first evaluation ...
	r.eval(first)
	if r.snap.offered[0] != 4 {
		t.Fatalf("setup: snapshot offered = %d, want 4", r.snap.offered[0])
	}
	r.add(100, 104) // ... their supersessions landing in the next window
	r.eval(p)
	if r.off() {
		t.Fatal("a window with Δsuperseded > Δoffered disabled a guard whose threshold is 1.0")
	}
}

// TestGuardEvalDisableIsOneWay: once off, a recovered ratio never re-enables
// it, the gauge stays 1, and a further bad window does not repeat the ERROR.
//
// WOULD HAVE CAUGHT: evaluating an entry already off, which re-logs the ERROR
// on every bad window.
func TestGuardEvalDisableIsOneWay(t *testing.T) {
	r := newEvalRig()
	r.add(100, 100)
	r.eval(evalParams(GuardAuto))
	if !r.off() || r.disabledLines() != 1 {
		t.Fatal("non-vacuity: 100 of 100 superseded did not disable the guard")
	}
	for range 5 {
		r.add(1000, 0)
		r.eval(evalParams(GuardAuto))
	}
	r.eval(evalParams(GuardOn))
	if !r.off() || r.gauge[0].Load() != 1 || r.disabledLines() != 1 {
		t.Fatalf("after healthy windows: off=%v gauge=%d errors=%d; want true, 1, 1",
			r.off(), r.gauge[0].Load(), r.disabledLines())
	}
	r.add(1000, 1000)
	r.eval(evalParams(GuardAuto))
	if !r.off() || r.gauge[0].Load() != 1 || r.disabledLines() != 1 {
		t.Fatalf("after another bad window: off=%v gauge=%d errors=%d; want true, 1, 1",
			r.off(), r.gauge[0].Load(), r.disabledLines())
	}
}

// TestGuardEvalOnNeverDisables: ordering.guard = on keeps the guard however
// bad the ratio; the same counters under auto disable it.
func TestGuardEvalOnNeverDisables(t *testing.T) {
	for _, mode := range []GuardMode{GuardOn, GuardOff} {
		r := newEvalRig()
		r.add(1000, 1000)
		r.eval(evalParams(mode))
		if r.off() || r.gauge[0].Load() != 0 || r.disabledLines() != 0 {
			t.Fatalf("ordering.guard mode %d auto-disabled the guard", mode)
		}
	}
	r := newEvalRig()
	r.add(1000, 1000)
	r.eval(evalParams(GuardAuto))
	if !r.off() {
		t.Fatal("non-vacuity: the same counters under auto did not disable the guard")
	}
}

// TestGuardEvalSkippedIntervalDoesNotAdvanceTheSnapshot is section 5.5 point
// 1: a low-rate transcode reaches the floor ACROSS intervals. 60 per interval
// never reaches 100 in any one window, but the second evaluation sees 120.
//
// WOULD HAVE CAUGHT: advancing the snapshot on a skip, after which a transcode
// below the floor per interval is never evaluated at all.
func TestGuardEvalSkippedIntervalDoesNotAdvanceTheSnapshot(t *testing.T) {
	r := newEvalRig()
	r.add(60, 60)
	r.eval(evalParams(GuardAuto))
	if r.off() || r.snap.offered[0] != 0 || r.snap.superseded[0] != 0 {
		t.Fatalf("after a skipped interval: off=%v snapshot=%d/%d; want on and 0/0",
			r.off(), r.snap.superseded[0], r.snap.offered[0])
	}
	r.add(60, 60)
	r.eval(evalParams(GuardAuto))
	if !r.off() {
		t.Fatal("120 offered across two intervals, all superseded, did not disable the guard")
	}

	// The converse: an ACTUAL evaluation does advance it, so an old bad
	// window is not re-judged together with a later healthy one.
	h := newEvalRig()
	h.add(100, 50)
	h.eval(evalParams(GuardAuto))
	if h.snap.offered[0] != 100 || h.snap.superseded[0] != 50 {
		t.Fatalf("snapshot after an evaluation = %d/%d, want 50/100", h.snap.superseded[0], h.snap.offered[0])
	}
}

// TestNewGuardSnapshotRejectsMiswiredGauges: a short gauge slice or a nil
// gauge panics at construction -- startup -- not at the first disable, which
// may be hours in and on the stats goroutine.
func TestNewGuardSnapshotRejectsMiswiredGauges(t *testing.T) {
	tab := newTestGuardTable(7200, 7208)
	panics := func(name string, g []*obs.Gauge) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: NewGuardSnapshot did not panic", name)
			}
		}()
		NewGuardSnapshot(tab, g)
	}
	panics("short", []*obs.Gauge{{}})
	panics("nil", []*obs.Gauge{{}, nil})
	panics("absent", nil)
	NewGuardSnapshot(tab, []*obs.Gauge{{}, {}}) // non-vacuity: a well-wired one is accepted
}

// ---- race ----

// TestGuardEvalConcurrentWithMerge runs the evaluator on one goroutine while
// another drives s.merge directly, as the stats goroutine and a shard do in
// production. Under -race this checks the off bit and the counters are the
// only shared state, and all of it atomic.
//
// The merging goroutine keeps going until it sees 7200's off bit, so the
// evaluator's Store provably happened while merges were running (against the
// merge's hoisted Load), then merges a while longer with it set. Non-vacuity:
// the bit must flip within the iteration bound, and the healthy transcode must
// still be on at the end.
func TestGuardEvalConcurrentWithMerge(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	slot08, slot00 := slotOf(t, s, tokenQuote, 7208), slotOf(t, s, tokenQuote, 7200)
	snap := NewGuardSnapshot(s.guards, s.inst.Transcode.GuardDisabled)
	log := obs.NewAppLogger(&bytes.Buffer{}, obs.LevelInfo, 1000, nil)
	p := evalParams(GuardAuto)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				EvaluateGuards(s.guards, p, snap, log)
			}
		}
	}()

	const bound, after = 2_000_000, 1000
	dead := s.guards.lookup(7200)
	var mono int64
	seenOff := -1
	for i := int32(0); i < bound && (seenOff < 0 || int(i) < seenOff+after); i++ {
		mono++
		u := model.UpdateUnit{Code: 7208, Seq: i + 1, LogTime: 100, RecvMono: mono}
		put(t, s, &u, tokenQuote, 1, slot08, fv{0, be32(uint32(i))})
		s.merge(u)
		mono++
		d := model.UpdateUnit{Code: 7200, Seq: deadSeqBase - i, LogTime: 100, RecvMono: mono}
		put(t, s, &d, tokenQuote, 2, slot00, fv{0, be32(uint32(i))})
		s.merge(d)
		if seenOff < 0 && dead.off.Load() {
			seenOff = int(i)
		}
	}
	close(done)
	wg.Wait()

	if seenOff < 0 {
		t.Fatalf("non-vacuity: 7200 was not disabled by the concurrent evaluator within %d merges", bound)
	}
	if s.guards.lookup(7208).off.Load() {
		t.Fatal("the healthy transcode 7208 was disabled")
	}
}
