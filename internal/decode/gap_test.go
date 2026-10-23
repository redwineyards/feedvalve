package decode

import (
	"math"
	"testing"

	"feedvalve/internal/model"
)

// Gap detection: Vol 1 section 7, and gate G-ORD-4.
//
// The test that matters most is the wrap. A 4-byte signed counter at 10,000
// messages/s wraps in about 59 hours, so it is reachable inside one session --
// and written as cur > last instead of int32(cur-last) > 0, every affected row
// freezes PERMANENTLY after it. That defect has no symptom at the moment it
// happens; it shows up as rows that stopped updating, hours later.

// recordingReports captures what the detector published.
type recordingReports struct {
	gaps     []gapReport
	resyncs  []gapReport
	regress  int
	auditOK  [2]int
	auditBad [2]int
}

type gapReport struct {
	scope GapScope
	code  model.TransCode
	from  int32
	to    int32
	n     int64
}

func (r *recordingReports) SeqGap(scope GapScope, code model.TransCode, from, to int32, missing int64) {
	r.gaps = append(r.gaps, gapReport{scope, code, from, to, missing})
}

func (r *recordingReports) SeqRegression() { r.regress++ }

func (r *recordingReports) SeqResync(scope GapScope, code model.TransCode, from, to int32, delta int64) {
	r.resyncs = append(r.resyncs, gapReport{scope, code, from, to, delta})
}

func (r *recordingReports) GapAudit(hypothesis int, agree bool) {
	if hypothesis < 0 || hypothesis > 1 {
		return
	}
	if agree {
		r.auditOK[hypothesis]++
	} else {
		r.auditBad[hypothesis]++
	}
}

// missing sums every reported gap, which is what seq_gap_messages accumulates.
func (r *recordingReports) missing() int64 {
	var total int64
	for _, g := range r.gaps {
		total += g.n
	}
	return total
}

// bcast builds a resolved BCAST_HEADER with one sequence number.
func bcast(seq int32) model.MsgHeader {
	return model.MsgHeader{Kind: model.HeaderBcast, Code: 7208, BCSeqNo: seq}
}

// feed observes a sequence of numbers under global scope.
func feed(t *testing.T, seqs ...int32) *recordingReports {
	t.Helper()
	rep := &recordingReports{}
	g := NewGapDetector(GapGlobal, 0, rep)
	for _, s := range seqs {
		g.Observe(bcast(s), 0)
	}
	return rep
}

// TestG_ORD_4_TheInt32WrapIsNewerNotOlder is gate G-ORD-4.
//
// LastSeq at int32 0x7FFFFFFF followed by BCSeqNo 0x80000000 must be treated as
// NEWER. As unsigned or as a naive comparison the second is "smaller", so a
// cur > last implementation reports a regression and never advances again --
// every row fed by that sequence space freezes for the rest of the session.
//
// Signed overflow in Go is DEFINED, so int32(0x80000000 - 0x7FFFFFFF) is
// exactly 1 and the arithmetic needs no special case.
func TestG_ORD_4_TheInt32WrapIsNewerNotOlder(t *testing.T) {
	const (
		maxInt32 = int32(math.MaxInt32) // 0x7FFFFFFF
		minInt32 = int32(math.MinInt32) // 0x80000000
	)

	rep := feed(t, maxInt32, minInt32)

	if len(rep.gaps) != 0 {
		t.Errorf("the wrap reported %d gaps: %+v", len(rep.gaps), rep.gaps)
	}
	if rep.regress != 0 {
		t.Errorf("the wrap reported %d regressions; 0x80000000 is NEWER than 0x7FFFFFFF under "+
			"serial-number arithmetic, and a cur > last implementation is what gets this wrong "+
			"-- after which every affected row freezes permanently", rep.regress)
	}
	if len(rep.resyncs) != 0 {
		t.Errorf("the wrap reported %d resyncs: %+v", len(rep.resyncs), rep.resyncs)
	}

	// The delta really is 1, which is the arithmetic the whole rule rests on.
	//
	// Through VARIABLES, and that is not incidental: Go's CONSTANT arithmetic
	// is arbitrary-precision and does not wrap, so writing this as
	// `minInt32 - maxInt32` on two constants is a compile error
	// ("overflows int32") rather than the answer 1. Variable int32 arithmetic
	// is the thing the detector does, and it wraps as the specification says.
	// Worth knowing before anyone reads a compile error here as evidence that
	// the rule does not work.
	lo, hi := minInt32, maxInt32
	if got := lo - hi; got != 1 {
		t.Fatalf("int32(%d - %d) = %d, want 1 -- Go's signed overflow is defined and this is "+
			"the serial-number subtraction", lo, hi, got)
	}

	// Section 7.5's own suggested sequence, straddling the wrap.
	rep = feed(t, 2_147_483_646, 2_147_483_647, -2_147_483_648, -2_147_483_647)
	if len(rep.gaps) != 0 || rep.regress != 0 || len(rep.resyncs) != 0 {
		t.Errorf("the four-step wrap sequence reported gaps=%d regressions=%d resyncs=%d, "+
			"want none of any", len(rep.gaps), rep.regress, len(rep.resyncs))
	}

	// And it keeps working after the wrap: the next message is in sequence.
	rep = feed(t, maxInt32, minInt32, minInt32+1, minInt32+2)
	if len(rep.gaps) != 0 || rep.regress != 0 {
		t.Errorf("continuing past the wrap reported gaps=%d regressions=%d", len(rep.gaps), rep.regress)
	}
}

// TestGapComparisonBranches walks section 7.3's five outcomes.
func TestGapComparisonBranches(t *testing.T) {
	for _, tc := range []struct {
		name        string
		seqs        []int32
		wantGaps    int
		wantMissing int64
		wantRegress int
		wantResync  int
	}{
		{"in sequence", []int32{100, 101, 102, 103}, 0, 0, 0, 0},
		{"a gap of one", []int32{100, 102}, 1, 1, 0, 0},
		{"a gap of 339", []int32{100, 440}, 1, 339, 0, 0},
		{"a duplicate", []int32{100, 100}, 0, 0, 1, 0},
		{"a regression", []int32{100, 99}, 0, 0, 1, 0},
		{"a far regression", []int32{100, -5000}, 0, 0, 1, 0},
		{"exactly the resync threshold is still a gap", []int32{0, seqResyncThreshold}, 1,
			seqResyncThreshold - 1, 0, 0},
		{"one past the threshold is a resync", []int32{0, seqResyncThreshold + 1}, 0, 0, 0, 1},
		{"a two-billion jump is a resync, not a gap", []int32{0, 2_000_000_000}, 0, 0, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep := feed(t, tc.seqs...)

			if len(rep.gaps) != tc.wantGaps {
				t.Errorf("gaps = %d, want %d: %+v", len(rep.gaps), tc.wantGaps, rep.gaps)
			}
			if got := rep.missing(); got != tc.wantMissing {
				t.Errorf("missing total = %d, want %d", got, tc.wantMissing)
			}
			if rep.regress != tc.wantRegress {
				t.Errorf("regressions = %d, want %d", rep.regress, tc.wantRegress)
			}
			if len(rep.resyncs) != tc.wantResync {
				t.Errorf("resyncs = %d, want %d: %+v", len(rep.resyncs), tc.wantResync, rep.resyncs)
			}
		})
	}
}

// TestGapReportsWhatSection7_4Specifies: from, to and missing, and nothing
// invented.
func TestGapReportsWhatSection7_4Specifies(t *testing.T) {
	rep := feed(t, 1000, 1340)

	if len(rep.gaps) != 1 {
		t.Fatalf("gaps = %d, want 1", len(rep.gaps))
	}
	g := rep.gaps[0]
	if g.from != 1000 || g.to != 1340 {
		t.Errorf("gap from=%d to=%d, want 1000 and 1340", g.from, g.to)
	}
	if g.n != 339 {
		t.Errorf("missing = %d, want 339 (delta-1, not delta)", g.n)
	}
	if g.scope != GapGlobal {
		t.Errorf("scope = %v, want GapGlobal", g.scope)
	}
}

// TestRegressionDoesNotRewindTheDetector is the subtle half of section 7.5.
//
// last is left UNCHANGED on a duplicate or a reorder, so one reordered datagram
// does not rewind the detector and manufacture a gap on the NEXT message. If
// last were advanced, the sequence 100, 99, 101 would report a gap of one at
// 101 -- a fabricated loss caused entirely by a reordering that lost nothing.
func TestRegressionDoesNotRewindTheDetector(t *testing.T) {
	rep := feed(t, 100, 99, 101)

	if rep.regress != 1 {
		t.Errorf("regressions = %d, want 1", rep.regress)
	}
	if len(rep.gaps) != 0 {
		t.Errorf("a reordered datagram manufactured %d gaps: %+v -- last must not be rewound "+
			"by a regression", len(rep.gaps), rep.gaps)
	}

	// The same for a duplicate: 100, 100, 101 is continuous.
	rep = feed(t, 100, 100, 101)
	if len(rep.gaps) != 0 {
		t.Errorf("a duplicate manufactured %d gaps: %+v", len(rep.gaps), rep.gaps)
	}
}

// TestResyncKeepsTheMagnitudeOutOfTheLossFigure is ADR-L12's whole purpose.
//
// The event is real -- something discontinuous happened -- so seq_gap_events
// moves. The magnitude is not trustworthy, so seq_gap_messages must NOT: one
// exchange-side counter reset would otherwise add two billion to a cumulative
// counter and make the session loss rate meaningless for the rest of the day,
// on the one instrument the triage of section 7.1 depends on.
func TestResyncKeepsTheMagnitudeOutOfTheLossFigure(t *testing.T) {
	rep := feed(t, 500, 1_500_000_000)

	if len(rep.resyncs) != 1 {
		t.Fatalf("resyncs = %d, want 1", len(rep.resyncs))
	}
	if len(rep.gaps) != 0 {
		t.Errorf("a resync also reported %d gaps: the magnitude must not reach "+
			"seq_gap_messages", len(rep.gaps))
	}
	if got := rep.missing(); got != 0 {
		t.Errorf("missing total = %d after a resync, want 0", got)
	}
	r := rep.resyncs[0]
	if r.from != 500 || r.to != 1_500_000_000 {
		t.Errorf("resync from=%d to=%d, want 500 and 1500000000", r.from, r.to)
	}

	// And the detector RESUMES from the new value rather than staying stuck.
	rep = feed(t, 500, 1_500_000_000, 1_500_000_001, 1_500_000_002)
	if len(rep.gaps) != 0 || rep.regress != 0 {
		t.Errorf("after a resync the detector reported gaps=%d regressions=%d; it must track "+
			"the new sequence", len(rep.gaps), rep.regress)
	}
}

// TestFirstMessagePerSlotEmitsNothing is section 7.5's first rule.
//
// We do not know what the counter was before we started listening, so
// reporting missing = cur would announce a gap of however many messages the
// exchange has sent since midnight. A restarted process is "first message"
// again, and that is correct as well as unavoidable.
func TestFirstMessagePerSlotEmitsNothing(t *testing.T) {
	for _, first := range []int32{0, 1, 1_000_000, math.MaxInt32, -1, math.MinInt32} {
		rep := feed(t, first)
		if len(rep.gaps) != 0 || rep.regress != 0 || len(rep.resyncs) != 0 {
			t.Errorf("first message %d emitted gaps=%d regressions=%d resyncs=%d, want silence",
				first, len(rep.gaps), rep.regress, len(rep.resyncs))
		}
	}

	// Zero is a LEGAL sequence number -- the counter passes through it on
	// every wrap -- so "have" must be tracked separately from "last == 0".
	// Were it not, 0 followed by 1 would be read as a first message twice and
	// 0 followed by 5 would report nothing.
	rep := feed(t, 0, 5)
	if len(rep.gaps) != 1 || rep.missing() != 4 {
		t.Errorf("a first message of ZERO was not recorded: gaps=%d missing=%d, want 1 and 4 "+
			"-- zero is a legal sequence number and `have` is what distinguishes it",
			len(rep.gaps), rep.missing())
	}
}

// TestHeaderMessageIsNeverGapDetected is section 7.2, and the numbers are the
// point.
//
// A MESSAGE_HEADER's offset 14 holds the top half of a nanosecond Timestamp.
// In 2026 that timestamp is about 1.45e18, whose top 32 bits are about 3.4e8
// and change every ~4.3 seconds -- so read as a BCSeqNo it fabricates a gap of
// hundreds of millions of messages on EVERY nine-second heartbeat. The test
// plants that realistic value rather than a token one, because a small value
// would not demonstrate the failure.
func TestHeaderMessageIsNeverGapDetected(t *testing.T) {
	const ts2026 = int64(1_450_000_000_000_000_000) // ns since 1 Jan 1980, roughly 2026
	topHalf := int32(ts2026 >> 32)

	if topHalf < 100_000_000 {
		t.Fatalf("the planted timestamp's top half is %d; the test needs a realistic value to "+
			"demonstrate anything", topHalf)
	}

	rep := &recordingReports{}
	g := NewGapDetector(GapGlobal, 0, rep)

	// A normal BCAST message, then two heartbeats, then the next BCAST in
	// sequence. The heartbeats must be invisible to the detector.
	g.Observe(bcast(100), 0)
	for _, code := range []model.TransCode{6541, 6503} {
		g.Observe(model.MsgHeader{
			Kind:    model.HeaderMessage,
			Code:    code,
			BCSeqNo: topHalf, // what offset 14 would yield if read as a sequence
		}, 0)
	}
	g.Observe(bcast(101), 0)

	if len(rep.gaps) != 0 {
		t.Errorf("a MESSAGE_HEADER was gap-detected: %+v. Its offset 14 is the top half of a "+
			"nanosecond Timestamp (~%d), so this fabricates a gap of hundreds of millions "+
			"every nine seconds", rep.gaps, topHalf)
	}
	if len(rep.resyncs) != 0 {
		t.Errorf("a MESSAGE_HEADER produced %d resyncs; it must not reach the detector at all",
			len(rep.resyncs))
	}
	if rep.regress != 0 {
		t.Errorf("a MESSAGE_HEADER produced %d regressions", rep.regress)
	}
}

// TestUnknownBcastCodesAreGapDetected is section 7.6, and it is deliberate
// rather than an oversight.
//
// A BCAST_HEADER carrying an unconfigured transcode has a perfectly good
// BCSeqNo and occupies a real sequence number. Excluding it would make global
// scope under-count by exactly the traffic we cannot decode, turning every
// unconfigured code into a PERMANENT PHANTOM GAP -- and on a segment where 7320
// status sweeps and 7220 LPP ranges are routine, that could be most of the
// counter.
func TestUnknownBcastCodesAreGapDetected(t *testing.T) {
	rep := &recordingReports{}
	g := NewGapDetector(GapGlobal, 0, rep)

	// 7208 is in the inventory; 7320 and 7220 are deliberately not (section
	// 9.1). All three share the sequence space.
	for i, code := range []model.TransCode{7208, 7320, 7220, 7208} {
		g.Observe(model.MsgHeader{
			Kind:    model.HeaderBcast,
			Code:    code,
			BCSeqNo: int32(500 + i),
		}, 0)
	}

	if len(rep.gaps) != 0 {
		t.Errorf("a continuous sequence across configured AND unconfigured codes reported "+
			"%d gaps: %+v. Skipping the codes we cannot decode turns each of them into a "+
			"phantom gap", len(rep.gaps), rep.gaps)
	}
}

// TestPerTranscodeScopeKeepsIndependentState: a gap in one transcode's space
// does not appear in another's.
func TestPerTranscodeScopeKeepsIndependentState(t *testing.T) {
	rep := &recordingReports{}
	g := NewGapDetector(GapPerTranscode, 0, rep)

	// Two transcodes interleaved, each internally continuous. Under GLOBAL
	// scope this would look like a storm of gaps; under per-transcode it is
	// silent, which is exactly the ambiguity the audit mode exists to settle.
	g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: 7208, BCSeqNo: 10}, 0)
	g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: 7305, BCSeqNo: 900}, 1)
	g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: 7208, BCSeqNo: 11}, 0)
	g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: 7305, BCSeqNo: 901}, 1)

	if len(rep.gaps) != 0 {
		t.Fatalf("interleaved continuous transcodes reported %d gaps under per-transcode "+
			"scope: %+v", len(rep.gaps), rep.gaps)
	}

	// Now a real gap in slot 0 only.
	g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: 7208, BCSeqNo: 20}, 0)
	if len(rep.gaps) != 1 {
		t.Fatalf("gaps = %d, want 1", len(rep.gaps))
	}
	if rep.gaps[0].code != 7208 {
		t.Errorf("the gap names code %d, want 7208", rep.gaps[0].code)
	}
	if rep.gaps[0].scope != GapPerTranscode {
		t.Errorf("scope = %v, want GapPerTranscode", rep.gaps[0].scope)
	}

	// Slot 1 is untouched by it.
	g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: 7305, BCSeqNo: 902}, 1)
	if len(rep.gaps) != 1 {
		t.Errorf("the other transcode's space saw the gap too: %+v", rep.gaps)
	}
}

// TestPerTranscodeOutOfRangeSlotGoesToTheAggregateBucket: a full dynamic table
// has no slot to give, and the aggregate bucket is the honest place for it --
// with code 0, so the log line says the scope is aggregate rather than naming a
// code it cannot name.
func TestPerTranscodeOutOfRangeSlotGoesToTheAggregateBucket(t *testing.T) {
	rep := &recordingReports{}
	g := NewGapDetector(GapPerTranscode, 0, rep)

	for _, slot := range []int{-1, gapSlots, gapSlots + 100} {
		g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: 9999, BCSeqNo: 1}, slot)
	}
	// First message for that shared slot, then a gap in it.
	g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: 9999, BCSeqNo: 50}, -1)

	if len(rep.gaps) != 1 {
		t.Fatalf("gaps = %d, want 1 from the aggregate bucket", len(rep.gaps))
	}
	if rep.gaps[0].code != 0 {
		t.Errorf("the aggregate gap names code %d; it must name none, because the table was "+
			"full and there is no code to attribute it to", rep.gaps[0].code)
	}
}

// TestGapOffObservesNothing.
func TestGapOffObservesNothing(t *testing.T) {
	rep := &recordingReports{}
	g := NewGapDetector(GapOff, 0, rep)
	for _, s := range []int32{1, 5000, 3, 2_000_000_000} {
		g.Observe(bcast(s), 0)
	}
	if len(rep.gaps) != 0 || rep.regress != 0 || len(rep.resyncs) != 0 {
		t.Errorf("scope off reported gaps=%d regressions=%d resyncs=%d",
			len(rep.gaps), rep.regress, len(rep.resyncs))
	}
}

// TestGapAuditRunsBothHypothesesAndFeedsNeither is the mechanism HLD section
// 6.5 names for closing req section 12.3 without a capture.
//
// The input is the discriminating one: two transcodes, each internally
// continuous, interleaved. Per-transcode sees perfect continuity; global sees a
// jump on every message. That is the whole question -- is BCSeqNo
// transport-level or per-message-type -- and after n messages the counters
// answer it.
func TestGapAuditRunsBothHypothesesAndFeedsNeither(t *testing.T) {
	rep := &recordingReports{}
	const audit = 6
	g := NewGapDetector(GapGlobal, audit, rep)

	seqs := []struct {
		code model.TransCode
		slot int
		seq  int32
	}{
		{7208, 0, 10}, {7305, 1, 900},
		{7208, 0, 11}, {7305, 1, 901},
		{7208, 0, 12}, {7305, 1, 902},
	}
	for _, s := range seqs {
		g.Observe(model.MsgHeader{Kind: model.HeaderBcast, Code: s.code, BCSeqNo: s.seq}, s.slot)
	}

	// NEITHER hypothesis may touch seq_gap_*: the audit's purpose is to
	// compare them before anyone trusts either one.
	if len(rep.gaps) != 0 || len(rep.resyncs) != 0 || rep.regress != 0 {
		t.Errorf("the audit fed seq_gap_*: gaps=%d resyncs=%d regressions=%d",
			len(rep.gaps), len(rep.resyncs), rep.regress)
	}

	total := rep.auditOK[auditGlobal] + rep.auditBad[auditGlobal]
	if total != audit {
		t.Errorf("the global hypothesis was reported %d times, want %d", total, audit)
	}

	// Per-transcode should agree far more often than global on this input,
	// which is the verdict the audit exists to produce.
	if rep.auditOK[auditPerTranscode] <= rep.auditOK[auditGlobal] {
		t.Errorf("per-transcode agreed %d times and global %d; on interleaved continuous "+
			"transcodes per-transcode must agree more often, or the audit cannot answer "+
			"req 12.3", rep.auditOK[auditPerTranscode], rep.auditOK[auditGlobal])
	}

	// And it STOPS after n messages, handing the feed back to the configured
	// scope: the first message past the window reports its gap, and the next
	// one tracks normally. Both halves matter -- a detector that kept auditing
	// would report neither.
	g.Observe(bcast(5000), 0)
	if len(rep.gaps) != 1 {
		t.Fatalf("the first message past the audit window reported %d gaps, want 1 -- the "+
			"audit must stop at n and hand back to the configured scope", len(rep.gaps))
	}
	g.Observe(bcast(5001), 0)
	if len(rep.gaps) != 1 {
		t.Errorf("a continuous message past the audit window reported another gap: %+v",
			rep.gaps)
	}
}

func TestGapDetectorAllocatesNothing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		scope GapScope
	}{
		{"global", GapGlobal},
		{"per-transcode", GapPerTranscode},
		{"off", GapOff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewGapDetector(tc.scope, 0, NopReports{})
			h := bcast(1)
			var seq int32

			if n := testing.AllocsPerRun(1000, func() {
				seq++
				h.BCSeqNo = seq
				g.Observe(h, 0)
			}); n != 0 {
				t.Fatalf("Observe allocates %v times per message, want 0", n)
			}
		})
	}

	// And on the REPORTING paths, which are the ones that would be tempting to
	// format a string on.
	t.Run("reporting", func(t *testing.T) {
		g := NewGapDetector(GapPerTranscode, 0, NopReports{})
		h := model.MsgHeader{Kind: model.HeaderBcast, Code: 7208, BCSeqNo: 1}
		g.Observe(h, 0)
		var seq int32 = 1

		if n := testing.AllocsPerRun(1000, func() {
			seq += 100 // a gap every time
			h.BCSeqNo = seq
			g.Observe(h, 0)
		}); n != 0 {
			t.Fatalf("reporting a gap allocates %v times, want 0", n)
		}
	})
}

func TestParseGapScope(t *testing.T) {
	for in, want := range map[string]GapScope{
		"global":        GapGlobal,
		"per-transcode": GapPerTranscode,
		"off":           GapOff,
		"":              GapGlobal, // the default, which config's range check guarantees
		"nonsense":      GapGlobal,
	} {
		if got := ParseGapScope(in); got != want {
			t.Errorf("ParseGapScope(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestGapReconfigureKeepsStateUnlessTheScopeChanges is the pickup contract for
// a reloaded ordering.gapScope and ordering.gapAuditPackets (P5-07).
//
// SETUP: a global-scope detector that has seen 10. STIMULUS: (a) Reconfigure
// with the same scope and audit; (b) the next message, 15; (c) Reconfigure to
// per-transcode and back to global; (d) the next message, 40.
// ASSERTION: (a) keeps the sequence state, so 15 is a 4-message gap; (c) clears
// it, because the two scopes track different sequence spaces and state recorded
// under one is meaningless under the other, so 40 is a first message and
// reports nothing.
// WOULD HAVE CAUGHT: a reload that resets the detector on every generation
// (an unrelated reload would forget the last sequence number and hide a gap
// that straddles it), and one that keeps stale state across a scope change
// (a fabricated gap on the first message after it).
// NON-VACUITY: 15 after (a) does report a gap, so the state survived.
func TestGapReconfigureKeepsStateUnlessTheScopeChanges(t *testing.T) {
	rep := &recordingReports{}
	g := NewGapDetector(GapGlobal, 0, rep)
	g.Observe(bcast(10), 0)

	g.Reconfigure(GapGlobal, 0)
	g.Observe(bcast(15), 0)
	if len(rep.gaps) != 1 || rep.missing() != 4 {
		t.Fatalf("an unchanged scope lost its state: gaps=%d missing=%d, want 1 and 4",
			len(rep.gaps), rep.missing())
	}

	g.Reconfigure(GapPerTranscode, 0)
	g.Reconfigure(GapGlobal, 0)
	g.Observe(bcast(40), 0)
	if len(rep.gaps) != 1 {
		t.Errorf("a scope change kept stale state: %d gaps, want still 1", len(rep.gaps))
	}
}

// TestGapReconfigureRearmsTheAuditOnlyWhenItsLengthChanges: the audit counts
// down a fixed number of messages; an unrelated reload must not restart it.
func TestGapReconfigureRearmsTheAuditOnlyWhenItsLengthChanges(t *testing.T) {
	g := NewGapDetector(GapGlobal, 5, nil)
	for i := range 3 {
		g.Observe(bcast(int32(i)), 0)
	}
	g.Reconfigure(GapGlobal, 5)
	if g.auditLeft != 2 {
		t.Errorf("auditLeft = %d after an unchanged Reconfigure, want 2 (not restarted)", g.auditLeft)
	}
	g.Reconfigure(GapGlobal, 9)
	if g.auditLeft != 9 {
		t.Errorf("auditLeft = %d after the length changed, want 9", g.auditLeft)
	}
	g.Reconfigure(GapGlobal, -3)
	if g.auditLeft != 0 {
		t.Errorf("auditLeft = %d for a negative length, want 0", g.auditLeft)
	}
}
