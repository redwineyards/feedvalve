package decode

import (
	"feedvalve/internal/model"
)

// Gap detection on BCSeqNo -- Vol 1 section 7.
//
// BCSeqNo is the LONG at BCAST_HEADER offset 14, which the bible describes as
// "BCAST sequence number (for the Ericsson switch). The only per-stream
// sequence number available; the natural basis for gap detection."
//
// # There is no recovery, and nothing here may imply there is
//
// The broadcast circuit is one-way, there is no ACK and no flow control, and
// "no host-end inquiries are permitted for NNF users" (bible section 1, 1.1).
// A detected gap is a REPORT, not a repair. What it buys is magnitude: it
// converts "the data looks odd" into "we lost 340 messages between 10:14:02 and
// 10:14:03", which is the difference between a diagnosable incident and a
// rumour.
//
// It also does one piece of triage nothing else can (HLD section 9.4): a gap
// correlated with netstat -s -p udp socket-buffer overflows is OUR stall; a gap
// without them is the circuit. There is no third case, because FeedValve
// discards nothing -- if it could not keep up it would have stopped.

// seqResyncThreshold is ADR-L12's bound: a delta above this is the sequence
// SOURCE having restarted -- an exchange-side counter reset, a switch
// failover, or our own misreading of the field -- and not a gap of that
// magnitude.
//
// A COMPILE-TIME CONSTANT and not a config key, which section 7.5 states
// outright. 1,000,000 is about 100 seconds of feed at the 10,000 msg/s
// planning ceiling and two minutes at 7,500/s (ASM-V4). It should fire
// approximately never, so changing it being a rebuild is the right trade.
//
// Without the rule one counter reset poisons seq_gap_messages permanently: the
// counter is cumulative, and a single two-billion increment makes the session
// loss rate meaningless for the rest of the day -- on the one instrument the
// triage above depends on. The cost is that a genuine outage longer than ~100 s
// is reported as a resync rather than a counted gap, and an outage that long is
// already loudly visible in circuit_state and in the absence of
// datagrams_received.
const seqResyncThreshold = 1_000_000

// GapScope is ordering.gapScope: which sequence space the detector tracks.
//
// Default global, because bible section 2.8 describes BCSeqNo as the sequence
// number "for the Ericsson switch", which reads as transport-level rather than
// per-message-type. Req section 12.3 leaves it formally open and HLD section
// 3.4(b) shows the ambiguity threatens only gap detection, not ordering -- so
// the audit mode below exists to settle it from production rather than from an
// argument.
type GapScope uint8

const (
	// GapOff keeps no state and observes nothing.
	GapOff GapScope = iota
	// GapGlobal is one sequence space for the whole feed.
	GapGlobal
	// GapPerTranscode is one sequence space per transaction code.
	GapPerTranscode
)

// ParseGapScope maps the configured string onto the enum. An unrecognised
// value yields GapGlobal, the default, because config's own range check has
// already restricted it -- this is the branch a validated Config cannot reach.
func ParseGapScope(s string) GapScope {
	switch s {
	case "off":
		return GapOff
	case "per-transcode":
		return GapPerTranscode
	default:
		return GapGlobal
	}
}

// Reports is what the detector publishes. Separate from Counters because the
// two have no caller in common: five decoders implement nothing of this, and
// the detector implements nothing of Counters. Declared here, implemented by
// app, so decode still does not import obs.
type Reports interface {
	// SeqGap is a real gap: missing is delta-1, and from/to bound it. The
	// implementation rate-limits the LINE but never the counters -- a
	// suppressed line still counts, which is the point of having both.
	SeqGap(scope GapScope, code model.TransCode, from, to int32, missing int64)

	// SeqRegression is a duplicate (delta == 0) or a reordered message
	// (delta < 0). Neither is a gap.
	SeqRegression()

	// SeqResync is ADR-L12: an implausible jump. The implementation counts the
	// EVENT but must not add the magnitude to seq_gap_messages.
	SeqResync(scope GapScope, code model.TransCode, from, to int32, delta int64)

	// GapAudit reports one message's verdict under one hypothesis, while the
	// audit is running. agree is true when that hypothesis saw no gap.
	GapAudit(hypothesis int, agree bool)
}

// NopReports discards everything. For tests that are not asserting on the
// reports, and for nothing else.
type NopReports struct{}

func (NopReports) SeqGap(GapScope, model.TransCode, int32, int32, int64)    {}
func (NopReports) SeqRegression()                                           {}
func (NopReports) SeqResync(GapScope, model.TransCode, int32, int32, int64) {}
func (NopReports) GapAudit(int, bool)                                       {}

// slotState is one sequence space.
//
// have is separate from "last == 0" because zero is a legal sequence number:
// the exchange's counter passes through it on every wrap, so a zero-value test
// would treat one message in every four billion as the first one.
type slotState struct {
	last int32
	have bool
}

// gapSlots bounds per-transcode state: one entry per configured transcode plus
// one per slot of the 64-entry dynamic table unknown_transcode{code} already
// needs (Vol 0 section 8.1). maxRegistered is 32 and the dynamic table is 64,
// so 96 covers both with the same index space the counter layer uses.
const gapSlots = ConfiguredSlots + dynamicSlots

// ConfiguredSlots is where the dynamic table's slots begin in the detector's
// index space, exported because the CALLER composes the index: Registry.Slot
// for a configured code, and ConfiguredSlots + the dynamic table's slot for
// one that is not.
//
// The offset is what stops the two index spaces colliding. Without it,
// configured transcode 3 and dynamic-table slot 3 would share one sequence
// space and interleave two unrelated counters into a storm of phantom gaps.
const ConfiguredSlots = maxRegistered

// dynamicSlots mirrors the 64-entry table unknown_transcode{code} already
// needs (Vol 0 section 8.1). Declared here rather than imported because
// decode may not import obs; the two are pinned equal by a test in app, which
// is the package that can see both.
const dynamicSlots = 64

// GapDetector tracks BCSeqNo continuity.
//
// OWNED EXCLUSIVELY BY ONE GOROUTINE (Vol 0 section 9.1): no atomics, no
// locks, plain memory read and written by the goroutine that drives the decode
// path. The only things it publishes are counters, and those are atomic adds on
// instruments resolved at startup.
//
// Both hypotheses' state is always allocated -- 96 slots of 8 bytes is 768
// bytes once -- because the audit mode needs both and sizing it by scope would
// make enabling the audit a reallocation on a running process.
type GapDetector struct {
	scope GapScope
	rep   Reports

	// global is scope GapGlobal's single space, and also the audit's global
	// hypothesis.
	global slotState

	// perCode is scope GapPerTranscode's state, and also the audit's
	// per-transcode hypothesis.
	perCode [gapSlots]slotState

	// auditLeft counts down the messages the audit still observes.
	// ordering.gapAuditPackets, default 0 = off.
	auditLeft int
}

// NewGapDetector builds a detector for one scope.
//
// auditPackets > 0 runs BOTH hypotheses in parallel for that many messages,
// neither feeding seq_gap_*, reporting which produced zero gaps. It is the
// mechanism HLD section 6.5 names for closing req section 12.3 "without
// needing a capture", and it costs ~768 bytes and two comparisons per message
// while enabled.
func NewGapDetector(scope GapScope, auditPackets int, rep Reports) *GapDetector {
	if rep == nil {
		rep = NopReports{}
	}
	if auditPackets < 0 {
		auditPackets = 0
	}
	return &GapDetector{scope: scope, rep: rep, auditLeft: auditPackets}
}

// Observe feeds one resolved message to the detector.
//
// It is a NO-OP for HeaderMessage, and that exclusion is the whole reason
// HeaderKind exists as a type. A MESSAGE_HEADER's offset 14 holds the top half
// of a nanosecond Timestamp, not a sequence number: in 2026 that timestamp is
// about 1.45e18, whose top 32 bits are about 3.4e8 and change every ~4.3
// seconds. Read as a BCSeqNo it produces an enormous positive delta on EVERY
// nine-second heartbeat -- a fabricated gap of hundreds of millions of
// messages, and without the resync rule it would pin seq_gap_messages at an
// absurd value inside the first minute of a session.
//
// slot is the index the counter layer already computed for this code (Vol 1
// section 7.7), so per-transcode scope needs no second lookup and no map. It is
// ignored under global scope.
//
// Allocates nothing.
func (g *GapDetector) Observe(h model.MsgHeader, slot int) {
	if h.Kind != model.HeaderBcast {
		// Section 7.2: 6541 and 6503 carry no BCSeqNo. Keyed on Kind, which
		// section 6.4 computed from the wire, and on nothing else.
		return
	}

	// The audit runs both hypotheses and reports; it does NOT touch
	// seq_gap_*, because its whole purpose is to compare the two before
	// anyone trusts either.
	if g.auditLeft > 0 {
		g.auditLeft--
		g.audit(h.BCSeqNo, slot)
		return
	}

	switch g.scope {
	case GapOff:
		return
	case GapGlobal:
		g.observeSlot(&g.global, h, 0)
	case GapPerTranscode:
		if slot < 0 || slot >= len(g.perCode) {
			// An unresolvable slot would otherwise index out of range. It
			// means the dynamic table was full, so the aggregate bucket is
			// the honest place for it -- and the log line says the scope is
			// aggregate rather than naming a code it cannot name.
			// Code 0 renders as the aggregate bucket: the table was full,
			// so there is no code to name and the line must say so rather
			// than name one it cannot.
			g.observeSlot(&g.perCode[len(g.perCode)-1], h, 0)
			return
		}
		g.observeSlot(&g.perCode[slot], h, h.Code)
	}
}

// observeSlot is section 7.3's comparison rule, applied to one sequence space.
//
// # The comparison, and why it is a subtraction
//
// BCSeqNo uses SERIAL-NUMBER ARITHMETIC: cur is newer than last iff
// int32(cur-last) > 0. A 4-byte signed counter at 10,000 messages/s wraps in
// about 59 hours, so a wrap inside a long-running session is reachable if the
// exchange does not reset it daily (ASM-11).
//
// Signed integer overflow in Go is DEFINED, not undefined: the specification
// permits signed operations to overflow, the result is deterministic under
// two's complement, and there is no run-time panic. So cur - last on two int32
// values IS the serial-number subtraction, with no uint32 round trip. A wrap
// from 2147483647 to -2147483648 yields delta == 1, which is the right answer
// and is exactly what a naive cur > last gets wrong -- that version freezes
// every affected row permanently after the wrap.
func (g *GapDetector) observeSlot(s *slotState, h model.MsgHeader, code model.TransCode) {
	cur := h.BCSeqNo

	// First message for this slot: record and emit NOTHING.
	//
	// 	There is no honest alternative. We do not know what the counter was
	// before we started listening, and reporting missing = cur would announce
	// a gap of however many messages the exchange has sent since midnight. A
	// restarted process is "first message" again, because this state is in
	// memory only -- and a persisted lastSeq would be worse than useless: it
	// would report everything that arrived while the process was down as a
	// feed problem, which is information the operator already has.
	if !s.have {
		s.last = cur
		s.have = true
		return
	}

	delta := cur - s.last // int32 arithmetic; wraps exactly as the counter does

	switch {
	case delta == 1:
		s.last = cur

	case delta < 0 || delta == 0:
		// Duplicate or reorder. NEITHER IS A GAP -- nothing is missing -- and
		// folding them into seq_gap_events would corrupt the one loss figure
		// the system produces.
		//
		// last is left UNCHANGED, so a single reordered datagram does not
		// rewind the detector and manufacture a gap on the NEXT message. IP
		// duplication and reordering are both possible on a routed path, and
		// the bible says nothing about ordering -- only that "IP
		// fragmentation is on, QoS is not".
		g.rep.SeqRegression()

	case int64(delta) > seqResyncThreshold:
		// ADR-L12. The event is real -- something discontinuous happened --
		// but the magnitude is not trustworthy, so seq_gap_messages must not
		// move.
		g.rep.SeqResync(g.scope, code, s.last, cur, int64(delta))
		s.last = cur

	default:
		// 1 < delta <= threshold: a gap of delta-1 messages.
		g.rep.SeqGap(g.scope, code, s.last, cur, int64(delta)-1)
		s.last = cur
	}
}

// audit updates both hypotheses and reports each one's verdict, without
// touching seq_gap_*.
//
// Both are advanced on every message regardless of what they saw, because the
// question is "which hypothesis sees a CONTINUOUS sequence", and a hypothesis
// that stopped advancing after its first disagreement would answer a different
// question from the second message onward.
func (g *GapDetector) audit(cur int32, slot int) {
	g.rep.GapAudit(auditGlobal, auditSlot(&g.global, cur))

	if slot < 0 || slot >= len(g.perCode) {
		slot = len(g.perCode) - 1
	}
	g.rep.GapAudit(auditPerTranscode, auditSlot(&g.perCode[slot], cur))
}

// auditSlot advances one hypothesis and reports whether it saw continuity.
//
// The first message for a slot agrees by definition: there is nothing to be
// discontinuous with, and counting it as a disagreement would penalise
// per-transcode scope once per code at startup for no reason.
func auditSlot(s *slotState, cur int32) bool {
	if !s.have {
		s.last = cur
		s.have = true
		return true
	}
	delta := cur - s.last
	s.last = cur
	return delta == 1
}

// The audit's hypothesis indices, matching obs's {hypothesis} label order.
// Passed as plain ints so decode names no obs type.
const (
	auditGlobal       = 0
	auditPerTranscode = 1
)
