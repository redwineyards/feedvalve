package app

import (
	"strconv"

	"feedvalve/internal/decode"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// decodeCounters implements decode.Counters over the resolved obs handles.
//
// This is the join decode cannot make for itself: decode reports what happened
// in its own vocabulary -- a clamped record count, a skipped BookType, a
// truncated value -- and app, which is the package allowed to see both obs and
// decode, turns each into the right instrument slot. The same shape as
// registryError mapping decode's problem kinds onto config's rule identifiers.
//
// Every method here is called at most once per message, and the per-transcode
// ones index by the REGISTRY's slot, which is the same index obs assigned that
// code's {code} label (Vol 1 section 7.7). The registry is consulted for the
// slot rather than a map: one linear scan over at most 32 uint16s, on a path
// that only runs when something was clamped.
type decodeCounters struct {
	in  *instruments
	reg decode.Registry
}

// newDecodeCounters binds the sink to one registry. It is constructed BEFORE
// the registry, because BuildRegistry needs the sink to construct the decoders
// -- so the registry is attached afterwards, by setRegistry, and the two
// together are what a decoder's counter call needs.
func newDecodeCounters(in *instruments) *decodeCounters {
	return &decodeCounters{in: in}
}

// setRegistry closes the loop left open by construction order. Called by Build
// immediately after BuildRegistry returns, before any goroutine starts, so the
// receive path never observes a nil registry here.
func (c *decodeCounters) setRegistry(reg decode.Registry) { c.reg = reg }

// slot resolves a transcode to its counter index, or -1 when the registry is
// not attached or does not hold the code.
//
// A miss cannot happen through the normal path -- a decoder only runs because
// Lookup found its entry -- so a negative result means the sink was used
// outside a registry, which a test does and production does not.
func (c *decodeCounters) slot(code model.TransCode) int {
	if c.reg == nil {
		return -1
	}
	return c.reg.Slot(code)
}

func (c *decodeCounters) RecordCountClamped(code model.TransCode) {
	if s := c.slot(code); s >= 0 {
		c.in.recordCountClamped[s].Add(1)
		// Rate-limited by class, because a feed that clamps is likely to
		// clamp on every message: the token bucket keeps the first few and
		// counts the rest (obs.AppLogger). Allow first, so the suppressed
		// majority does not box its argument (Vol 1 section 11.5).
		if c.in.log.Allow(obs.ClassRecordCountClamped) {
			c.in.log.Warnf(obs.ClassRecordCountClamped,
				"transcode %d declared a record count outside its array bound; clamped. "+
					"Parsing the unused slots would decode the message's trailing padding "+
					"into a fabricated security", code)
		}
	}
}

func (c *decodeCounters) BooktypeSkipped(code model.TransCode) {
	if s := c.slot(code); s >= 0 {
		c.in.booktypeSkipped[s].Add(1)
	}
}

func (c *decodeCounters) BlobOverrun(code model.TransCode) {
	if s := c.slot(code); s >= 0 {
		c.in.blobOverrun[s].Add(1)
		// A code defect rather than bad input, so it is an ERROR and not a
		// WARN: the decoder's declared ColSpec.Width and its actual writes
		// have diverged, and Phase 1's worst case is 181 of 256.
		if c.in.log.Allow(obs.ClassDecodeAnomaly) {
			c.in.log.Errorf(obs.ClassDecodeAnomaly,
				"transcode %d wrote past its declared blob width; no units emitted for this "+
					"message. This is a decoder defect, not bad input", code)
		}
	}
}

// MessageTruncated is labelled by {destination,column} rather than by
// transcode, because the question an operator asks is "which stored column is
// being cut?" -- and A-07 moved this counter's owner from the shard to decode
// precisely because the decoder is the only component that ever sees the
// unclamped length.
//
// Bounds-checked rather than trusted, because both indices come from a
// DECODER: a ColSpec whose bit has drifted past its destination's column list
// would otherwise index a slot the registry never allocated, inside the recover
// boundary, where the panic costs the message its units and says nothing useful
// about why.
func (c *decodeCounters) MessageTruncated(dest model.DestID, bit uint8) {
	if int(dest) >= len(c.in.messageTruncated) {
		return
	}
	row := c.in.messageTruncated[dest]
	if int(bit) >= len(row) {
		return
	}
	row[bit].Add(1)
}

// decodeCounters also implements decode.Reports, the gap detector's sink
// (P2-07). The two interfaces are separate in decode because they have no
// caller in common -- five decoders implement nothing of Reports -- but one
// type can satisfy both here, since app holds every handle either needs.

// SeqGap is Vol 1 section 7.4's line and its two counters.
//
// The LINE is rate-limited by the per-class token bucket; the COUNTERS are
// not. A suppressed line still increments both, which is the whole reason the
// design carries counters as well as logs: seq_gap_messages is the number to
// trend, and it is the only loss figure this system can produce, because
// everything after the socket is lossless by construction.
func (c *decodeCounters) SeqGap(scope decode.GapScope, code model.TransCode,
	from, to int32, missing int64) {

	c.in.seqGapEvents.Add(1)
	c.in.seqGapMessages.Add(uint64(missing))
	// Allow FIRST, because gapScopeLabel builds a string under per-transcode
	// scope and the four arguments box into the variadic (Vol 1 section 11.5).
	if c.in.log.Allow(obs.ClassSeqGap) {
		c.in.log.Warnf(obs.ClassSeqGap, "seq_gap scope=%s from=%d to=%d missing=%d",
			gapScopeLabel(scope, code), from, to, missing)
	}
}

// SeqRegression is a duplicate or a reordered message. NOT a gap -- nothing is
// missing -- and folding it into seq_gap_events would corrupt the loss figure
// above.
//
// It is also the receiver-side evidence for whether the ordering guard has
// anything to defend against at all, which is currently unknown (A-03).
func (c *decodeCounters) SeqRegression() { c.in.seqRegression.Add(1) }

// SeqResync is ADR-L12. The event is counted twice over -- once as a resync and
// once as a gap event, because something discontinuous really did happen -- but
// the MAGNITUDE never reaches seq_gap_messages.
//
// ERROR rather than WARN: a sequence source restarting mid-session is not
// routine, and ASM-V4 expects this to fire approximately never.
func (c *decodeCounters) SeqResync(scope decode.GapScope, code model.TransCode,
	from, to int32, delta int64) {

	c.in.seqResync.Add(1)
	c.in.seqGapEvents.Add(1)
	if c.in.log.Allow(obs.ClassSeqGap) {
		c.in.log.Errorf(obs.ClassSeqGap,
			"seq_resync scope=%s from=%d to=%d delta=%d -- the sequence source appears to have "+
				"restarted; the event is counted but the magnitude is NOT added to seq_gap_messages",
			gapScopeLabel(scope, code), from, to, delta)
	}
}

// GapAudit records one hypothesis's verdict on one message while the audit is
// running (Vol 0 section 8.3 gap G4).
func (c *decodeCounters) GapAudit(hypothesis int, agree bool) {
	if hypothesis < 0 || hypothesis >= numGapHypotheses {
		return
	}
	if agree {
		c.in.gapAuditAgree[hypothesis].Add(1)
		return
	}
	c.in.gapAuditDisagree[hypothesis].Add(1)
}

// gapScopeLabel renders section 7.4's scope=<global|transcode:NNNN>.
//
// Formatting happens HERE and not in the detector, which is why Reports takes
// a scope and a code rather than a string: the detector stays allocation-free
// on every path including the reporting ones.
//
// This allocates under per-transcode scope, so EVERY caller must sit inside an
// AppLogger.Allow check. It used to say it "runs only when a line is actually
// being written", which was false: Warnf takes the bucket after its arguments
// have already been built, so a suppressed gap line paid for this string
// anyway -- and a misframed feed suppresses almost all of them. That belief is
// precisely what Vol 1 section 11.5 exists to correct.
//
// A per-transcode report with code 0 is the aggregate bucket -- the dynamic
// table was full, so there is no code to name and the line says so rather than
// naming one it cannot.
func gapScopeLabel(scope decode.GapScope, code model.TransCode) string {
	switch {
	case scope != decode.GapPerTranscode:
		return "global"
	case code == 0:
		return "transcode:other"
	default:
		return "transcode:" + strconv.Itoa(int(code))
	}
}
