package app

import (
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
		// counts the rest (obs.AppLogger).
		c.in.log.Warnf(obs.ClassRecordCountClamped,
			"transcode %d declared a record count outside its array bound; clamped. "+
				"Parsing the unused slots would decode the message's trailing padding "+
				"into a fabricated security", code)
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
		c.in.log.Errorf(obs.ClassSessionAnomaly,
			"transcode %d wrote past its declared blob width; no units emitted for this "+
				"message. This is a decoder defect, not bad input", code)
	}
}

// MessageTruncated is labelled by {destination,column} rather than by
// transcode, because the question an operator asks is "which stored column is
// being cut?" -- and A-07 moved this counter's owner from the shard to decode
// precisely because the decoder is the only component that ever sees the
// unclamped length.
func (c *decodeCounters) MessageTruncated(dest model.DestID, bit uint8) {
	c.in.messageTruncated(int(dest), int(bit)).Add(1)
}
