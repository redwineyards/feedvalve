package framing

import (
	"encoding/binary"

	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// The envelope split (Vol 1 section 4): cNetId, iNoPackets, then
// length-prefixed sub-packets back to back in FIFO order (bible section 3.2,
// section 3.3).
//
// Every bound here is computed from a wire value and checked before use. That
// is layer 1 of HLD section 9.5's four-layer malformed-input defence -- cursor
// arithmetic before content -- and the reason there is no data[a:b] in this
// file whose bounds were not tested in the preceding lines.

// messageLengthHeaderOffset is MessageLength's offset WITHIN the 40-byte
// header: 38, in BOTH layouts (bible section 2.8, both tables).
//
// That coincidence is what lets framing run before header resolution, and it is
// load-bearing rather than lucky: an uncompressed sub-packet may be a 6541
// heartbeat carrying a MESSAGE_HEADER (bible section 4.12), and it has to be
// framed by the same loop as a 7208 carrying a BCAST_HEADER. The bible does not
// draw attention to it; the layout tables prove it.
const messageLengthHeaderOffset = 38

// msgLenOffset is where the uncompressed branch reads MessageLength: payload
// offset 46.
//
// DERIVED rather than written as 46, which is ADR-L06's one stated cost --
// "the offset 46 is a magic number in framing; it is a named constant derived
// as BroadcastDataPrefixLen + messageLengthHeaderOffset and covered by a layout
// gate, not a literal".
const msgLenOffset = wire.BroadcastDataPrefixLen + messageLengthHeaderOffset

// minPayloadLen is the 48 bytes an uncompressed sub-packet must have before
// MessageLength can be read at all: prefix 8 + header 40.
const minPayloadLen = wire.BroadcastDataPrefixLen + wire.BroadcastHeaderSize

// envelopeHeaderLen is cNetId[2] + iNoPackets[2] (bible section 3.2).
const envelopeHeaderLen = 4

// minSubPacketLen is R9's threshold. See RuleZeroAdvance.
const minSubPacketLen = 3

// The layout gate ADR-L06 asks for, in the two-sided form of Vol 0 section 2
// rule 7 that wire/const.go uses for G-BLD-1: each line compiles only when the
// constant holds its declared value, because otherwise one term is negative and
// converting a negative constant to uint is a compile-time error.
//
// Nothing here runs. It exists so the derivation cannot drift: if the prefix
// length or the header offset changed, 46 would move silently and every
// uncompressed sub-packet in the system would be framed from two bytes of
// Filler2.
const (
	_ = uint(messageLengthHeaderOffset-38) + uint(38-messageLengthHeaderOffset)
	_ = uint(msgLenOffset-46) + uint(46-msgLenOffset)
	_ = uint(minPayloadLen-48) + uint(48-minPayloadLen)
)

// SubPacketHandler receives each framed sub-packet, in the order it appeared in
// the datagram (bible section 3.2 requires FIFO processing within a buffer).
//
// An interface rather than a func, and the reason is allocation: a method value
// like r.handleSubPacket is a closure, and passing one to a function that does
// not inline it escapes to the heap -- once per datagram, on the path
// G-ALLOC-1 measures at zero. An interface holding a *Receiver allocates
// nothing.
//
// Declared here, by the consumer, so framing names nothing about the receiver
// (Vol 0 section 1.3's import direction).
type SubPacketHandler interface {
	// SubPacket is called with the sub-packet's bytes exactly as framed:
	// still COMPRESSED when compressed is true, and including the 8-byte
	// prefix when it is false (bible section 3.4).
	//
	// payload aliases the caller's datagram buffer and is valid only for the
	// duration of the call. idx is zero-based; count is iNoPackets, so a
	// handler can log "2/3" and so the netId oracle's hint can distinguish
	// sub-packet 1 from sub-packet 2 or later (Vol 1 section 6.2).
	SubPacket(payload []byte, compressed bool, idx, count int)
}

// Rule identifies which of Vol 1 section 4.2's numbered rejection rules ended
// the framing of a datagram.
//
// The rules are named in the specification as R1..R9, so they are named here
// too: they all increment the same counter (framing_abandoned), which means the
// counter alone cannot tell "the sender wrote fewer sub-packets than it
// claimed" from "a sub-packet declared a length that overruns the datagram".
// Section 4.2 requires the ERROR line to name the sub-packet index and the
// cursor offset; the rule is the third thing an operator needs and it is free.
type Rule uint8

const (
	// RuleNone means the datagram framed completely.
	RuleNone Rule = iota

	// RuleShortDatagram is R1: fewer than 4 bytes, so there is no cNetId and
	// no iNoPackets. The whole datagram is rejected.
	RuleShortDatagram

	// RuleNegativeCount is R2: iNoPackets below zero. SHORT is SIGNED (bible
	// section 2.7), and a negative count is an envelope we cannot interpret.
	RuleNegativeCount

	// RuleTruncatedLength is R4: not even two bytes left for the next
	// sub-packet's iCompLen. The sender declared more sub-packets than it
	// wrote.
	RuleTruncatedLength

	// RuleNegativeCompLen is R5: iCompLen below zero, again because SHORT is
	// signed.
	RuleNegativeCompLen

	// RuleTruncatedHeader is R6: fewer than 48 bytes left in the uncompressed
	// branch, so MessageLength cannot be read at all.
	RuleTruncatedHeader

	// RuleShortMessageLength is R7, the expensive one (Vol 1 section 4.3).
	// MessageLength is the ONLY length an uncompressed sub-packet carries, so
	// the cursor advance is entirely derived from it. Accepting 12 would
	// advance the cursor by 20 instead of the true size, and every later
	// sub-packet in the buffer would be read from the middle of this one --
	// framing "successfully", because a SHORT read from arbitrary bytes is
	// always a valid SHORT, and decoding into plausible garbage.
	RuleShortMessageLength

	// RuleLengthOverrun is R8: the declared length runs past the end of
	// cPackData.
	RuleLengthOverrun

	// RuleZeroAdvance is R9: subLen below 3.
	//
	// Section 4.4 calls this one unreachable and specifies it anyway, because
	// the loop's termination and its framing both depend on the cursor
	// strictly advancing, and today that follows from the BRANCH PREDICATE
	// rather than from anything the loop checks. A future "length zero means
	// something else" rule, or a refactor that moves the predicate, would
	// silently reintroduce a sub-packet that advances the cursor by 2 while
	// iNoPackets counts down -- iNoPackets reads of the same two bytes. R9
	// turns that from data fabrication into one counted framing_abandoned, for
	// one comparison per sub-packet.
	//
	// It is not quite unreachable, and the overlap is worth knowing: iCompLen
	// of 1 or 2 reaches it from the wire today. Section 4.4's own "related and
	// reachable" paragraph says a compressed sub-packet of 1 to 47 bytes is
	// NOT a framing failure and should be caught after decompression as
	// RejectShortPayload, which overlaps this rule at 1 and 2. The rule text
	// is implemented as written -- an LZO1Z stream of one or two bytes cannot
	// encode a message anyway -- and the disagreement is recorded here rather
	// than resolved silently in either direction.
	RuleZeroAdvance
)

// String names the rule the way section 4.2 does, with the reason, so a caller
// can put it straight into the ERROR line.
func (r Rule) String() string {
	switch r {
	case RuleNone:
		return "none"
	case RuleShortDatagram:
		return "R1: datagram shorter than the 4-byte envelope header"
	case RuleNegativeCount:
		return "R2: iNoPackets is negative"
	case RuleTruncatedLength:
		return "R4: no room for the next sub-packet's iCompLen"
	case RuleNegativeCompLen:
		return "R5: iCompLen is negative"
	case RuleTruncatedHeader:
		return "R6: fewer than 48 bytes left, so MessageLength cannot be read"
	case RuleShortMessageLength:
		return "R7: MessageLength below 40"
	case RuleLengthOverrun:
		return "R8: the declared length overruns cPackData"
	case RuleZeroAdvance:
		return "R9: sub-packet length below 3"
	}
	return "Rule(?)"
}

// Result is what framing one datagram produced. A value, so it costs nothing to
// return and nothing to ignore.
type Result struct {
	// SubPackets is how many were handed to the handler, in FIFO order.
	// Sub-packets already handled when a rule fires are KEPT: they were framed
	// from lengths read before the bad one and are independently valid (Vol 1
	// section 4.3). Only the remainder is abandoned.
	SubPackets int

	// NoPackets is iNoPackets as the envelope declared it, which is
	// authoritative for how many sub-packets exist (bible section 3.2).
	NoPackets int

	// Reject is the model-level outcome, and therefore the counter: every
	// rule here maps to framing_abandoned.
	Reject model.Reject

	// Rule is which of section 4.2's rules fired, RuleNone if none did.
	Rule Rule

	// Index is the sub-packet the rule fired on, zero-based, or -1 when the
	// rule is envelope-level (R1, R2). Offset is the cursor within cPackData
	// at that moment. Section 4.2 requires the ERROR line to name both.
	Index  int
	Offset int
}

// Abandoned reports whether framing stopped early.
func (r Result) Abandoned() bool { return r.Rule != RuleNone }

// Split walks a datagram's cPackData and hands each sub-packet to h in FIFO
// order, returning what happened.
//
// It does NOT decompress: the compressed flag is passed through so the handler
// can decompress into the buffer IT owns (Vol 1 section 5.2's decompBuf,
// receiver-owned and reused per sub-packet). That split is why a decompression
// failure costs exactly one sub-packet -- the cursor advanced by iCompLen,
// which was read from the wire BEFORE the decompressor ran, so every later
// sub-packet in the same datagram is still framed correctly (section 5.6,
// HLD F3).
//
// It also does not check the envelope's cNetId. That is a WARN whose value is
// entirely in its context (wire.EnvelopeNetIDMatches, section 6.2) and it never
// drops anything, so it belongs with the caller that holds the logger and the
// source address.
//
// Oversize datagrams are trimmed here rather than rejected: cPackData is a
// fixed 512-byte field (bible section 3.2), so nothing past it can be framed,
// but the sub-packets inside those 512 bytes are still well-framed and still
// good data. On a circuit with no retransmission (bible section 1), discarding
// them would cost real information to punish the sender for a framing surprise
// (section 3.8). Counting and logging the oversize is the caller's (G-FRM-3).
func Split(datagram []byte, h SubPacketHandler) Result {
	res := Result{Index: -1}

	// R1. No cNetId, no iNoPackets, nothing to frame.
	if len(datagram) < envelopeHeaderLen {
		res.Reject, res.Rule, res.Offset = model.RejectShortMessageLen, RuleShortDatagram, 0
		return res
	}

	noPackets := int(int16(binary.BigEndian.Uint16(datagram[2:4])))
	res.NoPackets = noPackets

	// R2. SHORT is signed, and a negative count is uninterpretable.
	if noPackets < 0 {
		res.Reject, res.Rule = model.RejectShortMessageLen, RuleNegativeCount
		return res
	}

	// R3. Nothing to do, and not an error -- see section 4.5 on residue.
	packedLen := min(len(datagram)-envelopeHeaderLen, wire.MaxPackedDataLen)
	packed := datagram[envelopeHeaderLen : envelopeHeaderLen+packedLen]

	off := 0
	for i := range noPackets {
		// R4.
		if off+2 > packedLen {
			return abandon(res, RuleTruncatedLength, i, off)
		}
		compLen := int(int16(binary.BigEndian.Uint16(packed[off : off+2])))
		off += 2

		// R5.
		if compLen < 0 {
			return abandon(res, RuleNegativeCompLen, i, off)
		}

		var subLen int
		if compLen == 0 {
			// UNCOMPRESSED. The length comes from the header's
			// MessageLength, which counts from offset 8 onward and EXCLUDES
			// the prefix (bible section 2.8, section 3.3).

			// R6. Without 48 bytes MessageLength is not there to read.
			if off+minPayloadLen > packedLen {
				return abandon(res, RuleTruncatedHeader, i, off)
			}
			msgLen := int(int16(binary.BigEndian.Uint16(packed[off+msgLenOffset : off+msgLenOffset+2])))

			// R7. Rejected, never clamped: see RuleShortMessageLength.
			if msgLen < wire.BroadcastHeaderSize {
				return abandon(res, RuleShortMessageLength, i, off)
			}
			subLen = wire.BroadcastDataPrefixLen + msgLen
		} else {
			// COMPRESSED. iCompLen is the length of the compressed bytes and
			// the cursor advance -- NOT the decompressed length, which is the
			// defect G-FRM-1 is written against.
			subLen = compLen
		}

		// R8.
		if off+subLen > packedLen {
			return abandon(res, RuleLengthOverrun, i, off)
		}
		// R9.
		if subLen < minSubPacketLen {
			return abandon(res, RuleZeroAdvance, i, off)
		}

		h.SubPacket(packed[off:off+subLen], compLen != 0, i, noPackets)
		res.SubPackets++
		off += subLen
	}

	// off may be below packedLen here. That is NOT an error and NOT counted:
	// if the exchange writes the whole fixed-size BcastPackData struct then
	// residue is normal and an instrument would fire on every datagram
	// (OQ-V1), and iNoPackets is authoritative for how many sub-packets exist
	// anyway. The thing residue would indicate -- lost framing -- is already
	// caught for free by the netId oracle on the next sub-packet (section 4.5).
	return res
}

// abandon records a rule firing.
//
// RejectShortMessageLen for every rule, which deserves a word because its name
// only describes R7. It is the only Reject value that maps to
// framing_abandoned (model/reject.go), and section 4.2 specifies exactly that
// counter for all of R1, R2, R4, R5, R6, R8 and R9 -- so the Reject selects the
// counter and Rule carries the diagnosis. Adding Reject values per rule would
// put eight names in model for one counter and give the receive path eight ways
// to spell one outcome.
func abandon(res Result, rule Rule, idx, off int) Result {
	res.Reject = model.RejectShortMessageLen
	res.Rule = rule
	res.Index = idx
	res.Offset = off
	return res
}
