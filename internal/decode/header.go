package decode

import (
	"encoding/binary"

	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// minPayloadLen is Vol 1 section 6.1's single length precondition, checked once
// before anything reads a header field: 8 + 40.
const minPayloadLen = wire.BroadcastDataPrefixLen + wire.BroadcastHeaderSize

// offsetErrorCode is payload offset 20. ErrorCode sits at header offset 12 in
// BOTH layouts (bible section 2.8), which is what lets it be read before the
// header kind is known -- and bible section 2.10 requires it checked before the
// transaction code is trusted.
const offsetErrorCode = wire.BroadcastDataPrefixLen + 12

// ReadHeader resolves and parses a broadcast payload's header, filling h.
//
// It is the left column of Vol 1 section 6.4's flowchart, in order, and the
// order is the design:
//
//	payloadLen >= 48 ?            -> RejectShortPayload, this sub-packet only
//	ErrorCode != 0 ?              -> RejectErrorCode: the payload is
//	                                 MS_ERROR_RESPONSE, 182 bytes, NOT the
//	                                 structure the transaction code implies
//	Resolve(payload, payloadLen)  -> the header kind, without the registry
//	parse per kind                -> BCSeqNo only for BCAST_HEADER
//	MsgLen >= 40 ?                -> RejectShortMessageLen
//
// The netId oracle is NOT here. It is a log line whose value depends on the
// sub-packet index (wire.NetIDMatches, Vol 1 section 6.2) and it never rejects
// anything, so folding it in would put a counter and a formatted hint on a
// function that must stay pure enough to test as one.
//
// trace may be nil, and normally is; pass a sink only when an operator has
// asked to trace this transcode (obs.DebugLogger.Enabled), because the value
// passed to a sink is boxed by the caller.
//
// On any non-None reject, h holds whatever had been parsed when the check
// fired, and the caller must not use it. The counters each reject maps to are
// the caller's to increment (model.Reject.Counter).
func ReadHeader(r *wire.FieldReader, payload []byte, payloadLen int, trace wire.TraceSink, h *model.MsgHeader) model.Reject {
	*h = model.MsgHeader{}

	if payloadLen < minPayloadLen || len(payload) < minPayloadLen {
		return model.RejectShortPayload
	}
	h.PayloadLen = int32(payloadLen)

	// Before resolution and before the transaction code is trusted
	// (bible section 2.10, Vol 1 section 6.8).
	if ec := int16(binary.BigEndian.Uint16(payload[offsetErrorCode : offsetErrorCode+2])); ec != 0 {
		h.ErrorCode = ec
		return model.RejectErrorCode
	}

	// Code is deliberately NOT taken from Resolve's return value even though
	// the two always agree: the per-kind parse below reads the same field
	// through the traced cursor, and one authoritative read is what keeps the
	// trace and the struct from being able to disagree. Resolve's code is the
	// resolution INPUT; h.Code is the parsed output.
	kind, _, _ := Resolve(payload, payloadLen)
	h.Kind = kind

	// Based at the header, not at payload offset 0: base is the payload offset
	// of data[0] (Vol 1 section 8.4), so the traced offsets stay absolute
	// within the broadcast data while the prefix contributes no trace line of
	// its own -- poc's construction exactly (poc/gobrdclnt.go:309), which is
	// what keeps the two field streams diffable for gate G-DIFF-1. This is the
	// one place the prefix is skipped for a header (bible section 3.4's "handle
	// the prefix in exactly one place").
	r.Reset(payload[wire.BroadcastDataPrefixLen:minPayloadLen], wire.BroadcastDataPrefixLen, trace)

	switch kind {
	case model.HeaderMessage:
		parseMessageHeader(r, h)
	default:
		parseBcastHeader(r, h)
	}

	// Vol 1 section 6.7 row 1. In the UNCOMPRESSED branch this is unreachable
	// -- framing already rejected it and abandoned the datagram (section 4.2
	// R7) -- but in the compressed branch it is reachable and costs this
	// message only, which is why framing_abandoned is NOT the counter here.
	if h.MsgLen < wire.BroadcastHeaderSize {
		return model.RejectShortMessageLen
	}
	return model.RejectNone
}

// parseBcastHeader fills h from a BCAST_HEADER at payload offset 8 (bible
// section 2.8). BCSeqNo is read; TimestampNs is left zero and is meaningless
// for this kind (Vol 0 section 3.6).
//
// The reader calls below ARE the wire order, so this function can be checked
// line-by-line against bible section 2.8's offset column. Reserved fields are
// read and discarded rather than jumped over: section 2.6 says they "still
// occupy bytes and must be read", and an explicit skip keeps the trace's
// offsets contiguous so a missing byte shows as a gap rather than a shift.
func parseBcastHeader(r *wire.FieldReader, h *model.MsgHeader) {
	r.Skip(wire.FieldReserved1, 2)                             // payload  8-10
	r.Skip(wire.FieldReserved2, 2)                             // payload 10-12
	h.LogTime = r.I32(wire.FieldLogTime)                       // payload 12-16, header offset 4
	r.Skip(wire.FieldAlphaChar, 2)                             // payload 16-18
	h.Code = model.TransCode(r.I16(wire.FieldTransactionCode)) // payload 18-20, header offset 10
	h.ErrorCode = r.I16(wire.FieldErrorCode)                   // payload 20-22
	h.BCSeqNo = r.I32(wire.FieldBCSeqNo)                       // payload 22-26, header offset 14
	r.Skip(wire.FieldReserved3, 1)                             // payload 26-27
	r.Skip(wire.FieldReserved4, 3)                             // payload 27-30
	r.Skip(wire.FieldTimeStamp2, 8)                            // payload 30-38
	r.Skip(wire.FieldFiller2, 8)                               // payload 38-46
	h.MsgLen = r.I16(wire.FieldMessageLength)                  // payload 46-48
}

// parseMessageHeader fills h from a MESSAGE_HEADER at payload offset 8 (bible
// section 2.8). TimestampNs is read from header offset 14 (nanoseconds since
// 1 Jan 1980, bible section 2.5); BCSeqNo is left zero and MUST NOT be read --
// that offset holds the TOP HALF of TimestampNs, which is the whole reason
// HeaderKind exists.
//
// Feeding that value to the gap detector would inject a number in the 10^18
// range on every nine-second heartbeat (bible section 4.12), fabricating a
// sequence gap each time and burying the real ones under it. Gate G-HDR-1 is
// written against exactly that defect, and it is worse than a crash: it would
// be discovered as "the gap counter is always non-zero so we stopped looking
// at it".
//
// LogTime is at header offset 2 here and offset 4 in a BCAST_HEADER. Read at
// one fixed offset, the other layout yields AlphaChar -- two characters of a
// Symbol construed as a timestamp. Vol 0 section 3.6 says "both layouts"
// without giving the two offsets; Vol 1's V-D4 corrects it, and this is the
// reconciliation most likely to be implemented from the stale text.
func parseMessageHeader(r *wire.FieldReader, h *model.MsgHeader) {
	h.Code = model.TransCode(r.I16(wire.FieldTransactionCode)) // payload  8-10, header offset 0
	h.LogTime = r.I32(wire.FieldLogTime)                       // payload 10-14, header offset 2
	r.Skip(wire.FieldAlphaChar, 2)                             // payload 14-16
	r.Skip(wire.FieldTraderId, 4)                              // payload 16-20
	h.ErrorCode = r.I16(wire.FieldErrorCode)                   // payload 20-22
	h.TimestampNs = r.I64(wire.FieldTimestamp)                 // payload 22-30, header offset 14
	r.Skip(wire.FieldTimeStamp1, 8)                            // payload 30-38
	r.Skip(wire.FieldTimeStamp2, 8)                            // payload 38-46
	h.MsgLen = r.I16(wire.FieldMessageLength)                  // payload 46-48
}
