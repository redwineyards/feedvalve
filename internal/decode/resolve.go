package decode

import (
	"encoding/binary"

	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// The two MESSAGE_HEADER shapes, derived rather than chosen (Vol 1
// section 6.4).
//
// 6541 is MESSAGE_HEADER only, 40 bytes (bible section 4.12), so the payload is
// 8 + 40. 6503 is 52 bytes (bible section 4.15), so 8 + 52. Writing them as
// sums of the two constants keeps the derivation visible: change the prefix
// length and these follow, which is the opposite of what a bare 48 and 60 would
// do.
const (
	heartbeatPayloadLen = wire.BroadcastDataPrefixLen + wire.MessageHeaderSize // 6541: 48
	assetRatePayloadLen = wire.BroadcastDataPrefixLen + 52                     // 6503: 60
)

// The two transaction codes that carry a MESSAGE_HEADER instead of a
// BCAST_HEADER (bible section 4.12, section 4.15). Recognised but not decoded:
// neither emits a unit (Vol 1 section 9.6).
const (
	codeHeartbeat    model.TransCode = 6541
	codeAssetRateInt model.TransCode = 6503
)

// Payload offsets of the two transaction-code candidates. Both are SHORTs, and
// which one is real depends on the layout -- that is the entire problem this
// function solves.
//
//	B at payload 8  is MESSAGE_HEADER.TransactionCode (header offset 0), and is
//	                BCAST_HEADER's first Reserved BYTE[2], expected zero
//	A at payload 18 is BCAST_HEADER.TransactionCode (header offset 10)
const (
	offsetCandidateB = wire.BroadcastDataPrefixLen + 0  // 8
	offsetCandidateA = wire.BroadcastDataPrefixLen + 10 // 18
)

// Resolve classifies a broadcast payload's header WITHOUT consulting the
// registry or any configuration (HLD section 6.4, INV-RESOLVE). payload must
// already be at least 48 bytes; the caller checks that (Vol 1 section 6.1).
//
// known is true only for the two confirmed MESSAGE_HEADER shapes; for a
// BCAST_HEADER it is always false and means nothing -- registry membership is
// the caller's separate question (Vol 1 section 8.3).
//
// payloadLen is the FRAMED length of the whole broadcast data, prefix included
// (Vol 0 section 3.6's MsgHeader.PayloadLen). It is passed rather than taken
// from len(payload) because the two can differ: the receiver's decompression
// buffer is 2048 bytes and holds one sub-packet's worth of real data at a time.
//
// # Why the code-plus-length pair is safe
//
// For a genuine BCAST_HEADER message to be misread as one of the two, payload
// offset 8 -- BCAST_HEADER's first Reserved BYTE[2], expected zero (bible
// section 2.8) -- would have to hold exactly 6541 (0x198D) or 6503 (0x1967),
// AND the message would have to be exactly 40 or 52 bytes from the header
// onward. The smallest catalogued BCAST_HEADER message is 7213 at 76 bytes
// (bible section 4.0), so no catalogued message has either shape.
//
// Testing the code ALONE would not be safe, which is what gate G-HDR-2 pins: a
// 7305 that happens to arrive at 48 bytes is still a BCAST_HEADER.
//
// # Why this does not consult the registry
//
// An earlier form of the rule tested registry membership first and fell through
// to the MESSAGE_HEADER candidates on a miss. It worked, and it was a defect
// (HLD section 6.4, Vol 1 section 6.5): adding a transcode to the inventory --
// an operator action, a config reload -- could change how a message's HEADER
// was classified. A configuration change silently altering byte-level
// interpretation produces an incident nobody can reproduce, because the
// reproduction needs the old config.
//
// This is a DESIGNED rule, not a protocol fact. The bible says 6503 and 6541
// use MESSAGE_HEADER; it states no discrimination procedure, because a C front
// end knows the layout from the circuit. The 48/60 pairing is ours (ASM-V2).
func Resolve(payload []byte, payloadLen int) (kind model.HeaderKind, code model.TransCode, known bool) {
	b := model.TransCode(binary.BigEndian.Uint16(payload[offsetCandidateB : offsetCandidateB+2]))

	if b == codeHeartbeat && payloadLen == heartbeatPayloadLen {
		return model.HeaderMessage, codeHeartbeat, true
	}
	if b == codeAssetRateInt && payloadLen == assetRatePayloadLen {
		return model.HeaderMessage, codeAssetRateInt, true
	}

	a := model.TransCode(binary.BigEndian.Uint16(payload[offsetCandidateA : offsetCandidateA+2]))
	return model.HeaderBcast, a, false
}
