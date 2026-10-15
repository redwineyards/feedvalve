// The fabricated inputs gate G-DIFF-1 runs through both decoders.
//
// # Every offset here is a literal, transcribed from the bible
//
// Vol 1 section 8.6 item 2 is explicit, and poc's own suite has the scar:
// "a fixture builder sharing the parser's field order agrees with the parser
// EVEN WHEN BOTH ARE WRONG, which is exactly how a 213-byte stride survives."
// So nothing in this file is computed from a decoder's constants, and nothing
// walks a decoder's field order. The header offsets are bible section 2.8's
// column plus the 8-byte prefix; the record offsets are bible section 4.8's.
//
// internal/decode/mbp_only_7208_test.go has its own transcription of the same
// table. That duplication is the point rather than a cost: two independent
// transcriptions disagree loudly if either is wrong, where one shared builder
// agrees with itself.
//
// # The filler is position-dependent, and that is load-bearing
//
// Unset bytes are byte(i*31+7) of their ABSOLUTE payload offset, not a constant
// like fixture_test.go's 0xDD. A constant filler lets a one-byte offset slip
// read the same value as the correct offset, so the hex matches and the gate
// passes on a decoder that is wrong. With a position-dependent pattern every
// slip changes the bytes, which is the property assertion 2 of the gate rests
// on.
package app

import (
	"encoding/binary"
	"testing"

	"feedvalve/internal/wire"
)

// Absolute offsets inside a broadcast payload, prefix included.
//
// BCAST_HEADER is bible section 2.8; the body offsets are each structure's
// Part V row. mbpDiff* repeat alloc_gate_test.go's constants rather than
// importing them, for the independence reason above.
const (
	// Both header kinds, and the fields a fixture must set for a payload to
	// be accepted at all.
	absTransCodeBcast = 18 // BCAST_HEADER: header offset 10
	absTransCodeMsg   = 8  // MESSAGE_HEADER: header offset 0
	absErrorCode      = 20 // both kinds
	absMessageLength  = 46 // both kinds
	absHeaderEnd      = 48

	// 7208, MS_BCAST_ONLY_MBP: 470 declared, NoOfRecords at 48, records from
	// 50 at a stride of 214.
	mbpDiffNoOfRecords = 48
	mbpDiffRecord0     = 50
	mbpDiffStride      = 214
	mbpDiffPayloadLen  = 478 // 8 prefix + 470 declared
	mbpDiffDeclaredLen = 470

	// Record-relative, bible section 4.8. Only the ones this fixture plants.
	mbpDiffRecToken           = 0
	mbpDiffRecLastTradedPrice = 12
	mbpDiffRecNetChangeInd    = 16
	mbpDiffRecPad             = 17
	mbpDiffRecNetPriceChange  = 18
	mbpDiffRecClosingPrice    = 198
	mbpDiffRecOpenPrice       = 202

	// 7305 / 7340, MS_SECURITY_UPDATE_INFO: 294 declared, no padding anywhere.
	secDiffPayloadLen  = 302 // 8 prefix + 294 declared
	secDiffDeclaredLen = 294

	// The two header-only payload lengths Resolve keys on.
	heartbeatDiffPayloadLen = 48 // 6541
	assetRateDiffPayloadLen = 60 // 6503

	// A BCAST code that is in the bible but out of Phase 1's inventory
	// (Vol 1 section 9.1): resolved as a BCAST_HEADER, gap-detected, counted
	// as unknown_transcode, no decoder. Used where a fixture needs a SMALL
	// sub-packet that is still comparable against poc.
	diffUnknownBcastCode = 7206
)

// diffFiller writes the position-dependent pattern over a whole payload.
func diffFiller(p []byte) {
	for i := range p {
		p[i] = byte(i*31 + 7)
	}
}

// newDiffPayload returns a payload of total bytes with a BCAST_HEADER declaring
// code and msgLen, everything else filler.
//
// ErrorCode is zeroed explicitly and that is not cosmetic: bible section 2.10
// makes a non-zero ErrorCode mean the payload is MS_ERROR_RESPONSE regardless
// of the transaction code, and both implementations check it BEFORE trusting
// the code. Left as filler, every fixture here would be rejected as an error
// response and the gate would compare two empty streams -- passing vacuously.
func newDiffPayload(total int, code uint16, msgLen uint16) []byte {
	p := make([]byte, total)
	diffFiller(p)
	p[0] = 8 // byte 0 repeats the segment netId (bible section 3.4)
	binary.BigEndian.PutUint16(p[absTransCodeBcast:], code)
	binary.BigEndian.PutUint16(p[absErrorCode:], 0)
	binary.BigEndian.PutUint16(p[absMessageLength:], msgLen)
	return p
}

// mbp7208DiffPayload builds a 7208 declaring noOfRecords, with values planted
// at absolute record offsets.
//
// negative plants a negative price in every price field the fixture sets, which
// is a required case: bible section 2.4 allows negative prices, and a decoder
// reading a LONG as unsigned produces a large positive number that looks
// entirely plausible in a price column. poc reads every price through its i32,
// so its trace prints the signed value and the two must agree.
func mbp7208DiffPayload(t *testing.T, noOfRecords int, negative bool) []byte {
	t.Helper()

	p := newDiffPayload(mbpDiffPayloadLen, 7208, mbpDiffDeclaredLen)
	binary.BigEndian.PutUint16(p[mbpDiffNoOfRecords:], uint16(int16(noOfRecords)))

	// Plant into every slot the message has room for, not just the declared
	// count: the trailing slots must stay UNREAD. If a decoder parsed them
	// anyway it would fabricate a security that looks real, and the planted
	// values make that visible rather than letting zeroed padding hide it.
	for rec := 0; rec < 2; rec++ {
		base := mbpDiffRecord0 + rec*mbpDiffStride
		if base+mbpDiffStride > len(p) {
			t.Fatalf("record %d at %d overruns the %d-byte payload", rec, base, len(p))
		}
		put32(p, base+mbpDiffRecToken, int32(90001+rec))

		ltp := int32(1234500 + rec)
		npc := int32(5000 + rec)
		cls := int32(999900 + rec)
		opn := int32(1000000 + rec)
		if negative {
			ltp, npc, cls, opn = -ltp, -npc, -cls, -opn
		}
		put32(p, base+mbpDiffRecLastTradedPrice, ltp)
		put32(p, base+mbpDiffRecNetPriceChange, npc)
		put32(p, base+mbpDiffRecClosingPrice, cls)
		put32(p, base+mbpDiffRecOpenPrice, opn)

		// '+' or '-', the only CHAR in the record and therefore the reason a
		// pad byte falls at record offset 17 at all.
		p[base+mbpDiffRecNetChangeInd] = '-'
		// The pad itself, left as a distinctive non-zero byte so a trace that
		// skipped it silently would show a gap rather than a plausible zero.
		p[base+mbpDiffRecPad] = 0xEE
	}
	return p
}

// sec7305DiffPayload builds a 7305 (or 7340) payload.
//
// The body is left entirely as filler, deliberately. 7305 has no padding
// anywhere and no grouped runs -- its fields sum to 294 exactly -- so every one
// of its ~50 fields is a 1:1 comparison against poc, and position-dependent
// filler exercises all of them without a table of planted values that could
// itself be transcribed wrongly. The bytes ARE the test data.
func sec7305DiffPayload(t *testing.T, code uint16) []byte {
	t.Helper()
	if code != 7305 && code != 7340 {
		t.Fatalf("sec7305DiffPayload: %d does not use MS_SECURITY_UPDATE_INFO", code)
	}
	return newDiffPayload(secDiffPayloadLen, code, secDiffDeclaredLen)
}

// unknownBcastDiffPayload is a header-only BCAST payload for a code with no
// decoder.
//
// Why it exists: the mixed-compression datagram needs a SMALL second
// sub-packet, and both header-only codes in the inventory (6541, 6503) resolve
// to a MESSAGE_HEADER -- which poc cannot read, because poc has no
// header-kind resolution at all and always parses the BCAST layout. An
// out-of-inventory BCAST code is header-only in practice (no decoder runs) and
// stays comparable.
func unknownBcastDiffPayload() []byte {
	return newDiffPayload(absHeaderEnd, diffUnknownBcastCode, wire.BroadcastHeaderSize)
}

// heartbeatDiffPayload is 6541, the one MESSAGE_HEADER fixture here.
//
// Resolve keys the kind on the code at payload offset 8 AND the payload length,
// so both must be exact or it comes back as a BCAST_HEADER for whatever
// two bytes happen to sit at offset 18.
func heartbeatDiffPayload() []byte {
	p := make([]byte, heartbeatDiffPayloadLen)
	diffFiller(p)
	p[0] = 8
	binary.BigEndian.PutUint16(p[absTransCodeMsg:], 6541)
	binary.BigEndian.PutUint16(p[absErrorCode:], 0)
	binary.BigEndian.PutUint16(p[absMessageLength:], wire.BroadcastHeaderSize)
	return p
}

func put32(p []byte, off int, v int32) {
	binary.BigEndian.PutUint32(p[off:off+4], uint32(v))
}

// envelopeOf assembles a BcastPackData datagram: cNetId, iNoPackets, then each
// sub-packet's 2-byte length prefix followed by its bytes.
//
// Written out here rather than reusing datagramOf, because that helper returns
// a *model.Datagram for the internal path and the poc side needs the raw wire
// bytes to put on a socket. The two must agree, and
// TestTheTwoEnvelopeBuildersAgree below asserts they do.
func envelopeOf(subs ...[]byte) []byte {
	total := 4
	for _, s := range subs {
		total += len(s)
	}
	d := make([]byte, total)
	d[0] = 8 // cNetId
	binary.BigEndian.PutUint16(d[2:4], uint16(len(subs)))
	off := 4
	for _, s := range subs {
		copy(d[off:], s)
		off += len(s)
	}
	return d
}

// compressedDiffSub wraps payload as a COMPRESSED sub-packet, declaring the
// stream's length in the 2-byte prefix. A non-zero prefix is what marks a
// sub-packet compressed in both implementations.
func compressedDiffSub(t *testing.T, payload []byte) []byte {
	t.Helper()
	stream := lzoLiteralOnly(t, payload)
	s := make([]byte, 2+len(stream))
	binary.BigEndian.PutUint16(s[0:2], uint16(len(stream)))
	copy(s[2:], stream)
	return s
}

// uncompressedDiffSub wraps payload as an UNCOMPRESSED sub-packet: a zero
// length prefix, after which the length comes from the header's MessageLength.
func uncompressedDiffSub(payload []byte) []byte {
	s := make([]byte, 2+len(payload))
	copy(s[2:], payload)
	return s
}

// TestTheTwoEnvelopeBuildersAgree guards the one place this file duplicates
// existing test scaffolding.
//
// envelopeOf produces wire bytes for the socket poc listens on; datagramOf
// produces a *model.Datagram for the internal path. If they ever disagreed the
// gate would compare two DIFFERENT datagrams and report a decoder defect that
// does not exist -- or, worse, agree because both were wrong in the same way.
func TestTheTwoEnvelopeBuildersAgree(t *testing.T) {
	subs := [][]byte{
		uncompressedDiffSub(sec7305DiffPayload(t, 7305)),
		uncompressedDiffSub(unknownBcastDiffPayload()),
	}

	raw := envelopeOf(subs...)
	d := datagramOf(t, subs...)

	if int(d.N) != len(raw) {
		t.Fatalf("datagramOf reports %d bytes, envelopeOf built %d", d.N, len(raw))
	}
	for i := range raw {
		if d.Buf[i] != raw[i] {
			t.Fatalf("the two envelope builders differ at byte %d: %#02x vs %#02x",
				i, d.Buf[i], raw[i])
		}
	}
}

// TestTheMixedDatagramFitsTheEnvelope pins the size arithmetic the literal-only
// encoder forces.
//
// internal/framing hard-trims packed data to wire.MaxPackedDataLen (512), so a
// fixture that overran it would have its last sub-packet silently truncated --
// and the gate would then report a framing disagreement that is really a
// fixture that does not fit. Asserted here so the failure names the cause.
func TestTheMixedDatagramFitsTheEnvelope(t *testing.T) {
	d := mixedCompressionDatagram(t)
	if len(d) > 4+wire.MaxPackedDataLen {
		t.Fatalf("the mixed-compression datagram is %d bytes, over the %d-byte envelope.\n"+
			"The literal-only encoding expands its input by 6 bytes, so two full messages "+
			"do not fit: the compressed sub-packet carries the 302-byte 7305 and the "+
			"uncompressed one is header-only.", len(d), 4+wire.MaxPackedDataLen)
	}
}

// mixedCompressionDatagram is section 8.6's "a compressed and an uncompressed
// sub-packet in one datagram", and the fixture that puts a real decoder on
// genuinely DECOMPRESSED bytes -- the gap P2-09's allocation gate left to this
// card.
//
// 4 envelope + (2 + 308) compressed 7305 + (2 + 48) uncompressed = 364 bytes.
func mixedCompressionDatagram(t *testing.T) []byte {
	t.Helper()
	return envelopeOf(
		compressedDiffSub(t, sec7305DiffPayload(t, 7305)),
		uncompressedDiffSub(unknownBcastDiffPayload()),
	)
}
