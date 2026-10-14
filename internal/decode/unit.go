package decode

import (
	"encoding/binary"

	"feedvalve/internal/model"
)

// Stamping a unit: Vol 1 section 10.2's table, in one place.
//
// Five decoders fill the same seven fields the same way, and the one that
// matters most is the one a reader would least suspect: Token is the ROUTING
// key and is left ZERO for a general unit even when the message carries a field
// called Token (INV-SCOPE, section 9.5). Writing that rule five times is five
// chances to write it four ways.
//
// RecvMono is NOT stamped here. It belongs to the datagram, not the decode --
// the receipt clock is read once per datagram by the caller (section 10.3) --
// and a decoder that set it would be inventing a receipt time per message.

// stamp fills the fields every unit has, from the header and the registry
// entry, and clears everything else.
//
// The clear is deliberate and not defensive: out is caller-owned and REUSED
// per packet, so a field this decoder does not set would otherwise still hold
// the previous message's value. For Contribs that would mean contributing to a
// destination this transcode does not touch, with a stale row key -- a
// fabricated row, which is the whole class of defect the design is organised
// against.
func stamp(u *model.UpdateUnit, h model.MsgHeader, e *RegistryEntry) {
	*u = model.UpdateUnit{
		Code:    h.Code,
		Seq:     h.BCSeqNo,
		LogTime: h.LogTime,
	}
	if e.Class == model.ClassNonRateLimited {
		u.Flags |= model.FlagNonRateLimited
	}
}

// contribute records that this unit writes destination d, with the row it
// targets. The mask, the packed length and the contributor slot come from the
// registry; nothing here is derived per message.
//
// BlobOff is zero because every transcode in the Phase 1 inventory touches
// exactly ONE destination, so its packed run starts at the beginning of the
// blob. A transcode contributing to two would place the second run after the
// first, and the assertion in sealBlob is what would catch a decoder that
// forgot to.
func contribute(u *model.UpdateUnit, d model.DestID, row model.RowKey, e *RegistryEntry) {
	spec := &e.Contrib[d]
	u.Contribs[d] = model.Contribution{
		Row:     row,
		Mask:    spec.Mask,
		BlobOff: 0,
		BlobLen: spec.Width,
		Slot:    e.Slots[d],
	}
	u.BlobLen = spec.Width
}

// sealBlob is the runtime half of the ASM-16a bound (Vol 1 section 10.2).
//
// Phase 1's worst case is 7208 at 181 of 256, a 29% margin, so this should be
// unreachable. If it ever fires, the decoder's declared ColSpec.Width and its
// actual writes have diverged, which is a CODE defect rather than bad input --
// so the message emits ZERO units rather than a plausible partial one, and the
// counter says which transcode to look at.
func sealBlob(u *model.UpdateUnit, code model.TransCode, cnt Counters) bool {
	if u.BlobLen > model.UnitBlobSize {
		cnt.BlobOverrun(code)
		return false
	}
	return true
}

// The blob writers. Each takes the bit's offset from the ColSpec, so a
// configuration declaring a subset of columns moves the field and the decoder
// needs no arithmetic.
//
// They are deliberately dumb: a stored field keeps its WIRE FORM (Vol 0
// section 4.1 R1), big-endian at its wire width, unscaled and untrimmed. There
// is no decode-then-re-encode anywhere in this file, because that would both
// allocate and make the stored bytes a function of our conversion rules rather
// than of what the exchange sent.

// put copies src into the blob at the offset of bit, bounded by the width the
// registry declared for it. A short src leaves the remainder as the zero it
// already is, which is R5's zero-fill for free.
func put(u *model.UpdateUnit, spec *ColSpec, bit uint8, src []byte) {
	off := int(spec.Off[bit])
	n := int(spec.Len[bit])
	if n > len(src) {
		n = len(src)
	}
	copy(u.Blob[off:off+n], src[:n])
}

// putZero clears the whole declared width of bit.
//
// Vol 0 section 4.1 R5 and section 5.2 normalisation rule 2: a field's full
// declared width is rewritten on EVERY contribution, zero-filled beyond its
// valid length. Skipping the fill makes the digest a function of history rather
// than of state, and suppression then stops working in a way nothing reports.
//
// # It is currently redundant, and kept anyway
//
// stamp assigns a fresh zero UpdateUnit, which clears all 256 blob bytes, so
// R5 already holds for every field before a decoder writes anything -- and a
// break check that removed this call did NOT fail, which is how that was
// discovered rather than assumed.
//
// Kept for one reason: R5 must not depend on that coincidence. stamp's clear
// exists to stop a STALE CONTRIBUTION reaching a shard, not to satisfy R5, and
// the obvious optimisation of it -- clearing only the fields stamp sets, or
// only the blob prefix the ColSpec declares -- would silently take R5 with it.
// A decoder that calls this is correct whichever way stamp evolves; one that
// relies on the clear is correct only by accident. The cost is one clear of at
// most 128 bytes on a path that has already copied more than that.
func putZero(u *model.UpdateUnit, spec *ColSpec, bit uint8) {
	off := int(spec.Off[bit])
	n := int(spec.Len[bit])
	clear(u.Blob[off : off+n])
}

// putI16 writes a two-byte control value the decoder computed rather than
// copied -- a clamped length, a transaction code. Big-endian, like the wire.
func putI16(u *model.UpdateUnit, spec *ColSpec, bit uint8, v int16) {
	off := int(spec.Off[bit])
	if spec.Len[bit] < 2 {
		return
	}
	binary.BigEndian.PutUint16(u.Blob[off:off+2], uint16(v))
}

// putI32 writes a four-byte value from the header rather than the body: every
// destination's effective-timestamp column is the header's LogTime, which is
// not a field of any record (Vol 1 sections 9.2, 9.4, 9.5).
//
// Big-endian, because the column holds wire form like every other: a consumer
// reading the image cannot tell which bytes came from the body and which from
// the header, and should not have to.
func putI32(u *model.UpdateUnit, spec *ColSpec, bit uint8, v int32) {
	off := int(spec.Off[bit])
	if spec.Len[bit] < 4 {
		return
	}
	binary.BigEndian.PutUint32(u.Blob[off:off+4], uint32(v))
}

// Destination IDs, named. They are the declared order of
// internal/destinations/mapping.json, and a test asserts the names still match
// -- the alternative is a bare 0, 1 or 2 at every call site, which is how a
// contribution ends up in the wrong table.
const (
	destQuote          model.DestID = 0 // MD_TOKEN_QUOTE
	destSecurityMaster model.DestID = 1 // MD_SECURITY_MASTER
	destMarketStatus   model.DestID = 2 // MD_MARKET_STATUS
)

// clampRecords applies bible section 4.8's rule for a declared record count:
// the count says how many records CARRY DATA, and the array is always full
// size.
//
// Returns the number to parse. A declared count above the array size is clamped
// and counted, never trusted: the message is a fixed length with room for every
// slot, so slot i of a shorter message is the message's trailing PADDING.
// Decoded, that padding becomes an entirely fabricated security -- a token, a
// price, a full ladder -- indistinguishable downstream from a real one, merged
// into the destination and written to Oracle.
//
// A declared count below zero is the same defect from the other side: SHORT is
// signed, so it is clamped to zero rather than converted into a huge unsigned
// loop bound.
func clampRecords(declared int16, max int, code model.TransCode, cnt Counters) int {
	n := int(declared)
	if n < 0 {
		cnt.RecordCountClamped(code)
		return 0
	}
	if n > max {
		cnt.RecordCountClamped(code)
		return max
	}
	return n
}
