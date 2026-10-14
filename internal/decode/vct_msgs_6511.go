package decode

import (
	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// MS_BCAST_VCT_MSGS -- bible section 4.5, Part V. Destination 2
// MD_MARKET_STATUS, one unit per message, not compressed.
//
// FIVE TRANSACTION CODES, ONE STRUCTURE: 6511 BC_OPEN_MSG, 6521 BC_CLOSE_MSG,
// 6522 BC_POSTCLOSE_MSG, 6531 BC_PRE_OR_POST_DAY_MSG, 6571 BC_PRE_OPEN_ENDED.
// One implementation registered under all five, each with its own contributor
// slot -- which is why Vol 0 section 3.0 had to raise maxContributors from the
// HLD's 4 to 8 (ADR-L03, delta D1).
//
// Declared 320, field sum 319, delta +1: ONE TRAILING PAD BYTE at offset 319,
// because the fields sum to an odd number and a struct of odd size is padded to
// even (bible section 2.2 rule 3, Appendix B item 7).
//
// # These are GENERAL units, whatever the Token field says
//
// FlagGeneral is set, UpdateUnit.Token is left ZERO, and the unit goes to the
// designated general shard (FR8). The message's own Token at offset 40 is
// carried as DATA -- image bit 4 -- and never as a routing key.
//
// Not an oversight. Destination 2's row key is MarketType, and routing five
// market-status messages by their token would scatter them across four shards
// while their rows all live on one, breaking ASM-15 and with it both the FR9
// single-writer argument and the deadlock argument. Stated as INV-SCOPE: a
// unit's scope comes from its transcode's declared inventory entry, never from
// whether the message happens to contain a field called Token.
//
// OQ-L2 asks whether that Token is meaningful on this circuit at all. If market
// status turns out to be per-security, destination 2 is collapsing up to 200,000
// rows into four and these are not general units -- so the field is stored, and
// the evidence is one query away once data is flowing.
const (
	vctDeclaredLen = 320 // bible section 4.5
	vctMessageLen  = 239 // BroadcastMessage CHAR[239]
	vctStoredText  = 128 // Vol 0 section 4.5 caps the stored text here
	vctTrailingPad = 1   // at offset 319, because 319 is odd
	vctSecInfoLen  = 30  // SEC_INFO, skipped entirely here

	// Payload offsets, from the table in Vol 1 section 9.5.
	vctBody0   = wire.BroadcastDataPrefixLen + wire.BroadcastHeaderSize // 48
	vctToken0  = vctBody0                                               // 48
	vctMarket0 = vctBody0 + 4 + vctSecInfoLen                           // 82
	vctText0   = vctBody0 + 40                                          // 88
)

// The layout gate (G-LAY-1), two-sided: Part V states 320 and the field sum is
// 319, so the trailing pad is part of the arithmetic rather than an afterthought.
//
//	40 header + 4 Token + 30 SEC_INFO + 2 MarketType + 2 ST_BCAST_DESTINATION
//	+ 2 BroadcastMessageLength + 239 BroadcastMessage + 1 pad = 320
const vctLayoutSlack = vctDeclaredLen - (wire.BroadcastHeaderSize +
	4 + vctSecInfoLen + 2 + 2 + 2 + vctMessageLen + vctTrailingPad)

const _ = uint(vctLayoutSlack) + uint(-vctLayoutSlack)

// The field sum WITHOUT the pad must be odd, which is what makes the pad
// necessary at all. 319 mod 2 == 1, so the one-sided form here asserts the
// remainder is exactly 1.
const vctOddSlack = (vctDeclaredLen - vctTrailingPad) % 2

const _ = uint(vctOddSlack-1) + uint(1-vctOddSlack)

// The stored text must fit the declared column, and the column must be the cap.
const vctTextSlack = vctStoredText - 128

const _ = uint(vctTextSlack) + uint(-vctTextSlack)

// Destination 2's mask bits, from internal/destinations/mapping.json.
const (
	m2Transcode   uint8 = 0 // the CONTRIBUTING transaction code, from the header
	m2MessageLen  uint8 = 1 // the CLAMPED length, never the declared one
	m2MessageText uint8 = 2 // 128 bytes
	m2BcastDest   uint8 = 3
	m2Token       uint8 = 4 // data, NOT a routing key -- see INV-SCOPE above
	m2EffectiveTS uint8 = 5
)

// vctCodes is the family, ascending. Listed once so the registrations and the
// tests cannot disagree about who is in it.
var vctCodes = [...]model.TransCode{6511, 6521, 6522, 6531, 6571}

func init() {
	for _, code := range vctCodes {
		c := code // one instance per code, each with its own Code() and entry
		register(c, func(cnt Counters, e *RegistryEntry) Decoder {
			return &vctMsgs{code: c, cnt: cnt, entry: e}
		})
	}
}

type vctMsgs struct {
	code  model.TransCode
	cnt   Counters
	entry *RegistryEntry
	r     wire.FieldReader
	trace wire.TraceSink
}

// SetTrace attaches the field-trace sink for the next Decode, or nil.
func (d *vctMsgs) SetTrace(t wire.TraceSink) { d.trace = t }

func (d *vctMsgs) Code() model.TransCode { return d.code }
func (d *vctMsgs) BodyLen() int          { return vctDeclaredLen }
func (d *vctMsgs) MaxUnits() int         { return 1 }

// Decode reads one market-status message.
//
// The row key is MarketType, not the token (Vol 0 section 4.5), and bit 0 is
// the contributing TRANSACTION CODE -- which is how a reader tells Open from
// Closed: the five codes share one row per MarketType and differ only in which
// one last wrote it.
func (d *vctMsgs) Decode(h model.MsgHeader, payload []byte,
	out []model.UpdateUnit) (int, error) {

	if len(payload) < wire.BroadcastDataPrefixLen+vctDeclaredLen {
		return 0, ErrShortBody
	}
	if len(out) < 1 {
		return 0, nil
	}

	e := d.entry
	spec := &e.Contrib[destMarketStatus]
	u := &out[0]
	r := &d.r
	r.Reset(payload[vctBody0:wire.BroadcastDataPrefixLen+vctDeclaredLen], vctBody0, d.trace)

	stamp(u, h, e)

	// FlagGeneral, and Token left ZERO: the routing key comes from the
	// inventory, not from the presence of a field called Token (INV-SCOPE).
	u.Flags |= model.FlagGeneral

	// The wire Token is DATA. Copied into bit 4 and never used for routing.
	tokenBytes := r.Raw(wire.FieldToken, 4)  // pay  48
	r.Skip(wire.FieldSecInfo, vctSecInfoLen) // pay  52

	marketType := r.I16(wire.FieldMarketType) // pay  82, THE ROW KEY
	contribute(u, destMarketStatus, model.SingleKey(uint32(uint16(marketType))), e)

	put(u, spec, m2Token, tokenBytes)
	put(u, spec, m2BcastDest, r.Raw(wire.FieldBcastDestination, 2)) // pay  84

	// The length rule, and all three parts must hold together (Vol 1 section
	// 9.5):
	//
	//  1. bible section 4.5: BroadcastMessageLength gives the VALID length of
	//     BroadcastMessage -- use it; the remainder of the 239 bytes is not
	//     meaningful, so those bytes must never be stored.
	//  2. Vol 0 section 4.1 R5 and section 5.2 normalisation rule 2: the
	//     field's FULL declared width is rewritten on every contribution,
	//     zero-filled beyond the valid length. Without that the digest becomes
	//     a function of history rather than of state and suppression silently
	//     stops working.
	//  3. Vol 0 section 4.5 caps the stored text at 128 bytes.
	declared := r.I16(wire.FieldBroadcastMessageLength) // pay  86
	valid := int(declared)
	if valid < 0 {
		valid = 0
	}
	if valid > vctMessageLen {
		// Beyond the field itself is nonsense, whatever the header says.
		valid = vctMessageLen
	}
	stored := min(valid, vctStoredText)
	if int(declared) != stored {
		d.cnt.MessageTruncated(destMarketStatus, m2MessageText)
	}

	// The CLAMPED length goes into the blob, never the declared one. Vol 0
	// section 4.5 says the value is "clamped to 128 on merge" and attributes
	// message_truncated to the shard; doing both here keeps the shard's merge
	// a pure copy loop over the mask's set bits with no per-bit special case,
	// and the decoder is the only component that ever SEES the unclamped
	// length -- so it is the only one that can count the truncation honestly
	// (addition A-07).
	putI16(u, spec, m2MessageLen, int16(stored))

	// R5: zero the whole 128 bytes first, then write the valid prefix. The
	// zero-fill is not an optimisation to skip -- it is what makes the stored
	// value a function of this message alone.
	text := r.Raw(wire.FieldBroadcastMessage, vctMessageLen) // pay  88
	putZero(u, spec, m2MessageText)
	put(u, spec, m2MessageText, text[:stored])

	// The trailing pad at offset 319, read explicitly so it shows in the trace.
	r.Pad(wire.FieldAlignmentPad, vctTrailingPad) // pay 327

	// Bit 0 is the contributing transaction code, from the HEADER. It is how a
	// reader tells Open from Closed, and it is also why the ordering guard must
	// be per contributor here: a same-day re-open sends 6531 -> 6571 -> 6511
	// inside one second (bible section 4.5) and LogTime is second-resolution,
	// so a shared per-row clock would discard the last two and the market would
	// never reach Open.
	putI16(u, spec, m2Transcode, int16(h.Code))
	putI32(u, spec, m2EffectiveTS, h.LogTime)

	if !sealBlob(u, d.code, d.cnt) {
		return 0, nil
	}
	return 1, nil
}
