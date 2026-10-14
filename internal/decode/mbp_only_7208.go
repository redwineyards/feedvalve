package decode

import (
	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// 7208 MS_BCAST_ONLY_MBP -- bible section 4.8, Part V, Appendix B item 1.
// Destination 0 MD_TOKEN_QUOTE, up to two units per message, compressed.
//
// THE HIGHEST-VALUE CORRECTION IN THE BIBLE, and the reason this project has
// build-time gates at all.
//
//	MS_BCAST_ONLY_MBP = 470 bytes
//	  BCAST_HEADER 40 + NoOfRecords 2 + 2 x INTERACTIVE_ONLY_MBP_DATA(214) = 470
//
// The record is DECLARED 213 and its true stride is 214. Bible section 2.2:
// CHAR and CHAR arrays are byte aligned, every other member is word aligned
// where a word is TWO bytes. NetChangeIndicator is the record's only odd-sized
// member and it sits at even offset 16, so it leaves the cursor at 17 -- odd --
// and the next member is a LONG, which must start even. ONE PAD BYTE AT 17.
// The field sum including it is 214, which is even, so there is no trailing
// pad, and 40 + 2 + 2*214 is the declared 470. At a 213-byte stride the total
// is 468 and the spec's own declared length is unreachable.
//
// # What getting it wrong looks like
//
// Token, BookType, TradingStatus, VolumeTradedToday and LastTradedPrice all sit
// BEFORE offset 17 and decode correctly. Everything after -- every price, the
// whole ladder -- becomes noise, and records[1] starts one byte early. It
// presents as a data-quality complaint about certain fields rather than as a
// framing bug, and it is investigated as a scaling problem for a week. poc
// carried exactly this for months.

// Record geometry. Each constant is a bible Part V fact, and the layout gate
// below fails the build if the arithmetic stops reconciling.
const (
	mbpDeclaredLen = 470 // the packet length bible section 4.8 declares
	mbpMaxRecords  = 2   // INTERACTIVE_ONLY_MBP_DATA[2]
	mbpRecordSize  = 214 // the TRUE stride: 213 declared + 1 interior pad at offset 17

	// mbpRecord0 is the payload offset of record 0: the 8-byte prefix, the
	// 40-byte header, and the 2-byte NoOfRecords.
	mbpRecord0 = wire.BroadcastDataPrefixLen + wire.BroadcastHeaderSize + 2

	// mbpLadderLen is MBP_INFORMATION[10]: ten 12-byte levels, five best buys
	// then five best sells, copied as ONE run because the side is positional.
	mbpLadderLen = 120
)

// The layout gate (G-LAY-1), two-sided because Part V states the packet length
// EXACTLY. It compiles only when slack is zero: a positive slack makes
// uint(-slack) overflow and a negative one makes uint(slack) overflow.
//
// This is the regression that a 213 would turn into a compile error.
const mbpLayoutSlack = mbpDeclaredLen -
	(wire.BroadcastHeaderSize + 2 + mbpMaxRecords*mbpRecordSize)

const _ = uint(mbpLayoutSlack) + uint(-mbpLayoutSlack)

// And the record's own field sum, which is where the pad has to be accounted
// for or the stride above is just an assertion about itself.
//
//	4+2+2+4+4+1 +1pad +4+4+4+4 +2+2+2+4+4+4+4 +120 +2+2+8+8+2+4+4+4+4 = 214
const mbpRecordSlack = mbpRecordSize - (4 + 2 + 2 + 4 + 4 + 1 + // Token..NetChangeIndicator
	1 + // *** the pad at offset 17 ***
	4 + 4 + 4 + 4 + // NetPriceChange, LastTradeQuantity, LastTradeTime, AverageTradePrice
	2 + 2 + 2 + 4 + 4 + 4 + 4 + // the seven auction fields
	mbpLadderLen + // MBP_INFORMATION[10]
	2 + 2 + // BbTotalBuyFlag, BbTotalSellFlag
	8 + 8 + // TotalBuyQuantity, TotalSellQuantity (DOUBLE)
	2 + // ST_INDICATOR
	4 + 4 + 4 + 4) // ClosingPrice, OpenPrice, HighPrice, LowPrice

const _ = uint(mbpRecordSlack) + uint(-mbpRecordSlack)

// And the ladder: ten elements of twelve bytes.
const mbpLadderSlack = mbpLadderLen - 10*(4+4+2+2)

const _ = uint(mbpLadderSlack) + uint(-mbpLadderSlack)

// Destination 0's mask bits, from internal/destinations/mapping.json. Named
// rather than numbered at the call sites, because "bit 7" tells a reader
// nothing and net_change_indicator tells them everything.
const (
	q0LastTradedPrice   uint8 = 0
	q0VolumeTradedToday uint8 = 1
	q0OpenPrice         uint8 = 2
	q0HighPrice         uint8 = 3
	q0LowPrice          uint8 = 4
	q0ClosingPrice      uint8 = 5
	q0TradingStatus     uint8 = 6
	q0NetChangeInd      uint8 = 7
	q0LastTradeQty      uint8 = 8
	q0LastTradeTime     uint8 = 9
	q0AvgTradePrice     uint8 = 10
	q0TotalBuyQty       uint8 = 11
	q0TotalSellQty      uint8 = 12
	q0BookType          uint8 = 13
	q0Ladder            uint8 = 14
	q0EffectiveTS       uint8 = 15
)

func init() {
	register(7208, func(c Counters, e *RegistryEntry) Decoder {
		return &mbpOnly{cnt: c, entry: e}
	})
}

// mbpOnly decodes 7208.
//
// entry is this decoder's own registry slot, so the ColSpec and the contributor
// slot are reads of constants resolved at config load rather than per-message
// lookups. r is reset per record and never allocated (ADR-L07).
//
// trace is nil in normal operation: the caller decides whether this transcode
// is being traced, because the value a sink receives is boxed by whoever calls
// it. It is a FIELD rather than a parameter because Decode's signature is fixed
// by Vol 0 section 3.9, and it is set by SetTrace before each Decode.
type mbpOnly struct {
	cnt   Counters
	entry *RegistryEntry
	r     wire.FieldReader
	trace wire.TraceSink
}

// SetTrace attaches the field-trace sink for the next Decode, or nil.
func (d *mbpOnly) SetTrace(t wire.TraceSink) { d.trace = t }

func (d *mbpOnly) Code() model.TransCode { return 7208 }
func (d *mbpOnly) BodyLen() int          { return mbpDeclaredLen }
func (d *mbpOnly) MaxUnits() int         { return mbpMaxRecords }

// Decode reads NoOfRecords, clamps it, and decodes exactly that many records.
//
// NoOfRecords is a COUNT, NOT THE ARRAY SIZE. The message is always 470 bytes
// and always carries room for two records, so slot 1 of a one-record message is
// the message's trailing padding. Decoded, it becomes a second, entirely
// fabricated security -- a token, a price, a full ten-level ladder -- that is
// indistinguishable downstream from a real one, merged into destination 0 and
// written to Oracle. n == 0 is legal and yields zero units.
func (d *mbpOnly) Decode(h model.MsgHeader, payload []byte,
	out []model.UpdateUnit) (int, error) {

	e := d.entry
	if len(payload) < wire.BroadcastDataPrefixLen+mbpDeclaredLen {
		return 0, ErrShortBody
	}

	// NoOfRecords, at payload offset 48: immediately after the header.
	d.r.Reset(payload[mbpRecord0-2:mbpRecord0], mbpRecord0-2, d.trace)
	declared := d.r.I16(wire.FieldNoOfRecords)
	n := clampRecords(declared, mbpMaxRecords, 7208, d.cnt)
	if n > len(out) {
		n = len(out)
	}

	spec := &e.Contrib[destQuote]
	for i := range n {
		off := mbpRecord0 + i*mbpRecordSize
		d.decodeRecord(&out[i], h, e, spec, payload, off)
		if !sealBlob(&out[i], 7208, d.cnt) {
			return 0, nil
		}
	}
	return n, nil
}

// decodeRecord reads one INTERACTIVE_ONLY_MBP_DATA.
//
// The reader calls below ARE the wire order, so this function can be checked
// line by line against bible Part V's offset column -- which is the whole
// reason the project reads through a cursor rather than slicing at computed
// offsets. The comment column carries the RECORD offset, so it reads directly
// against the spec's own table.
func (d *mbpOnly) decodeRecord(u *model.UpdateUnit, h model.MsgHeader, e *RegistryEntry,
	spec *ColSpec, payload []byte, off int) {

	r := &d.r
	r.Reset(payload[off:off+mbpRecordSize], off, d.trace)

	stamp(u, h, e)

	// rec 0: the routing key AND the row key, decoded to a value rather than
	// copied -- one of the few fields that is (section 9.7).
	token := r.I32(wire.FieldToken)
	u.Token = token
	contribute(u, destQuote, model.SingleKey(uint32(token)), e)

	put(u, spec, q0BookType, r.Raw(wire.FieldBookType, 2))               // rec  4
	put(u, spec, q0TradingStatus, r.Raw(wire.FieldTradingStatus, 2))     // rec  6
	put(u, spec, q0VolumeTradedToday, r.Raw(wire.FieldVolumeTradedToday, 4))        // rec  8
	put(u, spec, q0LastTradedPrice, r.Raw(wire.FieldLastTradedPrice, 4)) // rec 12
	put(u, spec, q0NetChangeInd, r.Raw(wire.FieldNetChangeIndicator, 1)) // rec 16

	// *** THE PAD AT RECORD OFFSET 17 ***
	//
	// Read explicitly rather than skipped by bumping the cursor, so it appears
	// in the -debug trace as padding and the logged offsets stay contiguous
	// against a hex dump (CLAUDE.md invariant 3).
	r.Pad(wire.FieldAlignmentPad, 1) // rec 17

	// OQ-L1: Vol 0 section 4.3 maps closing_price to ClosingPrice at record
	// offset 198, while HLD section 8.2 maps it to this field at offset 18 --
	// whose NAME contradicts its own description (bible Appendix B item 10
	// records that it presently carries the closing price). This volume
	// follows Vol 0: offset 198 is stored, this one is skipped. Both are
	// 4-byte LONGs, so resolving OQ-L1 swaps one Copy/Skip pair here and
	// changes neither the image, the mask nor the schema.
	r.Skip(wire.FieldNetPriceChange, 4) // rec 18

	put(u, spec, q0LastTradeQty, r.Raw(wire.FieldLastTradeQuantity, 4))  // rec 22
	put(u, spec, q0LastTradeTime, r.Raw(wire.FieldLastTradeTime, 4))     // rec 26
	put(u, spec, q0AvgTradePrice, r.Raw(wire.FieldAverageTradePrice, 4)) // rec 30

	// The seven auction fields: not in use for 7208 (bible section 4.8).
	r.Skip(wire.FieldAuctionNumber, 2)     // rec 34
	r.Skip(wire.FieldAuctionStatus, 2)     // rec 36
	r.Skip(wire.FieldInitiatorType, 2)     // rec 38
	r.Skip(wire.FieldInitiatorPrice, 4)    // rec 40
	r.Skip(wire.FieldInitiatorQuantity, 4) // rec 44
	r.Skip(wire.FieldAuctionPrice, 4)      // rec 48
	r.Skip(wire.FieldAuctionQuantity, 4)   // rec 52

	// The ladder, as ONE 120-byte run: ten 12-byte levels, five best buys then
	// five best sells. The side is positional, so no per-level work is needed
	// and the whole run is one Copy (destination 0 bit 14 is a group column).
	put(u, spec, q0Ladder, r.Raw(wire.FieldMBPInformation, mbpLadderLen)) // rec 56

	// Always zero -- "the buy-back concept is not implemented. Do not build
	// logic on them" (bible section 4.8).
	r.Skip(wire.FieldBbTotalBuyFlag, 2)  // rec 176
	r.Skip(wire.FieldBbTotalSellFlag, 2) // rec 178

	put(u, spec, q0TotalBuyQty, r.Raw(wire.FieldTotalBuyQuantity, 8))   // rec 180
	put(u, spec, q0TotalSellQty, r.Raw(wire.FieldTotalSellQuantity, 8)) // rec 188

	r.Skip(wire.FieldSTIndicator, 2) // rec 196, a bit field: no column consumes it

	put(u, spec, q0ClosingPrice, r.Raw(wire.FieldClosingPrice, 4)) // rec 198, see OQ-L1
	put(u, spec, q0OpenPrice, r.Raw(wire.FieldOpenPrice, 4))       // rec 202
	put(u, spec, q0HighPrice, r.Raw(wire.FieldHighPrice, 4))       // rec 206
	put(u, spec, q0LowPrice, r.Raw(wire.FieldLowPrice, 4))         // rec 210

	// Bit 15 is the header's LogTime, not a record field: the row's
	// effective timestamp.
	putI32(u, spec, q0EffectiveTS, h.LogTime)
}
