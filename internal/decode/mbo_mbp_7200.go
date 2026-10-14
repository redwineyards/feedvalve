package decode

import (
	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// 7200 MS_BCAST_MBO_MBP -- bible section 4.7, Part V, Appendix B item 2.
// Destination 0 MD_TOKEN_QUOTE, ONE unit per message, compressed.
//
// # Why 7200 ships at all
//
// Bible section 4.8a: "These packets of broadcast above mentioned, at any time
// either 7200 or 7208 only will come but not both. This decision of sending
// which packet is taken by NSEIL control." Part VI repeats it as a checklist
// item: "the handler supports 7200 or 7208, whichever the exchange sends -- not
// both, not a fixed choice."
//
// So selection is BY ARRIVING TRANSACTION CODE: no configuration switch, no
// build tag, no operator action (ADR-L08). Shipping 7208 alone would mean that
// the day NSEIL flips the segment, the entire quote stream goes dark with
// nothing but a rising unknown_transcode{7200} to show for it, and the fix
// would be a rebuild and a deployment during a trading session.
//
//	MS_BCAST_MBO_MBP = 410 bytes
//	  BCAST_HEADER 40 + ST_INTERACTIVE_MBO_DATA(236) + ST_MBP_INFO[10](100)
//	  + TotalBuyQuantity 8 + TotalSellQuantity 8 + ST_INDICATOR 2
//	  + ClosingPrice 4 + OpenPrice 4 + HighPrice 4 + LowPrice 4 = 410
//
// ST_INTERACTIVE_MBO_DATA is declared 235 and its true size is 236 -- THE SAME
// DEFECT AS 7208's record, from the same cause: NetChangeIndicator at even
// offset 16 is the only odd-sized member, so one pad byte sits at 17.
const (
	mboDeclaredLen = 410 // bible section 4.7
	mboInnerSize   = 236 // ST_INTERACTIVE_MBO_DATA: declared 235, +1 interior pad at 17
	mboOrderLadder = 180 // ST_MBO_INFO[10]: ten 18-byte order-level entries, SKIPPED
	mboPriceLadder = 100 // ST_MBP_INFO[10]: ten 10-byte price levels

	// Payload offsets. The OUTER ones are bible Appendix B item 2's CORRECTED
	// values -- the specification's own are one byte too low, which is the
	// kind of error that reads as a typo and decodes as garbage.
	mboInner0     = wire.BroadcastDataPrefixLen + wire.BroadcastHeaderSize // 48
	mboLadder0    = mboInner0 + mboInnerSize                               // 284
	mboTotalBuy0  = mboLadder0 + mboPriceLadder                            // 384
	mboTotalSell0 = mboTotalBuy0 + 8                                       // 392
	mboIndicator0 = mboTotalSell0 + 8                                      // 400
	mboClosing0   = mboIndicator0 + 2                                      // 402
)

// The layout gates (G-LAY-1). Two-sided: Part V states 410 exactly.
const mboLayoutSlack = mboDeclaredLen - (wire.BroadcastHeaderSize + mboInnerSize +
	mboPriceLadder + 8 + 8 + 2 + 4 + 4 + 4 + 4)

const _ = uint(mboLayoutSlack) + uint(-mboLayoutSlack)

// Appendix B item 2's corrections, pinned.
//
// The specification's own outer offsets are ONE BYTE TOO LOW, which is the most
// dangerous kind of error in a layout table: it reads as a typo and decodes as
// plausible garbage. Writing the corrected values as derived constants and then
// gating them means the derivation and the correction have to agree -- so a
// future reader who "fixes" one of these back to the spec's number breaks the
// build instead of the prices.
//
// Payload offsets, so 8 more than the message offsets Appendix B lists:
// ST_MBP_INFO at message 276, TotalBuyQuantity at 376, ClosingPrice at 394.
const (
	mboLadderSlack0  = mboLadder0 - (276 + wire.BroadcastDataPrefixLen)
	mboTotalBuySlack = mboTotalBuy0 - (376 + wire.BroadcastDataPrefixLen)
	mboClosingSlack  = mboClosing0 - (394 + wire.BroadcastDataPrefixLen)

	// And the whole payload: 8 + 410, which the last four LONGs must exactly
	// fill from mboClosing0.
	mboPayloadSlack = (wire.BroadcastDataPrefixLen + mboDeclaredLen) - (mboClosing0 + 4*4)
)

const (
	_ = uint(mboLadderSlack0) + uint(-mboLadderSlack0)
	_ = uint(mboTotalBuySlack) + uint(-mboTotalBuySlack)
	_ = uint(mboClosingSlack) + uint(-mboClosingSlack)
	_ = uint(mboPayloadSlack) + uint(-mboPayloadSlack)
)

// The inner struct's own field sum, where the pad has to be accounted for.
//
//	4+2+2+4+4+1 +1pad +4+4+4+4 +2+2+2+4+4+4+4 +180 = 236
const mboInnerSlack = mboInnerSize - (4 + 2 + 2 + 4 + 4 + 1 + // Token..NetChangeIndicator
	1 + // *** the pad at offset 17, identical cause to 7208's ***
	4 + 4 + 4 + 4 + // NetPriceChange, LastTradeQuantity, LastTradeTime, AverageTradePrice
	2 + 2 + 2 + 4 + 4 + 4 + 4 + // the seven auction fields
	mboOrderLadder) // ST_MBO_INFO[10], order-level detail with no column

const _ = uint(mboInnerSlack) + uint(-mboInnerSlack)

// The two ladders: ten elements each, 18 bytes of order detail and 10 bytes of
// price level.
const (
	mboOrderLadderSlack = mboOrderLadder - 10*18
	mboPriceLadderSlack = mboPriceLadder - 10*(4+4+2)
)

const (
	_ = uint(mboOrderLadderSlack) + uint(-mboOrderLadderSlack)
	_ = uint(mboPriceLadderSlack) + uint(-mboPriceLadderSlack)
)

// The re-pack (ADR-L08, addition A-08). Destination 0's bit 14 is a 120-byte
// run whose element is 7208's 12-byte MBP_INFORMATION -- Quantity, Price,
// NumberOfOrders, BbBuySellFlag. 7200's price ladder is ST_MBP_INFO, whose
// element is 10 bytes: the same three real fields and NO BbBuySellFlag.
//
// A verbatim copy is therefore impossible, and the decoder re-packs instead:
// ten iterations of three copies and one two-byte zero, about 40 ns. One image
// layout, one mask bit, one writer expansion, and destination 0 reads
// identically whichever code the exchange sends -- which is exactly what bible
// section 4.8a asks for.
//
// This is NOT a violation of Vol 0 section 4.1 R1 ("the image holds wire
// form"). Every stored byte is still the exchange's own big-endian value at its
// wire width, unscaled and unconverted; only the GROUPING changes, and
// BbBuySellFlag is written as the zero bible section 4.8 says it always carries
// anyway ("currently contain zero -- the buy-back concept is not implemented.
// Do not build logic on them").
const (
	mboLevels      = 10
	mboSrcElemLen  = 10 // Qty 4 + Price 4 + NoOfOrders 2
	mboDestElemLen = 12 // ... + BbBuySellFlag 2
)

// The re-pack's own gate: the destination element must be the source element
// plus exactly the two bytes being zeroed, and ten of them must fill bit 14.
const (
	mboRepackSlack = mboDestElemLen - (mboSrcElemLen + 2)
	mboLadderFits  = mbpLadderLen - mboLevels*mboDestElemLen
)

const (
	_ = uint(mboRepackSlack) + uint(-mboRepackSlack)
	_ = uint(mboLadderFits) + uint(-mboLadderFits)
)

// BookType values bible section 4.7 says to process. Appendix A: 1 RL Regular
// Lot, 2 ST Stop Loss, 5 Odd Lot, 6 Spread, 7 Auction.
const (
	bookTypeRegularLot = 1
	bookTypeStopLoss   = 2
)

func init() {
	register(7200, func(c Counters, e *RegistryEntry) Decoder {
		return &mboMBP{cnt: c, entry: e}
	})
}

type mboMBP struct {
	cnt   Counters
	entry *RegistryEntry
	r     wire.FieldReader
	trace wire.TraceSink
}

// SetTrace attaches the field-trace sink for the next Decode, or nil.
func (d *mboMBP) SetTrace(t wire.TraceSink) { d.trace = t }

func (d *mboMBP) Code() model.TransCode { return 7200 }
func (d *mboMBP) BodyLen() int          { return mboDeclaredLen }
func (d *mboMBP) MaxUnits() int         { return 1 }

// Decode reads the one security this message carries. There is no NoOfRecords:
// 7200 is one security per message (bible section 4.7), which is why MaxUnits
// is 1 and there is no clamp here.
//
// It returns ZERO units for a BookType outside {1,2}, which is a PROCESSING
// decision rather than a parse failure -- the message framed and parsed
// correctly and we are choosing not to store it.
func (d *mboMBP) Decode(h model.MsgHeader, payload []byte,
	out []model.UpdateUnit) (int, error) {

	if len(payload) < wire.BroadcastDataPrefixLen+mboDeclaredLen {
		return 0, ErrShortBody
	}
	if len(out) < 1 {
		return 0, nil
	}

	e := d.entry
	spec := &e.Contrib[destQuote]
	u := &out[0]
	r := &d.r

	// The inner struct, offsets relative to its own start so the calls read
	// against bible section 4.7's table directly.
	r.Reset(payload[mboInner0:mboInner0+mboInnerSize], mboInner0, d.trace)

	stamp(u, h, e)

	token := r.I32(wire.FieldToken) // off  0
	bookType := r.I16(wire.FieldBookType)

	// Bible section 4.7: "Process the message only if BookType is 1 or 2. Skip
	// it in all other cases."
	//
	// THE RULE IS NOT APPLIED TO 7208. Section 4.8 describes 7208's BookType
	// without restating the gate, and Part VI's checklist names 7200
	// specifically. Applying it to 7208 on the assumption that "it must be the
	// same" would silently discard traffic the bible never said to discard, on
	// a feed with no retransmission. Flagged as OQ-V2: one capture, or one
	// question to the exchange, settles whether 7208 ever carries a BookType
	// outside {1,2}.
	if bookType != bookTypeRegularLot && bookType != bookTypeStopLoss {
		d.cnt.BooktypeSkipped(7200)
		return 0, nil
	}

	u.Token = token
	contribute(u, destQuote, model.SingleKey(uint32(token)), e)
	putI16(u, spec, q0BookType, bookType)

	put(u, spec, q0TradingStatus, r.Raw(wire.FieldTradingStatus, 2))         // off  6
	put(u, spec, q0VolumeTradedToday, r.Raw(wire.FieldVolumeTradedToday, 4)) // off  8
	put(u, spec, q0LastTradedPrice, r.Raw(wire.FieldLastTradedPrice, 4))     // off 12
	put(u, spec, q0NetChangeInd, r.Raw(wire.FieldNetChangeIndicator, 1))     // off 16

	// *** THE PAD AT OFFSET 17 *** -- identical cause to 7208's.
	r.Pad(wire.FieldAlignmentPad, 1) // off 17

	r.Skip(wire.FieldNetPriceChange, 4) // off 18, OQ-L1: see mbp_only_7208.go

	put(u, spec, q0LastTradeQty, r.Raw(wire.FieldLastTradeQuantity, 4))  // off 22
	put(u, spec, q0LastTradeTime, r.Raw(wire.FieldLastTradeTime, 4))     // off 26
	put(u, spec, q0AvgTradePrice, r.Raw(wire.FieldAverageTradePrice, 4)) // off 30

	r.Skip(wire.FieldAuctionNumber, 2)     // off 34
	r.Skip(wire.FieldAuctionStatus, 2)     // off 36
	r.Skip(wire.FieldInitiatorType, 2)     // off 38
	r.Skip(wire.FieldInitiatorPrice, 4)    // off 40
	r.Skip(wire.FieldInitiatorQuantity, 4) // off 44
	r.Skip(wire.FieldAuctionPrice, 4)      // off 48
	r.Skip(wire.FieldAuctionQuantity, 4)   // off 52

	// ST_MBO_INFO[10]: order-level detail, 18 bytes each. SKIPPED ENTIRELY --
	// no destination-0 column consumes it, and it is the difference between
	// this structure and 7208's.
	r.Skip(wire.FieldMBOInformation, mboOrderLadder) // off 56

	// The price ladder, re-packed. A second reader, based at the ladder's own
	// payload offset, because the inner struct's cursor has just ended.
	d.repackLadder(u, spec, payload)

	// The outer fields, at Appendix B item 2's corrected offsets.
	r.Reset(payload[mboTotalBuy0:], mboTotalBuy0, d.trace)
	put(u, spec, q0TotalBuyQty, r.Raw(wire.FieldTotalBuyQuantity, 8))   // pay 384
	put(u, spec, q0TotalSellQty, r.Raw(wire.FieldTotalSellQuantity, 8)) // pay 392
	r.Skip(wire.FieldSTIndicator, 2)                                    // pay 400
	put(u, spec, q0ClosingPrice, r.Raw(wire.FieldClosingPrice, 4))      // pay 402
	put(u, spec, q0OpenPrice, r.Raw(wire.FieldOpenPrice, 4))            // pay 406
	put(u, spec, q0HighPrice, r.Raw(wire.FieldHighPrice, 4))            // pay 410
	put(u, spec, q0LowPrice, r.Raw(wire.FieldLowPrice, 4))              // pay 414

	putI32(u, spec, q0EffectiveTS, h.LogTime)

	if !sealBlob(u, 7200, d.cnt) {
		return 0, nil
	}
	return 1, nil
}

// repackLadder writes ST_MBP_INFO[10]'s ten 10-byte levels into bit 14's
// 12-byte element layout, zeroing BbBuySellFlag (ADR-L08).
//
// Level by level rather than as one run, which is the whole point: the shapes
// differ by two bytes per element, so a verbatim copy would put level 1's
// quantity where level 0's flag belongs and every level after it would drift
// further.
func (d *mboMBP) repackLadder(u *model.UpdateUnit, spec *ColSpec, payload []byte) {
	r := &d.r
	r.Reset(payload[mboLadder0:mboLadder0+mboPriceLadder], mboLadder0, d.trace)

	base := int(spec.Off[q0Ladder])
	for i := range mboLevels {
		dst := base + i*mboDestElemLen

		copy(u.Blob[dst:dst+4], r.Raw(wire.FieldMBPInformation, 4))    // Qty
		copy(u.Blob[dst+4:dst+8], r.Raw(wire.FieldMBPInformation, 4))  // Price
		copy(u.Blob[dst+8:dst+10], r.Raw(wire.FieldMBPInformation, 2)) // NoOfOrders
		clear(u.Blob[dst+10 : dst+12])                                 // BbBuySellFlag := 0
	}
}
