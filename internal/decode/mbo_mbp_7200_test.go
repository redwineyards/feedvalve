package decode

import (
	"encoding/binary"
	"testing"

	"feedvalve/internal/model"
)

// 7200's offset table, TRANSCRIBED BY HAND from bible section 4.7 / Part V with
// Appendix B item 2's corrections applied.
//
// 7200 has NO poc oracle -- poc decodes 7208 and 7305 only (Vol 1 section 8.6)
// -- so this table is the ONLY independent check that exists for this
// structure. If it and the decoder disagree, the specification settles it and
// not the code.
//
// Offsets are PAYLOAD-relative (8 more than the message offsets the bible
// lists), written out as literals:
//
//	 48  ST_INTERACTIVE_MBO_DATA, 236 bytes
//	   +0   Token               LONG
//	   +4   BookType            SHORT   <- the gating field
//	   +6   TradingStatus       SHORT
//	   +8   VolumeTradedToday   LONG
//	   +12  LastTradedPrice     LONG
//	   +16  NetChangeIndicator  CHAR
//	   +17  *** PAD ***
//	   +18  NetPriceChange      LONG    skipped, OQ-L1
//	   +22  LastTradeQuantity   LONG
//	   +26  LastTradeTime       LONG
//	   +30  AverageTradePrice   LONG
//	   +34  seven auction fields, 22 bytes
//	   +56  ST_MBO_INFO[10]     180 bytes, skipped entirely
//	284  ST_MBP_INFO[10]        100 bytes, ten 10-byte levels
//	384  TotalBuyQuantity       DOUBLE
//	392  TotalSellQuantity      DOUBLE
//	400  ST_INDICATOR           2, skipped
//	402  ClosingPrice           LONG
//	406  OpenPrice              LONG
//	410  HighPrice              LONG
//	414  LowPrice               LONG
//	     -> 418 = 8 prefix + 410 declared
const (
	absMBOInner    = 48
	absMBOToken    = absMBOInner + 0
	absMBOBookType = absMBOInner + 4
	absMBOTrading  = absMBOInner + 6
	absMBOVolume   = absMBOInner + 8
	absMBOLastPx   = absMBOInner + 12
	absMBONetChg   = absMBOInner + 16
	absMBOPad      = absMBOInner + 17
	absMBONetPxChg = absMBOInner + 18
	absMBOLastQty  = absMBOInner + 22
	absMBOLastTime = absMBOInner + 26
	absMBOAvgPx    = absMBOInner + 30
	absMBOOrderLad = absMBOInner + 56

	absMBOPriceLad = 284
	absMBOTotalBuy = 384
	absMBOTotalSel = 392
	absMBOIndic    = 400
	absMBOClosing  = 402
	absMBOOpen     = 406
	absMBOHigh     = 410
	absMBOLow      = 414
	absMBOPayload  = 418
)

type mboFixture struct{ payload []byte }

func newMBOFixture(t *testing.T, bookType int16) *mboFixture {
	t.Helper()
	f := &mboFixture{payload: make([]byte, absMBOPayload)}
	f.payload[0] = 8
	binary.BigEndian.PutUint16(f.payload[18:20], 7200)
	binary.BigEndian.PutUint16(f.payload[46:48], 410)
	f.put16(absMBOBookType, bookType)
	f.payload[absMBOPad] = 0xEE // loud, so a field read across it is obvious
	return f
}

func (f *mboFixture) put16(off int, v int16) {
	binary.BigEndian.PutUint16(f.payload[off:], uint16(v))
}

func (f *mboFixture) put32(off int, v int32) {
	binary.BigEndian.PutUint32(f.payload[off:], uint32(v))
}

func (f *mboFixture) put64(off int, v uint64) {
	binary.BigEndian.PutUint64(f.payload[off:], v)
}

// plantLevels writes the ten 10-byte price levels. Each level gets values
// derived from its index, so a re-pack that drifted by two bytes per level
// produces recognisably wrong numbers rather than plausible ones.
func (f *mboFixture) plantLevels() {
	for i := range 10 {
		off := absMBOPriceLad + i*10
		f.put32(off, int32(1000+i))   // Qty
		f.put32(off+4, int32(5000+i)) // Price
		f.put16(off+8, int16(70+i))   // NoOfOrders
	}
}

func mboDecoder(t *testing.T) (Decoder, *recordingCounters, *RegistryEntry) {
	t.Helper()

	cnt := &recordingCounters{}
	dests := testDests(t)
	q := dests.ByName("MD_TOKEN_QUOTE")
	cols := make([]string, 0, len(q.Columns))
	for i := range q.Columns {
		cols = append(cols, q.Columns[i].Name())
	}
	reg, err := BuildRegistry([]TranscodeSpec{{
		Code:          7200,
		Class:         model.ClassRateLimited,
		Contributions: []ContribSpec{{Destination: "MD_TOKEN_QUOTE", Columns: cols}},
	}}, dests, cnt)
	if err != nil {
		t.Fatalf("BuildRegistry: %v", err)
	}
	e := reg.Lookup(7200)
	return e.Dec, cnt, e
}

// TestG_FIX_1_MBOFieldsMatchTheHandWrittenOffsets is G-FIX-1's shape applied to
// 7200, which has no poc oracle at all.
func TestG_FIX_1_MBOFieldsMatchTheHandWrittenOffsets(t *testing.T) {
	dec, _, e := mboDecoder(t)
	spec := &e.Contrib[destQuote]

	f := newMBOFixture(t, bookTypeRegularLot)
	f.put32(absMBOToken, 424242)
	f.put16(absMBOTrading, 0x0A0B)
	f.put32(absMBOVolume, 1234567)
	f.put32(absMBOLastPx, 1000000)
	f.payload[absMBONetChg] = '+'
	f.put32(absMBONetPxChg, 0x7F7F7F7F) // skipped: OQ-L1
	f.put32(absMBOLastQty, 250)
	f.put32(absMBOLastTime, 1700000000)
	f.put32(absMBOAvgPx, 999999)
	f.put64(absMBOTotalBuy, 0x1122334455667788)
	f.put64(absMBOTotalSel, 0x8877665544332211)
	f.put16(absMBOIndic, 0x5555) // skipped
	f.put32(absMBOClosing, 111111)
	f.put32(absMBOOpen, 222222)
	f.put32(absMBOHigh, 333333)
	f.put32(absMBOLow, -44444)
	f.plantLevels()

	out := make([]model.UpdateUnit, 1)
	n, err := dec.Decode(model.MsgHeader{Code: 7200, LogTime: 1700001234}, f.payload, out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if n != 1 {
		t.Fatalf("Decode returned %d units, want 1", n)
	}
	u := &out[0]

	if u.Token != 424242 {
		t.Errorf("Token = %d, want 424242", u.Token)
	}
	for _, tc := range []struct {
		name string
		bit  uint8
		want int32
	}{
		{"VolumeTradedToday", q0VolumeTradedToday, 1234567},
		{"LastTradedPrice", q0LastTradedPrice, 1000000},
		{"LastTradeQuantity", q0LastTradeQty, 250},
		{"LastTradeTime", q0LastTradeTime, 1700000000},
		{"AverageTradePrice", q0AvgTradePrice, 999999},
		{"ClosingPrice", q0ClosingPrice, 111111},
		{"OpenPrice", q0OpenPrice, 222222},
		{"HighPrice", q0HighPrice, 333333},
		{"LowPrice", q0LowPrice, -44444},
		{"effective_ts (the header's LogTime)", q0EffectiveTS, 1700001234},
	} {
		if got := be32(blobAt(u, spec, tc.bit)); got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, got, tc.want)
		}
	}
	if got := be16(blobAt(u, spec, q0BookType)); got != bookTypeRegularLot {
		t.Errorf("BookType = %d, want %d", got, bookTypeRegularLot)
	}
	if got := be16(blobAt(u, spec, q0TradingStatus)); got != 0x0A0B {
		t.Errorf("TradingStatus = %#04x, want 0x0a0b", got)
	}
	if got := blobAt(u, spec, q0NetChangeInd)[0]; got != '+' {
		t.Errorf("NetChangeIndicator = %q, want '+'", got)
	}
	if got := be64(blobAt(u, spec, q0TotalBuyQty)); got != 0x1122334455667788 {
		t.Errorf("TotalBuyQuantity = %#016x", got)
	}
	if got := be64(blobAt(u, spec, q0TotalSellQty)); got != 0x8877665544332211 {
		t.Errorf("TotalSellQuantity = %#016x", got)
	}

	// The pad at offset 17, checked PRECISELY rather than by scanning for its
	// byte value.
	//
	// An earlier version of this test scanned every stored field for 0xEE and
	// it was unsound: level 6's planted quantity is 1006, which is 0x03EE, so
	// a legitimate value tripped it. The sound check is the field immediately
	// AFTER the pad -- a missing pad shifts it by one byte and nothing else
	// can produce its exact planted value.
	if f.payload[absMBOPad] != 0xEE {
		t.Fatalf("the fixture's own pad byte is %#02x; the check below proves nothing",
			f.payload[absMBOPad])
	}
	if got := be32(blobAt(u, spec, q0LastTradeQty)); got != 250 {
		t.Errorf("LastTradeQuantity = %d, want 250 -- the field after the pad at offset 17, "+
			"which is where a missing pad first shows", got)
	}
}

// TestADR_L08_LadderIsRepacked is the re-pack: ten 10-byte source levels into
// the 12-byte element layout destination 0 declares, with BbBuySellFlag zeroed.
//
// Element by element, because the whole point is that the shapes differ by two
// bytes: a verbatim copy would put level 1's quantity where level 0's flag
// belongs and drift two bytes further on every level after it. Level 9 would be
// 18 bytes out, which is why the last level is checked explicitly.
func TestADR_L08_LadderIsRepacked(t *testing.T) {
	dec, _, e := mboDecoder(t)
	spec := &e.Contrib[destQuote]

	f := newMBOFixture(t, bookTypeStopLoss)
	f.put32(absMBOToken, 1)
	f.plantLevels()

	out := make([]model.UpdateUnit, 1)
	if _, err := dec.Decode(model.MsgHeader{Code: 7200}, f.payload, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	ladder := blobAt(&out[0], spec, q0Ladder)
	if len(ladder) != 120 {
		t.Fatalf("ladder is %d bytes, want destination 0's declared 120", len(ladder))
	}

	for i := range 10 {
		el := ladder[i*12 : (i+1)*12]
		if got := be32(el[0:4]); got != int32(1000+i) {
			t.Errorf("level %d Quantity = %d, want %d", i, got, 1000+i)
		}
		if got := be32(el[4:8]); got != int32(5000+i) {
			t.Errorf("level %d Price = %d, want %d", i, got, 5000+i)
		}
		if got := be16(el[8:10]); got != int16(70+i) {
			t.Errorf("level %d NumberOfOrders = %d, want %d", i, got, 70+i)
		}
		// BbBuySellFlag: zero, because 7200's wire format has no such field
		// and bible section 4.8 says 7208's always carries zero anyway.
		if el[10] != 0 || el[11] != 0 {
			t.Errorf("level %d BbBuySellFlag = %#02x%#02x, want zero", i, el[10], el[11])
		}
	}
}

// TestMBOBookTypeGate is bible section 4.7's processing rule: store the message
// only for BookType 1 or 2.
//
// A PROCESSING decision, not a parse failure -- which is why it returns zero
// units with no error and counts booktype_skipped rather than a reject.
func TestMBOBookTypeGate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		bookType  int16
		wantUnits int
		wantSkip  int
	}{
		{"1 RL Regular Lot", 1, 1, 0},
		{"2 ST Stop Loss", 2, 1, 0},
		{"5 Odd Lot", 5, 0, 1},
		{"6 Spread", 6, 0, 1},
		{"7 Auction", 7, 0, 1},
		{"0", 0, 0, 1},
		{"negative", -1, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec, cnt, _ := mboDecoder(t)
			f := newMBOFixture(t, tc.bookType)
			f.put32(absMBOToken, 777)
			f.plantLevels()

			out := make([]model.UpdateUnit, 1)
			n, err := dec.Decode(model.MsgHeader{Code: 7200}, f.payload, out)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if n != tc.wantUnits {
				t.Errorf("BookType %d -> %d units, want %d", tc.bookType, n, tc.wantUnits)
			}
			if got := cnt.skipped[7200]; got != tc.wantSkip {
				t.Errorf("booktype_skipped{7200} = %d, want %d", got, tc.wantSkip)
			}
			// A skipped message must leave NOTHING behind: no token, no
			// contribution. out is reused per packet, so a half-stamped unit
			// would be routed by the caller as if it were real.
			if tc.wantUnits == 0 && (out[0].Token != 0 || out[0].Contribs[destQuote].Present()) {
				t.Errorf("a skipped message left a unit behind: token=%d present=%v",
					out[0].Token, out[0].Contribs[destQuote].Present())
			}
		})
	}
}

// TestMBOAndMBPProduceTheSameShape is bible section 4.8a's actual requirement:
// destination 0 must read identically whichever code the exchange sends.
//
// Same mask, same width, same row-key rule. If these ever diverged, a
// consumer's query would depend on which code was in force that day -- which is
// the outcome ADR-L08's re-pack exists to prevent.
func TestMBOAndMBPProduceTheSameShape(t *testing.T) {
	_, _, mbo := mboDecoder(t)
	_, _, mbp := mbpDecoder(t)

	a, b := mbo.Contrib[destQuote], mbp.Contrib[destQuote]
	if a.Mask != b.Mask {
		t.Errorf("7200 mask %b, 7208 mask %b -- destination 0 must read the same either way",
			a.Mask, b.Mask)
	}
	if a.Width != b.Width {
		t.Errorf("7200 width %d, 7208 width %d", a.Width, b.Width)
	}
	for bit := range uint8(16) {
		if a.Off[bit] != b.Off[bit] || a.Len[bit] != b.Len[bit] {
			t.Errorf("bit %d: 7200 at %d+%d, 7208 at %d+%d",
				bit, a.Off[bit], a.Len[bit], b.Off[bit], b.Len[bit])
		}
	}
}

func TestMBODecodeAllocatesNothing(t *testing.T) {
	dec, _, _ := mboDecoder(t)
	f := newMBOFixture(t, bookTypeRegularLot)
	f.put32(absMBOToken, 1)
	f.plantLevels()
	out := make([]model.UpdateUnit, 1)
	h := model.MsgHeader{Code: 7200, LogTime: 1}

	if n := testing.AllocsPerRun(1000, func() {
		if _, err := dec.Decode(h, f.payload, out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}); n != 0 {
		t.Fatalf("Decode allocates %v times per message, want 0", n)
	}
}

func TestMBOShortPayloadIsRejected(t *testing.T) {
	dec, _, _ := mboDecoder(t)
	out := make([]model.UpdateUnit, 1)
	f := newMBOFixture(t, bookTypeRegularLot)

	if _, err := dec.Decode(model.MsgHeader{Code: 7200}, f.payload[:417], out); err == nil {
		t.Error("a 417-byte payload was accepted for a 418-byte message")
	}
}
