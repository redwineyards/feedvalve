package decode

import (
	"encoding/binary"
	"testing"

	"feedvalve/internal/model"
)

// 7208's offset table, TRANSCRIBED BY HAND from bible section 4.8 / Part V.
//
// THIS IS THE POINT OF GATE G-FIX-1 AND IT IS BUILT THE HARD WAY ON PURPOSE.
// The offsets below were read off the specification's own table, not derived
// from mbp_only_7208.go's reader calls and not produced by any builder that
// walks the parser's field order. A fixture that shares the parser's
// assumptions agrees with it even when both are wrong, which is exactly how
// poc's 213-byte stride survived for months: every test it had was built from
// the same wrong arithmetic as the code.
//
// If this table and the decoder ever disagree, ONE of them is wrong and the
// test cannot say which -- that is the correct outcome, because it forces a
// reading of Part V rather than a reading of the code.
//
//	INTERACTIVE_ONLY_MBP_DATA, record-relative:
//	  0   Token               LONG
//	  4   BookType            SHORT
//	  6   TradingStatus       SHORT
//	  8   VolumeTradedToday   LONG
//	 12   LastTradedPrice     LONG
//	 16   NetChangeIndicator  CHAR
//	 17   *** PAD ***
//	 18   NetPriceChangeFromClosingPrice LONG
//	 22   LastTradeQuantity   LONG
//	 26   LastTradeTime       LONG
//	 30   AverageTradePrice   LONG
//	 34   AuctionNumber       SHORT
//	 36   AuctionStatus       SHORT
//	 38   InitiatorType       SHORT
//	 40   InitiatorPrice      LONG
//	 44   InitiatorQuantity   LONG
//	 48   AuctionPrice        LONG
//	 52   AuctionQuantity     LONG
//	 56   MBP_INFORMATION[10] 120 bytes
//	176   BbTotalBuyFlag      SHORT
//	178   BbTotalSellFlag     SHORT
//	180   TotalBuyQuantity    DOUBLE
//	188   TotalSellQuantity   DOUBLE
//	196   ST_INDICATOR        2
//	198   ClosingPrice        LONG
//	202   OpenPrice           LONG
//	206   HighPrice           LONG
//	210   LowPrice            LONG
//	      -> 214 with the pad; the array of two plus 40 header plus 2 count = 470
const (
	// Record-relative, from the table above.
	recToken             = 0
	recBookType          = 4
	recTradingStatus     = 6
	recVolumeTradedToday = 8
	recLastTradedPrice   = 12
	recNetChangeInd      = 16
	recPad               = 17
	recNetPriceChange    = 18
	recLastTradeQty      = 22
	recLastTradeTime     = 26
	recAvgTradePrice     = 30
	recLadder            = 56
	recTotalBuyQty       = 180
	recTotalSellQty      = 188
	recClosingPrice      = 198
	recOpenPrice         = 202
	recHighPrice         = 206
	recLowPrice          = 210

	// Absolute, written out rather than computed from the decoder's constants:
	// 8 prefix + 40 header = 48 is where NoOfRecords sits, and record 0 starts
	// at 50.
	absNoOfRecords = 48
	absRecord0     = 50
	absStride      = 214
	absPayloadLen  = 478 // 8 prefix + 470 declared
)

// mbpFixture builds a 7208 payload by PLACING each field at its absolute
// offset. No appends, no cursor, no shared field order with the decoder.
//
// Every field gets a DISTINCT recognisable value, and record 1's values differ
// from record 0's, so a one-byte shift or a wrong stride cannot produce a
// passing comparison by luck.
type mbpFixture struct {
	payload []byte
}

func newMBPFixture(t *testing.T, noOfRecords int) *mbpFixture {
	t.Helper()
	f := &mbpFixture{payload: make([]byte, absPayloadLen)}
	f.payload[0] = 8 // the netId repeated in byte 0
	binary.BigEndian.PutUint16(f.payload[18:20], 7208)
	binary.BigEndian.PutUint16(f.payload[46:48], 470)
	binary.BigEndian.PutUint16(f.payload[absNoOfRecords:absNoOfRecords+2], uint16(int16(noOfRecords)))
	return f
}

func (f *mbpFixture) at(rec, off int) int { return absRecord0 + rec*absStride + off }

func (f *mbpFixture) put32(rec, off int, v int32) {
	binary.BigEndian.PutUint32(f.payload[f.at(rec, off):], uint32(v))
}

func (f *mbpFixture) put16(rec, off int, v int16) {
	binary.BigEndian.PutUint16(f.payload[f.at(rec, off):], uint16(v))
}

func (f *mbpFixture) put64(rec, off int, v uint64) {
	binary.BigEndian.PutUint64(f.payload[f.at(rec, off):], v)
}

func (f *mbpFixture) putByte(rec, off int, v byte) { f.payload[f.at(rec, off)] = v }

func (f *mbpFixture) putRun(rec, off int, fill byte, n int) {
	for i := range n {
		f.payload[f.at(rec, off)+i] = fill
	}
}

// mbpRecordValues is one record's worth of planted values.
type mbpRecordValues struct {
	token        int32
	bookType     int16
	tradingState int16
	volume       int32
	lastPrice    int32
	netChange    byte
	netPriceChg  int32 // planted but SKIPPED by the decoder -- OQ-L1
	lastQty      int32
	lastTime     int32
	avgPrice     int32
	ladderFill   byte
	totalBuy     uint64
	totalSell    uint64
	closing      int32
	open         int32
	high         int32
	low          int32
}

// plant writes one record. The values are deliberately unlike each other.
func (f *mbpFixture) plant(rec int, v mbpRecordValues) {
	f.put32(rec, recToken, v.token)
	f.put16(rec, recBookType, v.bookType)
	f.put16(rec, recTradingStatus, v.tradingState)
	f.put32(rec, recVolumeTradedToday, v.volume)
	f.put32(rec, recLastTradedPrice, v.lastPrice)
	f.putByte(rec, recNetChangeInd, v.netChange)
	f.putByte(rec, recPad, 0xEE) // the pad: a loud value, so a field read from it is obvious
	f.put32(rec, recNetPriceChange, v.netPriceChg)
	f.put32(rec, recLastTradeQty, v.lastQty)
	f.put32(rec, recLastTradeTime, v.lastTime)
	f.put32(rec, recAvgTradePrice, v.avgPrice)
	f.putRun(rec, recLadder, v.ladderFill, 120)
	f.put64(rec, recTotalBuyQty, v.totalBuy)
	f.put64(rec, recTotalSellQty, v.totalSell)
	f.put32(rec, recClosingPrice, v.closing)
	f.put32(rec, recOpenPrice, v.open)
	f.put32(rec, recHighPrice, v.high)
	f.put32(rec, recLowPrice, v.low)
}

func mbpRecord0Values() mbpRecordValues {
	return mbpRecordValues{
		token: 98765, bookType: 1, tradingState: 0x0A0B, volume: 1234567,
		lastPrice: 1000000, netChange: '+', netPriceChg: 0x7F7F7F7F,
		lastQty: 250, lastTime: 1700000000, avgPrice: 999999,
		ladderFill: 0xA1, totalBuy: 0x1122334455667788, totalSell: 0x8877665544332211,
		closing: 111111, open: 222222, high: 333333, low: 44444,
	}
}

func mbpRecord1Values() mbpRecordValues {
	return mbpRecordValues{
		token: 55555, bookType: 2, tradingState: 0x0C0D, volume: 7654321,
		lastPrice: -500000, netChange: '-', netPriceChg: 0x6E6E6E6E,
		lastQty: 777, lastTime: 1700009999, avgPrice: -1,
		ladderFill: 0xB2, totalBuy: 0x9988776655443322, totalSell: 0x2233445566778899,
		closing: -222222, open: 888888, high: 999999, low: -1000,
	}
}

// mbpDecoder builds the 7208 decoder with a recording counter and a registry
// whose ColSpec came from the real destination table.
func mbpDecoder(t *testing.T) (Decoder, *recordingCounters, *RegistryEntry) {
	t.Helper()

	cnt := &recordingCounters{}
	dests := testDests(t)
	q := dests.ByName("MD_TOKEN_QUOTE")

	cols := make([]string, 0, len(q.Columns))
	for i := range q.Columns {
		cols = append(cols, q.Columns[i].Name())
	}
	reg, err := BuildRegistry([]TranscodeSpec{{
		Code:          7208,
		Class:         model.ClassRateLimited,
		Contributions: []ContribSpec{{Destination: "MD_TOKEN_QUOTE", Columns: cols}},
	}}, dests, cnt)
	if err != nil {
		t.Fatalf("BuildRegistry: %v", err)
	}
	e := reg.Lookup(7208)
	return e.Dec, cnt, e
}

// recordingCounters is a decode.Counters that counts, for tests that assert on
// the reports rather than ignoring them.
type recordingCounters struct {
	clamped   map[model.TransCode]int
	skipped   map[model.TransCode]int
	overrun   map[model.TransCode]int
	truncated map[[2]int]int
}

func (c *recordingCounters) RecordCountClamped(code model.TransCode) {
	if c.clamped == nil {
		c.clamped = map[model.TransCode]int{}
	}
	c.clamped[code]++
}

func (c *recordingCounters) BooktypeSkipped(code model.TransCode) {
	if c.skipped == nil {
		c.skipped = map[model.TransCode]int{}
	}
	c.skipped[code]++
}

func (c *recordingCounters) BlobOverrun(code model.TransCode) {
	if c.overrun == nil {
		c.overrun = map[model.TransCode]int{}
	}
	c.overrun[code]++
}

func (c *recordingCounters) MessageTruncated(dest model.DestID, bit uint8) {
	if c.truncated == nil {
		c.truncated = map[[2]int]int{}
	}
	c.truncated[[2]int{int(dest), int(bit)}]++
}

// blobAt reads a field back out of a unit's blob, by mask bit, through the
// registry's own packed offsets -- which is what the shard's merge will do.
func blobAt(u *model.UpdateUnit, spec *ColSpec, bit uint8) []byte {
	off := int(spec.Off[bit])
	return u.Blob[off : off+int(spec.Len[bit])]
}

func be32(b []byte) int32  { return int32(binary.BigEndian.Uint32(b)) }
func be16(b []byte) int16  { return int16(binary.BigEndian.Uint16(b)) }
func be64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// TestG_FIX_1_MBPFieldsMatchTheHandWrittenOffsets is gate G-FIX-1.
//
// Every stored field of record 0, read back out of the blob and compared with
// the value planted at its hand-transcribed absolute offset. The planted values
// are mutually unlike, so a field read one byte early, one field late, or from
// the pad cannot match by accident.
func TestG_FIX_1_MBPFieldsMatchTheHandWrittenOffsets(t *testing.T) {
	dec, _, e := mbpDecoder(t)
	spec := &e.Contrib[destQuote]

	f := newMBPFixture(t, 1)
	want := mbpRecord0Values()
	f.plant(0, want)

	out := make([]model.UpdateUnit, 2)
	n, err := dec.Decode(model.MsgHeader{Code: 7208, LogTime: 1700001234, BCSeqNo: 42},
		f.payload, out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if n != 1 {
		t.Fatalf("Decode returned %d units, want 1", n)
	}
	u := &out[0]

	if u.Token != want.token {
		t.Errorf("Token = %d, want %d", u.Token, want.token)
	}
	if got := be32(blobAt(u, spec, q0LastTradedPrice)); got != want.lastPrice {
		t.Errorf("LastTradedPrice = %d, want %d", got, want.lastPrice)
	}
	if got := be32(blobAt(u, spec, q0VolumeTradedToday)); got != want.volume {
		t.Errorf("VolumeTradedToday = %d, want %d", got, want.volume)
	}
	if got := be32(blobAt(u, spec, q0OpenPrice)); got != want.open {
		t.Errorf("OpenPrice = %d, want %d", got, want.open)
	}
	if got := be32(blobAt(u, spec, q0HighPrice)); got != want.high {
		t.Errorf("HighPrice = %d, want %d", got, want.high)
	}
	if got := be32(blobAt(u, spec, q0LowPrice)); got != want.low {
		t.Errorf("LowPrice = %d, want %d", got, want.low)
	}
	// OQ-L1: offset 198, not 18. If the decoder read offset 18 this would be
	// netPriceChg's 0x7F7F7F7F, which is why that value is planted.
	if got := be32(blobAt(u, spec, q0ClosingPrice)); got != want.closing {
		t.Errorf("ClosingPrice = %d, want %d (OQ-L1: record offset 198, not 18)",
			got, want.closing)
	}
	if got := be32(blobAt(u, spec, q0ClosingPrice)); got == want.netPriceChg {
		t.Error("ClosingPrice was read from record offset 18 (NetPriceChangeFromClosingPrice)")
	}
	if got := be16(blobAt(u, spec, q0TradingStatus)); got != want.tradingState {
		t.Errorf("TradingStatus = %#04x, want %#04x", got, want.tradingState)
	}
	if got := blobAt(u, spec, q0NetChangeInd)[0]; got != want.netChange {
		t.Errorf("NetChangeIndicator = %q, want %q", got, want.netChange)
	}
	if got := be32(blobAt(u, spec, q0LastTradeQty)); got != want.lastQty {
		t.Errorf("LastTradeQuantity = %d, want %d", got, want.lastQty)
	}
	if got := be32(blobAt(u, spec, q0LastTradeTime)); got != want.lastTime {
		t.Errorf("LastTradeTime = %d, want %d", got, want.lastTime)
	}
	if got := be32(blobAt(u, spec, q0AvgTradePrice)); got != want.avgPrice {
		t.Errorf("AverageTradePrice = %d, want %d", got, want.avgPrice)
	}
	if got := be64(blobAt(u, spec, q0TotalBuyQty)); got != want.totalBuy {
		t.Errorf("TotalBuyQuantity = %#016x, want %#016x", got, want.totalBuy)
	}
	if got := be64(blobAt(u, spec, q0TotalSellQty)); got != want.totalSell {
		t.Errorf("TotalSellQuantity = %#016x, want %#016x", got, want.totalSell)
	}
	if got := be16(blobAt(u, spec, q0BookType)); got != want.bookType {
		t.Errorf("BookType = %d, want %d", got, want.bookType)
	}

	// The ladder, as one 120-byte run. Every byte, because a short copy would
	// leave the tail zero and a long one would eat BbTotalBuyFlag.
	ladder := blobAt(u, spec, q0Ladder)
	if len(ladder) != 120 {
		t.Fatalf("ladder is %d bytes, want 120", len(ladder))
	}
	for i, b := range ladder {
		if b != want.ladderFill {
			t.Fatalf("ladder byte %d = %#02x, want %#02x", i, b, want.ladderFill)
		}
	}

	// Bit 15 is the HEADER's LogTime, not a record field.
	if got := be32(blobAt(u, spec, q0EffectiveTS)); got != 1700001234 {
		t.Errorf("effective_ts = %d, want the header's LogTime 1700001234", got)
	}

	// The pad is 0xEE in the fixture. No stored field may contain it.
	for bit := range uint8(16) {
		if !spec.Mask.Has(bit) {
			continue
		}
		for _, b := range blobAt(u, spec, bit) {
			if b == 0xEE {
				t.Errorf("bit %d contains the pad byte 0xEE: a field was read across "+
					"record offset 17", bit)
				break
			}
		}
	}
}

// TestMBPStrideIs214 is the 213-vs-214 regression, as a test rather than only a
// compile-time gate.
//
// Record 1's planted values are entirely different from record 0's, so a
// 213-byte stride makes record 1 decode as a byte-shifted mixture of the two
// and every comparison below fails. This is the assertion poc's suite never
// had.
func TestMBPStrideIs214(t *testing.T) {
	dec, _, e := mbpDecoder(t)
	spec := &e.Contrib[destQuote]

	f := newMBPFixture(t, 2)
	w0, w1 := mbpRecord0Values(), mbpRecord1Values()
	f.plant(0, w0)
	f.plant(1, w1)

	out := make([]model.UpdateUnit, 2)
	n, err := dec.Decode(model.MsgHeader{Code: 7208, LogTime: 1, BCSeqNo: 1}, f.payload, out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if n != 2 {
		t.Fatalf("Decode returned %d units, want 2", n)
	}

	for i, want := range []mbpRecordValues{w0, w1} {
		u := &out[i]
		if u.Token != want.token {
			t.Errorf("record %d Token = %d, want %d -- a 213-byte stride lands here first",
				i, u.Token, want.token)
		}
		if got := be32(blobAt(u, spec, q0LastTradedPrice)); got != want.lastPrice {
			t.Errorf("record %d LastTradedPrice = %d, want %d", i, got, want.lastPrice)
		}
		if got := be32(blobAt(u, spec, q0LowPrice)); got != want.low {
			t.Errorf("record %d LowPrice = %d, want %d", i, got, want.low)
		}
		if got := blobAt(u, spec, q0Ladder)[0]; got != want.ladderFill {
			t.Errorf("record %d ladder starts %#02x, want %#02x", i, got, want.ladderFill)
		}
	}

	// The two units are different rows, which is the whole reason a message
	// fans out at all.
	if out[0].Contribs[destQuote].Row == out[1].Contribs[destQuote].Row {
		t.Error("both records produced the same row key")
	}
}

// TestG_FRM_6_MBPRecordCountIsClamped is gate G-FRM-6.
//
// NoOfRecords = 5 against an array of 2: clamped, counted, and EXACTLY TWO
// records parsed. The message is always 470 bytes with room for both slots, so
// decoding a third would decode the message's own trailing padding into an
// entirely fabricated security -- a token, a price, a full ladder -- that is
// indistinguishable downstream from a real one.
func TestG_FRM_6_MBPRecordCountIsClamped(t *testing.T) {
	dec, cnt, _ := mbpDecoder(t)

	f := newMBPFixture(t, 5)
	f.plant(0, mbpRecord0Values())
	f.plant(1, mbpRecord1Values())

	// Three slots' worth of room, so a decoder that ignored the clamp would
	// have somewhere to write a third unit rather than panicking -- the test
	// has to be able to OBSERVE the fabrication, not be saved from it.
	out := make([]model.UpdateUnit, 3)
	n, err := dec.Decode(model.MsgHeader{Code: 7208}, f.payload, out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if n != 2 {
		t.Fatalf("Decode returned %d units for a declared count of 5; want 2 (mbpMaxRecords)", n)
	}
	if cnt.clamped[7208] != 1 {
		t.Errorf("record_count_clamped{7208} = %d, want 1", cnt.clamped[7208])
	}
	// The third slot must be untouched: zero units written there, not a unit
	// decoded from padding.
	if out[2].Token != 0 || out[2].BlobLen != 0 {
		t.Errorf("a third unit was written: Token=%d BlobLen=%d -- this is the fabricated "+
			"security G-FRM-6 exists to prevent", out[2].Token, out[2].BlobLen)
	}
}

func TestMBPRecordCountEdges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		declared  int
		wantUnits int
		wantClamp int
	}{
		{"zero records is legal and not a reject", 0, 0, 0},
		{"one record", 1, 1, 0},
		{"exactly the maximum", 2, 2, 0},
		{"above the maximum is clamped", 3, 2, 1},
		{"negative is clamped to zero, not reinterpreted", -1, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec, cnt, _ := mbpDecoder(t)
			f := newMBPFixture(t, tc.declared)
			f.plant(0, mbpRecord0Values())
			f.plant(1, mbpRecord1Values())

			out := make([]model.UpdateUnit, 2)
			n, err := dec.Decode(model.MsgHeader{Code: 7208}, f.payload, out)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if n != tc.wantUnits {
				t.Errorf("declared %d -> %d units, want %d", tc.declared, n, tc.wantUnits)
			}
			if got := cnt.clamped[7208]; got != tc.wantClamp {
				t.Errorf("record_count_clamped{7208} = %d, want %d", got, tc.wantClamp)
			}
		})
	}
}

// TestMBPNegativePricesStayNegative: prices may be negative (bible section
// 2.4), and the image holds wire form, so this is structural -- a Copy of
// big-endian bytes cannot lose the sign. The test pins it against a future
// "tidy-up" to an unsigned read, which would turn -500000 into 4294467296 and
// be noticed as a data-quality complaint weeks later.
func TestMBPNegativePricesStayNegative(t *testing.T) {
	dec, _, e := mbpDecoder(t)
	spec := &e.Contrib[destQuote]

	f := newMBPFixture(t, 1)
	v := mbpRecord0Values()
	v.lastPrice = -500000
	v.closing = -1
	v.low = -2147483648 // the most negative LONG
	f.plant(0, v)

	out := make([]model.UpdateUnit, 2)
	if _, err := dec.Decode(model.MsgHeader{Code: 7208}, f.payload, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for _, tc := range []struct {
		name string
		bit  uint8
		want int32
	}{
		{"LastTradedPrice", q0LastTradedPrice, -500000},
		{"ClosingPrice", q0ClosingPrice, -1},
		{"LowPrice", q0LowPrice, -2147483648},
	} {
		if got := be32(blobAt(&out[0], spec, tc.bit)); got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestMBPBlobMatchesTheRegistrysWidth ties the decoder's writes to the
// registry's declared ColSpec. BlobLen is what the shard's merge trusts, so a
// decoder writing more or less than it declared would corrupt a row image
// without any read going out of bounds.
func TestMBPBlobMatchesTheRegistrysWidth(t *testing.T) {
	dec, _, e := mbpDecoder(t)
	spec := &e.Contrib[destQuote]

	f := newMBPFixture(t, 1)
	f.plant(0, mbpRecord0Values())
	out := make([]model.UpdateUnit, 2)
	if _, err := dec.Decode(model.MsgHeader{Code: 7208}, f.payload, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	u := &out[0]

	// 181 is destination 0's ImageLen and 7208 supplies the full mask, so the
	// two must agree exactly (Vol 1 section 9.2).
	if spec.Width != 181 {
		t.Errorf("ColSpec.Width = %d, want 181", spec.Width)
	}
	if u.BlobLen != spec.Width {
		t.Errorf("BlobLen = %d, want the declared %d", u.BlobLen, spec.Width)
	}
	if u.BlobLen > model.UnitBlobSize {
		t.Errorf("BlobLen = %d, over unitBlobSize %d", u.BlobLen, model.UnitBlobSize)
	}
	c := u.Contribs[destQuote]
	if c.Mask != spec.Mask || c.BlobLen != spec.Width {
		t.Errorf("contribution = {mask %b, len %d}, want {%b, %d}",
			c.Mask, c.BlobLen, spec.Mask, spec.Width)
	}
	if c.Row != model.SingleKey(uint32(mbpRecord0Values().token)) {
		t.Errorf("row key = %d, want SingleKey(token)", c.Row)
	}
	if u.Contribs[destSecurityMaster].Present() || u.Contribs[destMarketStatus].Present() {
		t.Error("7208 contributed to a destination it does not supply")
	}
}

func TestMBPDecodeAllocatesNothing(t *testing.T) {
	dec, _, _ := mbpDecoder(t)
	f := newMBPFixture(t, 2)
	f.plant(0, mbpRecord0Values())
	f.plant(1, mbpRecord1Values())
	out := make([]model.UpdateUnit, 2)
	h := model.MsgHeader{Code: 7208, LogTime: 1, BCSeqNo: 1}

	if n := testing.AllocsPerRun(1000, func() {
		if _, err := dec.Decode(h, f.payload, out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}); n != 0 {
		t.Fatalf("Decode allocates %v times per message, want 0", n)
	}
}

// TestMBPShortPayloadIsRejected: the dispatcher's length checks make this
// unreachable in production, which is why it returns a sentinel rather than
// panicking -- a decoder reached with too few bytes is a caller defect.
func TestMBPShortPayloadIsRejected(t *testing.T) {
	dec, _, _ := mbpDecoder(t)
	f := newMBPFixture(t, 1)
	out := make([]model.UpdateUnit, 2)

	if _, err := dec.Decode(model.MsgHeader{Code: 7208}, f.payload[:477], out); err == nil {
		t.Error("a 477-byte payload was accepted for a 478-byte message")
	}
}

// TestDestinationIDsMatchTheirNames pins the named constants to the shipped
// mapping. A bare 0, 1 or 2 at a call site is how a contribution lands in the
// wrong table, and the names are only meaningful while they agree with the
// file.
func TestDestinationIDsMatchTheirNames(t *testing.T) {
	dests := testDests(t)
	for _, tc := range []struct {
		id   model.DestID
		name string
	}{
		{destQuote, "MD_TOKEN_QUOTE"},
		{destSecurityMaster, "MD_SECURITY_MASTER"},
		{destMarketStatus, "MD_MARKET_STATUS"},
	} {
		if got := dests.ByID(tc.id); got == nil || got.Name != tc.name {
			t.Errorf("destination %d is %v, want %s", tc.id, got, tc.name)
		}
	}
}
