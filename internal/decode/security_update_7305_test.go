package decode

import (
	"bytes"
	"encoding/binary"
	"testing"

	"feedvalve/internal/model"
)

// MS_SECURITY_UPDATE_INFO's offset table, TRANSCRIBED BY HAND from bible
// section 4.3 / Part V. Payload-relative.
//
// The offsets past 112 are the ones that matter most: they are only correct if
// ST_SEC_ELIGIBILITY_PER_MARKET[4] was read as 12 bytes and not 16. Reading 16
// shifts BoardLotQuantity, TickSize, DeleteFlag and BasePrice by four bytes
// each, so the four planted values below are what prove the unpadded 3-byte
// stride.
//
//	 48  Token                LONG
//	 52  SEC_INFO, 30 bytes
//	   52  InstrumentName     CHAR[6]   skipped
//	   58  Symbol             CHAR[10]  stored, bit 0
//	   68  Series             CHAR[2]   stored, bit 1
//	   70  ExpiryDate         LONG      stored, bit 2
//	   74  StrikePrice        LONG      stored, bit 3
//	   78  OptionType         CHAR[2]   skipped
//	   80  CALevel            SHORT     skipped
//	 82  PermittedToTrade     SHORT     skipped
//	 84  IssuedCapital        DOUBLE    skipped
//	 92  WarningQuantity      LONG      skipped
//	 96  FreezeQuantity       LONG      skipped
//	100  CreditRating         CHAR[12]  skipped
//	112  ST_SEC_ELIGIBILITY_PER_MARKET[4]  12 BYTES, STRIDE 3
//	124  IssueRate            SHORT     skipped
//	126  IssueStartDate       LONG      skipped
//	130  InterestPaymentDate  LONG      skipped
//	134  IssueMaturityDate    LONG      skipped
//	138  MarginPercentage     LONG      skipped
//	142  MinimumLotQuantity   LONG      skipped
//	146  BoardLotQuantity     LONG      stored, bit 4
//	150  TickSize             LONG      stored, bit 5
//	154  Name                 CHAR[25]  skipped
//	179  Reserved             CHAR[1]   skipped -- a REAL field
//	180..215  nine dates and ranges     skipped
//	216  ST_ELIGIBILITY_INDICATORS      skipped
//	218..237  book-closure and exercise dates, Multiplier
//	238  AssetInstrument      CHAR[6]   skipped
//	244  AssetName            CHAR[10]  skipped
//	254  UnderlyingToken      LONG      skipped
//	258  IntrinsicValue       LONG      skipped
//	262  ExtrinsicValue       LONG      skipped
//	266  ST_PURPOSE                     skipped
//	268  LocalUpdateDateTime  LONG      skipped
//	272  DeleteFlag           CHAR      stored, bit 7
//	273  Remark               CHAR[25]  skipped
//	298  BasePrice            LONG      stored, bit 6
//	     -> 302 = 8 prefix + 294 declared
const (
	absSecToken       = 48
	absSecInstrName   = 52
	absSecSymbol      = 58
	absSecSeries      = 68
	absSecExpiry      = 70
	absSecStrike      = 74
	absSecCreditRat   = 100
	absSecEligibility = 112
	absSecBoardLot    = 146
	absSecTickSize    = 150
	absSecName        = 154
	absSecReserved    = 179
	absSecDeleteFlag  = 272
	absSecRemark      = 273
	absSecBasePrice   = 298
	absSecPayload     = 302
)

type secFixture struct{ payload []byte }

func newSecFixture(t *testing.T, code uint16) *secFixture {
	t.Helper()
	f := &secFixture{payload: make([]byte, absSecPayload)}
	f.payload[0] = 8
	binary.BigEndian.PutUint16(f.payload[18:20], code)
	binary.BigEndian.PutUint16(f.payload[46:48], 294)

	// Fill the SKIPPED regions with a loud pattern, so a stored field that
	// picked one up is recognisable rather than plausible. 0xDD cannot appear
	// in any planted value below.
	for _, r := range [][2]int{
		{absSecInstrName, 6}, {absSecCreditRat, 12}, {absSecEligibility, 12},
		{absSecName, 25}, {absSecReserved, 1}, {absSecRemark, 25},
	} {
		for i := range r[1] {
			f.payload[r[0]+i] = 0xDD
		}
	}
	return f
}

func (f *secFixture) put32(off int, v int32) {
	binary.BigEndian.PutUint32(f.payload[off:], uint32(v))
}

func (f *secFixture) putText(off int, s string, width int) {
	// Blank-padded, NEVER NUL-terminated (bible section 2.6).
	for i := range width {
		if i < len(s) {
			f.payload[off+i] = s[i]
		} else {
			f.payload[off+i] = ' '
		}
	}
}

func secDecoder(t *testing.T, codes ...int) (Registry, *recordingCounters) {
	t.Helper()

	cnt := &recordingCounters{}
	dests := testDests(t)
	m := dests.ByName("MD_SECURITY_MASTER")
	cols := make([]string, 0, len(m.Columns))
	for i := range m.Columns {
		cols = append(cols, m.Columns[i].Name())
	}
	specs := make([]TranscodeSpec, 0, len(codes))
	for _, c := range codes {
		specs = append(specs, TranscodeSpec{
			Code:          c,
			Class:         model.ClassRateLimited,
			Contributions: []ContribSpec{{Destination: "MD_SECURITY_MASTER", Columns: cols}},
		})
	}
	reg, err := BuildRegistry(specs, dests, cnt)
	if err != nil {
		t.Fatalf("BuildRegistry: %v", err)
	}
	return reg, cnt
}

// TestG_FIX_1_SecurityUpdateFieldsMatchTheHandWrittenOffsets is G-FIX-1's shape
// applied to 7305, and the four fields past offset 112 are what prove the
// unpadded 3-byte eligibility stride.
func TestG_FIX_1_SecurityUpdateFieldsMatchTheHandWrittenOffsets(t *testing.T) {
	reg, _ := secDecoder(t, 7305)
	e := reg.Lookup(7305)
	spec := &e.Contrib[destSecurityMaster]

	f := newSecFixture(t, 7305)
	f.put32(absSecToken, 98765)
	f.putText(absSecSymbol, "NIFTY", 10)
	f.putText(absSecSeries, "XX", 2)
	f.put32(absSecExpiry, 1700000000)
	f.put32(absSecStrike, 2500000)
	f.put32(absSecBoardLot, 75)       // past 112: proves the stride
	f.put32(absSecTickSize, 5)        // past 112
	f.payload[absSecDeleteFlag] = 'N' // past 112
	f.put32(absSecBasePrice, 1234500) // past 112, the last stored field

	out := make([]model.UpdateUnit, 1)
	n, err := e.Dec.Decode(model.MsgHeader{Code: 7305, LogTime: 1700001234}, f.payload, out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if n != 1 {
		t.Fatalf("Decode returned %d units, want 1", n)
	}
	u := &out[0]

	if u.Token != 98765 {
		t.Errorf("Token = %d, want 98765", u.Token)
	}
	// Fixed-width text, blanks INCLUDED: the writer right-trims at bind time,
	// and copying verbatim is what keeps the digest stable.
	if got := blobAt(u, spec, s1Symbol); !bytes.Equal(got, []byte("NIFTY     ")) {
		t.Errorf("Symbol = %q, want %q -- blank-padded, verbatim", got, "NIFTY     ")
	}
	if got := blobAt(u, spec, s1Series); !bytes.Equal(got, []byte("XX")) {
		t.Errorf("Series = %q, want %q", got, "XX")
	}
	for _, tc := range []struct {
		name string
		bit  uint8
		want int32
	}{
		{"ExpiryDate", s1ExpiryDate, 1700000000},
		{"StrikePrice", s1StrikePrice, 2500000},
		{"BoardLotQuantity (past the eligibility array)", s1BoardLotQty, 75},
		{"TickSize (past the eligibility array)", s1TickSize, 5},
		{"BasePrice (the last stored field, at 298)", s1BasePrice, 1234500},
		{"effective_ts (the header's LogTime)", s1EffectiveTS, 1700001234},
	} {
		if got := be32(blobAt(u, spec, tc.bit)); got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, got, tc.want)
		}
	}
	if got := blobAt(u, spec, s1DeleteFlag)[0]; got != 'N' {
		t.Errorf("DeleteFlag = %q, want 'N'", got)
	}

	// No stored field may contain the skipped regions' 0xDD. Sound here, where
	// the 7208 version was not: every planted value above is checked and none
	// contains that byte.
	for bit := range uint8(9) {
		for _, b := range blobAt(u, spec, bit) {
			if b == 0xDD {
				t.Errorf("bit %d picked up a skipped region (0xDD): the eligibility array "+
					"was probably read as 16 bytes instead of 12", bit)
				break
			}
		}
	}

	// Destination 1's ImageLen, which 7305 supplies in full.
	if spec.Width != 37 {
		t.Errorf("ColSpec.Width = %d, want destination 1's 37", spec.Width)
	}
	if u.BlobLen != 37 {
		t.Errorf("BlobLen = %d, want 37", u.BlobLen)
	}
}

// TestSecurityUpdate7340IsTheSameStructure is section 8.6's own suggestion:
// reuse the 7305 fixture with the code changed and assert byte-identical field
// streams.
//
// 7340 is the identical structure re-sent periodically for every security that
// changed, so one implementation serves both -- while the two must still
// occupy DIFFERENT contributor slots, because the slot indexes the stored
// ordering state and sharing one would make each overwrite the other's.
func TestSecurityUpdate7340IsTheSameStructure(t *testing.T) {
	reg, _ := secDecoder(t, 7305, 7340)

	f05 := newSecFixture(t, 7305)
	f40 := newSecFixture(t, 7340)
	for _, f := range []*secFixture{f05, f40} {
		f.put32(absSecToken, 4242)
		f.putText(absSecSymbol, "BANKNIFTY", 10)
		f.putText(absSecSeries, "FU", 2)
		f.put32(absSecExpiry, 1710000000)
		f.put32(absSecStrike, 100)
		f.put32(absSecBoardLot, 25)
		f.put32(absSecTickSize, 5)
		f.payload[absSecDeleteFlag] = 'Y'
		f.put32(absSecBasePrice, 999)
	}

	out := make([]model.UpdateUnit, 2)
	h := model.MsgHeader{LogTime: 7}

	e05 := reg.Lookup(7305)
	h.Code = 7305
	if _, err := e05.Dec.Decode(h, f05.payload, out[0:1]); err != nil {
		t.Fatalf("7305: %v", err)
	}
	e40 := reg.Lookup(7340)
	h.Code = 7340
	if _, err := e40.Dec.Decode(h, f40.payload, out[1:2]); err != nil {
		t.Fatalf("7340: %v", err)
	}

	a, b := &out[0], &out[1]
	if a.BlobLen != b.BlobLen {
		t.Fatalf("blob lengths differ: %d and %d", a.BlobLen, b.BlobLen)
	}
	if !bytes.Equal(a.Blob[:a.BlobLen], b.Blob[:b.BlobLen]) {
		t.Error("7305 and 7340 produced different blobs from the same fields")
	}
	if a.Code != 7305 || b.Code != 7340 {
		t.Errorf("codes = %d and %d, want 7305 and 7340 -- Code() distinguishes the two",
			a.Code, b.Code)
	}

	// Different slots, same row.
	sa := a.Contribs[destSecurityMaster].Slot
	sb := b.Contribs[destSecurityMaster].Slot
	if sa == sb {
		t.Errorf("7305 and 7340 share contributor slot %d; the slot indexes stored ordering "+
			"state, so sharing one makes each overwrite the other's", sa)
	}
	if a.Contribs[destSecurityMaster].Row != b.Contribs[destSecurityMaster].Row {
		t.Error("the same token produced different rows")
	}
}

// TestSecurityUpdateTextKeepsItsBlanks is Vol 1 section 9.7: no blank-trimming
// in a decoder. Bible section 2.6 -- blank-padded, never NUL-terminated -- so a
// strlen-style read is a bug and a trimmed value would make the digest a
// function of our trimming rule.
func TestSecurityUpdateTextKeepsItsBlanks(t *testing.T) {
	reg, _ := secDecoder(t, 7305)
	e := reg.Lookup(7305)
	spec := &e.Contrib[destSecurityMaster]

	f := newSecFixture(t, 7305)
	f.putText(absSecSymbol, "A", 10) // one character, nine blanks
	f.putText(absSecSeries, "", 2)   // two blanks

	out := make([]model.UpdateUnit, 1)
	if _, err := e.Dec.Decode(model.MsgHeader{Code: 7305}, f.payload, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := blobAt(&out[0], spec, s1Symbol); !bytes.Equal(got, []byte("A         ")) {
		t.Errorf("Symbol = %q, want the full ten bytes with blanks", got)
	}
	if got := blobAt(&out[0], spec, s1Series); !bytes.Equal(got, []byte("  ")) {
		t.Errorf("Series = %q, want two blanks", got)
	}
}

func TestSecurityUpdateAllocatesNothing(t *testing.T) {
	reg, _ := secDecoder(t, 7305)
	e := reg.Lookup(7305)
	f := newSecFixture(t, 7305)
	f.put32(absSecToken, 1)
	out := make([]model.UpdateUnit, 1)
	h := model.MsgHeader{Code: 7305, LogTime: 1}

	if n := testing.AllocsPerRun(1000, func() {
		if _, err := e.Dec.Decode(h, f.payload, out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}); n != 0 {
		t.Fatalf("Decode allocates %v times per message, want 0", n)
	}
}

func TestSecurityUpdateShortPayloadIsRejected(t *testing.T) {
	reg, _ := secDecoder(t, 7305)
	e := reg.Lookup(7305)
	f := newSecFixture(t, 7305)
	out := make([]model.UpdateUnit, 1)

	if _, err := e.Dec.Decode(model.MsgHeader{Code: 7305}, f.payload[:301], out); err == nil {
		t.Error("a 301-byte payload was accepted for a 302-byte message")
	}
}
