package decode

import (
	"bytes"
	"encoding/binary"
	"testing"

	"feedvalve/internal/model"
)

// MS_BCAST_VCT_MSGS's offset table, TRANSCRIBED BY HAND from bible section 4.5
// / Part V. Payload-relative.
//
// Like 7200, this family has NO poc oracle (Vol 1 section 8.6), so the table is
// the only independent check that exists.
//
//	 48  Token                   LONG       stored as DATA, bit 4
//	 52  SEC_INFO                30 bytes   skipped
//	 82  MarketType              SHORT      THE ROW KEY, not in the image
//	 84  ST_BCAST_DESTINATION    SHORT      stored, bit 3
//	 86  BroadcastMessageLength  SHORT      stored CLAMPED, bit 1
//	 88  BroadcastMessage        CHAR[239]  stored, first 128 only, bit 2
//	327  *** TRAILING PAD ***    1          offset 319 of the message
//	     -> 328 = 8 prefix + 320 declared
const (
	absVctToken     = 48
	absVctSecInfo   = 52
	absVctMarket    = 82
	absVctBcastDest = 84
	absVctMsgLen    = 86
	absVctMsgText   = 88
	absVctPad       = 327
	absVctPayload   = 328
)

type vctFixture struct{ payload []byte }

func newVctFixture(t *testing.T, code uint16, marketType int16) *vctFixture {
	t.Helper()
	f := &vctFixture{payload: make([]byte, absVctPayload)}
	f.payload[0] = 8
	binary.BigEndian.PutUint16(f.payload[18:20], code)
	binary.BigEndian.PutUint16(f.payload[46:48], 320)
	binary.BigEndian.PutUint16(f.payload[absVctMarket:], uint16(marketType))
	f.payload[absVctPad] = 0xEE

	// SEC_INFO is skipped here: fill it loudly.
	for i := range 30 {
		f.payload[absVctSecInfo+i] = 0xDD
	}
	return f
}

// setMessage plants the broadcast text and its DECLARED length, which the test
// deliberately allows to disagree with the text's real length -- that
// disagreement is the whole subject of the length rule.
func (f *vctFixture) setMessage(declared int, text string) {
	binary.BigEndian.PutUint16(f.payload[absVctMsgLen:], uint16(int16(declared)))
	// Fill all 239 bytes with a recognisable pattern first, so the bytes PAST
	// the valid length are non-zero on the wire -- which is what makes "never
	// stored" and "zero-filled" testable claims rather than accidents.
	for i := range 239 {
		f.payload[absVctMsgText+i] = 0x5A
	}
	copy(f.payload[absVctMsgText:], text)
}

func vctRegistry(t *testing.T) (Registry, *recordingCounters) {
	t.Helper()

	cnt := &recordingCounters{}
	dests := testDests(t)
	m := dests.ByName("MD_MARKET_STATUS")
	cols := make([]string, 0, len(m.Columns))
	for i := range m.Columns {
		cols = append(cols, m.Columns[i].Name())
	}
	specs := make([]TranscodeSpec, 0, len(vctCodes))
	for _, c := range vctCodes {
		specs = append(specs, TranscodeSpec{
			Code:          int(c),
			Class:         model.ClassRateLimited,
			Contributions: []ContribSpec{{Destination: "MD_MARKET_STATUS", Columns: cols}},
		})
	}
	reg, err := BuildRegistry(specs, dests, cnt)
	if err != nil {
		t.Fatalf("BuildRegistry: %v", err)
	}
	return reg, cnt
}

// TestG_FIX_1_VctFieldsMatchTheHandWrittenOffsets is G-FIX-1's shape for the
// 6511 family.
func TestG_FIX_1_VctFieldsMatchTheHandWrittenOffsets(t *testing.T) {
	reg, _ := vctRegistry(t)
	e := reg.Lookup(6511)
	spec := &e.Contrib[destMarketStatus]

	f := newVctFixture(t, 6511, 2)
	binary.BigEndian.PutUint32(f.payload[absVctToken:], 123456)
	binary.BigEndian.PutUint16(f.payload[absVctBcastDest:], 0x0102)
	f.setMessage(11, "NORMAL OPEN")

	out := make([]model.UpdateUnit, 1)
	n, err := e.Dec.Decode(model.MsgHeader{Code: 6511, LogTime: 1700001234}, f.payload, out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if n != 1 {
		t.Fatalf("Decode returned %d units, want 1", n)
	}
	u := &out[0]

	// Bit 0 is the CONTRIBUTING TRANSACTION CODE, from the header: it is how a
	// reader tells Open from Closed, since the five codes share one row per
	// MarketType and differ only in which last wrote it.
	if got := be16(blobAt(u, spec, m2Transcode)); got != 6511 {
		t.Errorf("transcode = %d, want 6511", got)
	}
	if got := be32(blobAt(u, spec, m2Token)); got != 123456 {
		t.Errorf("token (as DATA) = %d, want 123456", got)
	}
	if got := be16(blobAt(u, spec, m2BcastDest)); got != 0x0102 {
		t.Errorf("ST_BCAST_DESTINATION = %#04x, want 0x0102", got)
	}
	if got := be16(blobAt(u, spec, m2MessageLen)); got != 11 {
		t.Errorf("message_len = %d, want 11", got)
	}
	if got := be32(blobAt(u, spec, m2EffectiveTS)); got != 1700001234 {
		t.Errorf("effective_ts = %d, want the header's LogTime", got)
	}

	text := blobAt(u, spec, m2MessageText)
	if len(text) != 128 {
		t.Fatalf("message_text is %d bytes, want the declared 128", len(text))
	}
	if !bytes.Equal(text[:11], []byte("NORMAL OPEN")) {
		t.Errorf("message_text = %q, want %q", text[:11], "NORMAL OPEN")
	}

	// The row key is MARKETTYPE, not the token.
	if got := u.Contribs[destMarketStatus].Row; got != model.SingleKey(2) {
		t.Errorf("row key = %d, want SingleKey(MarketType=2)", got)
	}
	if got := be32(blobAt(u, spec, m2Token)); got == 2 {
		t.Error("the row key appears to have been taken from the token")
	}

	// Destination 2's ImageLen.
	if spec.Width != 142 {
		t.Errorf("ColSpec.Width = %d, want destination 2's 142", spec.Width)
	}
	for bit := range uint8(6) {
		for _, b := range blobAt(u, spec, bit) {
			if b == 0xDD {
				t.Errorf("bit %d picked up the skipped SEC_INFO", bit)
				break
			}
		}
	}
}

// TestINV_SCOPE_VctUnitsAreGeneral is INV-SCOPE: a unit's scope comes from its
// transcode's declared inventory entry, never from whether the message happens
// to contain a field called Token.
//
// Routing these by their token would scatter five market-status messages across
// four shards while their rows all live on the designated one, breaking ASM-15
// and with it both the FR9 single-writer argument and the deadlock argument.
func TestINV_SCOPE_VctUnitsAreGeneral(t *testing.T) {
	reg, _ := vctRegistry(t)
	e := reg.Lookup(6511)
	spec := &e.Contrib[destMarketStatus]

	f := newVctFixture(t, 6511, 1)
	binary.BigEndian.PutUint32(f.payload[absVctToken:], 999999)
	f.setMessage(4, "OPEN")

	out := make([]model.UpdateUnit, 1)
	if _, err := e.Dec.Decode(model.MsgHeader{Code: 6511}, f.payload, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	u := &out[0]

	if !u.General() {
		t.Error("FlagGeneral is not set: the unit would be routed by token")
	}
	if u.Token != 0 {
		t.Errorf("UpdateUnit.Token = %d, want 0 -- the wire token is DATA, not a routing key",
			u.Token)
	}
	// ...and it IS stored, because OQ-L2 needs the evidence.
	if got := be32(blobAt(u, spec, m2Token)); got != 999999 {
		t.Errorf("the wire token was not stored as data: %d", got)
	}
}

// TestVctMessageLengthRule is the three rules that must hold together (Vol 1
// section 9.5): the valid length bounds what is read, 128 bounds what is
// stored, and the FULL declared width is rewritten every time.
func TestVctMessageLengthRule(t *testing.T) {
	for _, tc := range []struct {
		name       string
		declared   int
		text       string
		wantStored int
		wantTrunc  int
	}{
		{"zero length stores nothing", 0, "", 0, 0},
		{"a short message", 50, "fifty bytes of status text, give or take exactly", 50, 0},
		{"exactly the cap", 128, "", 128, 0},
		{"over the cap is truncated to 128", 200, "", 128, 1},
		{"the full field is truncated to 128", 239, "", 128, 1},
		{"beyond the field itself is nonsense", 300, "", 128, 1},
		{"negative is clamped to zero", -1, "", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, cnt := vctRegistry(t)
			e := reg.Lookup(6521)
			spec := &e.Contrib[destMarketStatus]

			f := newVctFixture(t, 6521, 1)
			f.setMessage(tc.declared, tc.text)

			out := make([]model.UpdateUnit, 1)
			if _, err := e.Dec.Decode(model.MsgHeader{Code: 6521}, f.payload, out); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			u := &out[0]

			// The CLAMPED length goes into the blob, never the declared one: a
			// consumer reading 200 from a 128-byte column would read 72 bytes
			// of the next column.
			if got := be16(blobAt(u, spec, m2MessageLen)); int(got) != tc.wantStored {
				t.Errorf("stored length = %d, want the clamped %d (declared was %d)",
					got, tc.wantStored, tc.declared)
			}
			if got := cnt.truncated[[2]int{int(destMarketStatus), int(m2MessageText)}]; got != tc.wantTrunc {
				t.Errorf("message_truncated = %d, want %d", got, tc.wantTrunc)
			}

			// R5: every byte past the valid length is ZERO, not the 0x5A the
			// wire carried. Without it the digest becomes a function of
			// history rather than of state and suppression stops working --
			// silently.
			//
			// TWO mechanisms enforce this and the test cannot tell them
			// apart: the decoder's explicit putZero, and stamp's assignment of
			// a fresh zero UpdateUnit. Removing the putZero does NOT fail this
			// assertion, which a break check established rather than assumed.
			// The PROPERTY is what is asserted here; unit.go records why the
			// redundant call is kept.
			text := blobAt(u, spec, m2MessageText)
			for i := tc.wantStored; i < len(text); i++ {
				if text[i] != 0 {
					t.Fatalf("byte %d past the valid length is %#02x, want zero: the bytes "+
						"beyond BroadcastMessageLength are not meaningful and must never "+
						"be stored", i, text[i])
				}
			}
		})
	}
}

// TestVctRewritesTheWholeFieldEveryTime is R5 from the other direction: the
// fan-out buffer is REUSED, so a long message followed by a short one must not
// leave the long one's tail behind.
func TestVctRewritesTheWholeFieldEveryTime(t *testing.T) {
	reg, _ := vctRegistry(t)
	e := reg.Lookup(6531)
	spec := &e.Contrib[destMarketStatus]
	out := make([]model.UpdateUnit, 1)

	long := newVctFixture(t, 6531, 1)
	long.setMessage(120, "")
	if _, err := e.Dec.Decode(model.MsgHeader{Code: 6531}, long.payload, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := blobAt(&out[0], spec, m2MessageText)[119]; got != 0x5A {
		t.Fatalf("setup: byte 119 is %#02x, want the planted 0x5a", got)
	}

	short := newVctFixture(t, 6531, 1)
	short.setMessage(3, "PRE")
	if _, err := e.Dec.Decode(model.MsgHeader{Code: 6531}, short.payload, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	text := blobAt(&out[0], spec, m2MessageText)
	if !bytes.Equal(text[:3], []byte("PRE")) {
		t.Errorf("message_text = %q, want PRE", text[:3])
	}
	for i := 3; i < 128; i++ {
		if text[i] != 0 {
			t.Fatalf("byte %d is %#02x after a shorter message: the previous message's tail "+
				"survived, which makes the digest a function of history", i, text[i])
		}
	}
}

// TestAllFiveVctCodesDecodeIdentically: one structure, five codes, five
// contributor slots. They share one row per MarketType and differ only in bit 0.
func TestAllFiveVctCodesDecodeIdentically(t *testing.T) {
	reg, _ := vctRegistry(t)

	slots := map[uint8]model.TransCode{}
	for _, code := range vctCodes {
		e := reg.Lookup(code)
		if e == nil {
			t.Fatalf("%d is not registered; the family is %v", code, vctCodes)
		}
		spec := &e.Contrib[destMarketStatus]

		f := newVctFixture(t, uint16(code), 3)
		f.setMessage(5, "HELLO")
		out := make([]model.UpdateUnit, 1)
		if _, err := e.Dec.Decode(model.MsgHeader{Code: code, LogTime: 5}, f.payload, out); err != nil {
			t.Fatalf("%d: %v", code, err)
		}
		u := &out[0]

		if got := be16(blobAt(u, spec, m2Transcode)); model.TransCode(got) != code {
			t.Errorf("%d stored transcode %d", code, got)
		}
		if got := u.Contribs[destMarketStatus].Row; got != model.SingleKey(3) {
			t.Errorf("%d row = %d, want SingleKey(3): the five share one row per MarketType",
				code, got)
		}
		if !u.General() {
			t.Errorf("%d is not a general unit", code)
		}

		// Five DISTINCT slots. The slot indexes stored ordering state, and a
		// same-day re-open sends 6531 -> 6571 -> 6511 inside one second, so a
		// shared slot would discard the last two and the market would never
		// reach Open.
		slot := u.Contribs[destMarketStatus].Slot
		if prev, dup := slots[slot]; dup {
			t.Errorf("%d and %d share contributor slot %d", prev, code, slot)
		}
		slots[slot] = code
	}
	if len(slots) != len(vctCodes) {
		t.Errorf("%d distinct slots for %d codes", len(slots), len(vctCodes))
	}
}

func TestVctAllocatesNothing(t *testing.T) {
	reg, _ := vctRegistry(t)
	e := reg.Lookup(6571)
	f := newVctFixture(t, 6571, 1)
	f.setMessage(100, "status")
	out := make([]model.UpdateUnit, 1)
	h := model.MsgHeader{Code: 6571, LogTime: 1}

	if n := testing.AllocsPerRun(1000, func() {
		if _, err := e.Dec.Decode(h, f.payload, out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}); n != 0 {
		t.Fatalf("Decode allocates %v times per message, want 0", n)
	}
}

func TestVctShortPayloadIsRejected(t *testing.T) {
	reg, _ := vctRegistry(t)
	e := reg.Lookup(6511)
	f := newVctFixture(t, 6511, 1)
	out := make([]model.UpdateUnit, 1)

	if _, err := e.Dec.Decode(model.MsgHeader{Code: 6511}, f.payload[:327], out); err == nil {
		t.Error("a 327-byte payload was accepted for a 328-byte message")
	}
}
