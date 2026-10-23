package mirror

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// sender merges units of one code into destination d of s, each with a fresh,
// rising Seq and RecvMono so the ordering guard never supersedes one.
type sender struct {
	t     testing.TB
	s     *Shard
	d     model.DestID
	code  model.TransCode
	flags model.UnitFlags
	seq   int32
}

func newSender(t testing.TB, s *Shard, d model.DestID, code model.TransCode, flags model.UnitFlags) *sender {
	return &sender{t: t, s: s, d: d, code: code, flags: flags}
}

func (x *sender) send(row model.RowKey, fs ...fv) {
	x.t.Helper()
	x.seq++
	u := model.UpdateUnit{Code: x.code, Flags: x.flags, Seq: x.seq, LogTime: 100, RecvMono: int64(x.seq)}
	put(x.t, x.s, &u, x.d, row, slotOf(x.t, x.s, x.d, x.code), fs...)
	x.s.merge(u)
}

func be64f(f float64) []byte { return binary.BigEndian.AppendUint64(nil, math.Float64bits(f)) }

// refusedCount is prevalidate_rejected{d,r}.
func refusedCount(s *Shard, d model.DestID, r obs.PrevalidateReason) uint64 {
	return s.inst.PrevalidateRejected[d][r].Load()
}

// refusedTC is prevalidate_rejected_transcode{code}.
func refusedTC(s *Shard, code model.TransCode) uint64 {
	return s.inst.Transcode.PrevalidateRejectedTC[s.guards.index(code)].Load()
}

// TestPrevalidateRefusesEachReason: one row per reason, each a value merged
// into a column carrying the fact that refuses it -- the image bytes and the
// ColMask bit stay untouched and prevalidate_rejected{d,reason} and
// prevalidate_rejected_transcode{code} each rise by one. Then the SAME value
// into the same column with the fact removed lands, and nothing is counted:
// the refusal is the fact's, not the value's.
//
// time and nonfinite have no column fact -- they are the kind's own range -- so
// "without the fact" is the same bytes declared as an unchecked LONG.
func TestPrevalidateRefusesEachReason(t *testing.T) {
	cases := []struct {
		name   string
		d      model.DestID
		code   model.TransCode
		bit    uint8
		val    []byte
		reason obs.PrevalidateReason
		fact   func(*destinations.Column) // installs a fact the default mapping lacks; nil if it has it
		strip  func(*destinations.Column)
	}{
		// The DDL's NUMBER(20,4) cannot refuse an int32 price, so the precision
		// rows tighten the column to the wire's own (7,2) -- a column sized for
		// the exchange's price format rather than Vol 3 section 10's DDL.
		{"precision: last_traded_price tightened to NUMBER(7,2), raw 10^7", tokenQuote, 7208, 0, be32(10_000_000),
			obs.PrevalidatePrecision, func(c *destinations.Column) { c.SQLPrecision = 7 },
			func(c *destinations.Column) { c.SQLPrecision = 0 }},
		{"precision: a negative raw past -10^7", tokenQuote, 7208, 0, be32(uint32(0xFF676980)), // -10,000,000
			obs.PrevalidatePrecision, func(c *destinations.Column) { c.SQLPrecision = 7 },
			func(c *destinations.Column) { c.SQLPrecision = 0 }},
		{"precision: total_buy_quantity NUMBER(38,10), 1e38", tokenQuote, 7208, 11, be64f(1e38),
			obs.PrevalidatePrecision, nil, func(c *destinations.Column) { c.SQLPrecision = 0 }},
		{"length: symbol tightened to VARCHAR2(8), 10 bytes", securityMaster, 7305, 0, []byte("ABCDEFGHIJ"),
			obs.PrevalidateLength, func(c *destinations.Column) { c.SQLMaxBytes = 8 },
			func(c *destinations.Column) { c.SQLMaxBytes = 0 }},
		{"domain: delete_flag in (Y,N), 'X'", securityMaster, 7305, 7, []byte("X"),
			obs.PrevalidateDomain, nil, func(c *destinations.Column) { c.Domain = nil }},
		{"time: expiry_date, -1 (pre-1980)", securityMaster, 7305, 2, be32(math.MaxUint32),
			obs.PrevalidateTime, nil, func(c *destinations.Column) { c.Kind = destinations.KindLong }},
		{"nonfinite: total_sell_quantity, NaN", tokenQuote, 7208, 12, be64f(math.NaN()),
			obs.PrevalidateNonfinite, nil, func(c *destinations.Column) {
				c.Kind, c.SQLPrecision = destinations.KindLong, 0 // a LONG reads its first 4 bytes
			}},
		{"nonfinite: total_sell_quantity, +Inf", tokenQuote, 7208, 12, be64f(math.Inf(1)),
			obs.PrevalidateNonfinite, nil, func(c *destinations.Column) {
				c.Kind, c.SQLPrecision = destinations.KindLong, 0
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newMergeShard(t, GuardAuto)
			col := s.dst.ByID(tc.d).Column(tc.bit)
			if tc.fact != nil {
				tc.fact(col)
			}
			x := newSender(t, s, tc.d, tc.code, 0)
			x.send(1, fv{tc.bit, tc.val})

			e := rowEntry(s, tc.d, 1)
			if e == nil {
				t.Fatal("step 1 did not create the row")
			}
			region := e.Image[col.Offset:col.End()]
			if !bytes.Equal(region, make([]byte, col.Width)) || e.ColMask.Has(tc.bit) || e.Dirty {
				t.Fatalf("a refused value reached the image, ColMask or the ring: region=%x mask=%#x dirty=%v",
					region, uint64(e.ColMask), e.Dirty)
			}
			for r := range obs.PrevalidateReason(numPrevalidateReasons) {
				want := uint64(0)
				if r == tc.reason {
					want = 1
				}
				if got := refusedCount(s, tc.d, r); got != want {
					t.Fatalf("prevalidate_rejected{%d,%d} = %d, want %d", tc.d, r, got, want)
				}
			}
			if got := refusedTC(s, tc.code); got != 1 {
				t.Fatalf("prevalidate_rejected_transcode{%d} = %d, want 1", tc.code, got)
			}

			// Without the fact, the same value merges.
			tc.strip(col)
			x.send(1, fv{tc.bit, tc.val})
			want := append(append([]byte{}, tc.val...), make([]byte, int(col.Width)-len(tc.val))...)
			if !bytes.Equal(e.Image[col.Offset:col.End()], want) || !e.ColMask.Has(tc.bit) || !e.Dirty {
				t.Fatal("non-vacuity: the same value into a column without the fact did not merge")
			}
			if got := refusedCount(s, tc.d, tc.reason); got != 1 {
				t.Fatalf("a column without the fact counted a refusal: %d", got)
			}
		})
	}
}

// TestPrevalidateBadAndGoodColumn: a contribution with one refused and one
// valid column -- the valid one lands, the refused one's image bytes and
// ColMask bit stay as they were, and the row is dirtied and queued for the
// valid one. The refused field's packed bytes do not shift the good one's.
func TestPrevalidateBadAndGoodColumn(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	x := newSender(t, s, securityMaster, 7305, 0)
	dst := s.dst.ByID(securityMaster)
	strike, flag := dst.Column(3), dst.Column(7)
	strike.SQLPrecision = 7 // tightened to the wire's (7,2): NUMBER(20,4) refuses no int32

	x.send(4, fv{3, be32(1500)}, fv{7, []byte("Y")})
	e := rowEntry(s, securityMaster, 4)
	e.ColMask, e.Dirty = 0, false
	s.Accum[securityMaster].Reset()

	// The bad field is bit 3, BELOW the good one: a copy that skipped the bad
	// field without advancing the cursor would copy its bytes into bit 7.
	x.send(4, fv{3, be32(99_999_999)}, fv{7, []byte("N")})

	if got := binary.BigEndian.Uint32(e.Image[strike.Offset:]); got != 1500 {
		t.Fatalf("the refused strike_price changed the image: %d, want 1500", got)
	}
	if e.Image[flag.Offset] != 'N' {
		t.Fatalf("the valid delete_flag did not land: %q", e.Image[flag.Offset])
	}
	if e.ColMask != model.Bit(7) {
		t.Fatalf("ColMask = %#x, want only bit 7", uint64(e.ColMask))
	}
	if !e.Dirty || !slices.Equal(queued(s, securityMaster), []int32{s.Index[securityMaster][4]}) {
		t.Fatal("the row was not dirtied and queued for its valid column")
	}
	if refusedCount(s, securityMaster, obs.PrevalidatePrecision) != 1 || refusedTC(s, 7305) != 1 {
		t.Fatal("the refusal was not counted exactly once")
	}
	if s.inst.UnitsMerged.Load() != 2 {
		t.Fatalf("units_merged = %d, want 2: a partly refused contribution still merged", s.inst.UnitsMerged.Load())
	}
}

// TestPrevalidateOnlyColumnRefusedDirtiesNothing: a non-rate-limited unit whose
// only column is refused dirties nothing and does not Trigger. The same unit
// with a valid value does both.
func TestPrevalidateOnlyColumnRefusedDirtiesNothing(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	x := newSender(t, s, securityMaster, 7305, model.FlagNonRateLimited)

	x.send(2, fv{7, []byte("X")})
	e := rowEntry(s, securityMaster, 2)
	if e.Dirty || e.ColMask != 0 || s.Accum[securityMaster].Len() != 0 {
		t.Fatal("a contribution whose only column was refused dirtied the row")
	}
	if s.bus.Pending(0) || s.inst.FlushTriggered.Load() != 0 {
		t.Fatal("a unit whose only column was refused raised a flush")
	}
	if s.inst.UnitsMerged.Load() != 0 || refusedCount(s, securityMaster, obs.PrevalidateDomain) != 1 {
		t.Fatal("the refused contribution was counted as merged, or its refusal was not counted")
	}

	x.send(2, fv{7, []byte("Y")})
	if !e.Dirty || !s.bus.Pending(0) || s.inst.FlushTriggered.Load() != 1 {
		t.Fatal("non-vacuity: the same unit with a valid value did not dirty the row and Trigger")
	}
}

// TestPrevalidateExplicitLengthIsOneValueWithItsLength: message_text's valid
// length is message_len, ANOTHER image field -- the incoming one when the
// contribution carries it, else the image's -- and the writer binds min(len,
// width) bytes right-trimmed of blanks. The two are one logical value (Vol 0
// section 4.5): either half refused withdraws both, so neither lands over a
// partner it was not checked with. With message_text tightened to
// VARCHAR2(20), each step from the state the previous one left:
func TestPrevalidateExplicitLengthIsOneValueWithItsLength(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	dst := s.dst.ByID(marketStatus)
	lenCol, text := dst.Column(1), dst.Column(2)
	text.SQLMaxBytes = 20
	x := newSender(t, s, marketStatus, 6511, model.FlagGeneral|model.FlagNonRateLimited)
	rep := func(c byte, n int) []byte { return bytes.Repeat([]byte{c}, n) }
	a30, b30 := rep('A', 30), rep('B', 30)
	a20sp := append(rep('A', 20), rep(' ', 10)...)
	hello := []byte("hello")

	steps := []struct {
		name     string
		fs       []fv
		wantLen  uint16 // the image's message_len after the step
		wantText []byte // the image's message_text, zero-filled past it
		wantMask model.ColMask
		lenRefs  uint64 // prevalidate_rejected{length}, cumulative
		precRefs uint64 // prevalidate_rejected{precision}, cumulative
	}{
		{"both halves, 30 > 20: neither lands",
			[]fv{{1, be16(30)}, {2, a30}}, 0, nil, 0, 1, 0},
		{"both halves, 5: both land",
			[]fv{{1, be16(5)}, {2, hello}}, 5, hello, model.Bit(1) | model.Bit(2), 1, 0},
		{"both halves, 30 > 20 over a landed value: the old pair stays whole",
			[]fv{{1, be16(30)}, {2, a30}}, 5, hello, 0, 2, 0},
		{"both halves, length 20: the bound prefix fits",
			[]fv{{1, be16(20)}, {2, a30}}, 20, a30, model.Bit(1) | model.Bit(2), 2, 0},
		{"text only, judged by the image's length 20: lands",
			[]fv{{2, b30}}, 20, b30, model.Bit(2), 2, 0},
		{"length only, 30 over the image's 30-byte text: neither lands",
			[]fv{{1, be16(30)}}, 20, b30, 0, 3, 0},
		{"both halves, length 30 but 10 trailing blanks the writer trims: lands",
			[]fv{{1, be16(30)}, {2, a20sp}}, 30, a20sp, model.Bit(1) | model.Bit(2), 3, 0},
		{"text only, judged by the image's length 30: refused",
			[]fv{{2, a30}}, 30, a20sp, 0, 4, 0},
		{"length only, 20 over the image's text: lands",
			[]fv{{1, be16(20)}}, 20, a20sp, model.Bit(1), 4, 0},
		{"length 0xFFFF reads unsigned, clamping to the full width: refused",
			[]fv{{1, be16(0xFFFF)}, {2, a30}}, 20, a20sp, 0, 5, 0},
	}
	run := func(i int, name string, fs []fv, wantLen uint16, wantText []byte, wantMask model.ColMask, lenRefs, precRefs uint64) {
		t.Helper()
		if e := rowEntry(s, marketStatus, 1); e != nil {
			e.ColMask = 0 // each step's mask is its own; nothing pumps here
		}
		x.send(1, fs...)
		e := rowEntry(s, marketStatus, 1)
		region := append(append([]byte{}, wantText...), make([]byte, int(text.Width)-len(wantText))...)
		if got := binary.BigEndian.Uint16(e.Image[lenCol.Offset:]); got != wantLen {
			t.Fatalf("step %d (%s): message_len = %d, want %d", i+1, name, got, wantLen)
		}
		if !bytes.Equal(e.Image[text.Offset:text.End()], region) {
			t.Fatalf("step %d (%s): message_text = %q, want %q", i+1, name,
				bytes.TrimRight(e.Image[text.Offset:text.End()], "\x00"), wantText)
		}
		if e.ColMask != wantMask {
			t.Fatalf("step %d (%s): ColMask = %#x, want %#x", i+1, name, uint64(e.ColMask), uint64(wantMask))
		}
		if l, p := refusedCount(s, marketStatus, obs.PrevalidateLength),
			refusedCount(s, marketStatus, obs.PrevalidatePrecision); l != lenRefs || p != precRefs {
			t.Fatalf("step %d (%s): refused{length,precision} = %d,%d, want %d,%d", i+1, name, l, p, lenRefs, precRefs)
		}
	}
	for i, st := range steps {
		run(i, st.name, st.fs, st.wantLen, st.wantText, st.wantMask, st.lenRefs, st.precRefs)
	}

	// The length refused for its own fact (message_len tightened to
	// NUMBER(1)) withdraws a text that would have fitted under it: counted
	// once, as precision, and the image keeps the old pair.
	lenCol.SQLPrecision = 1
	run(len(steps), "length refused for precision withdraws its text",
		[]fv{{1, be16(10)}, {2, b30}}, 20, a20sp, 0, 5, 1)
}

// TestPrevalidateDefaultMappingAcceptsEveryInt32Price: Vol 3 section 10's DDL
// declares every scaled price NUMBER(20,4), so no wire price -- MaxInt32 is
// Rs 2,14,74,836.47 -- can be refused for precision. A mapping carrying the
// exchange's (7,2) price FORMAT as the column's precision would refuse every
// price from Rs 1,00,000.00 up, silently, before Oracle could say otherwise.
func TestPrevalidateDefaultMappingAcceptsEveryInt32Price(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	checked := 0
	for _, d := range []model.DestID{tokenQuote, securityMaster, marketStatus} {
		dst := s.dst.ByID(d)
		for b := range dst.Columns {
			col := &dst.Columns[b]
			if col.Kind != destinations.KindLong || !col.Scaled || col.SQLPrecision == 0 {
				continue
			}
			checked++
			for _, v := range []int32{10_000_000, math.MaxInt32, math.MinInt32} {
				if _, bad := checkField(col, be32(uint32(v))); bad {
					t.Errorf("destination %d bit %d (%s, NUMBER(%d)) refused raw %d",
						d, b, col.SQL[0], col.SQLPrecision, v)
				}
			}
		}
	}
	if checked < 9 { // six quote prices and three master prices
		t.Fatalf("non-vacuity: only %d scaled price columns carry a precision fact", checked)
	}
}

// TestCheckFieldBoundaries pins the edges of each check, through checkField
// directly, on columns built here rather than read from the mapping.
func TestCheckFieldBoundaries(t *testing.T) {
	long7 := &destinations.Column{Kind: destinations.KindLong, Width: 4, SQLPrecision: 7}
	short3 := &destinations.Column{Kind: destinations.KindShort, Width: 2, SQLPrecision: 3}
	dbl := &destinations.Column{Kind: destinations.KindDouble, Width: 8, SQLPrecision: 4, Scaled: true, Scale: 2}
	dbl0 := &destinations.Column{Kind: destinations.KindDouble, Width: 8, SQLPrecision: 4}
	flag := &destinations.Column{Kind: destinations.KindChar1, Width: 1, Domain: []string{"+", "-"}}
	tm := &destinations.Column{Kind: destinations.KindTime1980, Width: 4}
	txt := &destinations.Column{Kind: destinations.KindText, Width: 6, LenBit: -1, SQLMaxBytes: 4}
	grp := &destinations.Column{Kind: destinations.KindGroup, Width: 4, SQLPrecision: 1}
	neg := func(v int32) []byte { return be32(uint32(v)) }

	cases := []struct {
		name string
		col  *destinations.Column
		v    []byte
		bad  bool
	}{
		{"long 9,999,999 fits NUMBER(7)", long7, be32(9_999_999), false},
		{"long 10,000,000 does not", long7, be32(10_000_000), true},
		{"long -9,999,999 fits", long7, neg(-9_999_999), false},
		{"long MinInt32 does not", long7, neg(math.MinInt32), true},
		{"short 999 fits NUMBER(3)", short3, be16(999), false},
		{"short -1000 does not", short3, be16(uint16(0xFC18)), true},
		// A scaled double is unscaled in the image: raw 9999.4 binds as 99.994,
		// rounds to 99.99 and fits NUMBER(4,2); raw 9999.5 binds as 99.995 and
		// rounds to 100.00, which does not.
		{"scaled double raw 9999.4 fits NUMBER(4,2)", dbl, be64f(9999.4), false},
		{"scaled double raw -9999.4 fits", dbl, be64f(-9999.4), false},
		{"scaled double raw 9999.5 does not", dbl, be64f(9999.5), true},
		{"scaled double raw 100 binds 1.00, fits", dbl, be64f(100), false},
		{"unscaled double 9999.4 fits NUMBER(4)", dbl0, be64f(9999.4), false},
		{"unscaled double 9999.5 rounds to 10000, does not", dbl0, be64f(9999.5), true},
		{"double -Inf", dbl, be64f(math.Inf(-1)), true},
		{"char1 '+'", flag, []byte("+"), false},
		{"char1 0x00 is absent, binds NULL", flag, []byte{0}, false},
		{"char1 'x'", flag, []byte("x"), true},
		{"time 0 is never-set, binds NULL", tm, be32(0), false},
		{"time MaxInt32", tm, be32(math.MaxInt32), false},
		{"time -1", tm, neg(-1), true},
		{"text 'AB    ' trims to 2", txt, []byte("AB    "), false},
		{"text 'ABCD  ' trims to 4", txt, []byte("ABCD  "), false},
		{"text 'ABCDE ' trims to 5", txt, []byte("ABCDE "), true},
		{"text all 0x00 is never-set, binds NULL", txt, make([]byte, 6), false},
		{"text 'AB' + four 0x00 binds all 6 bytes", txt, []byte("AB\x00\x00\x00\x00"), true},
		{"group is opaque", grp, be32(math.MaxInt32), false},
	}
	for _, tc := range cases {
		if _, bad := checkField(tc.col, tc.v); bad != tc.bad {
			t.Errorf("%s: refused = %v, want %v", tc.name, bad, tc.bad)
		}
	}
}

// TestPredictable: the three ORA codes pre-validation exists to predict, and
// not the ones it cannot (Vol 0 section 5A.2).
func TestPredictable(t *testing.T) {
	for code, want := range map[uint16]bool{
		1438: true, 12899: true, 2290: true, // precision, length, CHECK
		1: false, 60: false, 1400: false, 2291: false, 0: false, // unique, deadlock, NOT NULL, FK, none
	} {
		if got := predictable(code); got != want {
			t.Errorf("predictable(ORA-%05d) = %v, want %v", code, got, want)
		}
	}
}

// TestMergeWithPrevalidationAllocatesNothing: AllocsPerRun = 0 for merge with
// pre-validation on, along a contribution that is partly refused for every
// reason a default-mapping column can produce -- and, non-vacuously, the
// refusals and the merges are both counted on every run.
func TestMergeWithPrevalidationAllocatesNothing(t *testing.T) {
	s, _ := newMergeShard(t, GuardAuto)
	var u model.UpdateUnit
	put(t, s, &u, tokenQuote, 7, slotOf(t, s, tokenQuote, 7208),
		fv{0, be32(10_000_000)},     // valid: Rs 1,00,000.00 fits NUMBER(20,4)
		fv{1, be32(500)},            // valid
		fv{7, []byte("x")},          // domain
		fv{9, be32(math.MaxUint32)}, // time
		fv{11, be64f(1e38)},         // precision: NUMBER(38,10)
		fv{12, be64f(math.NaN())},   // nonfinite
	)
	u.Code, u.LogTime = 7208, 100
	next := func() {
		u.Seq++
		u.RecvMono++
		s.merge(u)
	}
	next()
	refusedBefore, mergedBefore := refusedTC(s, 7208), s.inst.UnitsMerged.Load()
	if got := testing.AllocsPerRun(1000, next); got != 0 {
		t.Fatalf("merge with pre-validation allocated %.1f times per call, want 0", got)
	}
	if got := refusedTC(s, 7208) - refusedBefore; got < 4*1000 {
		t.Fatalf("non-vacuity: prevalidate_rejected_transcode rose by %d, want >= 4000", got)
	}
	if got := s.inst.UnitsMerged.Load() - mergedBefore; got < 1000 {
		t.Fatalf("non-vacuity: units_merged rose by %d, want >= 1000", got)
	}
	if e := rowEntry(s, tokenQuote, 7); e.ColMask != model.Bit(0)|model.Bit(1) {
		t.Fatalf("ColMask = %#x, want only the two valid bits", uint64(e.ColMask))
	}
}
