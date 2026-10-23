package writer

import (
	"database/sql"
	"encoding/binary"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

var testUpdatedAt = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

func shippedDest(t *testing.T, name string) *destinations.Destination {
	t.Helper()
	set, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	return set.MustByName(name)
}

func newBinder(t *testing.T, d *destinations.Destination, mask model.ColMask, ceiling int) *Binder {
	t.Helper()
	b, err := NewBinder(d, mask, ceiling)
	if err != nil {
		t.Fatalf("NewBinder: %v", err)
	}
	return b
}

// img is a hand-placed row image. Every write names an ABSOLUTE offset taken
// from the spec tables, not from the parser's or the mapping's field order.
type img struct{ row model.BatchRow }

func (m *img) u32(off int, v uint32) { binary.BigEndian.PutUint32(m.row.Image[off:], v) }
func (m *img) u16(off int, v uint16) { binary.BigEndian.PutUint16(m.row.Image[off:], v) }
func (m *img) f64(off int, v float64) {
	binary.BigEndian.PutUint64(m.row.Image[off:], math.Float64bits(v))
}
func (m *img) raw(off int, s string) { copy(m.row.Image[off:], s) }

func ts(sec int64) sql.NullTime {
	return sql.NullTime{Time: time.Unix(sec+315532800, 0).UTC(), Valid: true}
}
func ns(s string) sql.NullString   { return sql.NullString{String: s, Valid: true} }
func nf(f float64) sql.NullFloat64 { return sql.NullFloat64{Float64: f, Valid: true} }

// want1 asserts that position p (0-based) of args holds exactly one element
// equal to v.
func want1(t *testing.T, args []any, p int, v any) {
	t.Helper()
	rv := reflect.ValueOf(args[p])
	if rv.Kind() != reflect.Slice || rv.Len() != 1 {
		t.Fatalf("position %d: %T len %d, want a 1-element slice", p+1, args[p], rv.Len())
	}
	if got := rv.Index(0).Interface(); got != v {
		t.Errorf("position %d (:%d) = %#v, want %#v", p, p+1, got, v)
	}
}

// TestBindQuoteAbsoluteOffsets pins destination 0 against absolute byte
// offsets written out by hand from Vol 0 section 4.3: price 0, volume 4,
// open 8, high 12, low 16, close 20, status 24, net-change 26, last qty 27,
// last time 31, average 35, total buy 39, total sell 47, book type 55, ladder
// 57..176, effective 177; 181 bytes.
//
// The ladder expectation is RECORD BY RECORD (bid_qty_1, bid_px_1, bid_ord_1,
// bid_qty_2, ...), which is mapping.json's order and BindPlan's, not the
// attribute-major order the LLD prints.
func TestBindQuoteAbsoluteOffsets(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	mask := d.FullMask()

	var m img
	m.row.Row, m.row.Mask = model.SingleKey(98765), mask
	m.u32(0, 2023)
	m.u32(4, 123456)
	m.u32(8, 2000)
	m.u32(12, 2100)
	m.u32(16, 1900)
	m.u32(20, 2050)
	m.u16(24, 0xFFFE) // -2
	m.raw(26, "+")
	m.u32(27, 77)
	m.u32(31, 1000)
	m.u32(35, 2011)
	m.f64(39, 1234.5)
	m.f64(47, 99.25)
	m.u16(55, 3)
	for k := 0; k < 10; k++ {
		o := 57 + 12*k
		m.u32(o, uint32(100*(k+1)))
		m.u32(o+4, uint32(10000+k))
		m.u16(o+8, uint16(k+1))
		m.u16(o+10, 0x7777) // BbBuySellFlag: must never be read
	}
	m.u32(177, 2000)

	b := newBinder(t, d, mask, 4)
	args, err := b.Bind([]model.BatchRow{m.row}, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 47 {
		t.Fatalf("%d binds, want 47", len(args))
	}
	exp := []any{
		int64(98765),
		int64(2023), int64(123456), int64(2000), int64(2100), int64(1900), int64(2050), // 1..6
		int64(-2), ns("+"), int64(77), ts(1000), int64(2011), nf(1234.5), nf(99.25), int64(3), // 7..14
	}
	for p, v := range exp {
		want1(t, args, p, v)
	}
	for k := 0; k < 10; k++ {
		p := 15 + 3*k
		want1(t, args, p, int64(100*(k+1)))
		want1(t, args, p+1, int64(10000+k)) // raw: the /100 is in the SQL
		want1(t, args, p+2, int64(k+1))
	}
	want1(t, args, 45, ts(2000))
	want1(t, args, 46, sql.NullTime{Time: testUpdatedAt, Valid: true})
}

// TestBindSecurityMasterAndStatusText covers a text image: destination 2
// (6511 market-status text) with message_text taking its length from
// message_len, and destination 1's blank-padded fixed text.
func TestBindSecurityMasterAndStatusText(t *testing.T) {
	d := shippedDest(t, "MD_MARKET_STATUS")
	// transcode 0, message_len 2, message_text 4 (128), bcast_destination 132,
	// token 134, effective_ts 138; 142 bytes.
	var m img
	m.row.Row, m.row.Mask = model.SingleKey(1), d.FullMask()
	m.u16(0, 6511)
	m.u16(2, 5)
	m.raw(4, "HELLO")
	m.raw(9, "GARBAGE") // past the valid length: must not be bound
	m.raw(132, "A ")
	m.u32(134, 55)
	m.u32(138, 5000)
	b := newBinder(t, d, d.FullMask(), 2)
	args, err := b.Bind([]model.BatchRow{m.row}, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	for p, v := range []any{int64(1), int64(6511), int64(5), ns("HELLO"), ns("A"), int64(55), ts(5000)} {
		want1(t, args, p, v)
	}

	// destination 1: symbol 0 (10), series 10 (2), expiry 12, strike 16, lot 20,
	// tick 24, base 28, delete_flag 32, effective_ts 33.
	sm := shippedDest(t, "MD_SECURITY_MASTER")
	var s img
	s.row.Row, s.row.Mask = model.SingleKey(7), sm.FullMask()
	s.raw(0, "RELIANCE  ")
	s.raw(10, "EQ")
	s.u32(12, 4000)
	s.u32(16, 0xFFFFFFFF) // -1
	s.raw(32, "N")
	sb := newBinder(t, sm, sm.FullMask(), 2)
	sargs, err := sb.Bind([]model.BatchRow{s.row}, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	for p, v := range []any{int64(7), ns("RELIANCE"), ns("EQ"), ts(4000), int64(-1), int64(0), int64(0), int64(0), ns("N"), ts(0)} {
		if p == 9 {
			// effective_ts is 0: NULL, not 1980-01-01.
			want1(t, sargs, p, sql.NullTime{})
			continue
		}
		want1(t, sargs, p, v)
	}
}

// TestBindConversionEdges walks every conversion's edge on destination 0 and 2.
func TestBindConversionEdges(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	var nonfinite, badTime [2]*obs.Counter
	inst := &Instruments{}
	for i := range nonfinite {
		nonfinite[i], badTime[i] = new(obs.Counter), new(obs.Counter)
	}
	inst.BindNonfinite[0][11], inst.BindNonfinite[0][12] = nonfinite[0], nonfinite[1]
	inst.BindBadTime[0][9], inst.BindBadTime[0][15] = badTime[0], badTime[1]

	mask := model.Bit(7).Union(model.Bit(9)).Union(model.Bit(11)).Union(model.Bit(12)).Union(model.Bit(15))
	rows := make([]model.BatchRow, 5)
	set := func(i int, flag byte, last uint32, buy, sell float64, eff uint32) {
		m := img{model.BatchRow{Row: model.SingleKey(uint32(i)), Mask: mask}}
		m.row.Image[26] = flag
		m.u32(31, last)
		m.f64(39, buy)
		m.f64(47, sell)
		m.u32(177, eff)
		rows[i] = m.row
	}
	set(0, 0x00, 0, math.NaN(), math.Inf(1), 0)
	set(1, '-', 0xFFFFFFFF, math.Inf(-1), -0.0, 0x80000000)
	set(2, 'Y', 1, 1e300, math.SmallestNonzeroFloat64, 0x7FFFFFFF)
	set(3, 0xC3, 2, 0, 0, 1)
	set(4, ' ', 3, 1, 2, 2)

	b := newBinder(t, d, mask, 8)
	b.Inst = inst
	args, err := b.Bind(rows, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	// positions: 0 key, 1 net_change(7), 2 last_trade_time(9), 3 buy(11), 4 sell(12), 5 effective(15), 6 updated_at
	if len(args) != 7 {
		t.Fatalf("%d binds, want 7", len(args))
	}
	flags := args[1].([]sql.NullString)
	if flags[0].Valid || flags[1] != ns("-") || flags[2] != ns("Y") || flags[3] != ns("\xc3") || flags[4] != ns(" ") {
		t.Errorf("char1 column = %#v", flags)
	}
	times := args[2].([]sql.NullTime)
	if times[0].Valid || times[1].Valid || times[2] != ts(1) || times[3] != ts(2) || times[4] != ts(3) {
		t.Errorf("time column = %#v", times)
	}
	buy := args[3].([]sql.NullFloat64)
	if buy[0].Valid || buy[1].Valid || buy[2] != nf(1e300) || buy[3] != nf(0) || buy[4] != nf(1) {
		t.Errorf("double column = %#v", buy)
	}
	sell := args[4].([]sql.NullFloat64)
	if sell[0].Valid || sell[1] != nf(math.Copysign(0, -1)) || sell[2] != nf(math.SmallestNonzeroFloat64) || sell[4] != nf(2) {
		t.Errorf("sell column = %#v", sell)
	}
	eff := args[5].([]sql.NullTime)
	if eff[0].Valid || eff[1].Valid || eff[2] != ts(math.MaxInt32) || eff[3] != ts(1) {
		t.Errorf("effective column = %#v", eff)
	}
	// Row 0: NaN + Inf = 1 each; row 1: -Inf = 1 (buy). Sell -0.0 is finite.
	if got := nonfinite[0].Load(); got != 2 {
		t.Errorf("bind_nonfinite col 11 = %d, want 2", got)
	}
	if got := nonfinite[1].Load(); got != 1 {
		t.Errorf("bind_nonfinite col 12 = %d, want 1", got)
	}
	if got := badTime[0].Load(); got != 2 { // rows 0 and 1
		t.Errorf("bind_bad_time col 9 = %d, want 2", got)
	}
	if got := badTime[1].Load(); got != 2 { // rows 0 and 1 (0x80000000 is negative)
		t.Errorf("bind_bad_time col 15 = %d, want 2", got)
	}

	// The configurable epoch offset is honoured.
	b.Time1980UnixOffsetSec = 0
	args, err = b.Bind(rows[2:3], testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if got := args[2].([]sql.NullTime)[0]; got != (sql.NullTime{Time: time.Unix(1, 0).UTC(), Valid: true}) {
		t.Errorf("offset 0: %v", got)
	}
}

// TestBindCountsLandInTheirOwnSlot: a NaN on destination 0 and a bad time on
// MD_MARKET_STATUS count in exactly [dest][bit], and BindRow after Bind adds
// nothing.
func TestBindCountsLandInTheirOwnSlot(t *testing.T) {
	inst := &Instruments{}
	all := map[string]*obs.Counter{}
	for d := 0; d < 3; d++ {
		for c := 0; c < model.MaxColumns; c++ {
			inst.BindNonfinite[d][c], inst.BindBadTime[d][c] = new(obs.Counter), new(obs.Counter)
			all["nf"+strconv.Itoa(d)+"/"+strconv.Itoa(c)] = inst.BindNonfinite[d][c]
			all["bt"+strconv.Itoa(d)+"/"+strconv.Itoa(c)] = inst.BindBadTime[d][c]
		}
	}
	others := func(except ...string) {
		t.Helper()
		skip := map[string]bool{}
		for _, e := range except {
			skip[e] = true
		}
		for k, c := range all {
			if !skip[k] && c.Load() != 0 {
				t.Errorf("counter %s = %d, want 0", k, c.Load())
			}
		}
	}

	d0 := shippedDest(t, "MD_TOKEN_QUOTE")
	mask0 := model.Bit(11)
	var q img
	q.row.Row, q.row.Mask = model.SingleKey(1), mask0
	q.f64(39, math.NaN())
	b0 := newBinder(t, d0, mask0, 2)
	b0.Inst = inst
	if _, err := b0.Bind([]model.BatchRow{q.row}, testUpdatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := b0.BindRow(&q.row, testUpdatedAt); err != nil {
		t.Fatal(err)
	}
	if got := all["nf0/11"].Load(); got != 1 {
		t.Errorf("Bind then BindRow counted the NaN %d times, want 1", got)
	}
	others("nf0/11")

	d2 := shippedDest(t, "MD_MARKET_STATUS")
	bit := -1
	for _, c := range d2.Columns {
		if c.Name() == "effective_ts" {
			bit = int(c.Bit)
		}
	}
	if bit < 0 {
		t.Fatal("no effective_ts column")
	}
	m2 := model.Bit(uint8(bit))
	b2 := newBinder(t, d2, m2, 2)
	b2.Inst = inst
	if _, err := b2.Bind([]model.BatchRow{{Row: model.SingleKey(1), Mask: m2}}, testUpdatedAt); err != nil {
		t.Fatal(err)
	}
	key := "bt" + strconv.Itoa(int(d2.ID)) + "/" + strconv.Itoa(bit)
	if got := all[key].Load(); got != 1 {
		t.Errorf("%s = %d, want 1", key, got)
	}
	others("nf0/11", key)
}

// TestBindTextEdges: explicit lengths are unsigned and clamp to the field
// width, a zero length is NULL, blanks and NULs trim on the right only, all-blank
// is NULL, and raw bytes (including invalid UTF-8) pass through unchanged.
func TestBindTextEdges(t *testing.T) {
	d := shippedDest(t, "MD_MARKET_STATUS")
	mask := model.Bit(1).Union(model.Bit(2)).Union(model.Bit(3))
	mk := func(length uint16, text, dest string) model.BatchRow {
		m := img{model.BatchRow{Row: model.SingleKey(1), Mask: mask}}
		m.u16(2, length)
		m.raw(4, text)
		m.raw(132, dest)
		return m.row
	}
	full := strings.Repeat("z", 200) // fills the 128-byte field and spills past it
	rows := []model.BatchRow{
		mk(0, "abc", "\x00\x00"),
		mk(0xFFFF, "abc", "  "),    // unsigned 65535: clamps to 128, NUL tail trims
		mk(3, "ab  ", "B\x00"),     // trailing blanks inside the valid length
		mk(500, full, "CD"),        // clamp to 128
		mk(4, "a\x00\x00\x00", ""), // NUL tail trims
		mk(5, "  ab ", "\xc3"),     // leading blanks kept; lone high byte passes through
		mk(0x8000, "q", ""),        // 0x8000 is large, not negative: clamps, not NULL
	}
	rows[4].Image[132], rows[4].Image[133] = ' ', 'Q'
	b := newBinder(t, d, mask, 8)
	args, err := b.Bind(rows, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	// 0 key, 1 message_len, 2 message_text, 3 bcast_destination, 4 updated_at
	text := args[2].([]sql.NullString)
	wantText := []sql.NullString{{}, ns("abc"), ns("ab"), ns(strings.Repeat("z", 128)), ns("a"), ns("  ab"), ns("q")}
	for i := range wantText {
		if text[i] != wantText[i] {
			t.Errorf("message_text[%d] = %#v, want %#v", i, text[i], wantText[i])
		}
	}
	dest := args[3].([]sql.NullString)
	wantDest := []sql.NullString{{}, {}, ns("B"), ns("CD"), ns(" Q"), ns("\xc3"), {}}
	for i := range wantDest {
		if dest[i] != wantDest[i] {
			t.Errorf("bcast_destination[%d] = %#v, want %#v", i, dest[i], wantDest[i])
		}
	}
}

// TestBindRowKeyZeroExtends: a key above 2^31 stays a positive int64.
func TestBindRowKeyZeroExtends(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	mask := model.Bit(0)
	rows := []model.BatchRow{
		{Row: model.SingleKey(0xFFFFFFFF), Mask: mask},
		{Row: model.SingleKey(1), Mask: mask},
	}
	b := newBinder(t, d, mask, 2)
	args, err := b.Bind(rows, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	keys := args[0].([]int64)
	if keys[0] != 4294967295 || keys[1] != 1 {
		t.Errorf("keys = %v", keys)
	}
}

// TestBindSliceLengths: runs of 1, 250 and ceiling rows give every position
// exactly that many elements, one distinct key and one updated_at per row.
func TestBindSliceLengths(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	mask := d.FullMask()
	const ceiling = 500
	b := newBinder(t, d, mask, ceiling)
	rows := make([]model.BatchRow, ceiling)
	for i := range rows {
		rows[i].Row, rows[i].Mask = model.SingleKey(uint32(1000+i)), mask
		binary.BigEndian.PutUint32(rows[i].Image[0:], uint32(i))
	}
	for _, n := range []int{1, 250, ceiling, 7, 1} {
		args, err := b.Bind(rows[:n], testUpdatedAt)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if len(args) != 47 {
			t.Fatalf("n=%d: %d binds", n, len(args))
		}
		for p, a := range args {
			if l := reflect.ValueOf(a).Len(); l != n {
				t.Fatalf("n=%d position %d: len %d", n, p+1, l)
			}
		}
		keys, px, at := args[0].([]int64), args[1].([]int64), args[46].([]sql.NullTime)
		for i := 0; i < n; i++ {
			if keys[i] != int64(1000+i) || px[i] != int64(i) || at[i].Time != testUpdatedAt {
				t.Fatalf("n=%d row %d: key %d px %d at %v", n, i, keys[i], px[i], at[i])
			}
		}
	}
	for _, n := range []int{0, ceiling + 1} {
		if _, err := b.Bind(make([]model.BatchRow, n), testUpdatedAt); err == nil {
			t.Errorf("Bind of %d rows succeeded", n)
		}
	}
}

// TestBindDoesNotAllocateInSteadyState: the second Bind of a same-length run
// on a numeric destination allocates nothing.
func TestBindDoesNotAllocateInSteadyState(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	mask := d.FullMask()
	rows := make([]model.BatchRow, 50)
	for i := range rows {
		rows[i].Row, rows[i].Mask = model.SingleKey(uint32(i+1)), mask
		rows[i].Image[26] = '+'
		binary.BigEndian.PutUint32(rows[i].Image[31:], 10)
	}
	b := newBinder(t, d, mask, 64)
	if _, err := b.Bind(rows, testUpdatedAt); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(20, func() {
		if _, err := b.Bind(rows, testUpdatedAt); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Errorf("steady-state Bind allocates %v times", got)
	}
}

// TestBindShortArenaFailsBeforeExec (W-3): an arena that cannot hold the run,
// and a bind slice of the wrong length, are both errors, never a silent
// truncation.
func TestBindShortArenaFailsBeforeExec(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	mask := model.Bit(0).Union(model.Bit(7)).Union(model.Bit(11)).Union(model.Bit(9))
	rows := make([]model.BatchRow, 3)
	for i := range rows {
		rows[i].Mask = mask
	}
	// One arena per kind in turn: key/int, string, double, time.
	for p := range 6 {
		b := newBinder(t, d, mask, 8)
		if _, err := b.Bind(rows, testUpdatedAt); err != nil {
			t.Fatal(err)
		}
		c := &b.cols[p]
		switch {
		case c.i64 != nil:
			c.i64 = make([]int64, 0)
		case c.str != nil:
			c.str = make([]sql.NullString, 0)
		case c.f64 != nil:
			c.f64 = make([]sql.NullFloat64, 0)
		default:
			c.tm = make([]sql.NullTime, 0)
		}
		if _, err := b.Bind(rows, testUpdatedAt); err == nil {
			t.Errorf("position %d: short arena bound without error", p+1)
		}
	}
	// The assertion itself, applied to args whose length disagrees.
	args := []any{make([]int64, 3), make([]sql.NullString, 2)}
	if checkLens(args, 3) == nil {
		t.Error("checkLens accepted a short slice")
	}
	if checkLens([]any{[]byte{1, 2, 3}}, 3) == nil {
		t.Error("checkLens accepted []byte")
	}
	if checkLens([]any{nil}, 3) == nil {
		t.Error("checkLens accepted an untyped nil")
	}
	if checkLens([]any{make([]int64, 3), make([]sql.NullTime, 3)}, 3) != nil {
		t.Error("checkLens rejected a correct set")
	}
}

// TestBindArgsAreConcretelyTyped: no []byte, no untyped nil, only the four
// array types go-ora can bind, in the batch and the scalar form. The type
// switch plus the element-kind check stands in for the card's reflect walk.
func TestBindArgsAreConcretelyTyped(t *testing.T) {
	for _, name := range []string{"MD_TOKEN_QUOTE", "MD_SECURITY_MASTER", "MD_MARKET_STATUS"} {
		d := shippedDest(t, name)
		rows := []model.BatchRow{{Row: model.SingleKey(1), Mask: d.FullMask()}, {Row: model.SingleKey(2), Mask: d.FullMask()}}
		b := newBinder(t, d, d.FullMask(), 4)
		args, err := b.Bind(rows, testUpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
		for p, a := range args {
			switch a.(type) {
			case []int64, []sql.NullString, []sql.NullFloat64, []sql.NullTime:
			default:
				t.Errorf("%s position %d: %T", name, p+1, a)
			}
			if rt := reflect.TypeOf(a); rt == nil || rt.Elem().Kind() == reflect.Uint8 || rt.Elem().Kind() == reflect.Interface {
				t.Errorf("%s position %d: bad element type %v", name, p+1, rt)
			}
		}
		scalar, err := b.BindRow(&rows[0], testUpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
		if len(scalar) != len(args) {
			t.Fatalf("%s: BindRow %d binds, Bind %d", name, len(scalar), len(args))
		}
		for p, a := range scalar {
			switch a.(type) {
			case int64, sql.NullString, sql.NullFloat64, sql.NullTime:
			default:
				t.Errorf("%s BindRow position %d: %T", name, p+1, a)
			}
		}
	}
}

// TestBindRowMatchesBind: phase 2 binds row r exactly as element r of phase 1.
func TestBindRowMatchesBind(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	mask := d.FullMask()
	var m img
	m.row.Row, m.row.Mask = model.SingleKey(9), mask
	m.u32(0, 12345)
	m.raw(26, "-")
	m.f64(39, 5.5)
	m.u32(31, 99)
	m.u32(60, 4242)
	rows := []model.BatchRow{{Row: model.SingleKey(8), Mask: mask}, m.row}
	b := newBinder(t, d, mask, 4)
	args, err := b.Bind(rows, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	scalar, err := b.BindRow(&rows[1], testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	for p := range args {
		if got, want := scalar[p], reflect.ValueOf(args[p]).Index(1).Interface(); got != want {
			t.Errorf("position %d: BindRow %#v, Bind %#v", p+1, got, want)
		}
	}
	// Independent oracle: hand-written values at absolute offsets (price 0,
	// net-change 26, last time 31, total buy 39).
	for p, v := range map[int]any{0: int64(9), 1: int64(12345), 8: ns("-"), 10: ts(99), 12: nf(5.5),
		46: sql.NullTime{Time: testUpdatedAt, Valid: true}} {
		if scalar[p] != v {
			t.Errorf("BindRow position %d = %#v, want %#v", p+1, scalar[p], v)
		}
	}
	if _, err := b.BindRow(&model.BatchRow{Mask: model.Bit(1)}, testUpdatedAt); err == nil {
		t.Error("BindRow accepted a row whose mask differs from the statement")
	}
}

// TestG_MASK_1_BindMatchesStatementPlaceholders: gate G-MASK-1, writer half.
//
// SETUP: for several masks of destination 0, generate the statement and build a
// Binder for the same mask; the image has a different non-zero value in every
// field.
// STIMULUS: Bind one row.
// ASSERTION: the argument count equals the statement's highest placeholder, the
// key is first and updated_at last, a column outside the mask contributes no
// position, and the value at each position is the one that column's bytes hold.
// NON-VACUITY: the same check against the FULL mask must see 47 positions, and
// a mask that differs only by one bit must see a different count (the first
// two cases: 3 vs 4), and hi > 0 is asserted so a scan finding nothing fails.
// WOULD HAVE CAUGHT: a binder that binds every column regardless of the mask,
// or binds in column order while the SET list follows the mask, so a partial
// mask writes a price into a volume column and reports success.
func TestG_MASK_1_BindMatchesStatementPlaceholders(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	var m img
	m.u32(0, 11)
	m.u32(4, 22)
	m.u32(8, 33)
	m.u32(177, 44)
	ladder := []any{nil}
	for k := 0; k < 10; k++ {
		o := 57 + 12*k
		m.u32(o, uint32(100*(k+1)))
		m.u32(o+4, uint32(10000+k))
		m.u16(o+8, uint16(k+1))
		ladder = append(ladder, int64(100*(k+1)), int64(10000+k), int64(k+1))
	}
	cases := []struct {
		mask  model.ColMask
		want  []any // by position, nil entries are not checked
		count int
	}{
		{model.Bit(1), []any{nil, int64(22)}, 3},
		{model.Bit(0).Union(model.Bit(2)), []any{nil, int64(11), int64(33)}, 4},
		{model.Bit(15), []any{nil, ts(44)}, 3},
		{model.Bit(14), ladder, 32}, // ladder only: record-by-record order
		{d.FullMask(), nil, 47},
	}
	for _, c := range cases {
		stmt, plan, err := destinations.StatementFor(d, c.mask)
		if err != nil {
			t.Fatal(err)
		}
		hi := 0
		for i := 1; strings.Contains(stmt, ":"+strconv.Itoa(i)+" "); i++ {
			hi = i
		}
		row := m.row
		row.Row, row.Mask = model.SingleKey(5), c.mask
		b := newBinder(t, d, c.mask, 2)
		args, err := b.Bind([]model.BatchRow{row}, testUpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
		if len(args) != hi || len(args) != len(plan) || len(args) != c.count {
			t.Errorf("mask %#x: %d args, statement highest placeholder %d, plan %d, want %d",
				uint64(c.mask), len(args), hi, len(plan), c.count)
		}
		if hi == 0 {
			t.Errorf("mask %#x: no placeholder found in the statement (scan is vacuous)", uint64(c.mask))
		}
		want1(t, args, 0, int64(5))
		want1(t, args, len(args)-1, sql.NullTime{Time: testUpdatedAt, Valid: true})
		for p, v := range c.want {
			if v != nil {
				want1(t, args, p, v)
			}
		}
	}
	// A row carrying a different mask than the statement is refused: its image
	// bytes would be bound to the wrong columns.
	b := newBinder(t, d, model.Bit(1), 2)
	if _, err := b.Bind([]model.BatchRow{{Mask: model.Bit(2)}}, testUpdatedAt); err == nil {
		t.Error("a row with a foreign mask was bound")
	}
	if _, err := NewBinder(d, 0, 2); err == nil {
		t.Error("an empty mask built a binder")
	}
}

func TestStmtKey(t *testing.T) {
	a := stmtKey{Dest: 1, Mask: 0xFF}
	if got := a.String(); got != "d1/mff" {
		t.Errorf("String() = %q", got)
	}
	m := map[stmtKey]int{a: 1}
	if m[stmtKey{1, 0xFF}] != 1 || m[stmtKey{2, 0xFF}] != 0 || m[stmtKey{1, 0xFE}] != 0 {
		t.Error("stmtKey is not a usable map key")
	}
}

// TestNewInstrumentsResolvesBeforeFreeze: the handles are real registry slots.
func TestNewInstrumentsResolvesBeforeFreeze(t *testing.T) {
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: 1})
	in := NewInstruments(reg, 3)
	reg.Freeze()
	in.BindNonfinite[2][63].Add(1)
	if in.BindNonfinite[0][0] == nil || in.BindBadTime[2][63] == nil {
		t.Fatal("handle missing")
	}
	if in.BindNonfinite[2][63] == in.BindBadTime[2][63] {
		t.Error("two instruments share one slot")
	}
}
