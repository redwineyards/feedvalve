package destinations

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"feedvalve/internal/model"
)

var update = flag.Bool("update", false, "rewrite testdata/*.golden from the generator (review the diff by hand)")

func shipped(t *testing.T) *Set {
	t.Helper()
	set, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	return set
}

// requireSourceTree skips when the module's source is absent. Gate G-AIX-1
// ships this test binary to the LPAR without testdata/ or db/schema/, and a
// file the target never had is not a failure there. On the build host go.mod is
// present, so a missing file stays a hard failure and nothing is quietly
// disabled (same convention as internal/app/testenv_test.go).
func requireSourceTree(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(filepath.Join("..", "..", "go.mod")); err != nil {
		t.Skip("no source tree: this test reads testdata/ or db/schema/, which the " +
			"cross-compiled test binary does not have on the LPAR; it runs on the build host")
	}
}

func mustStmt(t *testing.T, d *Destination, mask model.ColMask) (string, BindPlan) {
	t.Helper()
	sql, plan, err := StatementFor(d, mask)
	if err != nil {
		t.Fatalf("StatementFor(%s, %#x): %v", d.Name, uint64(mask), err)
	}
	return sql, plan
}

// squash normalises layout so generated text can be compared with prose copied
// from the LLD: whitespace runs collapse and brackets lose their inner space.
func squash(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, "( ", "(")
	return strings.ReplaceAll(s, " )", ")")
}

var placeholder = regexp.MustCompile(`:(\d+)\b`)

// section returns the text between two markers (to the end when to is empty).
func section(t *testing.T, sql, from, to string) string {
	t.Helper()
	i := strings.Index(sql, from)
	if i < 0 {
		t.Fatalf("no %q in\n%s", from, sql)
	}
	rest := sql[i+len(from):]
	if to == "" {
		return rest
	}
	j := strings.Index(rest, to)
	if j < 0 {
		t.Fatalf("no %q after %q in\n%s", to, from, sql)
	}
	return rest[:j]
}

// The LLD's own statements for destinations 1 and 2 (V3 sections 3.3, 3.4),
// copied by hand and independent of the generator. Destination 0 is not
// compared: the LLD prints its ladder attribute-major, while mapping.json
// declares it record by record.
const lldSecurityMaster = `MERGE INTO md_security_master t
USING (SELECT :1  AS token,
              :2  AS symbol,             :3  AS series,
              :4  AS expiry_date,        :5  AS strike_price,
              :6  AS board_lot_quantity, :7  AS tick_size,
              :8  AS base_price,         :9  AS delete_flag,
              :10 AS effective_ts,
              :11 AS updated_at
         FROM dual) s
   ON (t.token = s.token)
 WHEN MATCHED THEN UPDATE SET
       t.symbol             = s.symbol,
       t.series             = s.series,
       t.expiry_date        = s.expiry_date,
       t.strike_price       = s.strike_price / 100,
       t.board_lot_quantity = s.board_lot_quantity,
       t.tick_size          = s.tick_size  / 100,
       t.base_price         = s.base_price / 100,
       t.delete_flag        = s.delete_flag,
       t.effective_ts       = s.effective_ts,
       t.updated_at         = s.updated_at
 WHEN NOT MATCHED THEN INSERT (
       token, symbol, series, expiry_date, strike_price, board_lot_quantity,
       tick_size, base_price, delete_flag, effective_ts, updated_at)
 VALUES (
       s.token, s.symbol, s.series, s.expiry_date, s.strike_price / 100, s.board_lot_quantity,
       s.tick_size / 100, s.base_price / 100, s.delete_flag, s.effective_ts, s.updated_at)`

const lldMarketStatus = `MERGE INTO md_market_status t
USING (SELECT :1 AS market_type,
              :2 AS transcode,         :3 AS message_len,
              :4 AS message_text,      :5 AS bcast_destination,
              :6 AS token,             :7 AS effective_ts,
              :8 AS updated_at
         FROM dual) s
   ON (t.market_type = s.market_type)
 WHEN MATCHED THEN UPDATE SET
       t.transcode         = s.transcode,
       t.message_len       = s.message_len,
       t.message_text      = s.message_text,
       t.bcast_destination = s.bcast_destination,
       t.token             = s.token,
       t.effective_ts      = s.effective_ts,
       t.updated_at        = s.updated_at
 WHEN NOT MATCHED THEN INSERT (
       market_type, transcode, message_len, message_text, bcast_destination,
       token, effective_ts, updated_at)
 VALUES (
       s.market_type, s.transcode, s.message_len, s.message_text, s.bcast_destination,
       s.token, s.effective_ts, s.updated_at)`

// goldenMasks are the masks with a golden file per destination. Bit numbers
// are the mapping's: dest 0 bit 14 is the ladder, bit 9 last_trade_time.
var goldenMasks = []struct {
	dest  string
	masks []model.ColMask
}{
	{"MD_TOKEN_QUOTE", []model.ColMask{0xFFFF, 0x0003, 0xBFFF, 0x4000, 0x8200}},
	{"MD_SECURITY_MASTER", []model.ColMask{0x1FF, 0x081, 0x018}},
	{"MD_MARKET_STATUS", []model.ColMask{0x3F, 0x07, 0x20}},
}

// TestGoldenStatements is the V3 golden self-gate.
//
// SETUP: the shipped mapping.
// STIMULUS: generate full and partial masks for every destination.
// ASSERTION: text equals the reviewed file under testdata, and for
// destinations 1 and 2 the full-mask text equals the LLD's own, modulo layout.
// WOULD HAVE CAUGHT: a changed form, a lost scale divisor, a renumbered
// placeholder, a reordered column.
func TestGoldenStatements(t *testing.T) {
	set := shipped(t)
	for _, g := range goldenMasks {
		d := set.MustByName(g.dest)
		for _, m := range g.masks {
			name := fmt.Sprintf("%s_%016x.golden", strings.ToLower(g.dest), uint64(m))
			t.Run(name, func(t *testing.T) {
				requireSourceTree(t)
				got, _ := mustStmt(t, d, m)
				path := filepath.Join("testdata", name)
				if *update {
					if err := os.WriteFile(path, []byte(got+"\n"), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				want, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("%v (run with -update once and review the file)", err)
				}
				if strings.TrimRight(strings.ReplaceAll(string(want), "\r\n", "\n"), "\n") != got {
					t.Errorf("statement differs from %s:\n%s", path, got)
				}
			})
		}
	}

	for _, tc := range []struct{ dest, lld string }{
		{"MD_SECURITY_MASTER", lldSecurityMaster},
		{"MD_MARKET_STATUS", lldMarketStatus},
	} {
		d := set.MustByName(tc.dest)
		got, _ := mustStmt(t, d, d.FullMask())
		if !strings.EqualFold(squash(got), squash(tc.lld)) { // mapping.json spells table names in capitals; Oracle folds
			t.Errorf("%s full mask differs from the LLD:\n got: %s\nwant: %s", tc.dest, squash(got), squash(tc.lld))
		}
	}

	// Non-vacuity: the comparison rejects a one-token change.
	d := set.MustByName("MD_SECURITY_MASTER")
	got, _ := mustStmt(t, d, d.FullMask())
	if strings.EqualFold(squash(strings.Replace(got, "/ 100", "/ 10", 1)), squash(lldSecurityMaster)) {
		t.Fatal("LLD comparison is vacuous: a changed divisor still matched")
	}
}

// TestBindCounts pins the V3 bind counts: 1 key + data columns + updated_at.
func TestBindCounts(t *testing.T) {
	set := shipped(t)
	for dest, want := range map[string]int{"MD_TOKEN_QUOTE": 47, "MD_SECURITY_MASTER": 11, "MD_MARKET_STATUS": 8} {
		d := set.MustByName(dest)
		sql, plan := mustStmt(t, d, d.FullMask())
		if len(plan) != want {
			t.Errorf("%s: %d binds, want %d", dest, len(plan), want)
		}
		if n := len(placeholder.FindAllString(sql, -1)); n != want {
			t.Errorf("%s: %d placeholders in text, want %d", dest, n, want)
		}
	}
}

// statementProblems returns every structural-property violation of one
// generated statement, derived from the mask and the hand-known rules only.
func statementProblems(d *Destination, mask model.ColMask, sql string, plan BindPlan) []string {
	var bad []string
	add := func(f string, a ...any) {
		bad = append(bad, fmt.Sprintf("mask %#x: "+f, append([]any{uint64(mask)}, a...)...))
	}

	if !strings.HasPrefix(sql, "MERGE") {
		add("text does not start with MERGE")
	}
	for _, s := range []string{"'", "--", "/*", "\"", ";", "?"} {
		if strings.Contains(sql, s) {
			add("text contains %q", s)
		}
	}

	// Placeholders: :1..:n ascending, each exactly once.
	var nums []int
	for _, m := range placeholder.FindAllStringSubmatch(sql, -1) {
		n, _ := strconv.Atoi(m[1])
		nums = append(nums, n)
	}
	for i, n := range nums {
		if n != i+1 {
			add("placeholder #%d is :%d, want :%d", i, n, i+1)
			break
		}
	}
	if len(nums) != len(plan) {
		add("%d placeholders, %d plan entries", len(nums), len(plan))
	}

	var want []string
	for i := range d.Columns {
		if mask.Has(d.Columns[i].Bit) {
			want = append(want, d.Columns[i].SQL...)
		}
	}
	if len(plan) != len(want)+2 {
		add("%d binds, want %d", len(plan), len(want)+2)
		return bad
	}
	if plan[0].Role != RoleKey || plan[0].Name != d.KeyColumn || plan[len(plan)-1].Role != RoleUpdatedAt {
		add("plan must be key first, updated_at last")
	}
	for i, n := range want {
		if plan[i+1].Name != n || plan[i+1].Role != RoleData {
			add("bind %d is %q, want %q", i+2, plan[i+1].Name, n)
		}
	}

	// The four lists each carry exactly the wanted columns.
	using := sql[:strings.Index(sql, "FROM dual")]
	setL := sql[strings.Index(sql, "UPDATE SET"):strings.Index(sql, "WHEN NOT MATCHED")]
	ins := sql[strings.Index(sql, "INSERT ("):strings.Index(sql, "VALUES (")]
	vals := sql[strings.Index(sql, "VALUES ("):]
	all := append(slices.Clone(want), UpdatedAtColumn)
	for _, c := range all {
		if strings.Count(using, " AS "+c+",")+strings.Count(using, " AS "+c+"\n") != 1 {
			add("%s not exactly once in USING", c)
		}
		if !strings.Contains(setL, "t."+c+" =") {
			add("%s missing from SET", c)
		}
		if !regexp.MustCompile(`(?m)^  ` + c + `,?$`).MatchString(ins) {
			add("%s missing from INSERT", c)
		}
	}
	if got := strings.Count(setL, "t."); got != len(all) {
		add("%d SET targets, want %d", got, len(all))
	}
	if got := strings.Count(vals, "s."); got != len(all)+1 {
		add("%d VALUES refs, want %d", got, len(all)+1)
	}

	// Key: in USING, ON and INSERT, never a SET target.
	if strings.Count(using, " AS "+d.KeyColumn+",")+strings.Count(using, " AS "+d.KeyColumn+"\n") != 1 {
		add("key %s not exactly once in USING", d.KeyColumn)
	}
	if !regexp.MustCompile(`(?m)^  ` + d.KeyColumn + `,$`).MatchString(ins) {
		add("key %s missing from INSERT", d.KeyColumn)
	}
	if !strings.Contains(sql, "ON (t."+d.KeyColumn+" = s."+d.KeyColumn+")") {
		add("key missing from ON")
	}
	if strings.Contains(setL, "t."+d.KeyColumn+" =") {
		add("key appears in SET")
	}

	// Scaling: a divisor exactly where the plan says so, in SET and in VALUES,
	// matched with a trailing boundary so "/ 1000" cannot satisfy "/ 100".
	for _, b := range plan[1:] {
		for list, text := range map[string]string{"SET": setL, "VALUES": vals} {
			div := regexp.MustCompile(`s\.` + b.Name + ` / 10{` + strconv.Itoa(int(b.Scale)) + `}\b`)
			divided := regexp.MustCompile(`s\.` + b.Name + ` /`)
			switch {
			case b.Scale > 0 && !div.MatchString(text):
				add("%s lacks divisor 10^%d in %s", b.Name, b.Scale, list)
			case b.Scale == 0 && divided.MatchString(text):
				add("%s divided in %s but unscaled", b.Name, list)
			}
		}
	}
	return bad
}

// TestG_MASK_1_StatementProperties covers the statement half of G-MASK-1.
//
// SETUP: shipped mapping.
// STIMULUS: every nonzero mask of destination 2, a spread of destination 1 and
// 0 masks, and the interesting destination 0 ones.
// ASSERTION: statementProblems is empty for all of them.
// WOULD HAVE CAUGHT: a repeated or skipped placeholder, a key in SET, a column
// in one list but not the others.
func TestG_MASK_1_StatementProperties(t *testing.T) {
	set := shipped(t)
	run := func(d *Destination, m model.ColMask) {
		sql, plan := mustStmt(t, d, m)
		for _, p := range statementProblems(d, m, sql, plan) {
			t.Errorf("%s: %s", d.Name, p)
		}
	}
	d2 := set.MustByName("MD_MARKET_STATUS")
	for m := model.ColMask(1); m <= d2.FullMask(); m++ {
		run(d2, m)
	}
	d1 := set.MustByName("MD_SECURITY_MASTER")
	for m := model.ColMask(1); m <= d1.FullMask(); m += 7 {
		run(d1, m)
	}
	d0 := set.MustByName("MD_TOKEN_QUOTE")
	for m := model.ColMask(1); m <= d0.FullMask(); m += 251 {
		run(d0, m)
	}
	for _, m := range []model.ColMask{0xFFFF, 0x4000, 0x0001, 0x8000, 0xBFFF} {
		run(d0, m)
	}

	// Non-vacuity: the checker flags a repeated placeholder and a key in SET.
	sql, plan := mustStmt(t, d2, d2.FullMask())
	if p := statementProblems(d2, d2.FullMask(), sql, plan); len(p) != 0 {
		t.Fatalf("clean statement already has problems: %v", p)
	}
	if p := statementProblems(d2, d2.FullMask(), strings.Replace(sql, ":4 AS", ":3 AS", 1), plan); len(p) == 0 {
		t.Error("a repeated placeholder went unnoticed")
	}
	if p := statementProblems(d2, d2.FullMask(), strings.Replace(sql, "t.transcode =", "t.market_type =", 1), plan); len(p) == 0 {
		t.Error("the key in SET went unnoticed")
	}
}

// TestG_MASK_2_RemovedColumnsVanish: clearing a bit removes its columns from
// all four lists and renumbers the rest contiguously.
//
// SETUP: destination 0, full mask.
// STIMULUS: clear bit 14 (ladder) and bit 2 (open_price).
// ASSERTION: names absent, bind counts 17 and 46.
// WOULD HAVE CAUGHT: a generator that leaves a cleared column in one list.
func TestG_MASK_2_RemovedColumnsVanish(t *testing.T) {
	set := shipped(t)
	d := set.MustByName("MD_TOKEN_QUOTE")

	// Non-vacuity: the full mask names open_price in all four lists (the
	// SET list names it twice, target and source): 5 occurrences.
	sql, _ := mustStmt(t, d, d.FullMask())
	if n := strings.Count(sql, "open_price"); n != 5 {
		t.Fatalf("full mask names open_price %d times, want 5", n)
	}

	sql, plan := mustStmt(t, d, d.FullMask().Clear(14))
	if len(plan) != 17 {
		t.Errorf("ladder-less statement has %d binds, want 17", len(plan))
	}
	if strings.Contains(sql, "bid_") || strings.Contains(sql, "ask_") {
		t.Error("ladder columns survive in a mask without bit 14")
	}

	sql, plan = mustStmt(t, d, d.FullMask().Clear(2))
	if len(plan) != 46 || strings.Contains(sql, "open_price") {
		t.Errorf("open_price removal: %d binds, present=%v", len(plan), strings.Contains(sql, "open_price"))
	}
}

// TestBindPlanAbsoluteOffsets pins the plan against a hand-written table of
// absolute image offsets, not the mapping's own arithmetic.
func TestBindPlanAbsoluteOffsets(t *testing.T) {
	set := shipped(t)
	type row struct {
		name           string
		off, width     uint16
		kind           ColKind
		scale          uint8
		lenBit, ladder int
	}
	check := func(dest string, want []row) {
		d := set.MustByName(dest)
		_, plan := mustStmt(t, d, d.FullMask())
		byName := map[string]Bind{}
		for _, b := range plan {
			byName[b.Name] = b
		}
		for _, w := range want {
			b, ok := byName[w.name]
			if !ok {
				t.Errorf("%s: no bind %s", dest, w.name)
				continue
			}
			if b.Offset != w.off || b.Width != w.width || b.Kind != w.kind || b.Scale != w.scale ||
				int(b.LenBit) != w.lenBit || b.Ladder != w.ladder {
				t.Errorf("%s.%s = %+v, want %+v", dest, w.name, b, w)
			}
		}
	}
	check("MD_TOKEN_QUOTE", []row{
		{"last_traded_price", 0, 4, KindLong, 2, -1, -1},
		{"volume_traded_today", 4, 4, KindLong, 0, -1, -1},
		{"net_change_indicator", 26, 1, KindChar1, 0, -1, -1},
		{"last_trade_time", 31, 4, KindTime1980, 0, -1, -1},
		{"total_buy_quantity", 39, 8, KindDouble, 0, -1, -1},
		{"book_type", 55, 2, KindShort, 0, -1, -1},
		{"bid_qty_1", 57, 4, KindLong, 0, -1, 0},
		{"bid_px_1", 61, 4, KindLong, 2, -1, 1},
		{"bid_ord_1", 65, 2, KindShort, 0, -1, 2},
		{"bid_qty_2", 69, 4, KindLong, 0, -1, 3},
		{"bid_px_5", 57 + 48 + 4, 4, KindLong, 2, -1, 13},
		{"ask_qty_1", 57 + 60, 4, KindLong, 0, -1, 15},
		{"ask_ord_5", 57 + 108 + 8, 2, KindShort, 0, -1, 29},
		{"effective_ts", 177, 4, KindTime1980, 0, -1, -1},
	})
	check("MD_SECURITY_MASTER", []row{
		{"symbol", 0, 10, KindText, 0, -1, -1},
		{"series", 10, 2, KindText, 0, -1, -1},
		{"expiry_date", 12, 4, KindTime1980, 0, -1, -1},
		{"strike_price", 16, 4, KindLong, 2, -1, -1},
		{"delete_flag", 32, 1, KindChar1, 0, -1, -1},
		{"effective_ts", 33, 4, KindTime1980, 0, -1, -1},
	})
	check("MD_MARKET_STATUS", []row{
		{"transcode", 0, 2, KindShort, 0, -1, -1},
		{"message_len", 2, 2, KindShort, 0, -1, -1},
		{"message_text", 4, 128, KindText, 0, 1, -1},
		{"bcast_destination", 132, 2, KindText, 0, -1, -1},
		{"token", 134, 4, KindLong, 0, -1, -1},
		{"effective_ts", 138, 4, KindTime1980, 0, -1, -1},
	})
}

// TestBindPlanLadderAndRoles pins all 30 ladder binds of destination 0 with an
// independent formula (record r at 57+12r; qty LONG @0, px LONG @4, ord SHORT
// @8; only px scaled), plus each bind's role, bit and position.
func TestBindPlanLadderAndRoles(t *testing.T) {
	set := shipped(t)
	d := set.MustByName("MD_TOKEN_QUOTE")
	_, plan := mustStmt(t, d, d.FullMask())

	if plan[0].Role != RoleKey || plan[0].Name != "token" || plan[0].Bit != -1 {
		t.Errorf("plan[0] = %+v, want the token key", plan[0])
	}
	if last := plan[46]; last.Role != RoleUpdatedAt || last.Name != "updated_at" || last.Bit != -1 {
		t.Errorf("plan[46] = %+v, want updated_at", last)
	}
	if b := plan[10]; b.Name != "last_trade_time" || b.Bit != 9 || b.Role != RoleData || b.Ladder != -1 {
		t.Errorf("plan[10] = %+v, want last_trade_time at bit 9", b)
	}
	if b := plan[45]; b.Name != "effective_ts" || b.Bit != 15 {
		t.Errorf("plan[45] = %+v, want effective_ts at bit 15", b)
	}

	attrs := []struct {
		tag        string
		off, width uint16
		kind       ColKind
		scale      uint8
	}{{"qty", 0, 4, KindLong, 0}, {"px", 4, 4, KindLong, 2}, {"ord", 8, 2, KindShort, 0}}
	for j := 0; j < 30; j++ {
		rec, a := j/3, attrs[j%3]
		side, level := "bid", rec+1
		if rec >= 5 {
			side, level = "ask", rec-4
		}
		want := Bind{Role: RoleData, Name: fmt.Sprintf("%s_%s_%d", side, a.tag, level), Bit: 14,
			Kind: a.kind, Offset: uint16(57+12*rec) + a.off, Width: a.width, LenBit: -1, Scale: a.scale, Ladder: j}
		if got := plan[15+j]; got != want {
			t.Errorf("plan[%d] = %+v, want %+v", 15+j, got, want)
		}
	}
}

// lldDest0Columns are the 45 data columns LLD 3.2 names, written out from the
// LLD text by hand. Only the SET of names is compared: the LLD prints the ladder
// attribute by attribute, the mapping declares it record by record (erratum,
// phase4-tasks.md section 6).
var lldDest0Columns = []string{
	"last_traded_price", "volume_traded_today", "open_price", "high_price", "low_price",
	"closing_price", "trading_status", "net_change_indicator", "last_trade_quantity",
	"last_trade_time", "average_trade_price", "total_buy_quantity", "total_sell_quantity",
	"book_type",
	"bid_qty_1", "bid_qty_2", "bid_qty_3", "bid_qty_4", "bid_qty_5",
	"bid_px_1", "bid_px_2", "bid_px_3", "bid_px_4", "bid_px_5",
	"bid_ord_1", "bid_ord_2", "bid_ord_3", "bid_ord_4", "bid_ord_5",
	"ask_qty_1", "ask_qty_2", "ask_qty_3", "ask_qty_4", "ask_qty_5",
	"ask_px_1", "ask_px_2", "ask_px_3", "ask_px_4", "ask_px_5",
	"ask_ord_1", "ask_ord_2", "ask_ord_3", "ask_ord_4", "ask_ord_5",
	"effective_ts",
}

// lldDest0Scaled are the columns LLD 3.2 divides by 100, by hand.
var lldDest0Scaled = []string{
	"last_traded_price", "open_price", "high_price", "low_price", "closing_price",
	"average_trade_price",
	"bid_px_1", "bid_px_2", "bid_px_3", "bid_px_4", "bid_px_5",
	"ask_px_1", "ask_px_2", "ask_px_3", "ask_px_4", "ask_px_5",
}

// TestDestination0AgainstLLD gives destination 0 the independent oracle that 1
// and 2 have in the LLD text: same 45 columns, and /100 on exactly the LLD's
// sixteen, in SET and in VALUES.
//
// SETUP: the shipped mapping, full mask.
// STIMULUS: generate the statement.
// ASSERTION: data-bind names equal the LLD's set; scaled set equals the LLD's.
// WOULD HAVE CAUGHT: a wrong scale on a column only the self-generated golden
// pinned; a dropped or misspelt ladder column.
func TestDestination0AgainstLLD(t *testing.T) {
	set := shipped(t)
	d := set.MustByName("MD_TOKEN_QUOTE")
	sql, plan := mustStmt(t, d, d.FullMask())

	var names, scaled []string
	for _, b := range plan {
		if b.Role != RoleData {
			continue
		}
		names = append(names, b.Name)
		if b.Scale == 2 {
			scaled = append(scaled, b.Name)
		} else if b.Scale != 0 {
			t.Errorf("%s has scale %d; the LLD scales only by 100", b.Name, b.Scale)
		}
	}
	sortedEq := func(a, b []string) bool {
		a, b = slices.Clone(a), slices.Clone(b)
		slices.Sort(a)
		slices.Sort(b)
		return slices.Equal(a, b)
	}
	if len(lldDest0Columns) != 45 || len(lldDest0Scaled) != 16 {
		t.Fatalf("hand tables are %d and %d long, want 45 and 16", len(lldDest0Columns), len(lldDest0Scaled))
	}
	if !sortedEq(names, lldDest0Columns) {
		t.Errorf("destination 0 data columns differ from LLD 3.2:\n got %v\nwant %v", names, lldDest0Columns)
	}
	if !sortedEq(scaled, lldDest0Scaled) {
		t.Errorf("scaled columns differ from LLD 3.2:\n got %v\nwant %v", scaled, lldDest0Scaled)
	}
	for _, c := range lldDest0Scaled {
		if n := len(regexp.MustCompile(`s\.`+c+` / 100\b`).FindAllString(sql, -1)); n != 2 {
			t.Errorf("%s divided %d times in the text, want 2 (SET and VALUES)", c, n)
		}
	}

	// Non-vacuity: an extra name is noticed.
	if sortedEq(append(slices.Clone(names), "extra"), lldDest0Columns) {
		t.Error("set comparison is vacuous")
	}
}

// TestG_SC_1_HostileIdentifiersNeverReachSQL also covers the empty mask and
// undeclared bits.
//
// SETUP: a hand-built Destination that bypasses the loader's validateSet.
// STIMULUS: hostile table, key and column names.
// ASSERTION: StatementFor errors and emits nothing.
// WOULD HAVE CAUGHT: identifiers trusted because the loader "already checked".
func TestG_SC_1_HostileIdentifiersNeverReachSQL(t *testing.T) {
	set := shipped(t)
	d := set.MustByName("MD_MARKET_STATUS")
	if _, _, err := StatementFor(d, 0); !errors.Is(err, ErrEmptyMask) {
		t.Errorf("mask 0: err = %v, want ErrEmptyMask", err)
	}
	if _, _, err := StatementFor(d, model.Bit(6)); err == nil {
		t.Error("a bit the destination does not declare was accepted")
	}

	mk := func() *Destination {
		return &Destination{
			Name: "H", Table: "ok_table", KeyColumn: "ok_key",
			Columns: []Column{{Bit: 0, Width: 4, Kind: KindLong, LenBit: -1, SQL: []string{"ok_col"}}},
		}
	}
	if _, _, err := StatementFor(mk(), 1); err != nil {
		t.Fatalf("non-vacuity: the clean hand-built destination must generate: %v", err)
	}
	for _, h := range []string{"t; DROP TABLE x", "a b", "x\"y", "1abc", "a'--", "", "a.b"} {
		for _, which := range []string{"table", "key", "column"} {
			x := mk()
			switch which {
			case "table":
				x.Table = h
			case "key":
				x.KeyColumn = h
			case "column":
				x.Columns[0].SQL = []string{h}
			}
			if sql, _, err := StatementFor(x, 1); err == nil {
				t.Errorf("hostile %s %q produced a statement:\n%s", which, h, sql)
			}
		}
	}
}

// TestG_SC_1_GroupColumns extends G-SC-1 to the ladder path, which expands one
// column into many names and has its own width check.
//
// SETUP: a hand-built destination whose only column is a two-record group.
// STIMULUS: hostile and over-long names inside the group; a SQL list that does
// not fill the width.
// ASSERTION: StatementFor errors; the clean group generates (non-vacuity).
// WOULD HAVE CAUGHT: validation that looked only at single-name columns.
func TestG_SC_1_GroupColumns(t *testing.T) {
	names := func() []string { return []string{"q1", "p1", "o1", "q2", "p2", "o2"} }
	mk := func(sql []string, width uint16) *Destination {
		return &Destination{
			Name: "G", Table: "ok_table", KeyColumn: "ok_key",
			Columns: []Column{{Bit: 0, Width: width, Kind: KindGroup, LenBit: -1, Scaled: true, Scale: 2, SQL: sql}},
		}
	}
	if _, plan, err := StatementFor(mk(names(), 24), 1); err != nil || len(plan) != 8 {
		t.Fatalf("non-vacuity: clean group must generate 8 binds: %d, %v", len(plan), err)
	}
	for _, h := range []string{"p1; DROP TABLE x", "a b", "x\"y", "a'--", "", strings.Repeat("a", 200)} {
		n := names()
		n[4] = h
		if sql, _, err := StatementFor(mk(n, 24), 1); err == nil {
			t.Errorf("hostile group column %q produced a statement:\n%s", h, sql)
		}
	}
	for _, tc := range []struct {
		sql   []string
		width uint16
	}{
		{names(), 36},     // width fills three records, SQL names two
		{names()[:5], 24}, // not a whole number of records
		{names(), 12},     // SQL names more than the width holds
	} {
		if sql, _, err := StatementFor(mk(tc.sql, tc.width), 1); err == nil {
			t.Errorf("malformed group (%d names, width %d) produced a statement:\n%s", len(tc.sql), tc.width, sql)
		}
	}
}

// TestShippedKeyColumns pins mapping.json's keyColumn per destination.
func TestShippedKeyColumns(t *testing.T) {
	set := shipped(t)
	for name, want := range map[string]string{
		"MD_TOKEN_QUOTE": "token", "MD_SECURITY_MASTER": "token", "MD_MARKET_STATUS": "market_type",
	} {
		if got := set.MustByName(name).KeyColumn; got != want {
			t.Errorf("%s keyColumn = %q, want %q", name, got, want)
		}
	}
}

var (
	createTable = regexp.MustCompile(`(?is)CREATE\s+TABLE\s+(\w+)\s*\((.*?)\n\);`)
	lineComment = regexp.MustCompile(`--[^\n]*`)
)

// ddlColumns parses db/schema/*.sql into table -> column names, splitting each
// body on commas outside parentheses so NUMBER(20,4) stays whole.
func ddlColumns(t *testing.T) map[string][]string {
	t.Helper()
	requireSourceTree(t)
	files, err := filepath.Glob(filepath.Join("..", "..", "db", "schema", "*.sql"))
	if err != nil || len(files) != 3 {
		t.Fatalf("want 3 schema files under db/schema, got %v (%v)", files, err)
	}
	out := map[string][]string{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		txt := lineComment.ReplaceAllString(string(raw), "")
		m := createTable.FindStringSubmatch(txt)
		if m == nil {
			t.Fatalf("%s: no CREATE TABLE", f)
		}
		if strings.Contains(strings.ToUpper(txt), "GRANT") {
			t.Errorf("%s: a live GRANT statement; grants belong in comments (documentation only)", f)
		}
		table := strings.ToLower(m[1])
		body := m[2]
		depth, start := 0, 0
		for i := 0; i <= len(body); i++ {
			switch {
			case i == len(body) || (body[i] == ',' && depth == 0):
				fs := strings.Fields(body[start:i])
				if len(fs) > 0 && !strings.EqualFold(fs[0], "CONSTRAINT") {
					out[table] = append(out[table], strings.ToLower(fs[0]))
				}
				start = i + 1
			case body[i] == '(':
				depth++
			case body[i] == ')':
				depth--
			}
		}
	}
	return out
}

// TestDDLMatchesMapping: each CREATE TABLE's column list equals the mapping's
// SQL names plus the key and updated_at (45 data columns, 30 of them ladder,
// for destination 0).
func TestDDLMatchesMapping(t *testing.T) {
	ddl := ddlColumns(t)
	set := shipped(t)
	for _, d := range set.All() {
		want := []string{d.KeyColumn}
		for i := range d.Columns {
			want = append(want, d.Columns[i].SQL...)
		}
		want = append(want, UpdatedAtColumn)

		got := slices.Clone(ddl[strings.ToLower(d.Table)])
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: DDL columns\n %v\nmapping columns\n %v", d.Table, got, want)
		}
	}

	d0 := set.MustByName("MD_TOKEN_QUOTE")
	_, plan := mustStmt(t, d0, d0.FullMask())
	ladder := 0
	for _, b := range plan {
		if b.Ladder >= 0 {
			ladder++
		}
	}
	if len(plan) != 47 || ladder != 30 || len(ddl["md_token_quote"]) != 47 {
		t.Errorf("dest 0: %d binds, %d ladder, %d DDL columns; want 47, 30, 47",
			len(plan), ladder, len(ddl["md_token_quote"]))
	}
}
