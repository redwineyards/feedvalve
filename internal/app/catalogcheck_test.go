package app

import (
	"bytes"
	"context"
	"database/sql/driver"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
)

var ddlCols = []string{"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "DATA_PRECISION", "DATA_LENGTH", "NULLABLE"}

var (
	ddlCreate = regexp.MustCompile(`(?i)^CREATE TABLE (\w+)`)
	ddlColumn = regexp.MustCompile(`(?i)^\s+(\w+)\s+(NUMBER|VARCHAR2|TIMESTAMP)(?:\((\d+)(?:,\d+)?\))?(\s+NOT NULL)?,?\s*$`)
)

// shippedCatalogue is the catalogue a DBA would get by running db/schema/*.sql,
// read from those files rather than built from the field map, so the baseline
// is independent of the code under test.
//
// The DDL is untouched: bcast_destination is VARCHAR2(2) there, matching the
// mapping (kind text) and the binder, which binds it as a string.
func shippedCatalogue(t *testing.T) []oradb.CatalogColumn {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "db", "schema", "*.sql"))
	if err != nil || len(files) != 3 {
		t.Fatalf("schema files = %v, %v; want 3", files, err)
	}
	var out []oradb.CatalogColumn
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		table := ""
		for _, line := range strings.Split(string(raw), "\n") {
			if m := ddlCreate.FindStringSubmatch(line); m != nil {
				table = strings.ToUpper(m[1])
				continue
			}
			m := ddlColumn.FindStringSubmatch(line)
			if m == nil || table == "" {
				continue
			}
			c := oradb.CatalogColumn{Table: table, Name: strings.ToUpper(m[1]),
				DataType: strings.ToUpper(m[2]), Nullable: m[4] == ""}
			n, _ := strconv.Atoi(m[3])
			switch c.DataType {
			case "NUMBER":
				c.Precision = n
			case "VARCHAR2":
				c.Bytes = n
			}
			out = append(out, c)
		}
	}
	if len(out) < 60 {
		t.Fatalf("parsed only %d columns from the DDL", len(out))
	}
	return out
}

func defaultDests(t *testing.T) *destinations.Set {
	t.Helper()
	d, err := destinations.Default()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// mutate returns a copy of cols with fn applied to the named column, or with
// the column removed when fn is nil; table "" removes the whole table.
func mutate(cols []oradb.CatalogColumn, table, name string, fn func(*oradb.CatalogColumn)) []oradb.CatalogColumn {
	var out []oradb.CatalogColumn
	for _, c := range cols {
		if c.Table == table && (name == "" || c.Name == name) {
			if fn == nil {
				continue
			}
			fn(&c)
		}
		out = append(out, c)
	}
	return out
}

// SETUP: the catalogue the shipped DDL produces.
// STIMULUS: compare it with the default field map.
// ASSERTION: no findings. Non-vacuity: the sub-tests below each break exactly
// one thing and see exactly one finding, so silence here is not a comparison
// that cannot fail.
func TestCatalogueBaselineHasNoFindings(t *testing.T) {
	if got := compareCatalogue(defaultDests(t), shippedCatalogue(t)); len(got) != 0 {
		t.Fatalf("findings on the shipped schema: %q", got)
	}
}

// SETUP: the baseline catalogue with one fault injected.
// STIMULUS: compareCatalogue.
// ASSERTION: exactly one finding, with the expected text (hand-written).
// WOULD HAVE CAUGHT: a DBA narrowing NUMBER(20,4) or VARCHAR2(10), adding NOT
// NULL to a data column (ORA-01400 on the first partial-mask insert), dropping
// a column or a table -- each a Rejected verdict that never self-heals.
func TestCatalogueFindingsAreDistinct(t *testing.T) {
	base := shippedCatalogue(t)
	cases := []struct {
		name string
		cols []oradb.CatalogColumn
		want string
	}{
		{"short precision",
			mutate(base, "MD_TOKEN_QUOTE", "LAST_TRADED_PRICE", func(c *oradb.CatalogColumn) { c.Precision = 12 }),
			"MD_TOKEN_QUOTE.LAST_TRADED_PRICE: precision is 12, want at least 20"},
		{"short varchar",
			mutate(base, "MD_SECURITY_MASTER", "SYMBOL", func(c *oradb.CatalogColumn) { c.Bytes = 6 }),
			"MD_SECURITY_MASTER.SYMBOL: capacity is 6 bytes, want at least 10"},
		{"not null data column",
			mutate(base, "MD_TOKEN_QUOTE", "VOLUME_TRADED_TODAY", func(c *oradb.CatalogColumn) { c.Nullable = false }),
			"MD_TOKEN_QUOTE.VOLUME_TRADED_TODAY: NOT NULL on a data column; a partial-mask insert will fail with ORA-01400"},
		{"not null ladder column",
			mutate(base, "MD_TOKEN_QUOTE", "ASK_PX_5", func(c *oradb.CatalogColumn) { c.Nullable = false }),
			"MD_TOKEN_QUOTE.ASK_PX_5: NOT NULL on a data column; a partial-mask insert will fail with ORA-01400"},
		{"missing column",
			mutate(base, "MD_TOKEN_QUOTE", "LOW_PRICE", nil),
			"MD_TOKEN_QUOTE.LOW_PRICE: column not found"},
		{"missing ladder column",
			mutate(base, "MD_TOKEN_QUOTE", "BID_ORD_3", nil),
			"MD_TOKEN_QUOTE.BID_ORD_3: column not found"},
		{"missing updated_at",
			mutate(base, "MD_MARKET_STATUS", "UPDATED_AT", nil),
			"MD_MARKET_STATUS.UPDATED_AT: column not found"},
		{"missing table",
			mutate(base, "MD_MARKET_STATUS", "", nil),
			"MD_MARKET_STATUS: table not found in the current schema"},
		{"type family",
			mutate(base, "MD_SECURITY_MASTER", "EXPIRY_DATE", func(c *oradb.CatalogColumn) { c.DataType = "VARCHAR2" }),
			"MD_SECURITY_MASTER.EXPIRY_DATE: type is VARCHAR2, want TIMESTAMP"},
		{"key not a number",
			mutate(base, "MD_SECURITY_MASTER", "TOKEN", func(c *oradb.CatalogColumn) { c.DataType = "VARCHAR2" }),
			"MD_SECURITY_MASTER.TOKEN: key column is VARCHAR2, want NUMBER"},
		{"number column given varchar",
			mutate(base, "MD_TOKEN_QUOTE", "LOW_PRICE", func(c *oradb.CatalogColumn) { c.DataType = "VARCHAR2" }),
			"MD_TOKEN_QUOTE.LOW_PRICE: type is VARCHAR2, want NUMBER"},
		{"text column given number",
			mutate(base, "MD_SECURITY_MASTER", "SYMBOL", func(c *oradb.CatalogColumn) { c.DataType = "NUMBER" }),
			"MD_SECURITY_MASTER.SYMBOL: type is NUMBER, want VARCHAR2 or CHAR"},
		{"ladder column given varchar",
			mutate(base, "MD_TOKEN_QUOTE", "ASK_PX_5", func(c *oradb.CatalogColumn) { c.DataType = "VARCHAR2" }),
			"MD_TOKEN_QUOTE.ASK_PX_5: type is VARCHAR2, want NUMBER"},
		{"updated_at wrong type",
			mutate(base, "MD_TOKEN_QUOTE", "UPDATED_AT", func(c *oradb.CatalogColumn) { c.DataType = "NUMBER" }),
			"MD_TOKEN_QUOTE.UPDATED_AT: type is NUMBER, want TIMESTAMP"},
		{"bcast_destination as number (the old DDL)",
			mutate(base, "MD_MARKET_STATUS", "BCAST_DESTINATION", func(c *oradb.CatalogColumn) { c.DataType, c.Precision, c.Bytes = "NUMBER", 5, 0 }),
			"MD_MARKET_STATUS.BCAST_DESTINATION: type is NUMBER, want VARCHAR2 or CHAR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := compareCatalogue(defaultDests(t), tc.cols)
			if len(got) != 1 || got[0] != tc.want {
				t.Errorf("findings = %q\nwant      [%q]", got, tc.want)
			}
		})
	}
}

// ASSERTION: exact-boundary precision and capacity, and a bare NUMBER
// (Precision 0), are accepted. WOULD HAVE CAUGHT: "<" flipped to "<=", or the
// Precision > 0 guard dropped (a bare NUMBER reports as precision 0 and holds
// anything).
func TestCatalogueBoundariesAreAccepted(t *testing.T) {
	base := shippedCatalogue(t)
	cols := mutate(base, "MD_TOKEN_QUOTE", "LAST_TRADED_PRICE", func(c *oradb.CatalogColumn) { c.Precision = 20 })
	cols = mutate(cols, "MD_TOKEN_QUOTE", "LOW_PRICE", func(c *oradb.CatalogColumn) { c.Precision = 0 })
	cols = mutate(cols, "MD_SECURITY_MASTER", "SYMBOL", func(c *oradb.CatalogColumn) { c.Bytes = 10 })
	cols = mutate(cols, "MD_SECURITY_MASTER", "SERIES", func(c *oradb.CatalogColumn) { c.DataType = "CHAR" })
	if got := compareCatalogue(defaultDests(t), cols); len(got) != 0 {
		t.Errorf("findings: %q", got)
	}
}

// ASSERTION: the key and updated_at are NOT NULL in the shipped DDL and that is
// correct (Vol 3 10.4), so neither is a finding. Guards the exemption.
func TestCatalogueKeyAndUpdatedAtMayBeNotNull(t *testing.T) {
	notNull := 0
	for _, c := range shippedCatalogue(t) {
		if !c.Nullable {
			notNull++
		}
	}
	if notNull != 6 { // three keys and three updated_at
		t.Fatalf("NOT NULL columns in the shipped DDL = %d, want 6; the exemption is not being exercised", notNull)
	}
	if got := compareCatalogue(defaultDests(t), shippedCatalogue(t)); len(got) != 0 {
		t.Errorf("findings: %q", got)
	}
}

// catalogueFake serves cols through an oratest fake under oradb.CatalogQuery
// and records every catalogue query it sees.
func catalogueFake(t *testing.T, cols []oradb.CatalogColumn) (*oratest.Fake, func() []oratest.Call) {
	t.Helper()
	f := oratest.New()
	t.Cleanup(f.Close)
	rows := make([][]driver.Value, 0, len(cols))
	for _, c := range cols {
		var prec driver.Value
		if c.Precision > 0 {
			prec = int64(c.Precision)
		}
		null := "N"
		if c.Nullable {
			null = "Y"
		}
		rows = append(rows, []driver.Value{c.Table, c.Name, c.DataType, prec, int64(c.Bytes), null})
	}
	f.SetRows(oradb.CatalogQuery, ddlCols, rows)
	var mu sync.Mutex
	var calls []oratest.Call
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oradb.CatalogQuery {
			mu.Lock()
			calls = append(calls, c)
			mu.Unlock()
		}
		return oratest.Result{}
	})
	return f, func() []oratest.Call { mu.Lock(); defer mu.Unlock(); return append([]oratest.Call(nil), calls...) }
}

func openCatalogueDB(t *testing.T, f *oratest.Fake) *oradb.DB {
	t.Helper()
	s := testDBSettings()
	d, err := oradb.OpenDriver(context.Background(), oratest.DriverName, f.DSN(), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func testDBSettings() oradb.DBSettings {
	return oradb.DBSettings{Host: "h", Port: 1521, Service: "s", User: "u",
		ConnectTimeoutSec: 10, QueryTimeoutSec: 30, ConnMaxLifetimeMinutes: 30,
		MaxOpenConns: 5, MaxIdleConns: 5}
}

// SETUP: a fake serving the shipped catalogue.
// STIMULUS: catalogueFindings over the default destinations.
// ASSERTION: one owner-filtered :1-bound query per destination, binding the
// upper-case table names in destination order; no findings.
// WOULD HAVE CAUGHT: the owner filter being dropped (a same-named table in a
// granted schema standing in for ours), or lower-case names reaching a
// case-sensitive table_name comparison and reporting every table missing.
func TestCatalogueQueryIsOwnerFilteredPerTable(t *testing.T) {
	f, calls := catalogueFake(t, shippedCatalogue(t))
	got, err := catalogueFindings(context.Background(), openCatalogueDB(t, f), defaultDests(t))
	if err != nil || len(got) != 0 {
		t.Fatalf("findings = %q, err = %v", got, err)
	}
	cs := calls()
	want := []string{"MD_TOKEN_QUOTE", "MD_SECURITY_MASTER", "MD_MARKET_STATUS"}
	if len(cs) != len(want) {
		t.Fatalf("queries = %d, want %d", len(cs), len(want))
	}
	for i, c := range cs {
		if len(c.Args) != 1 || c.Args[0] != want[i] {
			t.Errorf("query %d args = %v, want [%s]", i, c.Args, want[i])
		}
		if !strings.Contains(c.SQL, "owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')") ||
			!strings.Contains(c.SQL, "table_name = :1") || strings.Contains(c.SQL, "?") {
			t.Errorf("query is not owner-filtered with a :1 bind: %s", c.SQL)
		}
	}
}

func dbcheckConfig() *config.Config {
	cfg := config.Defaults()
	cfg.HasDB = true
	cfg.DB = testDBSettings()
	return cfg
}

// stubDBCheck points both seams at f for the test. It mutates package globals,
// so tests using it must not call t.Parallel.
func stubDBCheck(t *testing.T, f *oratest.Fake) {
	t.Helper()
	oldOpen, oldCheck := openDB, checkDB
	openDB = func(ctx context.Context, s oradb.DBSettings) (*oradb.DB, error) {
		return oradb.OpenDriver(ctx, oratest.DriverName, f.DSN(), s)
	}
	checkDB = func(context.Context, oradb.DBSettings) (string, error) { return "Oracle fake", nil }
	t.Cleanup(func() { openDB, checkDB = oldOpen, oldCheck })
}

// SETUP: -dbcheck against a catalogue with a NOT NULL data column, then
// against the clean one.
// ASSERTION: the exit code is non-zero (ExitStartup) with the finding printed
// for the mismatch, and zero with PASSED for the clean catalogue.
// WOULD HAVE CAUGHT: -dbcheck reporting the round trip only and exiting 0 on a
// schema that will reject every partial-mask insert.
func TestDBCheckExitCodeFollowsTheCatalogue(t *testing.T) {
	dests := defaultDests(t)

	bad := mutate(shippedCatalogue(t), "MD_TOKEN_QUOTE", "OPEN_PRICE", func(c *oradb.CatalogColumn) { c.Nullable = false })
	f, _ := catalogueFake(t, bad)
	stubDBCheck(t, f)
	var out bytes.Buffer
	err := dbCheck(context.Background(), dbcheckConfig(), dests, &out)
	if err == nil || ExitCode(err) != ExitStartup {
		t.Fatalf("exit code = %d (err %v), want %d", ExitCode(err), err, ExitStartup)
	}
	if !AlreadyReported(err) {
		t.Error("the findings are already printed; main must not print them again")
	}
	if !strings.Contains(out.String(), "dbcheck: catalogue: MD_TOKEN_QUOTE.OPEN_PRICE: NOT NULL on a data column") ||
		strings.Contains(out.String(), "PASSED") {
		t.Errorf("output:\n%s", out.String())
	}

	good, _ := catalogueFake(t, shippedCatalogue(t))
	stubDBCheck(t, good)
	out.Reset()
	if err := dbCheck(context.Background(), dbcheckConfig(), dests, &out); err != nil {
		t.Fatalf("clean catalogue: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "dbcheck: PASSED") {
		t.Errorf("output:\n%s", out.String())
	}
}

// SETUP: -dbcheck against a fake with no canned catalogue rows.
// ASSERTION: non-zero ExitStartup, no PASSED. An unreadable catalogue is a
// failed check, not a pass.
func TestDBCheckFailsWhenCatalogueUnreadable(t *testing.T) {
	empty := oratest.New()
	t.Cleanup(empty.Close)
	stubDBCheck(t, empty)
	var out bytes.Buffer
	err := dbCheck(context.Background(), dbcheckConfig(), defaultDests(t), &out)
	if err == nil || ExitCode(err) != ExitStartup || strings.Contains(out.String(), "PASSED") {
		t.Fatalf("err = %v, code %d, output:\n%s", err, ExitCode(err), out.String())
	}
}

// SETUP: a catalogue with a missing column and a short varchar; a logger that
// counts ERROR lines.
// STIMULUS: logCatalogue, as relay runs it after the database opens.
// ASSERTION: it returns (startup proceeds) having logged one ERROR per finding;
// and an unreadable catalogue is one ERROR, not a panic or a return value.
func TestStartupLogsEachMismatchAndProceeds(t *testing.T) {
	cols := mutate(shippedCatalogue(t), "MD_TOKEN_QUOTE", "LOW_PRICE", nil)
	cols = mutate(cols, "MD_SECURITY_MASTER", "SYMBOL", func(c *oradb.CatalogColumn) { c.Bytes = 4 })
	f, _ := catalogueFake(t, cols)

	var buf bytes.Buffer
	lg := obs.NewAppLogger(&buf, obs.LevelInfo, 10000, nil)
	logCatalogue(context.Background(), lg, openCatalogueDB(t, f), defaultDests(t))

	var errs []string
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.Contains(l, "ERROR") {
			errs = append(errs, l)
		}
	}
	if len(errs) != 3 || !strings.Contains(errs[2], "catalogue check: 2 mismatch(es)") ||
		!strings.Contains(errs[0], "MD_TOKEN_QUOTE.LOW_PRICE: column not found") ||
		!strings.Contains(errs[1], "MD_SECURITY_MASTER.SYMBOL: capacity is 4 bytes, want at least 10") {
		t.Fatalf("ERROR lines = %q", errs)
	}

	// Unreadable: no canned rows for the query, so the driver errors.
	empty := oratest.New()
	t.Cleanup(empty.Close)
	buf.Reset()
	logCatalogue(context.Background(), lg, openCatalogueDB(t, empty), defaultDests(t))
	if n := strings.Count(buf.String(), "ERROR"); n != 1 || !strings.Contains(buf.String(), "catalogue check could not run") {
		t.Errorf("unreadable catalogue logged:\n%s", buf.String())
	}
}
