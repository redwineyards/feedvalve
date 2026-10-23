package oradb

import (
	"context"
	"database/sql/driver"
	"reflect"
	"strings"
	"sync"
	"testing"

	"feedvalve/internal/oradb/oratest"
)

var catalogCols = []string{"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "DATA_PRECISION", "DATA_LENGTH", "NULLABLE"}

func openFake(t *testing.T, f *oratest.Fake) *DB {
	t.Helper()
	s := DBSettings{Host: "h", Port: 1, Service: "S", User: "U", ConnectTimeoutSec: 5,
		QueryTimeoutSec: 5, ConnMaxLifetimeMinutes: 30, MaxOpenConns: 2, MaxIdleConns: 1}
	d, err := OpenDriver(context.Background(), oratest.DriverName, f.DSN(), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// SETUP: a fake serving two rows for the one catalogue query text.
// STIMULUS: Catalog for one table.
// ASSERTION: the rows come back neutral, NULL precision as 0, NULLABLE 'Y'/'N'
// as bool, in order -- against a hand-written expected slice.
func TestCatalogReturnsNeutralRows(t *testing.T) {
	f := oratest.New()
	f.SetRows(CatalogQuery, catalogCols, [][]driver.Value{
		{"T", "TOKEN", "NUMBER", int64(10), int64(22), "N"},
		{"T", "NOTE", "VARCHAR2", nil, int64(8), "Y"},
	})
	got, err := Catalog(context.Background(), openFake(t, f), []string{"T"})
	if err != nil {
		t.Fatal(err)
	}
	want := []CatalogColumn{
		{Table: "T", Name: "TOKEN", DataType: "NUMBER", Precision: 10, Bytes: 22, Nullable: false},
		{Table: "T", Name: "NOTE", DataType: "VARCHAR2", Precision: 0, Bytes: 8, Nullable: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
}

// SETUP: a script recording every call the fake sees.
// STIMULUS: Catalog for two tables.
// ASSERTION: one query per table, each with the table as the single :1 bind,
// and the text filters on the current schema owner with no '?' placeholder.
// WOULD HAVE CAUGHT: dropping the owner filter, which lets a same-named table
// in another schema stand in for ours; a '?' bind (ORA-00936).
func TestCatalogQueryIsOwnerFilteredWithPositionalBinds(t *testing.T) {
	f := oratest.New()
	f.SetRows(CatalogQuery, catalogCols, nil)
	var mu sync.Mutex
	var calls []oratest.Call
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == CatalogQuery {
			mu.Lock()
			calls = append(calls, c)
			mu.Unlock()
		}
		return oratest.Result{}
	})
	if _, err := Catalog(context.Background(), openFake(t, f), []string{"A", "B"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("queries = %d, want 2", len(calls))
	}
	for i, want := range []string{"A", "B"} {
		if !reflect.DeepEqual(calls[i].Args, []driver.Value{want}) {
			t.Errorf("query %d args = %v, want [%s]", i, calls[i].Args, want)
		}
	}
	upper := strings.ToUpper(calls[0].SQL)
	for _, must := range []string{"OWNER = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')", "TABLE_NAME = :1", "ALL_TAB_COLUMNS"} {
		if !strings.Contains(upper, must) {
			t.Errorf("query lacks %q: %s", must, calls[0].SQL)
		}
	}
	if strings.Contains(calls[0].SQL, "?") {
		t.Errorf("query uses ?: %s", calls[0].SQL)
	}
}

// ASSERTION: a row naming another table is not attributed to the one asked
// for, so a missing table stays missing.
func TestCatalogKeepsOnlyTheRequestedTable(t *testing.T) {
	f := oratest.New()
	f.SetRows(CatalogQuery, catalogCols, [][]driver.Value{
		{"OTHER", "X", "NUMBER", int64(1), int64(22), "Y"},
	})
	got, err := Catalog(context.Background(), openFake(t, f), []string{"T"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want none", got)
	}
}

// ASSERTION: a driver error is returned, not swallowed, and names the table.
func TestCatalogReportsQueryErrors(t *testing.T) {
	f := oratest.New()
	f.SetRows(CatalogQuery, catalogCols, nil)
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == CatalogQuery {
			return oratest.Result{Err: oratest.ORA(942)}
		}
		return oratest.Result{}
	})
	_, err := Catalog(context.Background(), openFake(t, f), []string{"T"})
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "catalogue for T") || !strings.Contains(err.Error(), "ORA-942") {
		t.Errorf("error lost the table or the cause: %v", err)
	}
}
