package oratest_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"feedvalve/internal/oradb/oratest"
)

func TestRecordedArgsAreCopies(t *testing.T) {
	f, db := open(t)
	keys := []int64{1, 2}
	if _, err := db.ExecContext(context.Background(), mergeSQL, keys, []int64{3, 4}); err != nil {
		t.Fatal(err)
	}
	keys[0] = 99 // the writer refills its arena
	if got := f.Execs()[0].Args[0]; !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Errorf("recorded args = %v, want the values at exec time", got)
	}
}

func TestSetRecordArgsFalseDropsArgs(t *testing.T) {
	f, db := open(t)
	f.SetRecordArgs(false)
	if _, err := db.ExecContext(context.Background(), mergeSQL, []int64{1}, []int64{2}); err != nil {
		t.Fatal(err)
	}
	e := f.Execs()
	if len(e) != 1 || e[0].Args != nil || e[0].Rows != 1 {
		t.Errorf("exec log = %+v, want one record with Rows=1 and no Args", e)
	}
}

func TestAffectOverridesRowsAffected(t *testing.T) {
	f, db := open(t)
	f.SetScript(func(oratest.Call) oratest.Result { return oratest.Result{Affected: oratest.Affect(0)} })
	r, err := db.ExecContext(context.Background(), mergeSQL, []int64{1, 2}, []int64{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := r.RowsAffected(); n != 0 {
		t.Errorf("RowsAffected = %d, want 0", n)
	}
}

func TestDelayIsAbortedByContext(t *testing.T) {
	f, db := open(t)
	f.SetScript(func(oratest.Call) oratest.Result { return oratest.Result{Delay: time.Hour} })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := db.ExecContext(ctx, "q"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestOpenFailureSurfacesFromPing(t *testing.T) {
	f, db := open(t)
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpOpen {
			return oratest.Result{Err: oratest.ORA(12541)}
		}
		return oratest.Result{}
	})
	if err := db.PingContext(context.Background()); oraCode(err) != 12541 {
		t.Errorf("Ping err = %v, want ORA-12541", err)
	}
}

func TestTotalIsMonotonicAcrossSQL(t *testing.T) {
	f, db := open(t)
	var totals, ns []int
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == "a" || c.SQL == "b" {
			totals, ns = append(totals, c.Total), append(ns, c.N)
		}
		return oratest.Result{}
	})
	for _, q := range []string{"a", "b", "a"} {
		if _, err := db.ExecContext(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	if !(totals[0] < totals[1] && totals[1] < totals[2]) || ns[0] != 1 || ns[1] != 1 || ns[2] != 2 {
		t.Errorf("totals=%v ns=%v, want strictly increasing totals and N 1,1,2", totals, ns)
	}
}

func TestCommitAndRollbackIgnoreLatency(t *testing.T) {
	f, db := open(t)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	f.SetLatency(time.Hour)
	start := time.Now()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("rollback waited out the latency")
	}
}
