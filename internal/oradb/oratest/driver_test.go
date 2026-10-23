package oratest_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sijms/go-ora/v2/network"

	"feedvalve/internal/oradb/oratest"
)

const mergeSQL = "MERGE INTO t USING dual ON (t.k = :1) WHEN MATCHED THEN UPDATE SET v = :2"

func open(t *testing.T) (*oratest.Fake, *sql.DB) {
	t.Helper()
	f := oratest.New()
	t.Cleanup(f.Close)
	db, err := sql.Open(oratest.DriverName, f.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return f, db
}

func oraCode(err error) int {
	var oe *network.OracleError
	if errors.As(err, &oe) {
		return oe.ErrCode
	}
	return -1
}

// An array bind arrives as one slice per placeholder, with the Go types the
// writer will bind, and RowsAffected defaults to the array length.
func TestArrayArgsArriveAsOneSlicePerPlaceholder(t *testing.T) {
	f, db := open(t)
	keys := []int64{10, 20, 30}
	vals := []sql.NullString{{String: "a", Valid: true}, {}, {String: "c", Valid: true}}

	res, err := db.ExecContext(context.Background(), mergeSQL, keys, vals)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 3 {
		t.Errorf("RowsAffected = %d, want 3", n)
	}
	execs := f.Execs()
	if len(execs) != 1 {
		t.Fatalf("recorded %d execs, want 1", len(execs))
	}
	if execs[0].Rows != 3 || len(execs[0].Args) != 2 {
		t.Fatalf("Rows=%d len(Args)=%d, want 3 and 2", execs[0].Rows, len(execs[0].Args))
	}
	if !reflect.DeepEqual(execs[0].Args[0], keys) || !reflect.DeepEqual(execs[0].Args[1], vals) {
		t.Errorf("args = %#v, want the two slices untouched", execs[0].Args)
	}
}

func TestAllFourArrayShapesAreAccepted(t *testing.T) {
	_, db := open(t)
	_, err := db.ExecContext(context.Background(), "x",
		[]int64{1}, []sql.NullString{{}}, []sql.NullFloat64{{}}, []sql.NullTime{{}})
	if err != nil {
		t.Fatalf("array shapes rejected: %v", err)
	}
}

// ORA-00001 on exactly the Nth execution of one statement, as the typed error
// go-ora would return.
func TestScriptedUniqueViolationAtExecN(t *testing.T) {
	f, db := open(t)
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == mergeSQL && c.N == 3 {
			return oratest.Result{Err: oratest.ORA(1), FailRow: 2}
		}
		return oratest.Result{}
	})

	for n := 1; n <= 4; n++ {
		_, err := db.ExecContext(context.Background(), mergeSQL, []int64{1, 2, 3}, []int64{4, 5, 6})
		if (n == 3) != (err != nil) {
			t.Fatalf("exec %d: err = %v, want failure only on exec 3", n, err)
		}
		if n == 3 && oraCode(err) != 1 {
			t.Fatalf("exec 3 error %v is not *network.OracleError ORA-00001", err)
		}
	}
	execs := f.Execs()
	if len(execs) != 4 || execs[2].FailRow != 2 || execs[2].Err == nil || execs[3].Err != nil {
		t.Errorf("exec log = %+v, want exec 3 failed at row 2 and exec 4 clean", execs)
	}
}

func TestScriptedScalarFailureLeavesOtherStatementsAlone(t *testing.T) {
	f, db := open(t)
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == "UPDATE a SET x = :1" {
			return oratest.Result{Err: oratest.ORA(942)}
		}
		return oratest.Result{}
	})

	if _, err := db.ExecContext(context.Background(), "UPDATE a SET x = :1", int64(7)); oraCode(err) != 942 {
		t.Errorf("err = %v, want ORA-00942", err)
	}
	if _, err := db.ExecContext(context.Background(), "UPDATE b SET x = :1", int64(7)); err != nil {
		t.Errorf("other statement failed: %v", err)
	}
}

// One driver Prepare per statement per connection, however often the
// statement runs, and a second connection prepares afresh.
func TestPrepareCountsPerConnection(t *testing.T) {
	f, db := open(t)
	db.SetMaxOpenConns(2)
	ctx := context.Background()

	st, err := db.PrepareContext(ctx, mergeSQL)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := st.ExecContext(ctx, []int64{1}, []int64{2}); err != nil {
			t.Fatal(err)
		}
	}
	got := f.Counts().Prepares
	if len(got) != 1 {
		t.Fatalf("Prepares = %v, want exactly one connection", got)
	}
	for _, n := range got {
		if n != 1 {
			t.Fatalf("Prepares = %v, want 1 on the one connection", got)
		}
	}

	// Hold the first connection so the next exec must use a second one.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := st.ExecContext(ctx, []int64{1}, []int64{2}); err != nil {
		t.Fatal(err)
	}
	got = f.Counts().Prepares
	if len(got) != 2 {
		t.Fatalf("Prepares = %v, want one entry on each of two connections", got)
	}
	for c, n := range got {
		if n != 1 {
			t.Errorf("connection %d prepared %d times, want 1", c, n)
		}
	}
	if n := f.Counts().Execs[mergeSQL]; n != 4 {
		t.Errorf("Execs = %d, want 4", n)
	}
}

// A dropped connection is evicted (closed, not reused) and database/sql
// retries on a fresh one.
func TestErrBadConnEvictsTheConnection(t *testing.T) {
	f, db := open(t)
	db.SetMaxOpenConns(1)
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == mergeSQL && c.N == 1 {
			return oratest.Result{Drop: true}
		}
		return oratest.Result{}
	})

	if _, err := db.ExecContext(context.Background(), mergeSQL, []int64{1}, []int64{2}); err != nil {
		t.Fatalf("exec after drop: %v (database/sql should have retried on a new connection)", err)
	}
	c := f.Counts()
	if c.Opens != 2 || c.Closes != 1 {
		t.Errorf("Opens=%d Closes=%d, want 2 and 1: the dead connection must be closed and replaced", c.Opens, c.Closes)
	}
	if c.Execs[mergeSQL] != 2 {
		t.Errorf("Execs = %d, want 2 (the dropped attempt and the retry)", c.Execs[mergeSQL])
	}
}

// A drop that surfaces ORA-03113 instead of ErrBadConn still kills the
// connection, and the typed error reaches the caller.
func TestDropWithORA03113(t *testing.T) {
	f, db := open(t)
	db.SetMaxOpenConns(1)
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.N == 1 && c.SQL == "q" {
			return oratest.Result{Drop: true, Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	_, err := db.ExecContext(context.Background(), "q")
	if oraCode(err) != 3113 {
		t.Fatalf("err = %v, want ORA-03113", err)
	}
	if _, err := db.ExecContext(context.Background(), "q"); err != nil {
		t.Fatal(err)
	}
	if c := f.Counts(); c.Opens != 2 || c.Closes != 1 {
		t.Errorf("Opens=%d Closes=%d, want 2 and 1", c.Opens, c.Closes)
	}
}

func TestLatencyIsAbortedByContext(t *testing.T) {
	f, db := open(t)
	f.SetLatency(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := db.ExecContext(ctx, mergeSQL, []int64{1}, []int64{2})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %v: latency was not interrupted by the context", d)
	}
}

func TestLatencyIsHonouredWhenContextAllows(t *testing.T) {
	f, db := open(t)
	f.SetLatency(40 * time.Millisecond)
	start := time.Now()
	if _, err := db.ExecContext(context.Background(), "q"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 40*time.Millisecond {
		t.Errorf("returned after %v, want at least the 40ms latency", d)
	}
}

func TestBeginCommitRollbackCounts(t *testing.T) {
	f, db := open(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "q"); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	c := f.Counts()
	if c.Begins != 3 || c.Commits != 2 || c.Rollbacks != 1 {
		t.Errorf("Begins=%d Commits=%d Rollbacks=%d, want 3, 2, 1", c.Begins, c.Commits, c.Rollbacks)
	}
}

func TestScriptedCommitFailure(t *testing.T) {
	f, db := open(t)
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpCommit {
			return oratest.Result{Err: oratest.ORA(3114)}
		}
		return oratest.Result{}
	})
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); oraCode(err) != 3114 {
		t.Errorf("Commit err = %v, want ORA-03114", err)
	}
	if c := f.Counts(); c.Commits != 0 {
		t.Errorf("Commits = %d after a failed commit, want 0", c.Commits)
	}
}

func TestPingAndPingFailure(t *testing.T) {
	f, db := open(t)
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpPing {
			return oratest.Result{Err: oratest.ORA(12541)}
		}
		return oratest.Result{}
	})
	if err := db.PingContext(context.Background()); oraCode(err) != 12541 {
		t.Errorf("Ping err = %v, want ORA-12541", err)
	}
}

func TestCannedRows(t *testing.T) {
	f, db := open(t)
	f.SetRows("SELECT a, b FROM t", []string{"a", "b"}, [][]driver.Value{{"x", int64(1)}, {"y", int64(2)}})

	r, err := db.QueryContext(context.Background(), "SELECT a, b FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got []string
	for r.Next() {
		var a string
		var b int64
		if err := r.Scan(&a, &b); err != nil {
			t.Fatal(err)
		}
		got = append(got, a)
	}
	if !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Errorf("rows = %v, want [x y]", got)
	}
	if _, err := db.QueryContext(context.Background(), "SELECT unknown"); err == nil {
		t.Error("query with no canned rows succeeded; want an error")
	}
}
