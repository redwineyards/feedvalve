package oradb

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/oradb/oratest"
)

const upd = "UPDATE t SET a = :1"

func oneConn() DBSettings {
	s := sample()
	s.MaxOpenConns, s.MaxIdleConns = 1, 1
	return s
}

func TestOneConnectionIsPinnedAcrossTwoTransactions(t *testing.T) {
	d, f := fakeDB(t, poolSettings())
	ss, err := d.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		err := ss.ExecTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, upd, int64(i))
			return err
		})
		if err != nil {
			t.Fatalf("ExecTx %d: %v", i, err)
		}
	}
	ex := f.Execs()
	if len(ex) != 2 || ex[0].Conn != ex[1].Conn {
		t.Fatalf("execs = %+v, want two on one connection", ex)
	}
	c := f.Counts()
	if c.Begins != 2 || c.Commits != 2 || c.Rollbacks != 0 {
		t.Errorf("begin/commit/rollback = %d/%d/%d, want 2/2/0", c.Begins, c.Commits, c.Rollbacks)
	}
	if err := ss.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestExecTxRollsBackOnError(t *testing.T) {
	d, f := fakeDB(t, poolSettings())
	ss, _ := d.Acquire(context.Background())
	defer ss.Release()
	boom := errors.New("boom")
	err := ss.ExecTx(context.Background(), func(context.Context, *sql.Tx) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	c := f.Counts()
	if c.Commits != 0 || c.Rollbacks != 1 {
		t.Errorf("commit/rollback = %d/%d, want 0/1", c.Commits, c.Rollbacks)
	}
}

func TestExecTxRollsBackOnPanicAndRepanics(t *testing.T) {
	d, f := fakeDB(t, poolSettings())
	ss, _ := d.Acquire(context.Background())
	defer ss.Release()
	defer func() {
		if p := recover(); p != "kaboom" {
			t.Fatalf("recovered %v, want the original panic value", p)
		}
		c := f.Counts()
		if c.Commits != 0 || c.Rollbacks != 1 {
			t.Errorf("commit/rollback = %d/%d, want 0/1", c.Commits, c.Rollbacks)
		}
	}()
	_ = ss.ExecTx(context.Background(), func(context.Context, *sql.Tx) error { panic("kaboom") })
	t.Fatal("ExecTx swallowed the panic")
}

func TestExecTxCommitFailureIsReturned(t *testing.T) {
	d, f := fakeDB(t, poolSettings())
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpCommit {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	ss, _ := d.Acquire(context.Background())
	defer ss.Release()
	err := ss.ExecTx(context.Background(), func(context.Context, *sql.Tx) error { return nil })
	e, ok := d.Classify(err)
	if !ok || e.Class != ErrSession || e.Code != 3113 {
		t.Errorf("Classify(%v) = %+v, %v; want Session 3113", err, e, ok)
	}
}

func TestAcquireIsBoundedByConnectTimeout(t *testing.T) {
	s := oneConn()
	s.ConnectTimeoutSec = 1
	d, _ := fakeDB(t, s)
	held, err := d.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	// The pool is saturated; without the timeout this blocks forever.
	start := time.Now()
	_, err = d.Acquire(context.Background())
	if err == nil {
		t.Fatal("second Acquire succeeded on a saturated one-connection pool")
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Errorf("Acquire took %v; ConnectTimeout (1s) did not bound it", el)
	}
	if e, ok := d.Classify(err); !ok || e.Class != ErrSession {
		t.Errorf("Classify(%v) = %+v, %v; want Session (deadline)", err, e, ok)
	}
}

func TestExecTxBeginFailureIsReturnedUnwrapped(t *testing.T) {
	d, f := fakeDB(t, poolSettings())
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpBegin {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	ss, _ := d.Acquire(context.Background())
	defer ss.Release()
	called := false
	err := ss.ExecTx(context.Background(), func(context.Context, *sql.Tx) error { called = true; return nil })
	if called {
		t.Error("fn ran though Begin failed")
	}
	e, ok := d.Classify(err)
	if !ok || e.Class != ErrSession || e.Code != 3113 {
		t.Errorf("Classify(%v) = %+v, %v; want Session 3113", err, e, ok)
	}
	if c := f.Counts(); c.Rollbacks != 0 || c.Commits != 0 {
		t.Errorf("commit/rollback = %d/%d, want 0/0 after a failed Begin", c.Commits, c.Rollbacks)
	}
}

func TestExecTxReturnsFnErrorWhenRollbackAlsoFails(t *testing.T) {
	d, f := fakeDB(t, poolSettings())
	f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpRollback {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	ss, _ := d.Acquire(context.Background())
	defer ss.Release()
	boom := errors.New("boom")
	err := ss.ExecTx(context.Background(), func(context.Context, *sql.Tx) error { return boom })
	if err != boom {
		t.Errorf("err = %v, want fn's own error unwrapped", err)
	}
}

func TestExecTxCancelInsideFnRollsBack(t *testing.T) {
	d, f := fakeDB(t, poolSettings())
	ss, _ := d.Acquire(context.Background())
	defer ss.Release()
	ctx, cancel := context.WithCancel(context.Background())
	err := ss.ExecTx(ctx, func(context.Context, *sql.Tx) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if c := f.Counts(); c.Commits != 0 {
		t.Errorf("commits = %d, want 0 after cancellation", c.Commits)
	}
}

func TestExpiredUsesInjectedNow(t *testing.T) {
	s := poolSettings()
	s.ConnMaxLifetimeMinutes = 30
	d, _ := fakeDB(t, s)
	ss, _ := d.Acquire(context.Background())
	defer ss.Release()
	at := ss.acquiredAt
	// "Held for longer than ConnMaxLifetime" (LLD 2.6): exactly 30m is not yet.
	for _, tc := range []struct {
		after time.Duration
		want  bool
	}{
		{0, false}, {29*time.Minute + 59*time.Second, false},
		{30 * time.Minute, false}, {30*time.Minute + time.Second, true},
		{time.Hour, true},
	} {
		if got := ss.Expired(at.Add(tc.after)); got != tc.want {
			t.Errorf("Expired after %v = %v, want %v", tc.after, got, tc.want)
		}
	}
	if !strings.Contains(at.String(), "m=") {
		t.Error("acquiredAt must carry a monotonic reading")
	}
	ss.lifetime = 0
	if ss.Expired(at.Add(100 * time.Hour)) {
		t.Error("zero lifetime means never retired, as in database/sql")
	}
}

func TestReleaseReturnsTheConnection(t *testing.T) {
	d, _ := fakeDB(t, oneConn())
	ss, err := d.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().InUse; got != 1 {
		t.Fatalf("InUse after Acquire = %d, want 1", got)
	}
	if err := ss.Release(); err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().InUse; got != 0 {
		t.Errorf("InUse after Release = %d, want 0", got)
	}
	// With a one-connection pool a second Acquire only succeeds if Release
	// really returned it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ss2, err := d.Acquire(ctx)
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	ss2.Release()
}

// TestADRW02_OneDriverPreparePerConnection is ADR-W02 part 1: a statement
// prepared on the DB and executed through tx.StmtContext on a pinned session
// costs one driver Prepare per connection, however many transactions use it.
func TestADRW02_OneDriverPreparePerConnection(t *testing.T) {
	d, f := fakeDB(t, oneConn())
	stmt, err := d.Prepare(context.Background(), upd)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	ss, _ := d.Acquire(context.Background())
	defer ss.Release()
	for i := 0; i < 3; i++ {
		err := ss.ExecTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, int64(i))
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	c := f.Counts()
	if c.Execs[upd] != 3 {
		t.Errorf("execs = %d, want 3", c.Execs[upd])
	}
	if len(c.Prepares) != 1 || c.Prepares[1] != 1 {
		t.Errorf("driver prepares per connection = %v, want map[1:1]", c.Prepares)
	}
}
