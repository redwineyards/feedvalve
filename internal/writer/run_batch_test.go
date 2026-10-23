package writer

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
)

// rig is a runner wired to an oratest fake, a frozen registry and a session.
type rig struct {
	t     *testing.T
	f     *oratest.Fake
	db    *oradb.DB
	reg   *obs.Reg
	r     *runner
	s     *oradb.Session
	unrep *obs.Counter

	clockCalls int // calls made to the runner's clock
}

const rigCeiling = 64

func newRig(t *testing.T) *rig {
	t.Helper()
	db, f := fakeOra(t)
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: 1})
	in := NewInstruments(reg, 3)
	in.ResolveCache(reg, 1, 3)
	run := NewRunInstruments(reg, 3)
	unrep := reg.CounterAt(obs.IDBatchRowUnreported, 0)
	reg.Freeze()
	g := &rig{t: t, f: f, db: db, reg: reg, unrep: unrep}
	cache := newStmtCache(db.Prepare, destSet(t), 64, in, 0)
	t.Cleanup(cache.closeAll)
	g.r = newRunner(0, db, destSet(t), cache, rigCeiling, 30*time.Second)
	g.r.inst, g.r.run = in, run
	g.r.oracleErr = func(code int) { reg.OracleError(code).Add(1) }
	g.r.now = func() time.Time {
		g.clockCalls++
		return testUpdatedAt.Add(time.Duration(g.clockCalls-1) * time.Second)
	}
	s, err := db.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	g.s = s
	return g
}

func (g *rig) rowsWritten(d int) uint64 { return g.r.run.RowsWritten[d].Load() }
func (g *rig) panics() uint64           { return g.r.run.Panics.Load() }
func (g *rig) oraErr(code int) uint64   { return g.reg.OracleError(code).Load() }

// standard returns a batch of rows (mask, key): destination 0 holds masks 3
// and 7 out of key order, destination 1 one mask, destination 2 one row.
func standard() *model.Batch {
	return mkBatch([3][][2]uint64{
		{{7, 30}, {3, 20}, {7, 10}, {3, 5}},
		{{0x1FF, 9}, {0x1FF, 8}},
		{{0x3F, 1}},
	})
}

func assertNoPending(t *testing.T, b *model.Batch) {
	t.Helper()
	failed := false
	for i, r := range b.Active() {
		if r.Outcome == model.OutcomePending {
			t.Errorf("row %d is still Pending (Err=%v)", i, b.Err)
		}
		failed = failed || r.Outcome == model.OutcomeFailed
	}
	if failed && b.Err == nil {
		t.Errorf("a row is Failed but Err is nil")
	}
}

func outcomes(b *model.Batch) []model.RowOutcome {
	var o []model.RowOutcome
	for _, r := range b.Active() {
		o = append(o, r.Outcome)
	}
	return o
}

func all(n int, o model.RowOutcome) []model.RowOutcome {
	out := make([]model.RowOutcome, n)
	for i := range out {
		out[i] = o
	}
	return out
}

// TestG_ROW_1_RunBatchCommitsEveryRowOnce is the success path.
//
// SETUP: 4 + 2 + 1 rows over three destinations, masks 3 and 7 on destination
// 0, keys deliberately out of order.
// STIMULUS: one runBatch.
// ASSERTION: one Begin and one Commit; exactly four array execs of the hand-
// written row counts, in destination order, masks ascending; the key array of
// each exec ascending; one updated_at for the whole batch; every row Committed
// with Err nil; rows_written per destination.
// WOULD HAVE CAUGHT: a transaction per destination; per-row execs on the
// common path; Committed set before Commit; keys bound in batch order.
func TestG_ROW_1_RunBatchCommitsEveryRowOnce(t *testing.T) {
	g := newRig(t)
	b := standard()
	res := g.r.runBatch(context.Background(), g.s, b)

	if g.clockCalls != 1 {
		t.Errorf("clock read %d times, want once per batch", g.clockCalls)
	}
	if b.Err != nil || res.failure != nil || res.panicked {
		t.Fatalf("Err=%v failure=%v panicked=%v", b.Err, res.failure, res.panicked)
	}
	if got, want := outcomes(b), all(7, model.OutcomeCommitted); !reflect.DeepEqual(got, want) {
		t.Fatalf("outcomes %v, want all committed", got)
	}
	c := g.f.Counts()
	if c.Begins != 1 || c.Commits != 1 || c.Rollbacks != 0 {
		t.Errorf("begins/commits/rollbacks = %d/%d/%d, want 1/1/0", c.Begins, c.Commits, c.Rollbacks)
	}
	execs := g.f.Execs()
	wantRows := []int{2, 2, 2, 1}
	wantKeys := [][]int64{{5, 20}, {10, 30}, {8, 9}, {1}}
	if len(execs) != len(wantRows) {
		t.Fatalf("%d execs, want %d", len(execs), len(wantRows))
	}
	for i, e := range execs {
		if e.Rows != wantRows[i] {
			t.Errorf("exec %d carried %d rows, want %d", i, e.Rows, wantRows[i])
		}
		if keys, _ := e.Args[0].([]int64); !reflect.DeepEqual(keys, wantKeys[i]) {
			t.Errorf("exec %d keys %v, want %v", i, e.Args[0], wantKeys[i])
		}
		ts, ok := e.Args[len(e.Args)-1].([]sql.NullTime)
		if !ok || len(ts) != e.Rows {
			t.Fatalf("exec %d last arg %T", i, e.Args[len(e.Args)-1])
		}
		for _, v := range ts {
			if !v.Valid || !v.Time.Equal(testUpdatedAt) {
				t.Errorf("exec %d updated_at %v, want %v", i, v, testUpdatedAt)
			}
		}
	}
	for d, want := range []uint64{4, 2, 1} {
		if got := g.rowsWritten(d); got != want {
			t.Errorf("rows_written{%d} = %d, want %d", d, got, want)
		}
	}
	if g.panics() != 0 || res.panicked {
		t.Errorf("panics=%d panicked=%v on a clean batch", g.panics(), res.panicked)
	}
	if g.r.run.Busy.Load() != 0 {
		t.Errorf("writers_busy = %d after the batch", g.r.run.Busy.Load())
	}
}

// TestRunBatchCommitFailureFailsEveryRow: the commit is the only moment a row
// becomes durable, so a failed Commit must leave nothing Committed or counted.
//
// WOULD HAVE CAUGHT: marking rows Committed as each exec returned; counting
// rows_written before Commit.
func TestRunBatchCommitFailureFailsEveryRow(t *testing.T) {
	g := newRig(t)
	g.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.SQL == oratest.OpCommit {
			return oratest.Result{Err: oratest.ORA(3113)}
		}
		return oratest.Result{}
	})
	b := standard()
	res := g.r.runBatch(context.Background(), g.s, b)

	if got, want := outcomes(b), all(7, model.OutcomeFailed); !reflect.DeepEqual(got, want) {
		t.Fatalf("outcomes %v, want all failed", got)
	}
	for i, r := range b.Active() {
		if r.ErrCode != 3113 {
			t.Errorf("row %d ErrCode %d, want 3113", i, r.ErrCode)
		}
	}
	if res.failure == nil || res.failure.Class != oradb.ErrSession || res.failure.Code != 3113 {
		t.Errorf("failure = %+v, want session ORA-3113", res.failure)
	}
	var oe *oradb.Error
	if !errors.As(b.Err, &oe) || oe.Code != 3113 {
		t.Errorf("Err = %v, want *oradb.Error 3113", b.Err)
	}
	for d := 0; d < 3; d++ {
		if g.rowsWritten(d) != 0 {
			t.Errorf("rows_written{%d} = %d after a failed commit", d, g.rowsWritten(d))
		}
	}
	if c := g.f.Counts(); c.Commits != 0 {
		t.Errorf("commits = %d", c.Commits)
	}
	if g.oraErr(3113) != 1 {
		t.Errorf("oracle_error{3113} = %d, want 1", g.oraErr(3113))
	}
}

// TestRunBatchPanicSweep: a panic at bind, exec or commit leaves every row
// terminal, sets Err, counts once, retires the session and returns once.
//
// WOULD HAVE CAUGHT: no recover outside ExecTx (the writer dies); a sweep
// that runs only when Err was set by a classified error; a missing retire.
func TestRunBatchPanicSweep(t *testing.T) {
	cases := []struct {
		name  string
		setup func(g *rig)
	}{
		{"bind", func(g *rig) {
			// A binder whose column offset points past the image.
			k := stmtKey{Dest: 0, Mask: 3}
			bd, err := g.r.binder(k)
			if err != nil {
				g.t.Fatal(err)
			}
			bd.cols[1].bind.Offset = 65535
		}},
		{"prepare", func(g *rig) {
			// Panics in the writer's own code on the way to the exec. A panic
			// raised inside a driver call is NOT covered: database/sql keeps
			// the transaction's close lock across that call, so ExecTx's
			// rollback would block (known risk, see runBatch).
			g.r.cache.prepare = func(context.Context, string) (*sql.Stmt, error) { panic("prepare boom") }
		}},
		{"commit", func(g *rig) {
			g.f.SetScript(func(c oratest.Call) oratest.Result {
				if c.SQL == oratest.OpCommit {
					panic("commit boom")
				}
				return oratest.Result{}
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := newRig(t)
			tc.setup(g)
			b := standard()
			calls := 0
			var res attempt
			func() {
				defer func() {
					if p := recover(); p != nil {
						t.Fatalf("panic escaped runBatch: %v", p)
					}
				}()
				calls++
				res = g.r.runBatch(context.Background(), g.s, b)
			}()
			if calls != 1 {
				t.Fatalf("runBatch ran %d times", calls)
			}
			if b.Err == nil || b.Err.Error() != "writer panic" {
				t.Errorf("Err = %v, want writer panic", b.Err)
			}
			if got, want := outcomes(b), all(7, model.OutcomeFailed); !reflect.DeepEqual(got, want) {
				t.Errorf("outcomes %v, want all failed", got)
			}
			if g.panics() != 1 || !res.panicked {
				t.Errorf("panics=%d panicked=%v, want 1/true", g.panics(), res.panicked)
			}
			if g.r.run.Busy.Load() != 0 {
				t.Errorf("writers_busy = %d after a panic", g.r.run.Busy.Load())
			}
			for d := 0; d < 3; d++ {
				if g.rowsWritten(d) != 0 {
					t.Errorf("rows_written{%d} = %d", d, g.rowsWritten(d))
				}
			}
		})
	}
}

// TestRunBatchPanicAfterCommitKeepsCommitted: the sweep never overwrites a
// terminal outcome. A panic once the rows are durable must not turn them
// Failed, or the shard would re-dirty and rewrite committed rows.
//
// WOULD HAVE CAUGHT: a sweep that sets every row Failed when Err != nil.
func TestRunBatchPanicAfterCommitKeepsCommitted(t *testing.T) {
	g := newRig(t)
	g.r.afterCommit = func() { panic("after commit") }
	b := standard()
	res := g.r.runBatch(context.Background(), g.s, b)

	if b.Err == nil || b.Err.Error() != "writer panic" {
		t.Errorf("Err = %v, want writer panic", b.Err)
	}
	if got, want := outcomes(b), all(7, model.OutcomeCommitted); !reflect.DeepEqual(got, want) {
		t.Errorf("outcomes %v, want all committed", got)
	}
	if g.panics() != 1 || !res.panicked {
		t.Errorf("panics=%d panicked=%v, want 1/true", g.panics(), res.panicked)
	}
}

// TestRunBatchSessionAndStatementErrors: a begin failure and a failure on the
// second exec each fail every row with the class code, nothing counted
// written, and the class reaches the caller for the retry decision.
func TestRunBatchSessionAndStatementErrors(t *testing.T) {
	cases := []struct {
		name   string
		script oratest.Script
		class  oradb.ErrClass
		code   int
	}{
		{"begin drop", func(c oratest.Call) oratest.Result {
			if c.SQL == oratest.OpBegin {
				return oratest.Result{Err: oratest.ORA(12541)}
			}
			return oratest.Result{}
		}, oradb.ErrSession, 12541},
		{"second exec deadlock", func(c oratest.Call) oratest.Result {
			if c.Total > 0 && c.Rows > 0 && c.N == 1 && c.Args != nil {
				if keys, _ := c.Args[0].([]int64); len(keys) == 2 && keys[0] == 10 {
					return oratest.Result{Err: oratest.ORA(60)}
				}
			}
			return oratest.Result{}
		}, oradb.ErrDeadlock, 60},
		{"unknown code", func(c oratest.Call) oratest.Result {
			if c.Rows > 0 {
				return oratest.Result{Err: oratest.ORA(24381)}
			}
			return oratest.Result{}
		}, oradb.ErrUnknown, 24381},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := newRig(t)
			g.f.SetScript(tc.script)
			b := standard()
			res := g.r.runBatch(context.Background(), g.s, b)
			if res.failure == nil || res.failure.Class != tc.class || res.failure.Code != tc.code {
				t.Fatalf("failure = %+v, want class %v code %d", res.failure, tc.class, tc.code)
			}
			if got, want := outcomes(b), all(7, model.OutcomeFailed); !reflect.DeepEqual(got, want) {
				t.Errorf("outcomes %v", got)
			}
			for i, r := range b.Active() {
				if int(r.ErrCode) != tc.code {
					t.Errorf("row %d ErrCode %d, want %d", i, r.ErrCode, tc.code)
				}
			}
			if g.oraErr(tc.code) != 1 {
				t.Errorf("oracle_error{%d} = %d, want 1", tc.code, g.oraErr(tc.code))
			}
			for d := 0; d < 3; d++ {
				if g.rowsWritten(d) != 0 {
					t.Errorf("rows_written{%d} = %d", d, g.rowsWritten(d))
				}
			}
			if g.panics() != 0 {
				t.Errorf("panics = %d", g.panics())
			}
		})
	}
}

// TestRunBatchCanceledIsShutdownNotFailure: a cancelled context fails the
// rows (so the drain re-dirties them) but counts nothing and is no panic.
//
// WOULD HAVE CAUGHT: counting oracle_error or a write failure on shutdown.
func TestRunBatchCanceledIsShutdownNotFailure(t *testing.T) {
	for _, mid := range []bool{false, true} {
		name := "before"
		if mid {
			name = "during exec"
		}
		t.Run(name, func(t *testing.T) {
			g := newRig(t)
			counted := false
			g.r.oracleErr = func(int) { counted = true }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mid {
				g.f.SetScript(func(c oratest.Call) oratest.Result {
					if c.Rows > 0 {
						go func() { time.Sleep(20 * time.Millisecond); cancel() }()
						return oratest.Result{Delay: 5 * time.Second}
					}
					return oratest.Result{}
				})
			} else {
				cancel()
			}
			b := standard()
			res := g.r.runBatch(ctx, g.s, b)
			if b.Err == nil || b.Err.Error() != "shutdown" {
				t.Errorf("Err = %v, want shutdown", b.Err)
			}
			if res.failure != nil || res.panicked {
				t.Errorf("failure=%v panicked=%v", res.failure, res.panicked)
			}
			if got, want := outcomes(b), all(7, model.OutcomeFailed); !reflect.DeepEqual(got, want) {
				t.Errorf("outcomes %v", got)
			}
			for i, r := range b.Active() {
				if r.ErrCode != 0 {
					t.Errorf("row %d ErrCode %d on shutdown", i, r.ErrCode)
				}
			}
			if g.panics() != 0 {
				t.Errorf("panics=%d", g.panics())
			}
			for d := 0; d < 3; d++ {
				if g.rowsWritten(d) != 0 {
					t.Errorf("rows_written{%d} = %d on shutdown", d, g.rowsWritten(d))
				}
			}
			if counted {
				t.Errorf("oracle_error counted on shutdown")
			}
		})
	}
}

// TestRunBatchEmptyMaskAndEmptyBatch: an empty-mask row is Rejected and the
// rest commits; an all-empty-mask batch commits nothing and has no Err; N==0
// touches nothing.
func TestRunBatchEmptyMaskAndEmptyBatch(t *testing.T) {
	t.Run("mixed", func(t *testing.T) {
		g := newRig(t)
		b := mkBatch([3][][2]uint64{{{3, 2}, {0, 1}}, {{0x1FF, 4}}, nil})
		g.r.runBatch(context.Background(), g.s, b)
		want := []model.RowOutcome{model.OutcomeCommitted, model.OutcomeRejected, model.OutcomeCommitted}
		if got := outcomes(b); !reflect.DeepEqual(got, want) {
			t.Errorf("outcomes %v, want %v", got, want)
		}
		if b.Err != nil {
			t.Errorf("Err = %v", b.Err)
		}
		if g.rowsWritten(0) != 1 || g.rowsWritten(1) != 1 {
			t.Errorf("rows_written = %d/%d, want 1/1", g.rowsWritten(0), g.rowsWritten(1))
		}
	})
	t.Run("all empty", func(t *testing.T) {
		g := newRig(t)
		b := mkBatch([3][][2]uint64{{{0, 2}, {0, 1}}, nil, nil})
		g.r.runBatch(context.Background(), g.s, b)
		if got, want := outcomes(b), all(2, model.OutcomeRejected); !reflect.DeepEqual(got, want) {
			t.Errorf("outcomes %v", got)
		}
		if b.Err != nil || len(g.f.Execs()) != 0 {
			t.Errorf("Err=%v execs=%d", b.Err, len(g.f.Execs()))
		}
	})
	t.Run("N zero", func(t *testing.T) {
		g := newRig(t)
		b := &model.Batch{Rows: make([]model.BatchRow, 4)}
		g.r.runBatch(context.Background(), g.s, b)
		if b.Err != nil {
			t.Errorf("Err = %v", b.Err)
		}
		if c := g.f.Counts(); c.Begins != 0 {
			t.Errorf("begins = %d for an empty batch", c.Begins)
		}
	})
	t.Run("N beyond Rows", func(t *testing.T) {
		g := newRig(t)
		b := &model.Batch{Rows: make([]model.BatchRow, 2), N: 5}
		g.r.runBatch(context.Background(), g.s, b)
		if b.Err == nil {
			t.Errorf("Err nil for N beyond Rows")
		}
	})
}

// TestRunBatchTimeoutIsPerBatchAndCallerDeadlineWins: with no caller deadline
// the batch gets queryTimeout (D13), and an exec that outlives it fails the
// batch as a session-class deadline; a caller deadline is left alone.
func TestRunBatchTimeoutIsPerBatchAndCallerDeadlineWins(t *testing.T) {
	g := newRig(t)
	g.r.queryTimeout = 30 * time.Millisecond
	g.f.SetScript(func(c oratest.Call) oratest.Result {
		if c.Rows > 0 {
			return oratest.Result{Delay: 2 * time.Second}
		}
		return oratest.Result{}
	})
	b := standard()
	res := g.r.runBatch(context.Background(), g.s, b)
	if res.failure == nil || res.failure.Class != oradb.ErrSession {
		t.Fatalf("failure = %+v, want a session-class deadline", res.failure)
	}
	assertNoPending(t, b)

	g2 := newRig(t)
	g2.f.SetLatency(100 * time.Millisecond)
	g2.r.queryTimeout = 10 * time.Millisecond
	b2 := standard()
	g2.r.runBatch(context.Background(), g2.s, b2)
	// no deadline on the caller: the 10ms timeout applies and the batch fails
	if b2.Err == nil {
		t.Fatalf("a 10ms per-batch timeout did not fail the batch")
	}
	g3 := newRig(t)
	g3.f.SetLatency(100 * time.Millisecond)
	g3.r.queryTimeout = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b3 := standard()
	g3.r.runBatch(ctx, g3.s, b3)
	if b3.Err != nil {
		t.Errorf("the caller's deadline should have won, Err = %v", b3.Err)
	}

	// the other direction: a short caller deadline beats a long queryTimeout
	g4 := newRig(t)
	g4.r.queryTimeout = time.Minute
	g4.f.SetLatency(200 * time.Millisecond)
	ctx4, cancel4 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel4()
	b4 := standard()
	res4 := g4.r.runBatch(ctx4, g4.s, b4)
	if res4.failure == nil || res4.failure.Class != oradb.ErrSession {
		t.Errorf("failure = %+v, want the caller's deadline as a session-class error", res4.failure)
	}
	assertNoPending(t, b4)
}

// TestRunBatchDefaultClockIsUTCNow: with no clock seam the bound updated_at is
// time.Now in UTC.
func TestRunBatchDefaultClockIsUTCNow(t *testing.T) {
	g := newRig(t)
	g.r.now = nil
	before := time.Now()
	g.r.runBatch(context.Background(), g.s, standard())
	after := time.Now()
	execs := g.f.Execs()
	if len(execs) == 0 {
		t.Fatal("no execs")
	}
	ts := execs[0].Args[len(execs[0].Args)-1].([]sql.NullTime)[0].Time
	if ts.Location() != time.UTC {
		t.Errorf("updated_at location %v, want UTC", ts.Location())
	}
	if ts.Before(before.Add(-time.Second)) || ts.After(after.Add(time.Second)) {
		t.Errorf("updated_at %v outside [%v, %v]", ts, before, after)
	}
}

// TestRunBatchRowOutsideEveryRangeIsNotLeftPending: B-V3 on the success path,
// and a wild range does not panic after the commit.
func TestRunBatchRowOutsideEveryRangeIsNotLeftPending(t *testing.T) {
	g := newRig(t)
	b := standard()
	b.DestCount[2] = 0 // the destination 2 row is in no range now
	g.r.runBatch(context.Background(), g.s, b)
	if b.Err == nil {
		t.Errorf("Err nil with a row outside every range")
	}
	assertNoPending(t, b)

	g2 := newRig(t)
	b2 := standard()
	b2.DestStart[1], b2.DestCount[1] = 1000, 1000
	res := g2.r.runBatch(context.Background(), g2.s, b2)
	if res.panicked {
		t.Errorf("a wild destination range panicked after commit")
	}
	assertNoPending(t, b2)
}

// TestBVThreeNoPendingSurvivesAnyPath is the B-V3 table: whatever happens,
// no row is Pending on return and Err is set whenever a row is Failed.
func TestBVThreeNoPendingSurvivesAnyPath(t *testing.T) {
	type tc struct {
		name   string
		script oratest.Script
		hook   func(*rig)
		cancel bool
		batch  func() *model.Batch
	}
	scr := func(sqlText string, r oratest.Result) oratest.Script {
		return func(c oratest.Call) oratest.Result {
			if c.SQL == sqlText {
				return r
			}
			return oratest.Result{}
		}
	}
	cases := []tc{
		{name: "success", batch: standard},
		{name: "begin fails", script: scr(oratest.OpBegin, oratest.Result{Err: oratest.ORA(3113)}), batch: standard},
		{name: "begin dropped", script: scr(oratest.OpBegin, oratest.Result{Drop: true}), batch: standard},
		{name: "commit fails", script: scr(oratest.OpCommit, oratest.Result{Err: oratest.ORA(1653)}), batch: standard},
		{name: "commit dropped", script: scr(oratest.OpCommit, oratest.Result{Drop: true}), batch: standard},
		{name: "exec statement error", script: func(c oratest.Call) oratest.Result {
			if c.Rows > 0 {
				return oratest.Result{Err: oratest.ORA(942)}
			}
			return oratest.Result{}
		}, batch: standard},
		{name: "canceled", cancel: true, batch: standard},
		{name: "panic after commit", hook: func(g *rig) { g.r.afterCommit = func() { panic("x") } }, batch: standard},
		{name: "empty masks", batch: func() *model.Batch {
			return mkBatch([3][][2]uint64{{{0, 1}}, {{0, 2}}, {{0, 3}}})
		}},
		{name: "single row dest 2 only", batch: func() *model.Batch {
			return mkBatch([3][][2]uint64{nil, nil, {{0x3F, 1}}})
		}},
		{name: "N zero", batch: func() *model.Batch { return &model.Batch{Rows: make([]model.BatchRow, 3)} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newRig(t)
			if c.script != nil {
				g.f.SetScript(c.script)
			}
			if c.hook != nil {
				c.hook(g)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if c.cancel {
				cancel()
			}
			b := c.batch()
			g.r.runBatch(ctx, g.s, b)
			assertNoPending(t, b)
			if g.r.run.Busy.Load() != 0 {
				t.Errorf("writers_busy = %d", g.r.run.Busy.Load())
			}
			// G-ROW-3 companion: the shard's defect counter is never fed by a
			// writer that kept its postcondition.
			if n := g.unrep.Load(); n != 0 {
				t.Errorf("batch_row_unreported = %d", n)
			}
		})
	}
}

// TestBindIdxMatchesBindOnAGatheredCopy: BindIdx over absolute indices binds
// exactly what Bind binds over a contiguous copy of the same rows in the same
// order, and refuses an index outside the rows.
//
// WOULD HAVE CAUGHT: binding rows[0..n) instead of rows[idx[i]]; an unchecked
// index panicking inside the transaction.
func TestBindIdxMatchesBindOnAGatheredCopy(t *testing.T) {
	d := shippedDest(t, "MD_TOKEN_QUOTE")
	rows := make([]model.BatchRow, 5)
	for i := range rows {
		rows[i] = model.BatchRow{Row: model.RowKey(100 + i), Mask: 7}
		for j := range rows[i].Image[:64] {
			rows[i].Image[j] = byte(i*37 + j)
		}
	}
	idx := []int32{3, 0, 4}
	var gathered []model.BatchRow
	for _, i := range idx {
		gathered = append(gathered, rows[i])
	}
	a := newBinder(t, d, 7, 8)
	want, err := a.Bind(gathered, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	wantCopy := copyArgs(want)
	got, err := newBinder(t, d, 7, 8).BindIdx(rows, idx, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, wantCopy) {
		t.Errorf("BindIdx differs from Bind on the gathered rows")
	}
	if keys := got[0].([]int64); !reflect.DeepEqual(keys, []int64{103, 100, 104}) {
		t.Errorf("keys %v, want [103 100 104]", keys)
	}
	for _, bad := range [][]int32{{5}, {-1}} {
		if _, err := newBinder(t, d, 7, 8).BindIdx(rows, bad, testUpdatedAt); err == nil {
			t.Errorf("BindIdx accepted index %v", bad)
		}
	}
	if _, err := newBinder(t, d, 7, 8).BindIdx(rows, nil, testUpdatedAt); err == nil {
		t.Errorf("BindIdx accepted an empty index list")
	}
	rows[1].Mask = 3
	if _, err := newBinder(t, d, 7, 8).BindIdx(rows, []int32{0, 1}, testUpdatedAt); err == nil {
		t.Errorf("BindIdx accepted a row with the wrong mask")
	}
}

func copyArgs(a []any) []any {
	out := make([]any, len(a))
	for i, v := range a {
		switch s := v.(type) {
		case []int64:
			out[i] = append([]int64(nil), s...)
		case []sql.NullString:
			out[i] = append([]sql.NullString(nil), s...)
		case []sql.NullFloat64:
			out[i] = append([]sql.NullFloat64(nil), s...)
		case []sql.NullTime:
			out[i] = append([]sql.NullTime(nil), s...)
		}
	}
	return out
}

// --- phase 2 (P4-07b) ---

// rowScript fails every array exec with arrayCode (0: none) and fails a scalar
// exec whose key (first argument) is in fails with that ORA code. Keys in
// standard(): destination 0 holds 5, 10, 20, 30, destination 1 holds 8, 9,
// destination 2 holds 1. Begin, commit and rollback always succeed. The
// oratest convention relied on: Call.Rows > 0 is an array bind, 0 a scalar
// exec, and an operation (begin, commit, ...) carries one of the oratest.Op*
// keys as its SQL.
func rowScript(arrayCode int, fails map[int64]int) oratest.Script {
	return func(c oratest.Call) oratest.Result {
		if isOp(c.SQL) {
			return oratest.Result{}
		}
		if c.Rows > 0 {
			if arrayCode != 0 {
				return oratest.Result{Err: oratest.ORA(arrayCode)}
			}
			return oratest.Result{}
		}
		if key, _ := c.Args[0].(int64); fails[key] != 0 {
			return oratest.Result{Err: oratest.ORA(fails[key])}
		}
		return oratest.Result{}
	}
}

// isOp reports whether sql is one of the fake's non-statement operations.
func isOp(sql string) bool {
	switch sql {
	case "", oratest.OpOpen, oratest.OpPing, oratest.OpBegin, oratest.OpCommit, oratest.OpRollback:
		return true
	}
	return false
}

// scalarKeys returns the key of every scalar exec in order, and how many
// array execs there were.
func scalarKeys(f *oratest.Fake) (keys []int64, arrays int) {
	for _, e := range f.Execs() {
		if e.Rows > 0 {
			arrays++
			continue
		}
		k, _ := e.Args[0].(int64)
		keys = append(keys, k)
	}
	return keys, arrays
}

// destOfRow is the destination of row i of standard().
func destOfRow(i int) int {
	switch {
	case i < 4:
		return 0
	case i < 6:
		return 1
	}
	return 2
}

// TestG_STMT_2_Phase2AttributesTheRejectedRow: one bad row in a batch whose
// array exec failed with a row-data error costs exactly that row.
//
// SETUP: standard() (7 rows). The array exec fails ORA-00001, which does not
// say which row; a scalar exec fails ORA-00001 only for one key.
// STIMULUS: one runBatch, phase 1 then phase 2.
// ASSERTION: the failing key's row is Rejected with ErrCode 1; the other six
// are Committed; Err nil; one array exec, then seven scalar execs in phase 1's
// order (hand-written key list), exactly one of them failed; begin 2, rollback
// 1, commit 1; row_rejected 1 on the right destination; rows_written the rest;
// batch_fallback_perrow 1; oracle_error{1}=1 (first-come, not once per row).
// Non-vacuity: key 9 rejects row 4 and nothing else, so attribution comes from
// the failing exec and not from position, and with no scalar failure nothing
// is rejected.
// WOULD HAVE CAUGHT: failing the batch on a row-data error; rejecting the
// first row or the whole run; phase 2 re-running only the failing group;
// committing phase 1's work as well as phase 2's.
func TestG_STMT_2_Phase2AttributesTheRejectedRow(t *testing.T) {
	type tc struct {
		name     string
		fails    map[int64]int
		rejected map[int]uint16 // row index -> ErrCode
	}
	for _, c := range []tc{
		{"key 20", map[int64]int{20: 1}, map[int]uint16{1: 1}},
		{"key 9", map[int64]int{9: 1}, map[int]uint16{4: 1}},
		{"none", nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			g := newRig(t)
			g.f.SetScript(rowScript(1, c.fails))
			b := standard()
			res := g.r.runBatch(context.Background(), g.s, b)

			if b.Err != nil || res.failure != nil {
				t.Fatalf("Err=%v failure=%v, want a clean batch", b.Err, res.failure)
			}
			wantOut := all(7, model.OutcomeCommitted)
			wantW := [3]uint64{4, 2, 1}
			wantR := [3]uint64{}
			for i := range c.rejected {
				wantOut[i] = model.OutcomeRejected
				wantW[destOfRow(i)]--
				wantR[destOfRow(i)]++
			}
			if got := outcomes(b); !reflect.DeepEqual(got, wantOut) {
				t.Fatalf("outcomes %v, want %v", got, wantOut)
			}
			for i, r := range b.Active() {
				if r.ErrCode != c.rejected[i] {
					t.Errorf("row %d ErrCode %d, want %d", i, r.ErrCode, c.rejected[i])
				}
			}
			keys, arrays := scalarKeys(g.f)
			if want := []int64{5, 20, 10, 30, 8, 9, 1}; !reflect.DeepEqual(keys, want) {
				t.Errorf("scalar keys %v, want %v", keys, want)
			}
			if arrays != 1 {
				t.Errorf("%d array execs, want 1 (phase 1 stops at its first failure)", arrays)
			}
			failed := 0
			for _, e := range g.f.Execs() {
				if e.Rows == 0 && e.Err != nil {
					failed++
				}
			}
			if failed != len(c.rejected) {
				t.Errorf("%d scalar execs failed, want %d", failed, len(c.rejected))
			}
			cn := g.f.Counts()
			if cn.Begins != 2 || cn.Rollbacks != 1 || cn.Commits != 1 {
				t.Errorf("begins/rollbacks/commits = %d/%d/%d, want 2/1/1", cn.Begins, cn.Rollbacks, cn.Commits)
			}
			for d := range 3 {
				if got := g.r.run.RowRejected[d].Load(); got != wantR[d] {
					t.Errorf("row_rejected{%d} = %d, want %d", d, got, wantR[d])
				}
				if got := g.rowsWritten(d); got != wantW[d] {
					t.Errorf("rows_written{%d} = %d, want %d", d, got, wantW[d])
				}
			}
			if g.r.run.FallbackPerrow.Load() != 1 {
				t.Errorf("batch_fallback_perrow = %d, want 1", g.r.run.FallbackPerrow.Load())
			}
			if g.oraErr(1) != 1 {
				t.Errorf("oracle_error{1} = %d, want 1", g.oraErr(1))
			}
		})
	}
}

// TestPhase2DirectSkipsPhase1: phase 2 is exercised on its own, not only by
// injecting a phase 1 fault (V3 self-gate). ORA-01438 on key 9 is attributed
// to that row, the destination 1 row index 4, and to nothing else.
//
// SETUP: standard(); runner.skipPhase1 set; scalar ORA-01438 for key 9.
// ASSERTION: no array exec; one begin and one commit and no rollback;
// row 4 Rejected with ErrCode 1438, row_rejected{1}=1 and none elsewhere;
// the rest Committed; oracle_error not counted for a rejected row.
// WOULD HAVE CAUGHT: a phase 2 that only works when phase 1 ran first (state
// left by phase 1: marked empty masks, a warmed binder); a rejection counted
// against the wrong destination.
func TestPhase2DirectSkipsPhase1(t *testing.T) {
	g := newRig(t)
	g.r.skipPhase1 = true
	g.f.SetScript(rowScript(0, map[int64]int{9: 1438}))
	b := standard()
	g.r.runBatch(context.Background(), g.s, b)

	want := all(7, model.OutcomeCommitted)
	want[4] = model.OutcomeRejected
	if got := outcomes(b); !reflect.DeepEqual(got, want) || b.Err != nil {
		t.Fatalf("outcomes %v Err=%v, want %v", got, b.Err, want)
	}
	if b.Rows[4].ErrCode != 1438 {
		t.Errorf("ErrCode %d, want 1438", b.Rows[4].ErrCode)
	}
	keys, arrays := scalarKeys(g.f)
	if arrays != 0 || len(keys) != 7 {
		t.Errorf("%d array and %d scalar execs, want 0 and 7", arrays, len(keys))
	}
	cn := g.f.Counts()
	if cn.Begins != 1 || cn.Commits != 1 || cn.Rollbacks != 0 {
		t.Errorf("begins/commits/rollbacks = %d/%d/%d, want 1/1/0", cn.Begins, cn.Commits, cn.Rollbacks)
	}
	for d, want := range []uint64{0, 1, 0} {
		if got := g.r.run.RowRejected[d].Load(); got != want {
			t.Errorf("row_rejected{%d} = %d, want %d", d, got, want)
		}
	}
	if g.oraErr(1438) != 0 {
		t.Errorf("oracle_error{1438} = %d: a rejected row is not an operational error", g.oraErr(1438))
	}
}

// TestPhase2NonRowErrorAbandons: a non-row error in phase 2 abandons the
// batch, once, with no phase 3, and earlier rejections survive (A15).
//
// SETUP: array exec ORA-00001; scalar key 20 ORA-00001 (rejected), then scalar
// key 10 ORA-00060.
// ASSERTION: execution stops at key 10 (keys 5, 20, 10 only); row 1 stays
// Rejected ErrCode 1; the other six are Failed with ErrCode 60; Err and
// failure are the deadlock; nothing committed or counted written; two
// rollbacks, no commit; oracle_error{60}=1.
// WOULD HAVE CAUGHT: continuing after a non-row error; a phase 3; the sweep
// overwriting Rejected with Failed; committing the rows executed before the
// error.
func TestPhase2NonRowErrorAbandons(t *testing.T) {
	g := newRig(t)
	g.f.SetScript(rowScript(1, map[int64]int{20: 1, 10: 60}))
	b := standard()
	res := g.r.runBatch(context.Background(), g.s, b)

	want := all(7, model.OutcomeFailed)
	want[1] = model.OutcomeRejected
	if got := outcomes(b); !reflect.DeepEqual(got, want) {
		t.Fatalf("outcomes %v, want %v", got, want)
	}
	for i, r := range b.Active() {
		wantCode := uint16(60)
		if i == 1 {
			wantCode = 1
		}
		if r.ErrCode != wantCode {
			t.Errorf("row %d ErrCode %d, want %d", i, r.ErrCode, wantCode)
		}
	}
	if res.failure == nil || res.failure.Class != oradb.ErrDeadlock || b.Err == nil {
		t.Errorf("failure=%+v Err=%v, want deadlock", res.failure, b.Err)
	}
	if keys, _ := scalarKeys(g.f); !reflect.DeepEqual(keys, []int64{5, 20, 10}) {
		t.Errorf("scalar keys %v, want [5 20 10]", keys)
	}
	cn := g.f.Counts()
	if cn.Commits != 0 || cn.Rollbacks != 2 || cn.Begins != 2 {
		t.Errorf("begins/commits/rollbacks = %d/%d/%d, want 2/0/2", cn.Begins, cn.Commits, cn.Rollbacks)
	}
	for d := range 3 {
		if g.rowsWritten(d) != 0 {
			t.Errorf("rows_written{%d} = %d", d, g.rowsWritten(d))
		}
	}
	if g.oraErr(60) != 1 {
		t.Errorf("oracle_error{60} = %d, want 1", g.oraErr(60))
	}
	// First-come (D16): the phase 1 trigger and the phase 2 abandon are each
	// counted once; the rejected row's own code is not counted again.
	if g.oraErr(1) != 1 {
		t.Errorf("oracle_error{1} = %d, want 1 (the phase 1 trigger only)", g.oraErr(1))
	}
}

// TestPhase2ShutdownAbandonsLikeAnyNonRowError: a cancelled context in phase 2
// is not a rejection and not a skipped error.
//
// SETUP: standard(); skipPhase1; scalar key 20 fails ORA-00001; the scalar
// exec for key 10 cancels the context and fails with context.Canceled.
// ASSERTION: execution stops at key 10 (keys 5, 20, 10); key 20's row stays
// Rejected, the other six are Failed; Err is "shutdown"; no commit; no failure
// class and no oracle_error counted.
// WOULD HAVE CAUGHT: execRows swallowing a cancel error; treating a
// not-classified error (ok=false) as a row rejection and carrying on.
func TestPhase2ShutdownAbandonsLikeAnyNonRowError(t *testing.T) {
	g := newRig(t)
	g.r.skipPhase1 = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.f.SetScript(func(c oratest.Call) oratest.Result {
		if isOp(c.SQL) || c.Rows > 0 {
			return oratest.Result{}
		}
		switch key, _ := c.Args[0].(int64); key {
		case 20:
			return oratest.Result{Err: oratest.ORA(1)}
		case 10:
			cancel()
			return oratest.Result{Err: context.Canceled}
		}
		return oratest.Result{}
	})
	b := standard()
	res := g.r.runBatch(ctx, g.s, b)

	want := all(7, model.OutcomeFailed)
	want[1] = model.OutcomeRejected
	if got := outcomes(b); !reflect.DeepEqual(got, want) {
		t.Fatalf("outcomes %v, want %v", got, want)
	}
	if b.Err == nil || b.Err.Error() != "shutdown" || res.failure != nil {
		t.Errorf("Err=%v failure=%+v, want shutdown and no failure", b.Err, res.failure)
	}
	if keys, _ := scalarKeys(g.f); !reflect.DeepEqual(keys, []int64{5, 20, 10}) {
		t.Errorf("scalar keys %v, want [5 20 10]", keys)
	}
	if cn := g.f.Counts(); cn.Commits != 0 {
		t.Errorf("commits = %d, want 0", cn.Commits)
	}
	if g.oraErr(1) != 0 || g.oraErr(60) != 0 {
		t.Errorf("oracle_error counted on shutdown")
	}
}

// TestPhase1NonRowErrorSkipsPhase2: only a row-data class enters phase 2.
//
// WOULD HAVE CAUGHT: entering phase 2 for every failure (rows attempts at the
// same failure, burning the query timeout).
func TestPhase1NonRowErrorSkipsPhase2(t *testing.T) {
	for _, code := range []int{60, 12541, 24381, 942, 1653} {
		g := newRig(t)
		g.f.SetScript(rowScript(code, nil))
		b := standard()
		g.r.runBatch(context.Background(), g.s, b)
		if keys, _ := scalarKeys(g.f); len(keys) != 0 {
			t.Errorf("ORA-%d: %d scalar execs ran", code, len(keys))
		}
		if g.r.run.FallbackPerrow.Load() != 0 {
			t.Errorf("ORA-%d: batch_fallback_perrow counted", code)
		}
	}
}

// TestRowsAffectedMismatchChangesNoOutcome: RowsAffected is checked in both
// phases, counted per destination and logged, and never changes a result.
//
// SETUP: every exec reports a wrong count, once through phase 1 and once
// through phase 2 direct, with a count below the rows bound (0) and one above
// it (9; the largest bound is 2). The log goes to a buffer.
// ASSERTION: all rows Committed, Err nil, rows_written by row count (not by
// RowsAffected); the counter is per exec: phase 1 four array execs (dest 0
// twice, 1 and 2 once each), phase 2 seven scalar execs (4, 2, 1); one WARN
// line per mismatching exec, naming the affected count.
// WOULD HAVE CAUGHT: treating a mismatch as a failure or a rejection; counting
// by RowsAffected; counting against the wrong destination; dropping the WARN;
// comparing with < instead of !=.
func TestRowsAffectedMismatchChangesNoOutcome(t *testing.T) {
	for _, tc := range []struct {
		name     string
		skip     bool
		affected int64
		want     [3]uint64
	}{
		{"phase 1 fewer", false, 0, [3]uint64{2, 1, 1}},
		{"phase 2 fewer", true, 0, [3]uint64{4, 2, 1}},
		{"phase 1 more", false, 9, [3]uint64{2, 1, 1}},
		{"phase 2 more", true, 9, [3]uint64{4, 2, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newRig(t)
			var logbuf bytes.Buffer
			g.r.log = obs.NewAppLogger(&logbuf, obs.LevelWarn, 1000, nil)
			g.r.skipPhase1 = tc.skip
			g.f.SetScript(func(c oratest.Call) oratest.Result {
				if !isOp(c.SQL) {
					return oratest.Result{Affected: oratest.Affect(tc.affected)}
				}
				return oratest.Result{}
			})
			b := standard()
			g.r.runBatch(context.Background(), g.s, b)
			if got := outcomes(b); !reflect.DeepEqual(got, all(7, model.OutcomeCommitted)) || b.Err != nil {
				t.Fatalf("outcomes %v Err=%v", got, b.Err)
			}
			var execs uint64
			for d, want := range tc.want {
				execs += want
				if got := g.r.run.RowsAffectedMismatch[d].Load(); got != want {
					t.Errorf("rows_affected_mismatch{%d} = %d, want %d", d, got, want)
				}
			}
			if n := strings.Count(logbuf.String(), "rows affected mismatch"); uint64(n) != execs {
				t.Errorf("%d mismatch WARN lines, want %d:\n%s", n, execs, logbuf.String())
			}
			if want := fmt.Sprintf("affected=%d", tc.affected); !strings.Contains(logbuf.String(), want) {
				t.Errorf("log lacks %q:\n%s", want, logbuf.String())
			}
			for d, want := range []uint64{4, 2, 1} {
				if got := g.rowsWritten(d); got != want {
					t.Errorf("rows_written{%d} = %d, want %d", d, got, want)
				}
			}
		})
	}
}

// TestPhase2EmptyMaskRowsCountedOnce: an empty-mask row is Rejected once and
// counted once whether phase 1 reached its destination or not.
//
// SETUP: standard() with row 1 (destination 0) and row 4 (destination 1) at
// mask 0; array exec ORA-00001. Phase 1 groups destination 0, counts row 1 and
// fails before reaching destination 1.
// ASSERTION: batch_row_empty_mask is 1 for destination 0 (not 2) and 1 for
// destination 1; both rows Rejected with ErrCode 0; the other five Committed;
// no scalar exec for either.
// WOULD HAVE CAUGHT: phase 2 re-counting phase 1's empty masks; phase 2 never
// seeing a destination phase 1 did not reach; executing a mask-0 row.
func TestPhase2EmptyMaskRowsCountedOnce(t *testing.T) {
	g := newRig(t)
	g.f.SetScript(rowScript(1, nil))
	b := standard()
	b.Rows[1].Mask, b.Rows[4].Mask = 0, 0
	g.r.runBatch(context.Background(), g.s, b)

	want := all(7, model.OutcomeCommitted)
	want[1], want[4] = model.OutcomeRejected, model.OutcomeRejected
	if got := outcomes(b); !reflect.DeepEqual(got, want) || b.Err != nil {
		t.Fatalf("outcomes %v Err=%v, want %v", got, b.Err, want)
	}
	if b.Rows[1].ErrCode != 0 || b.Rows[4].ErrCode != 0 {
		t.Errorf("empty-mask ErrCode %d/%d, want 0", b.Rows[1].ErrCode, b.Rows[4].ErrCode)
	}
	if a, c := g.r.inst.cache.BatchRowEmptyMask[0].Load(), g.r.inst.cache.BatchRowEmptyMask[1].Load(); a != 1 || c != 1 {
		t.Errorf("batch_row_empty_mask dest0=%d dest1=%d, want 1 and 1", a, c)
	}
	if keys, _ := scalarKeys(g.f); !reflect.DeepEqual(keys, []int64{5, 10, 30, 8, 1}) {
		t.Errorf("scalar keys %v, want [5 10 30 8 1]", keys)
	}
}
