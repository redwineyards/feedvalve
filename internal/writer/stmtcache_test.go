package writer

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
	"feedvalve/internal/oradb/oratest"
)

func fakeOra(t *testing.T) (*oradb.DB, *oratest.Fake) {
	t.Helper()
	t.Setenv(oradb.PasswordEnv, "")
	f := oratest.New()
	t.Cleanup(f.Close)
	d, err := oradb.OpenDriver(context.Background(), oratest.DriverName, f.DSN(), oradb.DBSettings{
		Host: "h", Port: 1521, Service: "S", User: "u",
		ConnectTimeoutSec: 10, QueryTimeoutSec: 30, ConnMaxLifetimeMinutes: 30,
		MaxOpenConns: 4, MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatalf("OpenDriver: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, f
}

func destSet(t *testing.T) *destinations.Set {
	t.Helper()
	set, err := destinations.Default()
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// isClosed probes by executing, which adds one exec to the fake: call it only
// where no later assertion reads f.Execs() or Counts().
func isClosed(st *sql.Stmt) bool {
	_, err := st.ExecContext(context.Background(), int64(1))
	return err != nil && strings.Contains(err.Error(), "closed")
}

func maxPrepares(f *oratest.Fake) (conn, n int) {
	for c, v := range f.Counts().Prepares {
		if v > n {
			conn, n = c, v
		}
	}
	return
}

// TestStmtCacheHitPreparesOnce: a second get for one identity returns the same
// statement without preparing; a different identity prepares again. The text
// reaching the driver is checked for the MERGE shape and for :1 binds only.
func TestStmtCacheHitPreparesOnce(t *testing.T) {
	db, f := fakeOra(t)
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: 1})
	in := NewInstruments(reg, 3)
	in.ResolveCache(reg, 2, 3)
	reg.Freeze()
	prepares := 0
	c := newStmtCache(func(ctx context.Context, s string) (*sql.Stmt, error) {
		prepares++
		if !strings.HasPrefix(s, "MERGE") || !strings.Contains(s, ":1") || strings.Contains(s, "?") {
			t.Errorf("unexpected statement text %q", s)
		}
		return db.Prepare(ctx, s)
	}, destSet(t), 8, in, 1)
	ctx := context.Background()
	k1, k2 := stmtKey{0, 3}, stmtKey{0, 7}
	a, cached, err := c.get(ctx, k1)
	if err != nil || !cached {
		t.Fatalf("get: %v cached=%v", err, cached)
	}
	b, _, _ := c.get(ctx, k1)
	if a != b || prepares != 1 {
		t.Fatalf("hit: same=%v prepares=%d, want true/1", a == b, prepares)
	}
	if _, _, err := c.get(ctx, k2); err != nil || prepares != 2 || c.len() != 2 {
		t.Fatalf("second identity: err=%v prepares=%d len=%d", err, prepares, c.len())
	}
	if g := in.cache.StmtCacheEntries[1].Load(); g != 2 {
		t.Errorf("stmt_cache_entries{1} = %d, want 2", g)
	}
	if in.cache.StmtCacheEntries[0].Load() != 0 {
		t.Error("other writer's gauge moved")
	}
	_ = f
}

// TestStmtCachePerWriterNotShared: two caches over one DB each prepare their
// own statement for the same identity (the cache is per writer, V3 4.4).
func TestStmtCachePerWriterNotShared(t *testing.T) {
	db, _ := fakeOra(t)
	n := 0
	prep := func(ctx context.Context, s string) (*sql.Stmt, error) { n++; return db.Prepare(ctx, s) }
	c1 := newStmtCache(prep, destSet(t), 4, nil, 0)
	c2 := newStmtCache(prep, destSet(t), 4, nil, 1)
	a, _, _ := c1.get(context.Background(), stmtKey{0, 3})
	b, _, _ := c2.get(context.Background(), stmtKey{0, 3})
	if a == b || n != 2 {
		t.Errorf("shared=%v prepares=%d, want separate and 2", a == b, n)
	}
}

// TestG_W_6_StmtCacheFullPreparesUncached is W-6: at the cap the statement is
// prepared, handed out uncached, closed by done, never entered in the map, and
// counted.
// WOULD HAVE CAUGHT: unbounded growth; leaking the uncached statement;
// forgetting the counter.
func TestG_W_6_StmtCacheFullPreparesUncached(t *testing.T) {
	db, _ := fakeOra(t)
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: 1})
	in := NewInstruments(reg, 3)
	in.ResolveCache(reg, 1, 3)
	reg.Freeze()
	c := newStmtCache(db.Prepare, destSet(t), 1, in, 0)
	ctx := context.Background()
	first, cached, _ := c.get(ctx, stmtKey{0, 3})
	if !cached {
		t.Fatal("first statement should be cached")
	}
	over, cached, err := c.get(ctx, stmtKey{0, 7})
	if err != nil || cached {
		t.Fatalf("over cap: err=%v cached=%v, want uncached", err, cached)
	}
	if c.len() != 1 {
		t.Errorf("len = %d, want 1 (cap)", c.len())
	}
	if isClosed(over) {
		t.Fatal("uncached statement closed before use")
	}
	c.done(over, false)
	if !isClosed(over) {
		t.Error("uncached statement not closed by done")
	}
	c.done(first, true)
	if isClosed(first) {
		t.Error("done closed a cached statement")
	}
	if got := in.cache.StmtCacheUncached.Load(); got != 1 {
		t.Errorf("stmt_cache_uncached = %d, want 1", got)
	}
	// a hit at the cap is still a hit
	if st, cached, _ := c.get(ctx, stmtKey{0, 3}); st != first || !cached {
		t.Error("hit at cap did not return the cached statement")
	}
	if in.cache.StmtCacheUncached.Load() != 1 {
		t.Error("hit counted as uncached")
	}
}

func TestStmtCacheCloseAll(t *testing.T) {
	db, _ := fakeOra(t)
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: 1})
	in := NewInstruments(reg, 3)
	in.ResolveCache(reg, 1, 3)
	reg.Freeze()
	c := newStmtCache(db.Prepare, destSet(t), 4, in, 0)
	a, _, _ := c.get(context.Background(), stmtKey{0, 3})
	b, _, _ := c.get(context.Background(), stmtKey{1, 1})
	c.closeAll()
	if !isClosed(a) || !isClosed(b) || c.len() != 0 {
		t.Errorf("closeAll: closed=%v/%v len=%d", isClosed(a), isClosed(b), c.len())
	}
	if in.cache.StmtCacheEntries[0].Load() != 0 {
		t.Error("gauge not reset")
	}
	c.closeAll() // idempotent
}

func TestStmtCacheErrors(t *testing.T) {
	db, _ := fakeOra(t)
	c := newStmtCache(db.Prepare, destSet(t), 4, nil, 0)
	if _, _, err := c.get(context.Background(), stmtKey{9, 1}); err == nil {
		t.Error("unknown destination accepted")
	}
	if _, _, err := c.get(context.Background(), stmtKey{0, 0}); err == nil {
		t.Error("empty mask accepted")
	}
	if c.len() != 0 {
		t.Error("failed get cached something")
	}
}

// execTwoTx runs st through tx.StmtContext on the session with a one-row bind
// built for the same identity.
func execTwoTx(t *testing.T, ss *oradb.Session, get func() *sql.Stmt, args []any) {
	t.Helper()
	for i := 0; i < 2; i++ {
		err := ss.ExecTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.StmtContext(ctx, get()).ExecContext(ctx, args...)
			return err
		})
		if err != nil {
			t.Fatalf("tx %d: %v", i, err)
		}
	}
}

// TestG_ADR_W02_PreparedOnDBReusedAcrossTransactions is the ADR-W02 self-gate:
// a statement prepared once through the cache and executed in two
// transactions on one pinned session reaches the driver's Prepare once per
// connection, never once per execution.
//
// SETUP: fake database; cache over db.Prepare; one Acquire'd session.
// STIMULUS: get once, then two ExecTx each running tx.StmtContext(st).Exec.
// ASSERTION: two executions were seen on one connection and no connection
// recorded more than one driver Prepare.
// NON-VACUITY: the same two executions through a statement prepared on the
// pinned *sql.Conn (the pattern ADR-W02 rejects) record two Prepares on the
// session's connection, so the counter can fail.
// WOULD HAVE CAUGHT: preparing on the Conn (finding W2).
func TestG_ADR_W02_PreparedOnDBReusedAcrossTransactions(t *testing.T) {
	d := destSet(t).ByID(2)
	key := stmtKey{Dest: 2, Mask: model.ColMask(1)}
	text, plan, err := destinations.StatementFor(d, key.Mask)
	if err != nil {
		t.Fatal(err)
	}
	_ = text
	b, err := NewBinder(d, key.Mask, 1)
	if err != nil {
		t.Fatal(err)
	}
	args, err := b.Bind([]model.BatchRow{{Row: 5, Mask: key.Mask}}, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != len(plan) {
		t.Fatalf("%d args for %d placeholders", len(args), len(plan))
	}

	db, f := fakeOra(t)
	cache := newStmtCache(db.Prepare, destSet(t), 4, nil, 0)
	ss, err := db.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Release()
	execTwoTx(t, ss, func() *sql.Stmt {
		st, _, err := cache.get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}, args)
	ex := f.Execs()
	if len(ex) != 2 || ex[0].Conn != ex[1].Conn {
		t.Fatalf("execs = %d on conns %v, want 2 on one connection", len(ex), ex)
	}
	if conn, n := maxPrepares(f); n != 1 {
		t.Errorf("connection %d saw %d driver Prepares, want 1 per connection", conn, n)
	}
	if got := f.Counts().Prepares[ex[0].Conn]; got != 1 {
		t.Errorf("session connection saw %d Prepares, want exactly 1", got)
	}
	cache.closeAll()

	// non-vacuity: the rejected pattern re-prepares on every execution.
	f2 := oratest.New()
	t.Cleanup(f2.Close)
	raw, err := sql.Open(oratest.DriverName, f2.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	conn, err := raw.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cst, err := conn.PrepareContext(context.Background(), text)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.StmtContext(context.Background(), cst).ExecContext(context.Background(), args...); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if _, n := maxPrepares(f2); n < 2 {
		t.Errorf("Conn-prepared statement saw %d Prepares; the gate cannot detect finding W2", n)
	}
}

// TestStmtCachePrepareErrorCachesNothing: a failing prepare returns the
// driver's own error unwrapped, caches nothing and does not count as uncached,
// even when the cache is at its cap.
func TestStmtCachePrepareErrorCachesNothing(t *testing.T) {
	sentinel := errors.New("prepare boom")
	reg := obs.New(obs.Options{Destinations: []string{"a", "b", "c"}, Shards: 1})
	in := NewInstruments(reg, 3)
	in.ResolveCache(reg, 1, 3)
	reg.Freeze()
	for _, max := range []int{4, 0} {
		c := newStmtCache(func(context.Context, string) (*sql.Stmt, error) { return nil, sentinel },
			destSet(t), max, in, 0)
		st, cached, err := c.get(context.Background(), stmtKey{0, 3})
		if err != sentinel || st != nil || cached {
			t.Errorf("max=%d: st=%v cached=%v err=%v, want nil/false/sentinel itself", max, st, cached, err)
		}
		if c.len() != 0 {
			t.Errorf("max=%d: len = %d", max, c.len())
		}
	}
	if got := in.cache.StmtCacheUncached.Load(); got != 0 {
		t.Errorf("stmt_cache_uncached = %d after failed prepares, want 0", got)
	}
}

// TestStmtCacheDisabled: max 0 caches nothing, so every get is an uncached
// prepare that done must close.
func TestStmtCacheDisabled(t *testing.T) {
	db, _ := fakeOra(t)
	c := newStmtCache(db.Prepare, destSet(t), 0, nil, 0)
	for i := 0; i < 2; i++ {
		st, cached, err := c.get(context.Background(), stmtKey{0, 3})
		if err != nil || cached || c.len() != 0 {
			t.Fatalf("get %d: err=%v cached=%v len=%d", i, err, cached, c.len())
		}
		c.done(st, cached)
		if !isClosed(st) {
			t.Errorf("get %d: uncached statement not closed", i)
		}
	}
}

// TestStmtCacheOnePreparePerConnection: one cached statement executed in
// transactions on two concurrently held sessions is prepared by the driver at
// most once per connection, never once per execution.
func TestStmtCacheOnePreparePerConnection(t *testing.T) {
	key := stmtKey{Dest: 2, Mask: model.ColMask(1)}
	d := destSet(t).ByID(2)
	b, err := NewBinder(d, key.Mask, 1)
	if err != nil {
		t.Fatal(err)
	}
	args, err := b.Bind([]model.BatchRow{{Row: 5, Mask: key.Mask}}, testUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	db, f := fakeOra(t)
	cache := newStmtCache(db.Prepare, destSet(t), 4, nil, 0)
	defer cache.closeAll()
	s1, err := db.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Release()
	s2, err := db.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Release()
	get := func() *sql.Stmt {
		st, _, err := cache.get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	execTwoTx(t, s1, get, args)
	execTwoTx(t, s2, get, args)
	ex := f.Execs()
	conns := map[int]bool{}
	for _, e := range ex {
		conns[e.Conn] = true
	}
	if len(ex) != 4 || len(conns) != 2 {
		t.Fatalf("execs = %d on %d connections, want 4 on 2", len(ex), len(conns))
	}
	for c, n := range f.Counts().Prepares {
		if n > 1 {
			t.Errorf("connection %d saw %d Prepares, want at most 1", c, n)
		}
	}
}

// len is the number of cached statements.
func (c *stmtCache) len() int { return len(c.m) }
