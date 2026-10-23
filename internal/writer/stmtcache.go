package writer

import (
	"context"
	"database/sql"
	"fmt"

	"feedvalve/internal/destinations"
	"feedvalve/internal/obs"
)

// stmtCache is one writer's prepared statements, keyed by statement identity
// (V3 section 4.4, ADR-W02). It is a plain map with no mutex because only its
// owning writer touches it.
//
// Statements are prepared on the *sql.DB (prepare is oradb.DB.Prepare) and
// executed through tx.StmtContext. A statement prepared on a pinned *sql.Conn
// would be re-parsed on every execution (finding W2).
//
// Statement text is immutable for the life of the process, so nothing is ever
// evicted: the cache only grows to max and is emptied by closeAll when the
// writer exits.
type stmtCache struct {
	prepare func(ctx context.Context, text string) (*sql.Stmt, error)
	dests   *destinations.Set
	max     int
	m       map[stmtKey]*sql.Stmt

	entries  *obs.Gauge   // nil is fine
	uncached *obs.Counter // nil is fine
}

// newStmtCache builds the cache for writer w. max is writers.stmtCacheSize
// (validated 8..4096 by config; max <= 0 makes every statement uncached);
// inst may be nil. The cache only counts stmt_cache_uncached: the once-per-
// bucket log line of LLD 4.3 is written at the P4-07a call site.
func newStmtCache(prepare func(context.Context, string) (*sql.Stmt, error),
	dests *destinations.Set, max int, inst *Instruments, w int) *stmtCache {
	c := &stmtCache{prepare: prepare, dests: dests, max: max, m: make(map[stmtKey]*sql.Stmt)}
	c.entries = inst.entriesGauge(w)
	if inst != nil {
		c.uncached = inst.cache.StmtCacheUncached
	}
	return c
}

// get returns the statement for key, preparing it on a miss. cached reports
// whether the cache owns the statement; when it is false (W-6, cache full) the
// caller must pass the statement to done after executing it. A prepare error
// is the driver's own, unclassified, for the caller to Classify.
func (c *stmtCache) get(ctx context.Context, key stmtKey) (st *sql.Stmt, cached bool, err error) {
	if st, ok := c.m[key]; ok {
		return st, true, nil
	}
	d := c.dests.ByID(key.Dest)
	if d == nil {
		return nil, false, fmt.Errorf("writer: statement %s: no such destination", key)
	}
	text, _, err := destinations.StatementFor(d, key.Mask)
	if err != nil {
		return nil, false, fmt.Errorf("writer: statement %s: %w", key, err)
	}
	st, err = c.prepare(ctx, text)
	if err != nil {
		return nil, false, err
	}
	if len(c.m) >= c.max {
		add(c.uncached)
		return st, false, nil
	}
	c.m[key] = st
	if c.entries != nil {
		c.entries.Store(int64(len(c.m)))
	}
	return st, true, nil
}

// done releases a statement get returned with cached == false. It is a no-op
// for a cached one, which stays until closeAll.
func (c *stmtCache) done(st *sql.Stmt, cached bool) {
	if !cached && st != nil {
		_ = st.Close()
	}
}

// closeAll closes every cached statement and empties the cache. A writer must
// call it on every exit path: each leaked *sql.Stmt stays registered with the
// DB for the life of the process.
func (c *stmtCache) closeAll() {
	for k, st := range c.m {
		_ = st.Close()
		delete(c.m, k)
	}
	if c.entries != nil {
		c.entries.Store(0)
	}
}
