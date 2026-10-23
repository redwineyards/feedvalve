package oradb

import (
	"context"
	"database/sql"
	"time"
)

// Session is one writer's pinned Oracle connection. *sql.DB.BeginTx takes an
// arbitrary pooled connection, and a writer needs its transaction, its
// statements and its row locks on one session only it can use.
//
// Finding W1 (V3 2.3): a pinned *sql.Conn is exempt from ConnMaxLifetime, so
// the Session records when it was acquired and Expired says when the holder
// must release it. A Session is used by one goroutine.
type Session struct {
	conn       *sql.Conn
	acquiredAt time.Time // carries a monotonic reading
	lifetime   time.Duration
}

// Acquire pins one connection from the pool under ConnectTimeoutSec. The error
// is the driver's own, for Classify.
func (d *DB) Acquire(ctx context.Context) (*Session, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(d.s.ConnectTimeoutSec)*time.Second)
	defer cancel()
	c, err := d.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Session{
		conn:       c,
		acquiredAt: time.Now(),
		lifetime:   time.Duration(d.s.ConnMaxLifetimeMinutes) * time.Minute,
	}, nil
}

// Expired reports whether the session has been held for longer than
// ConnMaxLifetime and must be released at the next batch boundary. A zero
// lifetime is not in the LLD's wording: it means the pool never retires
// connections, as in database/sql, so it never expires.
func (s *Session) Expired(now time.Time) bool {
	return s.lifetime > 0 && now.Sub(s.acquiredAt) > s.lifetime
}

// Release returns the connection to the pool, where database/sql applies the
// expiry check it cannot apply while the connection is pinned.
func (s *Session) Release() error { return s.conn.Close() }

// ExecTx runs fn inside one transaction on this session: commit if fn returns
// nil, roll back otherwise. A panic in fn also rolls back, then continues to
// unwind, so a bug in a callback cannot leave a transaction open holding row
// locks. Callers must not rely on ExecTx surviving the panic; the writer's
// recover sits outside it. Errors are the driver's own, for Classify.
func (s *Session) ExecTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
