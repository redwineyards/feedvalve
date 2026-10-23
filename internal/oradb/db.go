package oradb

import (
	"context"
	"database/sql"
	"time"
)

// DB is the opened database: the *sql.DB, the settings it was opened with and
// the redactor built from its DSN and password. It exists so that nothing past
// Open has to remember either the DSN or the scrubbing -- a caller holds a DB
// and can ask it to ping, prepare, classify or hand out a Session.
//
// A DB is safe for concurrent use. app owns it and closes it.
type DB struct {
	db  *sql.DB
	red Redactor
	s   DBSettings
}

// PingContext pings under ConnectTimeoutSec. A failure comes back redacted:
// it is the startup error and goes straight into a log.
func (d *DB) PingContext(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(d.s.ConnectTimeoutSec)*time.Second)
	defer cancel()
	if err := d.db.PingContext(ctx); err != nil {
		return d.red.redact(err)
	}
	return nil
}

// String and GoString keep the DSN and password inside the redactor out of any
// formatting verb: fmt prints the fields of a struct it has no Stringer for, and
// %+v of a DB would otherwise print them. Gate G-SEC-1.
func (DB) String() string   { return "oradb.DB" }
func (DB) GoString() string { return "oradb.DB" }

// Stats reports the pool's counters.
func (d *DB) Stats() sql.DBStats { return d.db.Stats() }

// Close closes the pool. Like *sql.DB's Close it may be called twice.
func (d *DB) Close() error { return d.db.Close() }

// Classify classifies and redacts a driver error; see Redactor.Classify.
func (d *DB) Classify(err error) (*Error, bool) { return d.red.Classify(err) }

// Prepare prepares text on the owning *sql.DB, not on a session (ADR-W02): that
// is the only form Tx.StmtContext can reuse a driver cursor from. The error is
// returned as the driver gave it, so the caller can Classify it before it is
// logged.
func (d *DB) Prepare(ctx context.Context, text string) (*sql.Stmt, error) {
	return d.db.PrepareContext(ctx, text)
}
