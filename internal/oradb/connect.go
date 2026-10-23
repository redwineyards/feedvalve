package oradb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

// dsn builds the connection string. It is UNEXPORTED and returns a value that
// never leaves this package, which is the structural half of the rule that
// the DSN must not reach a log: nothing outside oradb can obtain one.
//
// The password is a parameter rather than a field of DBSettings for the same
// reason -- there is no moment at which a settings value holds it.
func (s DBSettings) dsn(password string) string {
	return go_ora.BuildUrl(s.Host, s.Port, s.Service, s.User, password, nil)
}

// realDriver is the go-ora driver's registered name.
const realDriver = "oracle"

// Open connects and verifies the connection before returning it.
//
// sql.Open does NOT connect -- it only validates the DSN's syntax -- so a bad
// host or credential would otherwise surface on the first write rather than
// at startup. Open pings under ConnectTimeoutSec so startup is where it
// fails.
//
// The password is read here, from the environment, and never stored.
func Open(ctx context.Context, s DBSettings) (*DB, error) {
	return open(ctx, realDriver, s)
}

// open is Open over a named driver; the tests that need a driver whose errors
// quote the DSN go through it.
func open(ctx context.Context, name string, s DBSettings) (*DB, error) {
	password, err := Password()
	if err != nil {
		return nil, err
	}
	return openDriver(ctx, name, s.dsn(password), password, s)
}

// OpenDriver is Open for a caller-chosen database/sql driver and DSN. It is the
// documented substitutability seam: the writer and app tests live in other
// packages and open an oratest fake through it, and everything past this call
// (pool settings, ping under ConnectTimeout, redaction, sessions) behaves the
// same for the fake as for go-ora.
//
// The password, if the environment supplies one, is used only to build the
// redactor; a fake's DSN carries none and the variable may be unset.
func OpenDriver(ctx context.Context, driverName, dsn string, s DBSettings) (*DB, error) {
	password, err := Password()
	if err != nil && !errors.Is(err, ErrNoPassword) {
		return nil, err
	}
	return openDriver(ctx, driverName, dsn, password, s)
}

func openDriver(ctx context.Context, driverName, dsn, password string, s DBSettings) (*DB, error) {
	red := Redactor{dsn: dsn, password: password, s: s}
	sdb, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", s.Describe(), red.redact(err))
	}

	// The pool is deliberately small and derived from the writer count:
	// Oracle caps concurrent sessions through the instance's sessions and
	// processes parameters, so an unbounded pool fails as ORA-00020 under
	// load rather than queueing.
	sdb.SetMaxOpenConns(s.MaxOpenConns)
	sdb.SetMaxIdleConns(s.MaxIdleConns)

	// Oracle profiles (IDLE_TIME) and stateful firewalls kill idle sessions
	// silently; the first reuse of a dead one surfaces as ORA-03113 or
	// ORA-03114. Retiring connections before that turns an error into a
	// reconnect.
	sdb.SetConnMaxLifetime(time.Duration(s.ConnMaxLifetimeMinutes) * time.Minute)

	d := &DB{db: sdb, red: red, s: s, live: &liveSettings{}}
	d.live.lifetime.Store(int64(time.Duration(s.ConnMaxLifetimeMinutes) * time.Minute))
	d.live.connectTimeout.Store(int64(time.Duration(s.ConnectTimeoutSec) * time.Second))
	if err := d.PingContext(ctx); err != nil {
		sdb.Close()
		return nil, fmt.Errorf("connect to %s: %w", s.Describe(), err)
	}
	return d, nil
}

// Check is -dbcheck: connect, prove a round trip, report the server version,
// and close. Read-only, and the way to prove credentials and reachability
// from a deployment host before the relay is ever started.
func Check(ctx context.Context, s DBSettings) (version string, err error) {
	return check(ctx, realDriver, s)
}

func check(ctx context.Context, name string, s DBSettings) (version string, err error) {
	d, err := open(ctx, name, s)
	if err != nil {
		return "", err
	}
	defer d.Close()
	db, red := d.db, d.red

	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(s.QueryTimeoutSec)*time.Second)
	defer cancel()

	// SELECT 1 FROM dual proves the round trip rather than merely the
	// handshake: a ping can succeed against a session that cannot execute.
	var one int
	if err := db.QueryRowContext(queryCtx, "SELECT 1 FROM dual").Scan(&one); err != nil {
		return "", fmt.Errorf("round trip to %s: %w", s.Describe(), red.redact(err))
	}
	if one != 1 {
		return "", fmt.Errorf("round trip to %s returned %d, want 1", s.Describe(), one)
	}

	// Best effort. A server that answers SELECT 1 but not this is still
	// reachable, and refusing to report success over a banner would be
	// wrong.
	_ = db.QueryRowContext(queryCtx,
		"SELECT banner FROM v$version WHERE ROWNUM = 1").Scan(&version)
	if version == "" {
		version = "(version unavailable)"
	}
	return version, nil
}
