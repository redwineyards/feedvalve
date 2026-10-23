package oradb

import (
	"context"
	"database/sql"
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

// driverName is the database/sql driver Open uses. It is a variable only so a
// test can substitute a driver whose errors quote the DSN.
// Known interim: P4-03's OpenDriver seam replaces this. Tests that swap it
// must not use t.Parallel.
var driverName = "oracle"

// Open connects and verifies the connection before returning it.
//
// sql.Open does NOT connect -- it only validates the DSN's syntax -- so a bad
// host or credential would otherwise surface on the first write rather than
// at startup. Open pings under ConnectTimeoutSec so startup is where it
// fails.
//
// The password is read here, from the environment, and never stored.
func Open(ctx context.Context, s DBSettings) (*sql.DB, error) {
	password, err := Password()
	if err != nil {
		return nil, err
	}

	dsn := s.dsn(password)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", s.Describe(), redactDSN(err, dsn, password, s))
	}

	// The pool is deliberately small and derived from the writer count:
	// Oracle caps concurrent sessions through the instance's sessions and
	// processes parameters, so an unbounded pool fails as ORA-00020 under
	// load rather than queueing.
	db.SetMaxOpenConns(s.MaxOpenConns)
	db.SetMaxIdleConns(s.MaxIdleConns)

	// Oracle profiles (IDLE_TIME) and stateful firewalls kill idle sessions
	// silently; the first reuse of a dead one surfaces as ORA-03113 or
	// ORA-03114. Retiring connections before that turns an error into a
	// reconnect.
	db.SetConnMaxLifetime(time.Duration(s.ConnMaxLifetimeMinutes) * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, time.Duration(s.ConnectTimeoutSec)*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to %s: %w", s.Describe(), redactDSN(err, dsn, password, s))
	}
	return db, nil
}

// Check is -dbcheck: connect, prove a round trip, report the server version,
// and close. Read-only, and the way to prove credentials and reachability
// from a deployment host before the relay is ever started.
func Check(ctx context.Context, s DBSettings) (version string, err error) {
	db, err := Open(ctx, s)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// Open has already read the password; reading it again is cheap and keeps
	// Open's signature, and the redactor below is what scrubs driver errors.
	password, _ := Password()
	red := NewRedactor(s, password)

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
