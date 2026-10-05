package oradb

import (
	"errors"
	"fmt"
	"os"
)

// PasswordEnv is the only place the database password ever comes from.
//
// Not a flag, because flags are visible in ps to every user on the host. Not
// a configuration key, because configuration files get committed to version
// control and copied between environments. There is no third option and there
// will not be one.
const PasswordEnv = "FEEDVALVE_DB_PASSWORD"

// ErrNoPassword is returned when a db object is configured but the
// environment variable is unset or empty.
//
// It is a returned error and not a fatal log, because a fatal log calls
// os.Exit and skips every deferred cleanup. Startup failures return.
var ErrNoPassword = errors.New("oradb: " + PasswordEnv + " is not set")

// The other two sentinels of Vol 0 section 3.11. Declared here with
// ErrNoPassword so the set is complete and callers in later phases branch on
// identity rather than on error text; the code that returns them arrives with
// the writer pool (Phase 4) and the drain (Phase 5).
var (
	// ErrPoolStopped is returned once the statement-failure rule has stopped
	// the writer pool: the same statement identity failed on enough distinct
	// writers inside the window that the fault is the statement, not the
	// session.
	ErrPoolStopped = errors.New("oradb: writer pool stopped by the statement-failure rule")

	// ErrDrainTimeout is returned when the shutdown drain deadline passes
	// with dirty rows still outstanding. The process exits anyway; this is
	// what makes the loss explicit rather than silent.
	ErrDrainTimeout = errors.New("oradb: drain deadline reached with dirty rows outstanding")
)

// DBSettings is everything needed to reach the database EXCEPT the password.
//
// The omission is structural, not an oversight. A type that cannot hold the
// secret cannot leak it, which is what lets the startup "effective
// configuration" line dump the whole Config without a redaction pass. Note
// this is narrower than poc's DBConfig, which did carry a Password field.
//
// Two rules about this type, both enforced by gate G-SEC-1:
//
// It has no String() and no MarshalJSON(). Describe is the only rendering, so
// a hurried %v in a log line produces a struct dump of non-secret fields
// rather than anything containing a DSN. Adding a String() for convenience
// would put the DSN into an append-only, world-readable log permanently the
// first time someone formatted one of these in a hurry.
//
// And the DSN is never built here. It is assembled inside the connection
// layer at open time, from Password read directly out of the environment,
// and never returned to a caller.
type DBSettings struct {
	Host    string
	Port    int
	Service string
	User    string

	ConnectTimeoutSec      int
	QueryTimeoutSec        int
	ConnMaxLifetimeMinutes int

	// MaxOpenConns and MaxIdleConns are DERIVED from writers.count + 1 and
	// re-applied on every accepted reload; they are not configuration keys.
	//
	// Otherwise the documented remedy for a slow write path -- raise
	// writers -- silently does nothing past a separately fixed pool limit,
	// which is a throughput cliff on the exact lever the runbook names.
	MaxOpenConns int
	MaxIdleConns int
}

// Describe renders the settings for a log line or an error, built from the
// individual fields so that no URL-escaping edge case can leak anything.
//
// This is the ONLY safe rendering and it is exported for that reason: making
// the safe path easy to reach is what stops someone reaching for %v instead.
func (s DBSettings) Describe() string {
	return fmt.Sprintf("oracle %s@%s:%d/%s", s.User, s.Host, s.Port, s.Service)
}

// Password reads the password from the environment. It is a function rather
// than a field so there is no moment at which a DBSettings value holds it.
func Password() (string, error) {
	p := os.Getenv(PasswordEnv)
	if p == "" {
		return "", ErrNoPassword
	}
	return p, nil
}

// HasPassword reports whether the environment supplies one, without returning
// it. -selfcheck uses this to say something useful without needing the secret.
func HasPassword() bool { return os.Getenv(PasswordEnv) != "" }
