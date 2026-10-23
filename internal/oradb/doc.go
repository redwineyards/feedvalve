// Package oradb is the Oracle connection layer: DBConfig, the DSN builder,
// Describe, redactDSN and Redactor, Open and Check, the pool settings and the
// error classification.
//
// oradb MUST NOT import framing, decode, mirror or destinations.
//
// It is built on github.com/sijms/go-ora/v2, a pure Go driver that needs no cgo
// and no Oracle Instant Client -- which is what keeps CGO_ENABLED=0 GOOS=aix
// GOARCH=ppc64 working. This is the only package in the tree permitted to name
// it, or database/sql.
//
// The password comes only from FEEDVALVE_DB_PASSWORD. There is no flag, because
// flags are visible in ps, and no config-file key, because config files get
// committed and copied between environments.
//
// The DSN contains the password and must never reach a log. Build anything
// user-visible from DBConfig.describe, which is assembled from the individual
// fields so no URL-escaping edge case can leak. redactDSN is a second line of
// defence that scrubs driver errors quoting the connection string back at us,
// not the first.
//
// Three operational facts worth keeping in view. sql.Open does not connect, so
// OpenDB pings under ConnectTimeout and a bad host or credential fails at
// startup rather than on the first write. The pool is deliberately small,
// because Oracle caps concurrent sessions through the instance's sessions and
// processes parameters. And connection lifetime is capped because Oracle
// profiles and stateful firewalls kill idle sessions silently, so the first
// reuse of a dead one surfaces as ORA-03113 or ORA-03114; retiring connections
// first turns that error into a reconnect.
//
// Classify errors with Redactor.Classify, which finds the ORA number through
// the unexported oracleErrCode (errors.As on the driver's *network.OracleError)
// and never by matching strings. The result is an *Error carrying an ErrClass
// from the V3 §8.2 table and an already-redacted cause; context.Canceled is
// not a failure and classifies as ok == false.
//
// Carried over from poc/db.go.
//
// Specified by docs/lld/03-writer-persistence.md. Built in Phase 4.
package oradb
