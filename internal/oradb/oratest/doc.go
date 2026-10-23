// Package oratest is a scripted fake Oracle driver for tests. It registers a
// database/sql driver named "oratest" and lets a test decide, per SQL text and
// per call number, what the database does: succeed, fail with a typed ORA
// error, drop the connection, or take a while.
//
// It exists because the writer, oradb and app tests live in different
// packages and all need a database/sql driver that behaves like the real one
// where it matters (positional array binds, per-connection prepare, ErrBadConn
// eviction, commit and rollback) without needing Oracle. Ruling D1.
//
// IMPORT RULE. oratest imports database/sql, database/sql/driver and go-ora's
// network package (for *network.OracleError, the type oradb classifies on). It
// MUST NOT import oradb: oradb's own tests import oratest, so the reverse is a
// cycle. It is not production code and must never appear in
// `go list -deps ./cmd/feedvalve`; internal/app/deps_test.go checks that.
//
// Cost and limits. Every statement execution allocates an args copy, a Call and
// an ExecRecord (SetRecordArgs(false) drops the stored copy), so allocation
// budgets must be measured on the binder, not on a path through the fake.
// Commit and rollback ignore SetLatency and Result.Delay because driver.Tx
// carries no context to abort them with. Result.FailRow is log-only: it appears
// in Execs() and is not reachable through the returned error.
//
// What it cannot prove. A stub reports whatever it is told, so Oracle's own
// semantics -- RowsAffected on an array MERGE, an all-NULL NCHAR bind, t_commit
// -- are not demonstrated here. Those belong to the live spike (P4-13).
package oratest
