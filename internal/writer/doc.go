// Package writer owns statement assembly from a batch mask, the array binds, the
// transaction boundary, and per-row outcome attribution.
//
// writer MUST NOT import framing, decode, mirror or oratest, and never names
// go-ora or net itself. It may name database/sql (it executes through
// *sql.Tx and *sql.Stmt), and it reaches the driver and the sockets only
// through oradb (ruling D11).
//
// The Pool is the package's one entry point: NewPool, Start, Resize, Stopped,
// StopReason, Close. Its configuration arrives as plain values -- Config at
// startup and a func returning *Params per batch -- because writer may not
// import config (the same seam mirror uses).
//
// The writer pool is stateless: any writer can take any batch. The SET list of a
// statement comes from the mask the batch carries, never from the live entry's
// column mask, which may have moved on since the batch was pumped.
//
// Bind placeholders are positional -- :1, :2 -- and never ?. A ? is not a
// placeholder in Oracle; it fails at parse time with ORA-00936, which points
// nowhere near the real problem. There is no ON CONFLICT either: upserts are
// MERGE INTO ... USING (SELECT ... FROM dual) ON (...) WHEN MATCHED THEN UPDATE
// ... WHEN NOT MATCHED THEN INSERT.
//
// Per-row attribution uses a two-phase execute. go-ora supports array binds for
// MERGE, but it parses the per-row result array and then discards it behind
// unexported state, so the counts cannot be recovered through the driver. The
// writer therefore array-binds the whole batch and falls back to row-by-row only
// when a row-data error comes back, which recovers attribution without forking
// the driver and without paying row-by-row cost on the common path.
//
// Nothing the writer logs may contain the password, a bind value or statement
// text (gate G-SEC-1): errors reach the log only after oradb has redacted them,
// and a rejected row is identified by destination, key and slot, never by its
// contents. integration_test.go holds that scan over a full failure run, and
// TestDBIntegrationWriter, which skips unless FEEDVALVE_ORACLE_DSN is set, is
// the non-gating live spike of the open questions OQ-W3 lists.
//
// Carried over in spirit from poc/db.go's Exec and ExecTx usage.
//
// Specified by docs/lld/03-writer-persistence.md. Built in Phase 4.
package writer
