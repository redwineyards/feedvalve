package writer

import (
	"feedvalve/internal/obs"
	"feedvalve/internal/oradb"
)

// retryDecision is what the writer does after a failed execution (V3 9.2).
type retryDecision uint8

const (
	// giveBack: return the batch now with Err set and every row Failed.
	giveBack retryDecision = iota
	// retryOnce: run the same batch again on a fresh session.
	retryOnce
	// phase2: go to the row-by-row execution that attributes the bad row.
	phase2
)

// batchRetry is one batch's retry budget. The zero value is a fresh batch.
type batchRetry struct{ used bool }

// decide classifies a failure. RowData always goes to phase 2 (D6b: 01722 is
// a trigger; D6d: the caller must not retry after a phase 2 non-row failure,
// which it enforces by not calling decide again). Statement is never
// retried. Session, Deadlock, Space and Unknown (D6a: 24381 is Unknown) get
// exactly one retry per batch.
func (r *batchRetry) decide(c oradb.ErrClass) retryDecision {
	switch c {
	case oradb.ErrRowData:
		return phase2
	case oradb.ErrSession, oradb.ErrDeadlock, oradb.ErrSpace, oradb.ErrUnknown:
		if !r.used {
			r.used = true
			return retryOnce
		}
	}
	return giveBack
}

// retryClassOf maps a retryable class to its write_retry{class} label. ok is
// false for RowData and Statement, which are never counted as a retry.
func retryClassOf(c oradb.ErrClass) (obs.RetryClass, bool) {
	switch c {
	case oradb.ErrSession:
		return obs.RetrySession, true
	case oradb.ErrDeadlock:
		return obs.RetryDeadlock, true
	case oradb.ErrSpace:
		return obs.RetrySpace, true
	case oradb.ErrUnknown:
		return obs.RetryUnknown, true
	}
	return 0, false
}
