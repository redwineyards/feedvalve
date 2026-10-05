package model

import "errors"

// Reject is the receive path's error type: a small enum, not an error.
//
// Returned by value, costs nothing, and maps 1:1 onto a counter name and onto a
// row of HLD section 9.1. Using an error here would allocate on a path that
// must not -- at 10,000 packets/s during a misframe, a wrapped error is both
// the loudest and the slowest thing in the process. Context belongs in the
// counter label, not in an error string.
type Reject uint8

const (
	// RejectNone is the zero value: no rejection.
	RejectNone Reject = iota
	// RejectOversizeDatagram is over 516 bytes; trimmed to 512 and processed (F6).
	RejectOversizeDatagram
	// RejectShortMessageLen is MessageLength below 40, which abandons the
	// remainder of the datagram (F4).
	RejectShortMessageLen
	// RejectDecompress means DecompressSafe failed; this sub-packet only (F3).
	RejectDecompress
	// RejectErrorCode is a non-zero header ErrorCode, meaning MS_ERROR_RESPONSE (F7).
	RejectErrorCode
	// RejectBodyLenMismatch is a declared MessageLength disagreeing with the
	// registry's BodyLen.
	RejectBodyLenMismatch
	// RejectShortPayload is a payload shorter than the declared body.
	RejectShortPayload
	// RejectUnknownTranscode means no decoder is configured (F2).
	RejectUnknownTranscode
	// RejectRecordCountClamped is NoOfRecords out of range; clamped and parsed (F8).
	RejectRecordCountClamped
	// RejectPanic is the value the recover boundary returns. Every other
	// outcome on that path has one, so this one exists too.
	RejectPanic
)

// rejectCounter maps each rejection to the obs counter it increments.
//
// This table is the whole reason Reject carries a Counter method: one place,
// so a rejection cannot be counted under two names in two volumes. Every name
// here already exists in the instrument ledger (Vol 0 section 8.2); none is
// invented.
//
// The index is the Reject value, so a value added without a counter shifts
// nothing and simply yields the empty string -- which TestCounterIsTotal
// rejects.
var rejectCounter = [...]string{
	RejectNone:               "",
	RejectOversizeDatagram:   "datagrams_oversize",
	RejectShortMessageLen:    "framing_abandoned",
	RejectDecompress:         "decompress_failed",
	RejectErrorCode:          "error_response",
	RejectBodyLenMismatch:    "body_len_mismatch",
	RejectShortPayload:       "short_payload",
	RejectUnknownTranscode:   "unknown_transcode",
	RejectRecordCountClamped: "record_count_clamped",
	RejectPanic:              "panics_recovered",
}

// Counter returns the obs counter name this rejection increments. RejectNone
// returns the empty string; every other value returns a name.
func (r Reject) Counter() string {
	if int(r) >= len(rejectCounter) {
		return ""
	}
	return rejectCounter[r]
}

func (r Reject) String() string {
	if c := r.Counter(); c != "" {
		return c
	}
	if r == RejectNone {
		return "none"
	}
	return "Reject(" + string(rune('0'+r)) + ")"
}

// Rejected reports whether this is an actual rejection.
func (r Reject) Rejected() bool { return r != RejectNone }

// ErrStallBudgetExpired is returned when a stall budget runs out -- either
// the receiver's enqueue budget or the decoder's fan-out budget.
//
// It is a RETURNED ERROR and not a fatal log, and that is the whole point of
// its existence. Overflow of a critical-path queue must stop the process
// loudly rather than discard silently, but stopping via os.Exit would skip
// every deferred cleanup: the drain would not run, in-flight batches would be
// abandoned and Oracle sessions left open. This travels up to main, which
// maps it to an exit code after the ordered shutdown has run.
//
// A sentinel rather than a formatted error because the paths that return it
// are the hot paths, and they must not allocate.
var ErrStallBudgetExpired = errors.New("model: stall budget expired")
