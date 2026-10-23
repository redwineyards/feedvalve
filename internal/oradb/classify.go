package oradb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/sijms/go-ora/v2/network"
)

// ErrClass is what the writer branches on. Its values are the closed set of
// V3 §8.2; ErrSpace is the A4 addition.
type ErrClass int

const (
	// ErrUnknown: anything the table does not name. It is the zero value so
	// an uninitialised Error never reads as the lenient row-data class.
	ErrUnknown ErrClass = iota
	// ErrRowData: a single row is the culprit; the rest of the batch would
	// have committed.
	ErrRowData
	// ErrSession: the connection or instance is the problem. Retry.
	ErrSession
	// ErrDeadlock: lock contention. Retry once.
	ErrDeadlock
	// ErrSpace: tablespace exhaustion. Retries, but never self-heals.
	ErrSpace
	// ErrStatement: the SQL itself is wrong. Never retry.
	ErrStatement
)

func (c ErrClass) String() string {
	switch c {
	case ErrRowData:
		return "row_data"
	case ErrSession:
		return "session"
	case ErrDeadlock:
		return "deadlock"
	case ErrSpace:
		return "space"
	case ErrStatement:
		return "statement"
	default:
		return "unknown"
	}
}

// Error is a classified database failure. Err is already redacted: that
// happens at construction, not at print time, so every rendering is safe.
//
// Callers must branch on Class and Code, never on errors.Is/As through Err:
// when redaction changed the text the original chain is dropped, so the
// result would differ between leaky and clean errors.
type Error struct {
	Class ErrClass
	Code  int // the bare ORA number; 0 when the failure carried none
	Err   error
}

func (e *Error) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("oradb: %s ORA-%05d: %v", e.Class, e.Code, e.Err)
	}
	return fmt.Sprintf("oradb: %s: %v", e.Class, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// oracleErrCode returns the ORA number of err, found with errors.As on the
// driver's *network.OracleError. It never inspects text: OracleError.Error()
// is synthesised and renders code 60 as "ORA-60".
func oracleErrCode(err error) (int, bool) {
	var oe *network.OracleError
	if errors.As(err, &oe) {
		return oe.ErrCode, true
	}
	return 0, false
}

// codeClass is the table of V3 §8.2. 24381 is deliberately absent (D6a): it
// falls through to ErrUnknown. 01722 is ErrRowData (D6b).
var codeClass = map[int]ErrClass{
	1: ErrRowData, 1400: ErrRowData, 1438: ErrRowData, 1722: ErrRowData,
	2290: ErrRowData, 2291: ErrRowData, 12899: ErrRowData,

	28: ErrSession, 1012: ErrSession, 1033: ErrSession, 1034: ErrSession,
	1089: ErrSession, 2396: ErrSession, 2399: ErrSession, 3113: ErrSession,
	3114: ErrSession, 3135: ErrSession, 12170: ErrSession, 12514: ErrSession,
	12537: ErrSession, 12541: ErrSession,

	1653: ErrSpace, 1654: ErrSpace, 1688: ErrSpace, 1691: ErrSpace,

	60: ErrDeadlock, 54: ErrDeadlock,

	904: ErrStatement, 936: ErrStatement, 942: ErrStatement, 917: ErrStatement,
	920: ErrStatement, 933: ErrStatement, 947: ErrStatement, 1031: ErrStatement,
}

// Classify maps a driver error to the class the writer branches on. Context and
// driver sentinels come first, then the ORA code; error text is never examined.
//
// ok is false for nil and for context.Canceled: that is shutdown, not a
// failure, and the caller must not count, retry or escalate it (V3 §8.3).
func (r Redactor) Classify(err error) (e *Error, ok bool) {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil, false
	}
	safe := r.redact(err)
	code, found := oracleErrCode(err)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, driver.ErrBadConn) {
		return &Error{Class: ErrSession, Code: code, Err: safe}, true
	}
	if !found {
		return &Error{Class: ErrUnknown, Err: safe}, true
	}
	class, known := codeClass[code]
	if !known {
		class = ErrUnknown
	}
	return &Error{Class: class, Code: code, Err: safe}, true
}
