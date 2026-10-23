package writer

import (
	"strconv"

	"feedvalve/internal/oradb"
)

// StopKind says which rule stopped the pool. app maps FailFast to F13 and the
// other two to F20; both exit 4 (ruling D3).
type StopKind uint8

const (
	// StopFailFast: the 30 s pool clock or a writer's consecutive-failure
	// count fired (V3 9.3).
	StopFailFast StopKind = iota + 1
	// StopStatement: one statement identity failed on enough distinct writers
	// inside the window (V3 9.4).
	StopStatement
	// StopNoWriters: every writer retired itself and none is running (D4).
	StopNoWriters
)

func (k StopKind) String() string {
	switch k {
	case StopFailFast:
		return "fail_fast"
	case StopStatement:
		return "statement"
	case StopNoWriters:
		return "no_writers"
	}
	return "stop(" + strconv.Itoa(int(k)) + ")"
}

// StopError is why Pool.Stopped was closed (ruling D3). Cause is the redacted
// database failure that tripped the rule; it is nil for StopNoWriters after a
// panic. Identity is the statement identity ("d<dest>/m<mask hex>") for
// StopStatement and empty otherwise.
type StopError struct {
	Kind     StopKind
	Identity string
	Cause    error
}

func (e *StopError) Error() string {
	s := "writer: pool stopped (" + e.Kind.String() + ")"
	if e.Identity != "" {
		s += " statement " + e.Identity
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

func (e *StopError) Unwrap() error { return e.Cause }

// Is lets the statement and no-writers stops match oradb.ErrPoolStopped, the
// sentinel Vol 0 section 3.11 declares for exactly this.
func (e *StopError) Is(target error) bool {
	return target == oradb.ErrPoolStopped && e.Kind != StopFailFast
}
