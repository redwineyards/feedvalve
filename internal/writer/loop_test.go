package writer

import (
	"errors"
	"testing"

	"feedvalve/internal/model"
	"feedvalve/internal/oradb"
)

// TestFailPendingNeverOverwritesATerminalOutcome: the loop's own sweep (acquire
// failure, FailFast) has the same contract as runBatch's.
func TestFailPendingNeverOverwritesATerminalOutcome(t *testing.T) {
	b := standard()
	b.Rows[1].Outcome = model.OutcomeRejected
	b.Rows[2].Outcome = model.OutcomeCommitted
	failPending(b, 942)
	want := []model.RowOutcome{
		model.OutcomeFailed, model.OutcomeRejected, model.OutcomeCommitted, model.OutcomeFailed,
		model.OutcomeFailed, model.OutcomeFailed, model.OutcomeFailed,
	}
	for i, o := range outcomes(b) {
		if o != want[i] {
			t.Errorf("row %d = %v, want %v", i, o, want[i])
		}
	}
	if b.Rows[0].ErrCode != 942 || b.Rows[1].ErrCode != 0 {
		t.Errorf("ErrCodes %d/%d, want 942/0", b.Rows[0].ErrCode, b.Rows[1].ErrCode)
	}

	// N beyond Rows must not panic.
	b2 := standard()
	b2.N = 99
	failPending(b2, 0)
	b2.N = -1
	failPending(b2, 0)
}

// TestResetForRetryRestoresOnlyFailedRows: a retry starts from Pending, but a
// row the grouper Rejected for an empty mask stays rejected.
func TestResetForRetryRestoresOnlyFailedRows(t *testing.T) {
	b := standard()
	b.Rows[0].Outcome = model.OutcomeRejected
	b.Err = errors.New("first attempt")
	failPending(b, 3113)
	resetForRetry(b)
	if b.Err != nil {
		t.Errorf("Err = %v after reset", b.Err)
	}
	for i, o := range outcomes(b) {
		want := model.OutcomePending
		if i == 0 {
			want = model.OutcomeRejected
		}
		if o != want || (want == model.OutcomePending && b.Rows[i].ErrCode != 0) {
			t.Errorf("row %d = %v/%d, want %v/0", i, o, b.Rows[i].ErrCode, want)
		}
	}
}

func TestStopErrorMatchesThePoolStoppedSentinelExceptForFailFast(t *testing.T) {
	cause := errors.New("db")
	cases := []struct {
		e    *StopError
		want bool
	}{
		{&StopError{Kind: StopFailFast, Cause: cause}, false},
		{&StopError{Kind: StopStatement, Identity: "d0/m3", Cause: cause}, true},
		{&StopError{Kind: StopNoWriters}, true},
	}
	for _, c := range cases {
		if got := errors.Is(c.e, oradb.ErrPoolStopped); got != c.want {
			t.Errorf("%v: Is(ErrPoolStopped) = %v, want %v", c.e.Kind, got, c.want)
		}
		if c.e.Cause != nil && !errors.Is(c.e, cause) {
			t.Errorf("%v: cause not reachable through Unwrap", c.e.Kind)
		}
		if c.e.Error() == "" {
			t.Errorf("%v: empty message", c.e.Kind)
		}
	}
}
