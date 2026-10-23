package health

import (
	"testing"

	"feedvalve/internal/model"
)

func ev(mt uint16, code model.TransCode, logTime int32) model.SessionEvent {
	return model.SessionEvent{MarketType: mt, Code: code, LogTime: logTime}
}

// TestG_SESS_1_RegularOpenIsSixFiveOneOneAlone is gate G-SESS-1.
//
// SETUP: a fresh Unknown machine for market type 1.
// STIMULUS: 6511 alone.
// ASSERTION: Open, and no anomaly.
// WOULD HAVE CAUGHT: the stall bible 4.5 warns about, a machine requiring a
// pre-open before every open.
func TestG_SESS_1_RegularOpenIsSixFiveOneOneAlone(t *testing.T) {
	var s Sessions
	if st, _ := s.State(1); st != SessionUnknown {
		t.Fatalf("fresh state = %v, want unknown", st)
	}
	r := s.Apply(ev(1, 6511, 100))
	if st, _ := s.State(1); st != SessionOpen {
		t.Errorf("state = %v, want open", st)
	}
	if r.Anomaly || r.Ignored || r.Repeat || r.From != SessionUnknown || r.To != SessionOpen {
		t.Errorf("result = %+v, want a plain Unknown to Open", r)
	}
}

// TestG_SESS_2_SameDayReopenWithOneLogTime is gate G-SESS-2.
//
// SETUP: state Closed. STIMULUS: 6531, 6571, 6511 with the SAME LogTime.
// ASSERTION: ends Open; all three transitions reported, in order; no anomaly.
// Non-vacuity: the same three codes fed out of arrival order (6511 first) DO
// leave a different state, so arrival order is what is being decided.
// WOULD HAVE CAUGHT: a machine ordering by LogTime and collapsing the re-open
// (bible 4.5), which is seconds-resolution and ties on all three.
func TestG_SESS_2_SameDayReopenWithOneLogTime(t *testing.T) {
	var s Sessions
	s.Apply(ev(1, 6521, 50))
	var got []SessionResult
	for _, c := range []model.TransCode{6531, 6571, 6511} {
		got = append(got, s.Apply(ev(1, c, 200)))
	}
	if st, _ := s.State(1); st != SessionOpen {
		t.Fatalf("state = %v, want open", st)
	}
	want := [][2]SessionState{
		{SessionClosed, SessionPreOpen},
		{SessionPreOpen, SessionPreOpenEnded},
		{SessionPreOpenEnded, SessionOpen},
	}
	for i, w := range want {
		if got[i].From != w[0] || got[i].To != w[1] || got[i].Anomaly || got[i].Repeat {
			t.Errorf("step %d = %+v, want %v to %v, expected", i, got[i], w[0], w[1])
		}
	}

	var rev Sessions
	rev.Apply(ev(1, 6521, 50))
	for _, c := range []model.TransCode{6511, 6571, 6531} {
		rev.Apply(ev(1, c, 200))
	}
	if st, _ := rev.State(1); st != SessionPreOpen {
		t.Errorf("reverse arrival ended %v, want pre-open: arrival order must win", st)
	}
}

// TestG_SESS_3_RepeatIsIdempotentAndASkipStillAdvances is gate G-SESS-3.
//
// SETUP: state Open. STIMULUS: 6511 again, then 6522 (skipping 6521).
// ASSERTION: the first is a Repeat with no anomaly; the second sets PostClose
// and is an Anomaly; the machine advanced both times.
// WOULD HAVE CAUGHT: a machine that rejects an unexpected transition,
// turning one lost datagram (unrecoverable, bible 1) into a session-long
// wrong state.
func TestG_SESS_3_RepeatIsIdempotentAndASkipStillAdvances(t *testing.T) {
	var s Sessions
	s.Apply(ev(1, 6511, 1))
	r := s.Apply(ev(1, 6511, 2))
	if !r.Repeat || r.Anomaly || r.From != SessionOpen || r.To != SessionOpen {
		t.Errorf("repeat = %+v, want Repeat without Anomaly", r)
	}
	r = s.Apply(ev(1, 6522, 3))
	if !r.Anomaly || r.Repeat || r.To != SessionPostClose {
		t.Errorf("skip = %+v, want an Anomaly to post-close", r)
	}
	if st, _ := s.State(1); st != SessionPostClose {
		t.Errorf("state = %v, want post-close: the machine must still advance", st)
	}
}

// TestSessionEdgeSetIsPinned writes Figure 4.7 out by hand, as a 6x6 matrix
// of (from, code), and compares every cell. Every non-edge pair must count an
// anomaly AND still advance; every same-state pair is a Repeat. Non-vacuity:
// the table must contain both kinds of cell.
//
// If the user accepts D4, the two extra rows below change with expectedEdge.
func TestSessionEdgeSetIsPinned(t *testing.T) {
	type edge struct {
		from SessionState
		code model.TransCode
	}
	// Expected (non-anomalous, non-repeat) edges, Figure 4.7 as written.
	expected := map[edge]bool{
		{SessionUnknown, 6511}: true, {SessionUnknown, 6531}: true,
		{SessionUnknown, 6521}: true, {SessionUnknown, 6522}: true,
		{SessionUnknown, 6571}:      true,
		{SessionPreOpen, 6571}:      true,
		{SessionPreOpenEnded, 6511}: true,
		{SessionOpen, 6521}:         true,
		{SessionClosed, 6522}:       true,
		{SessionClosed, 6531}:       true,
		{SessionPostClose, 6531}:    true,
		{SessionOpen, 6531}:         true,
	}
	toState := map[model.TransCode]SessionState{
		6511: SessionOpen, 6521: SessionClosed, 6522: SessionPostClose,
		6531: SessionPreOpen, 6571: SessionPreOpenEnded,
	}
	var nExpected, nAnomaly, nRepeat int
	for from := SessionUnknown; from < numSessionStates; from++ {
		for code, to := range toState {
			s := Sessions{}
			s.m[0].state = from
			r := s.Apply(ev(1, code, 1))
			switch {
			case from == to:
				nRepeat++
				if !r.Repeat || r.Anomaly {
					t.Errorf("%v+%d: %+v, want Repeat", from, code, r)
				}
			case expected[edge{from, code}]:
				nExpected++
				if r.Repeat || r.Anomaly {
					t.Errorf("%v+%d: %+v, want an expected edge", from, code, r)
				}
			default:
				nAnomaly++
				if r.Repeat || !r.Anomaly {
					t.Errorf("%v+%d: %+v, want an Anomaly", from, code, r)
				}
			}
			if st, _ := s.State(1); st != to {
				t.Errorf("%v+%d advanced to %v, want %v (total machine)", from, code, st, to)
			}
		}
	}
	if nExpected != len(expected) || nAnomaly == 0 || nRepeat == 0 {
		t.Errorf("matrix covered %d expected (want %d), %d anomalies, %d repeats; the table is vacuous",
			nExpected, len(expected), nAnomaly, nRepeat)
	}
}

// TestSessionsAreIndependentPerMarketTypeAndIgnoreTheRest: types 1-4 each have
// their own machine; 0, 5 and a non-status code change nothing (D15).
func TestSessionsAreIndependentPerMarketTypeAndIgnoreTheRest(t *testing.T) {
	var s Sessions
	s.Apply(ev(1, 6511, 1))
	s.Apply(ev(4, 6531, 1))
	for mt, want := range map[uint16]SessionState{1: SessionOpen, 2: SessionUnknown, 3: SessionUnknown, 4: SessionPreOpen} {
		if got, ok := s.State(mt); !ok || got != want {
			t.Errorf("market type %d = %v ok=%v, want %v", mt, got, ok, want)
		}
	}
	for _, e := range []model.SessionEvent{ev(0, 6511, 1), ev(5, 6511, 1), ev(1, 7208, 1)} {
		if r := s.Apply(e); !r.Ignored {
			t.Errorf("%+v not ignored", e)
		}
	}
	if st, _ := s.State(1); st != SessionOpen {
		t.Errorf("an ignored event changed market type 1 to %v", st)
	}
	if _, ok := s.State(5); ok {
		t.Error("State(5) reported ok")
	}
	// Gauge values are pinned by Vol 0 section 8.2.
	for st, v := range map[SessionState]uint8{SessionUnknown: 0, SessionPreOpen: 1,
		SessionPreOpenEnded: 2, SessionOpen: 3, SessionClosed: 4, SessionPostClose: 5} {
		if uint8(st) != v {
			t.Errorf("%v gauge = %d, want %d", st, uint8(st), v)
		}
	}
}
