package health

import "feedvalve/internal/model"

// SessionState is the session_state gauge value (Vol 4 section 4.2).
type SessionState uint8

const (
	SessionUnknown      SessionState = 0
	SessionPreOpen      SessionState = 1
	SessionPreOpenEnded SessionState = 2
	SessionOpen         SessionState = 3
	SessionClosed       SessionState = 4
	SessionPostClose    SessionState = 5
)

const numSessionStates = 6

func (s SessionState) String() string {
	switch s {
	case SessionUnknown:
		return "unknown"
	case SessionPreOpen:
		return "pre-open"
	case SessionPreOpenEnded:
		return "pre-open-ended"
	case SessionOpen:
		return "open"
	case SessionClosed:
		return "closed"
	case SessionPostClose:
		return "post-close"
	}
	return "invalid"
}

// StateOf is the whole machine: the state a market-status code establishes.
// ok is false for any other code.
func StateOf(code model.TransCode) (SessionState, bool) {
	switch code {
	case 6511:
		return SessionOpen, true
	case 6521:
		return SessionClosed, true
	case 6522:
		return SessionPostClose, true
	case 6531:
		return SessionPreOpen, true
	case 6571:
		return SessionPreOpenEnded, true
	}
	return SessionUnknown, false
}

// expectedEdge is Figure 4.7 as written, one table so that a change to the
// edge set (D4: Closed to Open and PostClose to Open are a pending proposal)
// is a one-line change here. It decides expected versus anomaly and nothing
// else. Unknown is a source only: no code leads back to it.
var expectedEdge = [numSessionStates][numSessionStates]bool{
	SessionUnknown: {
		SessionOpen: true, SessionPreOpen: true, SessionClosed: true,
		SessionPostClose: true, SessionPreOpenEnded: true,
	},
	SessionPreOpen:      {SessionPreOpenEnded: true},
	SessionPreOpenEnded: {SessionOpen: true},
	SessionOpen:         {SessionClosed: true, SessionPreOpen: true},
	SessionClosed:       {SessionPostClose: true, SessionPreOpen: true},
	SessionPostClose:    {SessionPreOpen: true},
}

// Session is the state machine of one market type. Next state is always
// StateOf(code): the machine never refuses to advance, because refusing turns
// one lost datagram into a session-long wrong state (Vol 4 section 4.3).
type Session struct{ state SessionState }

// State is the gauge value.
func (s *Session) State() SessionState { return s.state }

// SessionResult is what one Apply did.
type SessionResult struct {
	// Ignored: the market type is outside 1-4 or the code is not a status
	// code. Nothing changed. The caller logs this rate-limited and counts
	// nothing (D15).
	Ignored    bool
	MarketType uint16
	Code       model.TransCode
	From, To   SessionState
	// Repeat: the same state again. Applied, not an anomaly, not logged.
	Repeat bool
	// Anomaly: the edge is not in Figure 4.7. Applied all the same.
	Anomaly bool
}

// Sessions holds one Session per market type 1-4 (1 Normal, 2 Odd Lot, 3 Spot,
// 4 Auction). It is owned by one goroutine.
type Sessions struct{ m [4]Session }

// Apply consumes one event in arrival order. LogTime and RecvMono are not
// consulted: health describes what arrived, and the mirror alone decides what
// is written (Vol 4 section 4.3).
func (s *Sessions) Apply(ev model.SessionEvent) SessionResult {
	r := SessionResult{MarketType: ev.MarketType, Code: ev.Code}
	next, ok := StateOf(ev.Code)
	if !ok || ev.MarketType < 1 || ev.MarketType > 4 {
		r.Ignored = true
		return r
	}
	m := &s.m[ev.MarketType-1]
	r.From, r.To = m.state, next
	r.Repeat = r.From == next
	r.Anomaly = !r.Repeat && !expectedEdge[r.From][next]
	m.state = next
	return r
}

// State is the gauge value for a market type, and false outside 1-4.
func (s *Sessions) State(marketType uint16) (SessionState, bool) {
	if marketType < 1 || marketType > 4 {
		return SessionUnknown, false
	}
	return s.m[marketType-1].state, true
}
