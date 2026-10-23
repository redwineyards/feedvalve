package decode

import "feedvalve/internal/model"

// SessionEventOf reports the market-status observation a decoded unit carries,
// for the receive path's non-blocking hand-off to health (Vol 4 section 4.5,
// channel H).
//
// It lives here because MarketType is a decoder fact: all five status codes
// (6511, 6521, 6522, 6531, 6571) are one structure decoded by vctMsgs, which
// reads MarketType at payload offset 82 and files it as destination 2's row
// key. Reading it back from the unit's contribution needs no new field on
// UpdateUnit (whose layout is pinned) and no change to any decoder; ok is
// false for every other unit.
//
// RecvMono is whatever the unit carries, so call this AFTER the fan-out has
// stamped it.
func SessionEventOf(u *model.UpdateUnit) (ev model.SessionEvent, ok bool) {
	if !isVCTCode(u.Code) {
		return ev, false
	}
	c := &u.Contribs[destMarketStatus]
	if !c.Present() {
		return ev, false
	}
	return model.SessionEvent{
		MarketType: uint16(c.Row.Lo()),
		Code:       u.Code,
		LogTime:    u.LogTime,
		RecvMono:   u.RecvMono,
	}, true
}

func isVCTCode(code model.TransCode) bool {
	for _, c := range vctCodes {
		if c == code {
			return true
		}
	}
	return false
}
