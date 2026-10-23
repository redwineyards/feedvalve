package decode

import (
	"testing"

	"feedvalve/internal/model"
)

// TestSessionEventOfReadsTheWireMarketTypeForAllFiveCodes is the P5-12
// verification that the market type reaches the receive path for every code in
// Vol 4 section 4.5's table.
//
// SETUP: each of 6511, 6521, 6522, 6531 and 6571 decoded from a payload with
// MarketType planted at absolute offset 82 (the hand-written table above),
// for market types 1 to 4.
// STIMULUS: SessionEventOf on the decoded unit.
// ASSERTION: the event carries that market type, the header's code and
// LogTime, and the unit's RecvMono; any other unit yields no event.
// WOULD HAVE CAUGHT: a producer reading the market type from the token (which
// is data here, not the key), or from a decoder that handles only 6511.
// NON-VACUITY: the loop runs 20 combinations and counts them.
func TestSessionEventOfReadsTheWireMarketTypeForAllFiveCodes(t *testing.T) {
	reg, _ := vctRegistry(t)
	seen := 0
	for _, code := range vctCodes {
		for mt := int16(1); mt <= 4; mt++ {
			e := reg.Lookup(code)
			f := newVctFixture(t, uint16(code), mt)
			f.setMessage(0, "")
			out := make([]model.UpdateUnit, 1)
			if _, err := e.Dec.Decode(model.MsgHeader{Code: code, LogTime: 77}, f.payload, out); err != nil {
				t.Fatalf("%d: %v", code, err)
			}
			out[0].RecvMono = 1234
			ev, ok := SessionEventOf(&out[0])
			want := model.SessionEvent{MarketType: uint16(mt), Code: code, LogTime: 77, RecvMono: 1234}
			if !ok || ev != want {
				t.Errorf("code %d market type %d: got %+v ok=%v, want %+v", code, mt, ev, ok, want)
			}
			seen++
		}
	}
	if seen != 20 {
		t.Fatalf("ran %d combinations, want 20", seen)
	}

	for _, code := range []model.TransCode{7208, 7305, 6541, 0} {
		u := model.UpdateUnit{Code: code}
		if _, ok := SessionEventOf(&u); ok {
			t.Errorf("code %d produced a session event", code)
		}
	}
	// A status code whose destination contribution is absent is not an event.
	if _, ok := SessionEventOf(&model.UpdateUnit{Code: 6511}); ok {
		t.Error("a 6511 unit with no destination 2 contribution produced a session event")
	}
}
