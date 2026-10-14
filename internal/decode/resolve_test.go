package decode

import (
	"testing"

	"feedvalve/internal/model"
)

// TestG_HDR_2_ResolutionIsByCodeAndLength is gate G-HDR-2 (Vol 4 section 7.3).
//
// The gate's own wording: payloads for 6503 at 60 bytes, 6541 at 48 and 7208 at
// 478 resolve to HeaderMessage, HeaderMessage and HeaderBcast -- "and a 7305
// whose payload happens to be 48 bytes long is still HeaderBcast, because
// resolution is by (code, length) and not by length alone".
//
// The defect it guards is a length-only rule over-matching and misreading an
// ordinary message's header (HLD R10, OQ-6). The rule is DESIGNED, not
// specified: the bible says which codes use which header but states no
// discrimination procedure, so this test is where the design is held to its
// claim.
func TestG_HDR_2_ResolutionIsByCodeAndLength(t *testing.T) {
	for _, tc := range []struct {
		name       string
		payload    []byte
		payloadLen int
		wantKind   model.HeaderKind
		wantCode   model.TransCode
		wantKnown  bool
	}{
		{
			name:       "6541 at 48 is the heartbeat",
			payload:    messageHeader(t, 48, 6541, 40, heartbeatTimestamp),
			payloadLen: 48,
			wantKind:   model.HeaderMessage, wantCode: 6541, wantKnown: true,
		},
		{
			name:       "6503 at 60 is the asset interest rate",
			payload:    messageHeader(t, 60, 6503, 52, heartbeatTimestamp),
			payloadLen: 60,
			wantKind:   model.HeaderMessage, wantCode: 6503, wantKnown: true,
		},
		{
			name:       "7208 at 478 is a broadcast",
			payload:    bcastHeader(t, 478, 7208, 470, 1234),
			payloadLen: 478,
			wantKind:   model.HeaderBcast, wantCode: 7208, wantKnown: false,
		},

		// The gate's named case. A 7305 arriving at 48 bytes has the right
		// LENGTH for a heartbeat and the wrong code at offset 8, so a
		// length-only rule would read its BCAST_HEADER as a MESSAGE_HEADER and
		// hand the gap detector eight bytes of Timestamp.
		{
			name:       "7305 that happens to be 48 bytes is still a broadcast",
			payload:    bcastHeader(t, 48, 7305, 40, 99),
			payloadLen: 48,
			wantKind:   model.HeaderBcast, wantCode: 7305, wantKnown: false,
		},

		// And the mirror image: the right code at the wrong length. Both
		// halves of the pair have to be load-bearing or the pair is decoration.
		{
			name:       "6541 at the wrong length is not a heartbeat",
			payload:    messageHeader(t, 60, 6541, 52, heartbeatTimestamp),
			payloadLen: 60,
			wantKind:   model.HeaderBcast, wantKnown: false,
		},
		{
			name:       "6503 at the heartbeat's length is not an asset rate",
			payload:    messageHeader(t, 48, 6503, 40, heartbeatTimestamp),
			payloadLen: 48,
			wantKind:   model.HeaderBcast, wantKnown: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, code, known := Resolve(tc.payload, tc.payloadLen)
			if kind != tc.wantKind {
				t.Errorf("kind = %v, want %v", kind, tc.wantKind)
			}
			if tc.wantCode != 0 && code != tc.wantCode {
				t.Errorf("code = %d, want %d", code, tc.wantCode)
			}
			if known != tc.wantKnown {
				t.Errorf("known = %v, want %v", known, tc.wantKnown)
			}
			// known is meaningful only for the two MESSAGE_HEADER shapes; for
			// a BCAST_HEADER it says nothing about the registry, which is a
			// separate question asked later (Vol 1 section 8.3).
			if kind == model.HeaderBcast && known {
				t.Error("known must be false for a BCAST_HEADER -- registry membership is not resolution's question")
			}
		})
	}
}

// TestResolveReadsOnlyFourBytes is INV-RESOLVE, asserted rather than asserted
// about: "Resolve reads exactly four bytes of the payload -- offsets 8-9 and
// 18-19 -- plus payloadLen. It takes no registry, no config and no state."
//
// The test mutates every OTHER byte of a payload and requires the answer not to
// move. That covers the invariant from the outside: any new read of a field
// this function has no business consulting -- a length word, a netId, a
// sequence number -- changes an answer here.
func TestResolveReadsOnlyFourBytes(t *testing.T) {
	base := bcastHeader(t, 478, 7208, 470, 4242)
	wantKind, wantCode, wantKnown := Resolve(base, 478)

	for off := range base {
		if off == offsetCandidateB || off == offsetCandidateB+1 ||
			off == offsetCandidateA || off == offsetCandidateA+1 {
			continue
		}
		mutated := append([]byte(nil), base...)
		mutated[off] ^= 0xFF

		kind, code, known := Resolve(mutated, 478)
		if kind != wantKind || code != wantCode || known != wantKnown {
			t.Fatalf("flipping payload[%d] changed resolution to (%v, %d, %v), want (%v, %d, %v): "+
				"Resolve is reading a byte it has no business reading",
				off, kind, code, known, wantKind, wantCode, wantKnown)
		}
	}
}

// TestResolveIsPure covers the other half of INV-RESOLVE: same input, same
// answer, no accumulated state between calls. A registry or a cache sneaking in
// later would show up as an answer that depends on call order.
func TestResolveIsPure(t *testing.T) {
	heartbeat := messageHeader(t, 48, 6541, 40, heartbeatTimestamp)
	broadcast := bcastHeader(t, 478, 7208, 470, 1)

	for i := range 100 {
		if k, c, known := Resolve(heartbeat, 48); k != model.HeaderMessage || c != 6541 || !known {
			t.Fatalf("call %d: heartbeat resolved as (%v, %d, %v)", i, k, c, known)
		}
		if k, c, known := Resolve(broadcast, 478); k != model.HeaderBcast || c != 7208 || known {
			t.Fatalf("call %d: broadcast resolved as (%v, %d, %v)", i, k, c, known)
		}
	}
}

func TestResolveAllocatesNothing(t *testing.T) {
	p := bcastHeader(t, 478, 7208, 470, 1)
	if n := testing.AllocsPerRun(1000, func() {
		_, _, _ = Resolve(p, 478)
	}); n != 0 {
		t.Fatalf("Resolve allocates %v times per call, want 0", n)
	}
}

// TestResolveLengthConstantsAreDerived documents why 48 and 60 are not written
// as literals in resolve.go. 6541 is MESSAGE_HEADER only, 40 bytes (bible
// section 4.12); 6503 is 52 (section 4.15); both gain the 8-byte prefix.
func TestResolveLengthConstantsAreDerived(t *testing.T) {
	if heartbeatPayloadLen != 48 {
		t.Errorf("heartbeat payload length = %d, want 8 + 40", heartbeatPayloadLen)
	}
	if assetRatePayloadLen != 60 {
		t.Errorf("asset rate payload length = %d, want 8 + 52", assetRatePayloadLen)
	}
}
