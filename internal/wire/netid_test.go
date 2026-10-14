package wire

import "testing"

// The netId oracle's tolerance, and its one hard rule: it cannot drop anything.
//
// These are the predicate half of gate G-FRM-5. The other half -- WARN,
// netid_mismatch++, and the payload processed anyway -- needs the receiver and
// lands with P2-04; what can be pinned here is that the only thing this package
// offers is a bool, so there is no drop for a caller to reach for.
func TestNetIDMatches(t *testing.T) {
	const commodity = 8

	for _, tc := range []struct {
		name     string
		observed byte
		expected int
		want     bool
	}{
		{"binary encoding", 8, commodity, true},
		{"ascii digit, because the bible declares cNetId CHAR", '8', commodity, true},
		{"equity against commodity -- G-FRM-5's payload", 4, commodity, false},
		{"ascii digit of the wrong segment", '4', commodity, false},
		{"expected 0 disables the comparison entirely", 4, 0, true},
		{"expected 0 accepts anything at all", 0xFF, 0, true},
		{"zero byte against a real expectation", 0, commodity, false},

		// The ASCII arm stops at 9 deliberately. A two-digit segment has no
		// single-byte ASCII form, so extending the arm would mean accepting
		// '0'+10 == ':' as segment 10 -- inventing an encoding rather than
		// tolerating an unknown one.
		{"two-digit segment, binary", 10, 10, true},
		{"two-digit segment does NOT get an ascii arm", ':', 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := NetIDMatches(tc.observed, tc.expected); got != tc.want {
				t.Errorf("NetIDMatches(%d, %d) = %v, want %v", tc.observed, tc.expected, got, tc.want)
			}
		})
	}
}

// TestEnvelopeNetIDMatches covers the once-per-datagram check. cNetId is
// CHAR[2] holding a small number and we do not know whether it is written left-
// or right-aligned, so either byte may carry it.
func TestEnvelopeNetIDMatches(t *testing.T) {
	const commodity = 8

	for _, tc := range []struct {
		name     string
		netID    []byte
		expected int
		want     bool
	}{
		{"left-aligned binary", []byte{8, 0}, commodity, true},
		{"right-aligned binary", []byte{0, 8}, commodity, true},
		{"left-aligned ascii", []byte{'8', ' '}, commodity, true},
		{"right-aligned ascii", []byte{' ', '8'}, commodity, true},
		{"neither byte carries it", []byte{4, 4}, commodity, false},
		{"check disabled", []byte{4, 4}, 0, true},

		// A two-byte field that did not arrive is a framing problem, not a
		// segment problem, and must not be reported as agreement.
		{"short slice cannot match", []byte{8}, commodity, false},
		{"empty slice cannot match", nil, commodity, false},

		// ...but with the check disabled there is nothing to disagree with,
		// and the caller logs the observed bytes rather than comparing them.
		{"short slice with the check disabled", []byte{8}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := EnvelopeNetIDMatches(tc.netID, tc.expected); got != tc.want {
				t.Errorf("EnvelopeNetIDMatches(%v, %d) = %v, want %v", tc.netID, tc.expected, got, tc.want)
			}
		})
	}
}

func TestNetIDChecksAllocateNothing(t *testing.T) {
	envelope := []byte{0, 8}
	if n := testing.AllocsPerRun(1000, func() {
		_ = NetIDMatches(4, 8)
		_ = EnvelopeNetIDMatches(envelope, 8)
	}); n != 0 {
		t.Fatalf("the netId oracle allocates %v times per datagram, want 0", n)
	}
}
