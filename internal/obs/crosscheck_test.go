// This file is the external half of obs's tests, and it is the only place obs
// is tested against another package in the tree.
//
// It can import model because gate G-DEP-1 reads `go list -deps` WITHOUT
// -test, and internal/app/deps_test.go documents that exclusion deliberately:
// "Test-only imports are deliberately excluded: lzo's tests import a reference
// compressor that must never reach the binary, and that distinction is the
// whole point of gate G-DEP-3."
//
// That exclusion is what makes these checks possible at all. obs MUST NOT
// import model in the build, so two constants and one counter-name table exist
// in both packages; the only thing that can keep them from drifting apart is a
// test that compares them. Two sources of truth are only safe when something
// checks them against each other.
package obs_test

import (
	"testing"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// TestLocalConstantsMatchModel defends the duplication that gate G-DEP-1
// forces.
//
// model/const.go says re-declaring these per package "would defeat gate
// G-BLD-2, whose entire purpose is that these numbers cannot drift apart", and
// it is right. obs has to anyway: it may not import model, because obs is
// imported from everywhere and one edge back would make half the tree cyclic.
// So the compile-time pin lives in obs/const.go and the agreement lives here.
//
// If this fails, the fix is never to change obs's constant alone -- a label
// array sized for three destinations against a model that now has four writes
// past the end of a shard's slot.
func TestLocalConstantsMatchModel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		local int
		model int
	}{
		{"maxDestinations", obs.MaxDestinationsForTest, model.MaxDestinations},
		{"maxColumns", obs.MaxColumnsForTest, model.MaxColumns},
	} {
		if tc.local != tc.model {
			t.Errorf("obs's %s is %d but model's is %d. obs may not import model "+
				"(gate G-DEP-1), so these are two declarations of one number and they "+
				"have drifted. Re-derive the label arrays, do not just edit obs.",
				tc.name, tc.local, tc.model)
		}
	}
}

// TestEveryRejectCounterExistsInTheLedger closes the loop P1-03 left open.
//
// model.Reject is the receive path's error type, and its doc comment says it
// "maps 1:1 onto a counter name", with rejectCounter asserting that "every name
// here already exists in the instrument ledger (Vol 0 section 8.2); none is
// invented". Until now nothing checked that claim, because the ledger did not
// exist yet. This is the check.
//
// A Reject naming a counter the ledger does not have would be counted nowhere:
// the receive path would reject a packet, look up a name, and increment
// nothing -- the silent-rejection failure mode that gate G-OBS-3 exists to
// prevent at the other end of the same pipe.
func TestEveryRejectCounterExistsInTheLedger(t *testing.T) {
	names := obs.LedgerNamesForTest()
	inLedger := make(map[string]bool, len(names))
	for _, n := range names {
		inLedger[n] = true
	}

	checked := 0
	for r := model.Reject(0); ; r++ {
		name := r.Counter()
		if name == "" {
			// RejectNone, and the end of the table. model's own
			// TestCounterIsTotal proves there are no interior gaps, so the
			// first empty name after RejectNone means the enum is exhausted.
			if r > model.RejectNone {
				break
			}
			continue
		}
		checked++
		if !inLedger[name] {
			t.Errorf("model.Reject(%d).Counter() is %q, which is not in the obs "+
				"ledger. A rejection that names a counter nobody declared increments "+
				"nothing: the packet is dropped and the only record of it is absent.",
				r, name)
		}
	}

	// model has nine rejections that name a counter. Pinned, so that a Reject
	// added in Phase 2 without a ledger entry fails here rather than passing a
	// loop that happens to find nothing to check.
	if checked != 9 {
		t.Errorf("checked %d Reject counter names, want 9. model.rejectCounter "+
			"changed; confirm the new name is in Vol 0 section 8.2 or section 8.2a "+
			"before adding it to the ledger", checked)
	}
}
