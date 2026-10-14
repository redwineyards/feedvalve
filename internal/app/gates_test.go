// The build-time gate failure harness: P1-10's answer to the one sentence that
// defines the task.
//
//	"Each landed gate is shown to FAIL when its invariant is deliberately
//	 broken, then restored. A gate that has never been seen to fail has not
//	 been tested."
//
// Most of Vol 4 section 7.1's gates are constant expressions -- the two-sided
// uint(x-N) + uint(N-x) idiom, and the one-sided uint(limit-x) for a bound.
// They are the cheapest and strongest gates in the design, and until now NONE
// of them had ever been demonstrated failing by anything reproducible, for a
// structural reason: breaking a constant expression breaks the build, so it
// cannot be exercised from inside the test binary that would assert it. The
// verification existed only as sentences in commit messages.
//
// go build -overlay closes that. For each gate this writes a mutated copy of
// the real source into t.TempDir(), builds the REAL package against it through
// an overlay, and asserts the build fails. Real compiler, real types, real
// gate, and the working tree is never touched -- there is nothing to revert and
// nothing a crashed test could leave behind.
package app_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// mutation is one gate, one deliberate break.
type mutation struct {
	// gate is the identifier from Vol 4 section 7.1, or the section that
	// states the invariant when it has no gate id of its own.
	gate string

	// name distinguishes several mutations of one gate.
	name string

	// pkg is the package to build, as a go command pattern.
	pkg string

	// file is the source to mutate, relative to the module root.
	file string

	// from must appear EXACTLY ONCE in file; to replaces it.
	from, to string

	// wantErr is a substring the compiler's complaint must contain. Checked so
	// a build that fails for an unrelated reason -- a typo in the mutation, a
	// broken overlay path -- cannot be mistaken for the gate firing.
	wantErr string

	// why is what the gate protects, printed when it does not fire.
	why string
}

// mutations is one row per build-time gate in the tree.
//
// Adding a gate means adding a row here. A gate with no row has never been seen
// to fail, which by the card's own standard means it has not been tested.
var mutations = []mutation{
	{
		gate: "G-BLD-2", name: "maxDestinations",
		pkg: "./internal/model", file: "internal/model/const.go",
		from: "MaxDestinations = 3", to: "MaxDestinations = 4",
		wantErr: "overflows",
		why: "MaxContributors was once four, and raising it to fix one destination " +
			"silently changed Entry's size and every memory figure in the design. " +
			"These five constants are what make the next such change deliberate.",
	},
	{
		gate: "G-BLD-2", name: "imageSize",
		pkg: "./internal/model", file: "internal/model/const.go",
		from: "ImageSize = 256", to: "ImageSize = 192",
		wantErr: "overflows",
		why: "ImageSize bounds every destination's accumulated row image (ASM-16b). " +
			"Shrinking it must break the build, not overwrite the neighbouring arena slot.",
	},
	{
		gate: "G-LAY-2", name: "Entry-size",
		pkg: "./internal/model", file: "internal/model/layout.go",
		from: "entrySize        = 424", to: "entrySize        = 432",
		wantErr: "overflows",
		why: "A field inserted without updating the explicit _ [n]byte padding changes " +
			"every memory figure AND desynchronises Vol 0's offset tables from the " +
			"compiler -- the exact drift Vol 0 section 2 rule 6 exists to prevent.",
	},
	{
		gate: "G-LAY-2", name: "Datagram-size",
		pkg: "./internal/model", file: "internal/model/layout.go",
		from: "datagramSize     = 1040", to: "datagramSize     = 1048",
		wantErr: "overflows",
		why: "Datagram is the seventh size-pinned type. Vol 4's section 7.1 table lists " +
			"only six because it predates the QD merge; the pin is what makes the " +
			"ring's 8.5 MB figure a fact rather than a claim.",
	},
	{
		gate: "G-LAY-2", name: "Datagram-field-offset",
		pkg: "./internal/model", file: "internal/model/layout.go",
		from:    "unsafe.Offsetof(Datagram{}.Buf)-16) + uint(16-",
		to:      "unsafe.Offsetof(Datagram{}.Buf)-17) + uint(17-",
		wantErr: "overflows",
		why: "The offsets are pinned as well as the totals, because a struct can keep " +
			"its size while two fields swap places -- which would move every field " +
			"the wire layout depends on without changing a single byte count.",
	},
	{
		gate: "G-LAY-1", name: "7208-record-stride",
		pkg: "./internal/decode", file: "internal/decode/mbp_only_7208.go",
		from: "mbpRecordSize  = 214", to: "mbpRecordSize  = 213",
		wantErr: "overflows",
		why: "THE regression this project's gates exist for. 213 is the length the bible " +
			"DECLARES for INTERACTIVE_ONLY_MBP_DATA and 214 is its true stride: " +
			"NetChangeIndicator is the record's only odd-sized member, at even offset 16, so " +
			"pack(2) puts one pad byte at 17. At 213 the message totals 468 and the spec's " +
			"own 470 is unreachable -- and the symptom is not a crash: every field before " +
			"offset 17 still decodes correctly while the prices turn to noise and records[1] " +
			"starts a byte early. poc carried it for months.",
	},
	{
		gate: "G-LAY-1", name: "registry-headroom",
		pkg: "./internal/decode", file: "internal/decode/registry.go",
		from: "maxRegistered = 32", to: "maxRegistered = 24",
		wantErr: "overflows",
		why: "The first G-LAY-1 row, and the one that proves the IDIOM rather than a " +
			"decoder: internal/decode/layout.go holds the two-sided form and the rule for " +
			"choosing it over the one-sided one, and each decoder's own constant joins the " +
			"table in P2-06. 32 is sized from the bible's catalogue of 21 broadcast codes, " +
			"so shrinking it must break the build rather than silently bound the inventory " +
			"below what the protocol defines.",
	},
	{
		gate: "G-LAY-1", name: "fanout-buffer-overrun",
		pkg: "./internal/decode", file: "internal/decode/decoder.go",
		from: "maxDecoderUnits = 35", to: "maxDecoderUnits = 48",
		wantErr: "overflows",
		why: "The ONE-SIDED form, which is correct here because the rule is 'at most': a " +
			"decoder's declared fan-out must fit fanoutBuf's 40 units. A decoder claiming " +
			"more would write past the buffer on its first message, and 35 (7320/7210) is " +
			"the largest the bible catalogues -- so raising it past 40 has to be a build " +
			"failure rather than a buffer overrun at market open.",
	},
	{
		gate: "G-LAY-3", name: "destination-image-overruns",
		pkg: "./internal/destinations", file: "internal/destinations/layout.go",
		from: "quoteImageLen  = 181", to: "quoteImageLen  = 300",
		wantErr: "overflows",
		why: "ASM-16b: a destination whose accumulated columns grow past Entry.Image's " +
			"256 bytes silently overwrites the neighbouring arena slot. The one-sided " +
			"form is correct here because the rule is 'at most', not 'exactly'.",
	},
	{
		gate: "G-BLD-1", name: "prefix-length",
		pkg: "./internal/wire", file: "internal/wire/const.go",
		from: "BroadcastDataPrefixLen = 8", to: "BroadcastDataPrefixLen = 9",
		wantErr: "overflows",
		why: "bible section 3.4 calls the 8-byte prefix 'the single most common decoding " +
			"error'. Getting it wrong shifts EVERY field by eight bytes, and the " +
			"symptom is plausible-looking data rather than a crash.",
	},
	{
		gate: "G-BLD-1", name: "datagram-envelope-cap",
		pkg: "./internal/wire", file: "internal/wire/const.go",
		from:    "MaxDatagramLen = 2 + 2 + MaxPackedDataLen",
		to:      "MaxDatagramLen = 2 + 2 + MaxPackedDataLen + 4",
		wantErr: "overflows",
		why: "516 is derived from the envelope's own fields, not asserted independently, " +
			"so the derivation and the declared total have to agree. A datagram over " +
			"516 bytes is an envelope-assumption break and must be reported, not trimmed.",
	},
	{
		gate: "G-BLD-2", name: "obs-local-copy",
		pkg: "./internal/obs", file: "internal/obs/const.go",
		from: "maxDestinations = 3", to: "maxDestinations = 4",
		wantErr: "overflows",
		why: "obs may not import model (G-DEP-1), so it re-declares this constant and " +
			"pins it locally. The pin plus TestLocalConstantsMatchModel is what keeps " +
			"two declarations of one number from drifting apart.",
	},
	{
		gate: "Vol 0 section 8.1", name: "dynamic-table-power-of-two",
		pkg: "./internal/obs", file: "internal/obs/const.go",
		from: "unknownTranscodeSlots = 64", to: "unknownTranscodeSlots = 63",
		wantErr: "overflows",
		why: "The unknown-transcode table masks a code to find its probe, and a mask is " +
			"only a modulo for a power of two. A non-power-of-two biases every probe " +
			"toward the low slots and silently loses codes to the overflow bucket.",
	},
	{
		gate: "Vol 0 section 9.5.3", name: "qd-cache-line-separation",
		pkg: "./internal/framing", file: "internal/framing/ring.go",
		from: "const cacheLine = 128", to: "const cacheLine = 64",
		wantErr: "overflows",
		why: "head and tail must sit one ppc64 cache line apart, and the line is 128 " +
			"bytes there, not 64. Unpadded, the producer and consumer write the same " +
			"line on every operation and false sharing costs more than the lock the " +
			"structure exists to avoid -- with no correctness symptom and no profiler " +
			"on the target to find it.",
	},
	{
		gate: "Vol 0 section 9.5.4", name: "spin-bound-ceiling",
		pkg: "./internal/framing", file: "internal/framing/consume.go",
		from: "const spinBeforePark = 64", to: "const spinBeforePark = 4096",
		wantErr: "overflows",
		why: "A spin an order of magnitude longer is a different design -- it starts " +
			"burning the entitled capacity that section 9.5.4 rejects busy-waiting " +
			"over -- and should fail the build rather than be reviewed.",
	},
}

// TestEveryBuildTimeGateCanBeSeenToFail is the harness.
func TestEveryBuildTimeGateCanBeSeenToFail(t *testing.T) {
	if testing.Short() {
		t.Skip("invokes the Go toolchain once per gate")
	}
	requireGoToolchain(t)
	root := moduleRoot(t)

	// FIRST, the clean build of every package a mutation targets.
	//
	// Without this the harness is worthless: an overlay whose paths do not
	// match is silently ignored by the go command, the build fails for some
	// unrelated reason, and every row reports its gate "firing correctly"
	// while nothing has been tested at all.
	for _, pkg := range distinctPkgs() {
		if out, err := goBuild(t, root, pkg, ""); err != nil {
			t.Fatalf("%s does not build cleanly, so no mutation of it proves anything:\n%s",
				pkg, out)
		}
	}

	for _, m := range mutations {
		t.Run(m.gate+"/"+m.name, func(t *testing.T) {
			overlay := writeMutation(t, root, m)

			out, err := goBuild(t, root, m.pkg, overlay)
			if err == nil {
				t.Fatalf("THE GATE DID NOT FIRE.\n"+
					"Replacing %q with %q in %s still compiles, so %s is not actually "+
					"enforcing its invariant.\n\n%s",
					m.from, m.to, m.file, m.gate, m.why)
			}
			if !strings.Contains(out, m.wantErr) {
				t.Errorf("the build failed, but not in the way %s should fail.\n"+
					"want a message containing %q, got:\n%s\n\n"+
					"A build that fails for an unrelated reason -- a stale mutation, a "+
					"broken overlay -- must not be mistaken for the gate firing.",
					m.gate, m.wantErr, out)
			}
			if !strings.Contains(out, filepath.Base(m.file)) {
				t.Errorf("the failure does not mention %s, so it is probably not this "+
					"gate:\n%s", filepath.Base(m.file), out)
			}
		})
	}
}

// TestTheHarnessLeavesTheWorkingTreeUntouched is not housekeeping.
//
// A harness that wrote its mutation into the repo instead of t.TempDir() would
// behave identically -- every gate would fire, every assertion would pass --
// right up to the moment it corrupted the working tree, or crashed and left a
// deliberately broken constant behind for the next person to find.
func TestTheHarnessLeavesTheWorkingTreeUntouched(t *testing.T) {
	if testing.Short() {
		t.Skip("invokes the Go toolchain")
	}
	requireGoToolchain(t)
	root := moduleRoot(t)

	for _, m := range mutations {
		before, err := os.ReadFile(filepath.Join(root, m.file))
		if err != nil {
			t.Fatalf("reading %s: %v", m.file, err)
		}
		overlay := writeMutation(t, root, m)
		_, _ = goBuild(t, root, m.pkg, overlay)

		after, err := os.ReadFile(filepath.Join(root, m.file))
		if err != nil {
			t.Fatalf("reading %s after the mutation: %v", m.file, err)
		}
		if string(before) != string(after) {
			t.Fatalf("%s WAS MODIFIED IN THE WORKING TREE by the %s/%s mutation. "+
				"The mutated copy must be written to t.TempDir() and reached through "+
				"the overlay; writing it in place leaves a broken constant behind when "+
				"the test crashes.", m.file, m.gate, m.name)
		}
	}
}

// TestTheHarnessDetectsAGateThatDoesNotFire is the recursive case the card's
// sentence implies: a harness that cannot tell a firing gate from a silent one
// is the same defect it exists to prevent, one level up.
//
// It runs a mutation that is deliberately INERT -- replacing a constant with
// itself -- and asserts the build still succeeds. If it failed, the harness's
// own failure signal would be meaningless, because everything would look like a
// firing gate.
func TestTheHarnessDetectsAGateThatDoesNotFire(t *testing.T) {
	if testing.Short() {
		t.Skip("invokes the Go toolchain")
	}
	requireGoToolchain(t)
	root := moduleRoot(t)

	inert := mutation{
		gate: "harness-self-test", name: "inert",
		pkg: "./internal/model", file: "internal/model/const.go",
		from: "MaxDestinations = 3", to: "MaxDestinations = 3 + 0",
	}
	overlay := writeMutation(t, root, inert)

	out, err := goBuild(t, root, inert.pkg, overlay)
	if err != nil {
		t.Fatalf("an inert mutation broke the build, so the harness cannot distinguish "+
			"a gate firing from its own machinery misbehaving:\n%s", out)
	}
}

// writeMutation writes the mutated copy into t.TempDir() and returns the path
// of an overlay that maps the real file onto it.
func writeMutation(t *testing.T, root string, m mutation) string {
	t.Helper()

	real := filepath.Join(root, filepath.FromSlash(m.file))
	src, err := os.ReadFile(real)
	if err != nil {
		t.Fatalf("reading %s: %v", m.file, err)
	}

	// The mutation must actually apply. A renamed or reformatted constant would
	// otherwise turn this row into a no-op that still reports success -- the
	// vacuous pass that every one of this project's gates has a guard against.
	if n := strings.Count(string(src), m.from); n != 1 {
		t.Fatalf("%q appears %d times in %s, want exactly 1. The mutation is stale: "+
			"update it to match the current source, or this gate is no longer being "+
			"demonstrated to fail.", m.from, n, m.file)
	}
	mutated := strings.Replace(string(src), m.from, m.to, 1)

	dir := t.TempDir()
	fake := filepath.Join(dir, filepath.Base(m.file))
	if err := os.WriteFile(fake, []byte(mutated), 0o600); err != nil {
		t.Fatalf("writing the mutated copy: %v", err)
	}

	// Absolute paths on both sides. The go command matches overlay keys against
	// the paths it resolves itself, and a relative key is silently ignored --
	// which would make the build succeed and look like a gate that does not
	// fire.
	doc := struct {
		Replace map[string]string
	}{Replace: map[string]string{real: fake}}
	blob, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshalling the overlay: %v", err)
	}
	path := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(path, blob, 0o600); err != nil {
		t.Fatalf("writing the overlay: %v", err)
	}
	return path
}

// goBuild builds pkg from the module root, optionally through an overlay, and
// returns the combined output.
func goBuild(t *testing.T, root, pkg, overlay string) (string, error) {
	t.Helper()

	args := []string{"build"}
	if overlay != "" {
		args = append(args, "-overlay", overlay)
	}
	// -o discards the result: this is a compile check, and writing a binary
	// into the module root would be the one way this test could dirty the tree.
	args = append(args, "-o", os.DevNull, pkg)

	cmd := exec.Command("go", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func distinctPkgs() []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range mutations {
		if !seen[m.pkg] {
			seen[m.pkg] = true
			out = append(out, m.pkg)
		}
	}
	return out
}

// devNull is os.DevNull, as a function so the cross-compile gate can name it
// without importing os at the top of a file that otherwise does not need it
// for anything else.
func devNull() string { return os.DevNull }
