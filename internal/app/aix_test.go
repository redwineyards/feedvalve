package app_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestG_AIX_1_TestBinariesCrossCompileForTheTarget gates the PREPARATION half
// of G-AIX-1.
//
// The gate itself is "cross-compile the test binaries with CGO_ENABLED=0
// GOOS=aix GOARCH=ppc64 and run them ON the LPAR -- five assertions no amd64
// run can make". Running them needs the hardware. Producing them does not, and
// producing them is the half that can silently stop working: a new import in a
// test file that happens to be unavailable on aix/ppc64 breaks the shipment and
// nothing else notices, because `go test ./...` on the build host never
// cross-compiles anything.
//
// So this asserts what is assertable here: every package's test binary still
// builds for the target. It is the difference between G-AIX-1 being blocked on
// access and G-AIX-1 being blocked on access AND quietly broken.
//
// Of the five assertions, Phase 1 owns three. Assertion 1 (big-endian decode
// correctness) needs the Phase 2 decoders -- Phase 1 decodes nothing, so there
// is nothing yet for a byte-order bug to be wrong about. Assertion 2
// (hash/maphash allocation-free, OQ-L3) needs Phase 3's mirror. Assertions 3
// (time.LoadLocation), 4 (the layout constants under the ppc64 compiler) and 5
// (the receive buffer actually granted) are live now.
//
// Note that assertion 4's compile-time half is ALREADY satisfied off-target, and
// section 7.5 says why: the constants hold "because unsafe.Sizeof is
// architecture-dependent", which makes it a property the cross-compile
// evaluates under ppc64's own rules. What the LPAR adds is the runtime half --
// TestMemoryFigures' arithmetic and the reflect-based pointer-free walk.
func TestG_AIX_1_TestBinariesCrossCompileForTheTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-compiles every package's test binary; slow")
	}
	requireGoToolchain(t)
	root := moduleRoot(t)

	pkgs, err := os.ReadDir(filepath.Join(root, "internal"))
	if err != nil {
		t.Fatalf("listing internal/: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("internal/ is empty, so this gate would pass whatever the tree held")
	}

	out := t.TempDir()
	built := 0
	for _, p := range pkgs {
		if !p.IsDir() {
			continue
		}
		pkg := "./internal/" + p.Name()
		t.Run(p.Name(), func(t *testing.T) {
			bin := filepath.Join(out, p.Name()+".test")
			cmd := exec.Command("go", "test", "-c", "-o", bin, pkg)
			cmd.Dir = root
			cmd.Env = append(cmd.Environ(), "CGO_ENABLED=0", "GOOS=aix", "GOARCH=ppc64")
			if cmdOut, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go test -c %s failed:\n%s\n"+
					"G-AIX-1 ships these to the LPAR. A test-only import that does not "+
					"exist on aix/ppc64 breaks the shipment, and nothing on the build "+
					"host would notice, because go test ./... never cross-compiles.",
					pkg, cmdOut)
				return
			}
			// A package with no test files produces no binary and exits 0,
			// which is fine and must not be reported as a success at building
			// one.
			if _, err := os.Stat(bin); err == nil {
				built++
			}
		})
	}

	if built == 0 {
		t.Error("no test binary was produced for any package, so there is nothing for " +
			"G-AIX-1 to run on the target")
	}
	t.Logf("%d test binaries cross-compiled for aix/ppc64", built)
}

// TestTheSmokeScriptAndBuildScriptAreShipped keeps the handoff honest.
//
// P1-11 cannot be finished on this host -- all five of its assertions are
// specified on the target. What it can guarantee is that the two artefacts
// somebody with access needs are present and syntactically sound. A plan that
// ends "and then run the script" is worth nothing if the script is missing.
func TestTheSmokeScriptAndBuildScriptAreShipped(t *testing.T) {
	root := requireSourceTree(t)

	for _, name := range []string{"scripts/build-aix.sh", "scripts/lpar-smoke.sh"} {
		path := filepath.Join(root, filepath.FromSlash(name))
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("%s is missing: %v. P1-11 hands off to another team through "+
				"these two files; without them the handoff is a paragraph in a commit "+
				"message.", name, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("%s is empty", name)
		}
	}

	// The shell is not available to check on every build host, so this is a
	// presence check rather than a syntax check. `sh -n scripts/*.sh` is in the
	// verification list and was run by hand.
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh on PATH; cannot syntax-check the scripts here")
	}
	for _, name := range []string{"scripts/build-aix.sh", "scripts/lpar-smoke.sh"} {
		cmd := exec.Command("sh", "-n", filepath.FromSlash(name))
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("%s is not valid POSIX sh:\n%s", name, out)
		}
	}
}
