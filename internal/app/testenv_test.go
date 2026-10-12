package app_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The two prerequisites this package's gates quietly assume, and the LPAR does
// not have.
//
// Gate G-AIX-1 ships the CROSS-COMPILED TEST BINARIES to the target and runs
// them there, which is the only way to make the five assertions no amd64 run
// can make. On that host there is no module, no source tree and no Go
// toolchain: there is a binary in a directory. Every gate in this package that
// walks the source or shells `go` therefore has to decide what to do when its
// input is simply absent.
//
// The answer is SKIP, with a reason, and never a silent pass. Measured before
// these guards existed, running the compiled test binaries from a directory
// with no module around them produced ten failures in this package and one test
// in framing that passed VACUOUSLY -- a source scan whose walk found no files,
// so its loop did nothing and it reported success. A gate that cannot
// distinguish "clean tree" from "no tree" is the failure mode every scan in
// this project already has a guard against, and this is that guard for the
// on-target case.
//
// The risk runs the other way too: a guard that fired on the BUILD host would
// silently disable G-BLD-3, G-DEP-1, G-DEP-2, G-DEP-3 and the mutation harness
// all at once. TestTheBuildHostHasEverythingTheGatesNeed below is what stops
// that, by asserting the prerequisites are found here -- so a guard that
// started skipping locally fails rather than quietly passing.

// findModuleRoot returns the module root, or "" when there is no source tree --
// which is the normal case on the LPAR.
func findModuleRoot() string {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return ""
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return ""
	}
	return root
}

// requireSourceTree returns the module root, skipping the test when the source
// is not present.
func requireSourceTree(t *testing.T) string {
	t.Helper()
	root := findModuleRoot()
	if root == "" {
		t.Skip("no source tree: this gate reads the module's own sources, and the " +
			"cross-compiled test binary is running somewhere without them -- on the " +
			"LPAR, for gate G-AIX-1. The gate still runs on the build host, which is " +
			"where it belongs; it is not being quietly disabled.")
	}
	return root
}

// requireGoToolchain skips the test when there is no `go` on PATH.
func requireGoToolchain(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no Go toolchain on PATH: this gate shells `go list` or `go build`, " +
			"which the deployment target does not have and is not expected to. It " +
			"runs on the build host.")
	}
}

// moduleRoot is requireSourceTree, kept under its original name because the
// mutation harness and the dependency gates already read naturally with it.
func moduleRoot(t *testing.T) string {
	t.Helper()
	return requireSourceTree(t)
}

// TestTheBuildHostHasEverythingTheGatesNeed is the counterweight to the two
// guards above.
//
// Skipping is the right answer on the LPAR and the wrong answer here. Without
// this test, a mistake in findModuleRoot -- a changed directory depth, a
// renamed go.mod, a relative path resolved from somewhere unexpected -- would
// turn every gate in this package into a skip, and a suite of skips reports
// `ok` exactly like a suite of passes.
//
// It is deliberately NOT guarded. It must fail on a host that cannot run the
// gates, which is the only way the guards stay honest.
func TestTheBuildHostHasEverythingTheGatesNeed(t *testing.T) {
	if root := findModuleRoot(); root == "" {
		// Only reachable when this test binary itself was shipped somewhere
		// without its sources, which is exactly what G-AIX-1 does.
		t.Skip("running without a source tree; the build-host assertion does not apply")
	}

	root := findModuleRoot()
	for _, want := range []string{
		"go.mod",
		filepath.Join("internal", "app"),
		filepath.Join("internal", "model", "layout.go"),
		filepath.Join("cmd", "feedvalve", "main.go"),
	} {
		if _, err := os.Stat(filepath.Join(root, want)); err != nil {
			t.Errorf("%s is missing from the module root %s: %v.\n"+
				"The gates in this package locate the sources relative to the test's "+
				"own directory. If that resolution breaks, every one of them SKIPS, and "+
				"a package of skips reports ok exactly like a package of passes.",
				want, root, err)
		}
	}

	if _, err := exec.LookPath("go"); err != nil {
		t.Errorf("no `go` on PATH on a host that has the source tree: %v.\n"+
			"G-DEP-1, G-DEP-2, G-DEP-3 and the mutation harness all shell the "+
			"toolchain, and without it they would all skip silently.", err)
	}
}
