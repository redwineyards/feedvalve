// Gates G-DEP-2 and G-DEP-3 of Vol 4 section 7.1 -- the two dependency gates
// that are specified as CI steps and had no implementation of any kind.
//
// They are tests rather than a CI job because there is no CI configuration in
// this repository and no remote to run one against. The only shell script in
// the tree is poc/build_aix.sh, which is the OPPOSITE of G-DEP-2: it sets
// CGO_ENABLED=1, needs liblzo2 headers and a C compiler, and must run ON an AIX
// host. CLAUDE.md already records it as stale. A gate nobody runs is not a
// gate, so these run wherever `go test ./...` runs.
//
// They live in app for the reason deps_test.go's own header gives: app is the
// only package allowed to see both halves of the tree, so it is where the
// cross-tree structural gates belong.
package app_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestG_DEP_2_CrossCompilesForAIXWithoutCgo is gate G-DEP-2.
//
// `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./...` as a required step, plus
// `go list -deps` asserting zero runtime/cgo.
//
// The defect it catches is a dependency added on the dev host that pulls cgo or
// is simply not portable to a second-tier port. Without the gate that failure
// surfaces at release time, on the one target that matters, after everything
// else is finished -- and AIX/ppc64 is a port where "it compiles on Windows" is
// no evidence at all.
//
// The cross-compile is the slow half, so it skips under -short. The dependency
// audit is instant and always runs.
func TestG_DEP_2_CrossCompilesForAIXWithoutCgo(t *testing.T) {
	requireGoToolchain(t)
	root := moduleRoot(t)

	// The audit half. Run on BOTH patterns deliberately.
	//
	// ./... includes feedvalve/poc, which is a second main; Vol 4 section 8.2
	// audits with ./cmd/feedvalve while the P1-10 card says ./... . They are
	// not interchangeable and both answers are worth having: cgo reaching
	// cmd/feedvalve is what breaks the deployment, while cgo reaching only poc
	// is still something to know before someone promotes poc code.
	for _, pattern := range []string{"./cmd/feedvalve", "./..."} {
		t.Run("cgo-free "+pattern, func(t *testing.T) {
			deps := goListDepsAt(t, root, pattern)
			var cgo []string
			for _, d := range deps {
				if d == "runtime/cgo" || strings.HasPrefix(d, "runtime/cgo/") {
					cgo = append(cgo, d)
				}
			}
			if len(cgo) != 0 {
				t.Errorf("%s depends on %v.\n"+
					"The AIX build is CGO_ENABLED=0, and the whole reason this project "+
					"ports LZO1Z by hand and uses a pure-Go Oracle driver is to keep it "+
					"that way. A cgo dependency here means the binary cannot be built "+
					"for the target at all.", pattern, cgo)
			}
			if len(deps) == 0 {
				t.Fatalf("go list -deps %s returned nothing, so this gate would pass "+
					"whatever the dependency set contained", pattern)
			}
		})
	}

	if testing.Short() {
		t.Skip("the AIX cross-compile is slow")
	}

	t.Run("aix/ppc64 cross-compile", func(t *testing.T) {
		cmd := exec.Command("go", "build", "-o", devNull(), "./...")
		cmd.Dir = root
		cmd.Env = append(cmd.Environ(), "CGO_ENABLED=0", "GOOS=aix", "GOARCH=ppc64")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./... failed:\n%s\n"+
				"This is the only compilation that matters: the development host is "+
				"windows/amd64 and the deployment target is AIX on ppc64, big-endian.",
				out)
		}
	})
}

// TestG_DEP_3_TheReferenceCompressorIsAbsent is gate G-DEP-3.
//
// `github.com/rhnvrm/lzo1z` must not reach the non-test build. It is the
// reference LZO1Z *compressor* used to build decompression fixtures; this
// repository's decompression is its own port, and importing someone else's
// compressor into the binary would add an unaudited dependency for zero
// benefit.
//
// THIS GATE PASSES VACUOUSLY TODAY, and saying so is the point. Vol 0 section 2
// records that the module no longer requires lzo1z at all -- the compressed
// bytes it produced are frozen in poc/testdata/lzo_fixtures.json, keyed by the
// SHA-256 of the input -- so "the gate now guards against reintroduction rather
// than against leakage into the binary". A reader who assumes it is proving
// something about the current dependency set has the wrong model: what it
// actually does is fail the day somebody runs `go get` to regenerate a fixture
// and forgets to drop the requirement again. The regeneration recipe at the top
// of poc/decompress_test.go is exactly that sequence.
func TestG_DEP_3_TheReferenceCompressorIsAbsent(t *testing.T) {
	const forbidden = "github.com/rhnvrm/lzo1z"
	requireGoToolchain(t)
	root := moduleRoot(t)

	// The NON-TEST build: goListDepsAt does not pass -test, which is the
	// distinction the whole gate rests on. The fixture recipe legitimately
	// imports this package from a _test.go file under a rename; what must never
	// happen is it reaching the shipped binary.
	for _, pattern := range []string{"./cmd/feedvalve", "./..."} {
		deps := goListDepsAt(t, root, pattern)
		for _, d := range deps {
			if d == forbidden || strings.HasPrefix(d, forbidden+"/") {
				t.Errorf("%s reaches %s in the non-test build.\n"+
					"This repository's LZO1Z decompression is its own port. The "+
					"reference compressor exists only to produce test fixtures, and "+
					"those are frozen in poc/testdata/lzo_fixtures.json -- so if this "+
					"fires, a `go get` from the regeneration recipe was left in "+
					"go.mod.", pattern, d)
			}
		}
	}
}

// TestTheOnlyThirdPartyDependencyIsTheOracleDriver is the positive companion.
//
// G-DEP-2 and G-DEP-3 both forbid; neither notices a new third-party dependency
// that happens to be pure Go and is not lzo1z. Vol 4 section 8.2 asks for a
// committed golden list for exactly that reason, and this is the smallest
// version of one: the module has precisely one non-stdlib requirement, and
// adding a second should be a decision somebody makes on purpose.
func TestTheOnlyThirdPartyDependencyIsTheOracleDriver(t *testing.T) {
	requireGoToolchain(t)
	root := moduleRoot(t)
	const driver = "github.com/sijms/go-ora/v2"

	var thirdParty []string
	for _, d := range goListDepsAt(t, root, "./...") {
		// A stdlib path has no dot in its first element; our own module has no
		// dot either, so it is excluded by name.
		if d == "feedvalve" || strings.HasPrefix(d, "feedvalve/") {
			continue
		}
		first, _, _ := strings.Cut(d, "/")
		if !strings.Contains(first, ".") {
			continue
		}
		if d == driver || strings.HasPrefix(d, driver+"/") {
			continue
		}
		thirdParty = append(thirdParty, d)
	}
	if len(thirdParty) != 0 {
		t.Errorf("the non-test build reaches third-party packages other than the "+
			"Oracle driver: %v.\n"+
			"Every dependency has to survive CGO_ENABLED=0 on aix/ppc64, which is "+
			"where most of them fail before the argument starts. Adding one is a "+
			"decision; update this gate in the same commit.", thirdParty)
	}
}

// goListDepsAt is deps_test.go's goListDeps with an explicit working directory,
// because these gates run patterns rooted at the module rather than at app.
func goListDepsAt(t *testing.T, root, pattern string) []string {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", pattern)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("go list -deps %s: %v\n%s", pattern, err, stderr)
	}
	return strings.Fields(string(out))
}
