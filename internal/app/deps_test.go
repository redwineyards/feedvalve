// Gate G-DEP-1 — the import-direction rule of docs/lld/00-foundations.md
// sections 1.2 and 1.3.
//
// Every package in this tree carries a doc comment saying what it must never
// import. This file is what makes those comments true. The rule it defends is
// the one the whole mirror design rests on: the receive path cannot reach the
// database, so a shard goroutine cannot block on a commit even by accident.
// The HLD states that property in prose; here it is a test failure.
//
// It lives in app because app is the only package allowed to see both halves of
// the tree, and because adding a package of its own would break the fifteen-
// package tree that section 1.1 freezes.
package app_test

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

const module = "feedvalve"

func pkg(name string) string { return module + "/internal/" + name }

// allInternal is the tree as section 1.1 freezes it. TestEveryPackageHasARule
// checks this list against what is actually on disk, so a package added without
// a rule fails rather than passing silently.
var allInternal = []string{
	"app", "config", "decode", "destinations", "framing", "health", "lzo",
	"mirror", "model", "obs", "oradb", "policy", "wire", "writer",
}

const (
	databaseSQL = "database/sql"
	goOra       = "github.com/sijms/go-ora/v2"
)

// rule is one row of the section 1.2 table.
type rule struct {
	// pkg is the internal package under test.
	pkg string
	// stdlibOnly fails on any dependency outside the standard library. It is
	// stricter than any forbidden list, because it also catches imports nobody
	// thought to forbid.
	stdlibOnly bool
	// forbidden are paths this package may not reach, directly or
	// transitively. A path matches itself and everything beneath it, so
	// database/sql also covers database/sql/driver.
	forbidden []string
	// why says what breaks when the rule is violated. It is printed on failure,
	// because "forbidden import" on its own does not tell the next person why
	// anyone cared.
	why string
}

// otherInternal is every internal package except the named one. obs is
// importable from anywhere, which is only safe while it imports nothing of ours.
func otherInternal(name string) []string {
	out := make([]string, 0, len(allInternal)-1)
	for _, p := range allInternal {
		if p != name {
			out = append(out, pkg(p))
		}
	}
	return out
}

// recvPath is section 1.3's rule, stated once: no package on the receive path
// -- model, wire, lzo, framing, decode, destinations, policy -- may reach any
// of these, directly or transitively.
//
// It has to be spelled out even for the packages already marked stdlibOnly.
// database/sql is in the standard library, so "standard library only" permits
// it; the receive path must refuse it anyway. Leaving that to stdlibOnly let
// model import database/sql with the gate green, which is exactly the coupling
// this file exists to prevent.
func recvPath(extra ...string) []string {
	base := []string{databaseSQL, goOra, pkg("oradb"), pkg("writer")}
	return append(base, extra...)
}

var rules = []rule{
	{
		pkg:        "model",
		stdlibOnly: true,
		forbidden:  recvPath("log"),
		why: "model is the vocabulary every other package speaks. A sql.NullString or a " +
			"time.Time carrying a location pointer inside Batch would drag the database into " +
			"the receive path and break the pointer-free arena rule in one stroke. log is " +
			"forbidden too: these types are copied into arenas on the hot path and must not " +
			"be able to write anywhere.",
	},
	{
		pkg:       "wire",
		forbidden: recvPath(pkg("mirror"), pkg("config")),
		why: "wire is pure layout arithmetic. It may not reach configuration either, because a " +
			"protocol constant that can be reconfigured is not a constant.",
	},
	{
		pkg:        "lzo",
		stdlibOnly: true,
		forbidden:  recvPath(),
		why: "lzo is a self-contained port of LZO1Z decompression. Keeping it stdlib-only is " +
			"what guarantees the binary needs no cgo and no liblzo2, which is what keeps the " +
			"AIX ppc64 cross-compile working.",
	},
	{
		pkg:       "framing",
		forbidden: recvPath(pkg("mirror")),
		why:       "framing runs on the receive goroutine; nothing it calls may be able to block on Oracle.",
	},
	{
		pkg:       "decode",
		forbidden: recvPath(pkg("mirror")),
		why: "this is the refactor the gate exists for: importing oradb into decode " +
			"\"just for the error type\" re-couples the receive path to the database.",
	},
	{
		pkg:       "destinations",
		forbidden: recvPath(pkg("mirror")),
		why: "destinations carries the SQL column metadata but must not carry the database. " +
			"That is why the pre-validation fields live on Column rather than next to oradb: " +
			"mirror needs them and mirror may not import the database packages.",
	},
	{
		pkg:       "policy",
		forbidden: recvPath(pkg("mirror")),
		why:       "policy is a lookup table from transaction code to class, and nothing more.",
	},
	{
		pkg:       "mirror",
		forbidden: []string{pkg("oradb"), pkg("writer"), databaseSQL, goOra, "net"},
		why: "a shard goroutine that cannot name a database handle cannot block on a commit, " +
			"and one that cannot name the socket cannot block on the network. Both halves of " +
			"that are load-bearing: the mirror is the queue, and a shard that stalls stops " +
			"draining it.",
	},
	{
		pkg:       "writer",
		forbidden: []string{pkg("framing"), pkg("decode"), pkg("mirror"), "net"},
		why: "the writer pool is stateless and sees batches, never the feed. If it could reach " +
			"mirror it could read shard-owned state off the shard's goroutine, which is a data " +
			"race on a platform with no race detector.",
	},
	{
		pkg:       "oradb",
		forbidden: []string{pkg("framing"), pkg("decode"), pkg("mirror"), pkg("destinations")},
		why: "oradb is the only package permitted to name database/sql and go-ora. It stays a " +
			"leaf so that privilege cannot spread by transitive import.",
	},
	{
		pkg:       "config",
		forbidden: []string{pkg("framing"), pkg("decode"), pkg("mirror"), pkg("writer")},
		why:       "configuration is read by everything, so it must depend on almost nothing.",
	},
	{
		pkg:       "obs",
		forbidden: otherInternal("obs"),
		why: "obs is importable from every package in the tree. That is only safe while it " +
			"imports none of them; one edge back would make half the tree cyclic.",
	},
	{
		pkg:       "health",
		forbidden: []string{pkg("oradb"), pkg("writer"), pkg("mirror")},
		why: "health observes state machines and reports. It must not be able to reach into " +
			"the things it is reporting on.",
	},
	// app has no rule. It is the one package allowed to import both halves,
	// which is exactly why every other package's restriction is enforced here
	// rather than left to discipline: app is where a shortcut would hide.
}

// TestImportDirection is gate G-DEP-1.
func TestImportDirection(t *testing.T) {
	for _, r := range rules {
		t.Run(r.pkg, func(t *testing.T) {
			target := pkg(r.pkg)
			deps := goListDeps(t, target)

			for _, dep := range deps {
				if dep == target {
					continue // -deps includes the named package itself
				}
				if r.stdlibOnly && !isStdlib(dep) {
					t.Errorf("%s imports %s, but it may only import the standard library.\n%s",
						target, dep, r.why)
					continue
				}
				if f, bad := matchForbidden(dep, r.forbidden); bad {
					t.Errorf("%s reaches %s (forbidden: %s).\n%s", target, dep, f, r.why)
				}
			}
		})
	}
}

// TestEveryPackageHasARule fails when a package is added to the tree without
// deciding what it may import. Without this, a new package silently inherits no
// restrictions at all, and the gate above would still pass.
func TestEveryPackageHasARule(t *testing.T) {
	onDisk := goList(t, module+"/internal/...")

	var names []string
	for _, p := range onDisk {
		names = append(names, strings.TrimPrefix(p, module+"/internal/"))
	}
	slices.Sort(names)

	if !slices.Equal(names, allInternal) {
		t.Errorf("packages on disk do not match the frozen tree of section 1.1.\n"+
			"on disk: %v\nexpected: %v\n"+
			"Adding or removing a package is a change to the contract: update "+
			"docs/lld/00-foundations.md section 1.1, allInternal, and the rules table together.",
			names, allInternal)
	}

	for _, name := range names {
		if name == "app" {
			continue // documented exception: app may import both halves
		}
		if !slices.ContainsFunc(rules, func(r rule) bool { return r.pkg == name }) {
			t.Errorf("package %s has no import rule. Every package except app must declare "+
				"what it may not import.", name)
		}
	}
}

// TestReceivePathRuleIsComplete pins a hole that this gate shipped with until a
// probe found it: model was marked stdlibOnly and nothing else, but database/sql
// is in the standard library, so "standard library only" happily permitted the
// one import the receive path most needs to refuse. lzo had the same gap.
//
// The lesson is in the asymmetry -- stdlibOnly is stricter than a forbidden list
// for third-party and internal packages, and weaker for stdlib ones. Section 1.3
// names database/sql explicitly, so the rules must too.
func TestReceivePathRuleIsComplete(t *testing.T) {
	receivePath := []string{"model", "wire", "lzo", "framing", "decode", "destinations", "policy"}
	required := []string{databaseSQL, goOra, pkg("oradb"), pkg("writer")}

	for _, name := range receivePath {
		i := slices.IndexFunc(rules, func(r rule) bool { return r.pkg == name })
		if i < 0 {
			t.Errorf("receive-path package %s has no rule at all", name)
			continue
		}
		for _, req := range required {
			if _, ok := matchForbidden(req, rules[i].forbidden); !ok {
				t.Errorf("the rule for %s does not forbid %s, which section 1.3 requires of "+
					"every receive-path package. Note that stdlibOnly does not cover it: "+
					"database/sql is in the standard library.", name, req)
			}
		}
	}
}

// TestMatchForbidden exercises the matcher directly, so the gate cannot rot into
// one that passes everything. A prefix match must cover subpackages without
// matching a merely similar name.
func TestMatchForbidden(t *testing.T) {
	forbidden := []string{"database/sql", "feedvalve/internal/writer"}

	cases := []struct {
		dep  string
		want bool
	}{
		{"database/sql", true},
		{"database/sql/driver", true}, // subpackage
		{"database/sqlx", false},      // similar name, not a subpackage
		{"feedvalve/internal/writer", true},
		{"feedvalve/internal/writerpool", false}, // ditto
		{"feedvalve/internal/model", false},
		{"strings", false},
	}
	for _, c := range cases {
		if _, got := matchForbidden(c.dep, forbidden); got != c.want {
			t.Errorf("matchForbidden(%q) = %v, want %v", c.dep, got, c.want)
		}
	}
}

func TestIsStdlib(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"strings", true},
		{"database/sql", true},
		{"hash/maphash", true},
		{"github.com/sijms/go-ora/v2", false},
		{"feedvalve/internal/model", false}, // our own module has no dot; special-cased
		{"feedvalve", false},
	}
	for _, c := range cases {
		if got := isStdlib(c.path); got != c.want {
			t.Errorf("isStdlib(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

// matchForbidden reports the forbidden entry dep falls under, if any. A path
// matches itself and anything beneath it, so database/sql also covers
// database/sql/driver, while database/sqlx matches neither.
func matchForbidden(dep string, forbidden []string) (string, bool) {
	for _, f := range forbidden {
		if dep == f || strings.HasPrefix(dep, f+"/") {
			return f, true
		}
	}
	return "", false
}

// isStdlib reports whether an import path belongs to the standard library.
// The usual test is a dot in the first path element, which fails for our own
// module -- "feedvalve" has no dot either -- so that is excluded first.
func isStdlib(path string) bool {
	if path == module || strings.HasPrefix(path, module+"/") {
		return false
	}
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

// goListDeps returns the transitive dependencies of a package, including the
// package itself. Test-only imports are deliberately excluded: lzo's tests
// import a reference compressor that must never reach the binary, and that
// distinction is the whole point of gate G-DEP-3.
func goListDeps(t *testing.T, pattern string) []string {
	t.Helper()
	return goListArgs(t, "-deps", pattern)
}

func goList(t *testing.T, pattern string) []string {
	t.Helper()
	return goListArgs(t, pattern)
}

func goListArgs(t *testing.T, args ...string) []string {
	t.Helper()
	requireGoToolchain(t)
	root := requireSourceTree(t)

	// Run from the module root rather than from wherever the test binary
	// happens to sit. Before P1-11 this inherited the working directory, so the
	// gate's answer depended on where it was invoked from -- which is how it
	// came to fail rather than skip when the cross-compiled binary ran on a
	// host with no module around it.
	cmd := exec.Command("go", append([]string{"list"}, args...)...)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("go list %v: %v\n%s", args, err, stderr)
	}
	return strings.Fields(string(out))
}
