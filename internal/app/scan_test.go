// Gate G-BLD-3 -- the source scan of Vol 4 section 7.1.
//
// Two rules, from two different places, both of which can only be enforced by
// looking at the source:
//
//	(a) no log.Fatal*, no logger.Fatal*, no os.Exit and no runtime.Goexit
//	    outside cmd/feedvalve/main.go -- condition C-1, Vol 0 section 10.4;
//	(b) no reference to runtime.NumCPU ANYWHERE -- ADR-009.
//
// It lives in app because app is where the cross-tree structural gates live:
// deps_test.go's own header says so, and the reason is that app is the only
// package allowed to see both halves of the tree.
//
// AN AST SCAN AND NOT A GREP, and this file is the proof of why: grepping
// internal/ and cmd/ for "os.Exit" today returns six hits, every one of them in
// a comment explaining why os.Exit must not be called. A textual gate would
// either fail on its own documentation or be weakened until it missed the real
// thing.
package app_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// scanRoots are the trees this gate governs.
//
// poc/ is excluded, and the exclusion is the point rather than a convenience:
// poc/gobrdclnt.go:24 calls os.Exit(1), and that call is the REGRESSION THIS
// GATE EXISTS TO PREVENT RECURRING. poc is the superseded proof-of-concept that
// CLAUDE.md keeps for reference; it was restructured precisely because
// appLogger.Fatalf skipped the defer that closed the database, and the comment
// at poc/gobrdclnt.go:18 records that. The gate protects the replacement, so it
// scans the replacement.
var scanRoots = []string{"internal", "cmd"}

// exemptFile is the single place os.Exit is permitted: the one mapping from an
// error to a process exit code, after every defer above it has been registered
// and app.Run has returned.
const exemptFile = "cmd/feedvalve/main.go"

// TestG_BLD_3_NoFatalLoggerAndNoExitOutsideMain is rule (a).
//
// The defect it catches is the one CLAUDE.md is written about: poc had to be
// restructured because appLogger.Fatalf called os.Exit, which skipped the defer
// that closed the database. With four Oracle sessions and eight in-flight
// batches, one reintroduced Fatalf in an error path nobody exercises leaves
// sessions open on every terminal condition. It also catches the literal
// reading of HLD section 10.2's "FATAL-then-exit" -- which is why obs names its
// method Terminalf and lets it return.
func TestG_BLD_3_NoFatalLoggerAndNoExitOutsideMain(t *testing.T) {
	for _, f := range scanTree(t) {
		if f.rel == exemptFile {
			continue
		}
		for _, bad := range f.calls {
			switch {
			case bad.pkg == "os" && bad.sel == "Exit",
				bad.pkg == "runtime" && bad.sel == "Goexit":
				t.Errorf("%s: %s.%s is forbidden outside %s.\n"+
					"os.Exit skips every defer, so the drain never runs, the Oracle "+
					"pool is never closed and in-flight batches are abandoned. Return "+
					"an error and let main map it to an exit code (Vol 0 section 10.4, "+
					"condition C-1).", bad.pos, bad.pkg, bad.sel, exemptFile)
			case strings.HasPrefix(bad.sel, "Fatal"):
				t.Errorf("%s: %s.%s is a fatal logger call, forbidden outside %s.\n"+
					"Every Fatal* in the standard logger calls os.Exit. This is the "+
					"regression poc was restructured to fix: appLogger.Fatalf skipped "+
					"the defer that closed the database. obs.AppLogger.Terminalf writes "+
					"the FATAL severity and RETURNS; terminating is the lifecycle's job.",
					bad.pos, bad.pkg, bad.sel, exemptFile)
			}
		}
	}
}

// TestG_BLD_3_NoRuntimeNumCPUAnywhere is rule (b), and it has no exemption.
//
// ADR-009: nothing is sized from a detected CPU count. The LPAR is capped at
// entitled capacity 4 while the Go runtime may observe 32, so a pool or a
// GOMAXPROCS derived from NumCPU would silently over-commit the partition by
// 8x. Every such number comes from configuration -- runtime.gomaxprocs,
// runtime.shards, writers.count -- which is also what makes them auditable in
// the effective-configuration line.
func TestG_BLD_3_NoRuntimeNumCPUAnywhere(t *testing.T) {
	for _, f := range scanTree(t) {
		for _, c := range f.calls {
			if c.pkg == "runtime" && c.sel == "NumCPU" {
				t.Errorf("%s: runtime.NumCPU is forbidden anywhere in the tree.\n"+
					"The LPAR is capped at entitled capacity 4 while the runtime may "+
					"observe 32 (ADR-009). Size it from configuration instead: "+
					"runtime.gomaxprocs, runtime.shards, writers.count.", c.pos)
			}
		}
	}
}

// TestGomaxprocsComesFromConfiguration is the positive half of ADR-009. A gate
// that only forbids NumCPU would pass on a process that never set GOMAXPROCS at
// all and inherited the partition's 32.
func TestGomaxprocsComesFromConfiguration(t *testing.T) {
	found := false
	for _, f := range scanTree(t) {
		for _, c := range f.calls {
			if c.pkg == "runtime" && c.sel == "GOMAXPROCS" {
				found = true
			}
		}
	}
	if !found {
		t.Error("nothing calls runtime.GOMAXPROCS. Startup step 8 must set it from " +
			"runtime.gomaxprocs BEFORE the first goroutine, or every goroutine is " +
			"scheduled under whatever parallelism the partition happens to report")
	}
}

// --- the scanner ---------------------------------------------------------

type selCall struct {
	pkg string // the qualifier: "os" in os.Exit, "logger" in logger.Fatalf
	sel string // the selected name: "Exit", "Fatalf"
	pos string // file:line
}

type scannedFile struct {
	rel   string
	calls []selCall
}

// scanTree parses every non-test .go file under scanRoots and returns the
// qualified calls in each.
//
// Test files are excluded because testing.T.Fatalf is both legitimate and
// ubiquitous, and the rule is about the shipped binary. That exclusion is the
// same one gate G-DEP-1 makes for test-only imports, and for the same reason.
func scanTree(t *testing.T) []scannedFile {
	t.Helper()

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("locating the module root: %v", err)
	}

	var out []scannedFile
	fset := token.NewFileSet()
	for _, sub := range scanRoots {
		dir := filepath.Join(root, sub)
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			out = append(out, scannedFile{
				rel:   filepath.ToSlash(rel),
				calls: qualifiedCalls(fset, file),
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", sub, err)
		}
	}
	if len(out) == 0 {
		t.Fatal("the scan found no files, so it would pass whatever the tree contained")
	}
	return out
}

// qualifiedCalls returns every call of the form x.Y(...) in a file.
func qualifiedCalls(fset *token.FileSet, file *ast.File) []selCall {
	var out []selCall
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		out = append(out, selCall{
			pkg: id.Name,
			sel: sel.Sel.Name,
			pos: fset.Position(call.Pos()).String(),
		})
		return true
	})
	return out
}

// TestFatalScanDetectorFindsWhatItMustFind exercises the scanner against
// synthetic sources, following TestMatchForbidden's precedent in deps_test.go --
// "so the gate cannot rot into one that passes everything".
//
// This is the test that matters most about the two gates above, because both of
// them pass on an empty result set. A scanner that silently stopped finding
// calls would report success forever.
func TestFatalScanDetectorFindsWhatItMustFind(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		wantPkg  string
		wantSel  string
		wantHits int
	}{
		{"os.Exit", "os.Exit(1)", "os", "Exit", 1},
		{"log.Fatalf", `log.Fatalf("x")`, "log", "Fatalf", 1},
		{"logger.Fatal", "logger.Fatal()", "logger", "Fatal", 1},
		{"appLogger.Fatalln", "appLogger.Fatalln()", "appLogger", "Fatalln", 1},
		{"runtime.Goexit", "runtime.Goexit()", "runtime", "Goexit", 1},
		{"runtime.NumCPU", "_ = runtime.NumCPU()", "runtime", "NumCPU", 1},
		// The case that makes an AST scan necessary rather than a grep.
		{"a comment naming os.Exit", "// os.Exit(1) must never be called\n_ = 1", "", "", 0},
		{"a string naming os.Exit", `_ = "os.Exit"`, "", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := "package p\nimport (\"os\";\"log\";\"runtime\")\nvar logger, appLogger any\n" +
				"func f() {\n" + tc.body + "\n}\n"
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", src, parser.ParseComments)
			if err != nil {
				t.Fatalf("parse: %v\n%s", err, src)
			}
			calls := qualifiedCalls(fset, file)

			hits := 0
			for _, c := range calls {
				if c.pkg == tc.wantPkg && c.sel == tc.wantSel {
					hits++
				}
			}
			if tc.wantHits == 0 {
				for _, c := range calls {
					if (c.pkg == "os" && c.sel == "Exit") ||
						strings.HasPrefix(c.sel, "Fatal") ||
						(c.pkg == "runtime" && (c.sel == "NumCPU" || c.sel == "Goexit")) {
						t.Errorf("flagged %s.%s in %q, but it is not a call",
							c.pkg, c.sel, tc.body)
					}
				}
				return
			}
			if hits != tc.wantHits {
				t.Errorf("found %d hits for %s.%s in %q, want %d",
					hits, tc.wantPkg, tc.wantSel, tc.body, tc.wantHits)
			}
		})
	}
}
