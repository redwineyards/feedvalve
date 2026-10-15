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

	// file and fset are kept so a scan with different questions -- the
	// single-consumer scan below -- can walk the same parse rather than
	// reading the tree twice.
	file *ast.File
	fset *token.FileSet
}

// scanTree parses every non-test .go file under scanRoots and returns the
// qualified calls in each.
//
// Test files are excluded because testing.T.Fatalf is both legitimate and
// ubiquitous, and the rule is about the shipped binary. That exclusion is the
// same one gate G-DEP-1 makes for test-only imports, and for the same reason.
func scanTree(t *testing.T) []scannedFile {
	t.Helper()

	root := requireSourceTree(t)

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
				file:  file,
				fset:  fset,
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

// TestVol0Section955_DrainIsNeverStartedInALoop is the tree-wide half of the
// single-consumer invariant, and it is the gate P1-08 could not finish.
//
// Vol 0 section 9.5.5 asks Volume 4 for "a test that asserts exactly one
// goroutine consumes QD, and a source scan that fails the build if the consumer
// is started in a loop". P1-08 built the runtime guard -- a compare-and-swap in
// framing.Consumer.Drain -- and scoped its scan to framing's own sources,
// saying so in code: "P1-10 is where the tree-wide version belongs, next to the
// other gates in internal/app." P1-09 then created the consumer
// (Graph.startDecoder), so there is finally something tree-wide to scan.
//
// Why it matters more than it looks: a second consumer does not corrupt
// anything or crash. It reorders datagrams relative to arrival, and arrival
// order at a single goroutine is ADR-007's mechanism 1 -- the thing that makes
// ordering STRUCTURAL rather than dependent on the sequence guard, which is a
// defence that can auto-disable itself per transcode. Widening QD would
// silently demote the whole ordering story to that defence, and the symptom
// would be occasional wrong-order updates in Oracle months later.
//
// The compare-and-swap in Drain is the real guard, because it holds whatever
// the source looks like. This is belt and braces: it catches the shape the
// contract names, `for ... { go c.Drain(fn) }`, at review time rather than at
// 03:00.
func TestVol0Section955_DrainIsNeverStartedInALoop(t *testing.T) {
	for _, f := range scanTree(t) {
		forEachGoStmtInLoop(f.file, func(pos token.Pos, callee string) {
			if callee != "Drain" {
				return
			}
			t.Errorf("%s: Drain is started inside a loop at %s.\n"+
				"QD has exactly one consumer -- not a pool, not now, not later "+
				"(Vol 0 section 9.5.5). A second consumer reorders datagrams relative "+
				"to arrival and silently demotes the whole ordering story to the "+
				"sequence guard, which can disable itself per transcode.",
				f.rel, f.fset.Position(pos))
		})
	}
}

// forEachGoStmtInLoop reports every `go f()` that appears lexically inside a
// for or range statement, with the called function's final selector name.
//
// Lexical nesting, not reachability: a Drain started from a helper that a loop
// calls would not be caught. That limit is why the runtime CAS is the real
// guard and this is the cheap check -- stated rather than left for a reader to
// discover when it misses one.
func forEachGoStmtInLoop(root ast.Node, report func(token.Pos, string)) {
	var depth int
	var walk func(ast.Node) bool
	walk = func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			depth++
			for _, child := range childrenOf(n) {
				ast.Inspect(child, walk)
			}
			depth--
			return false
		case *ast.GoStmt:
			if depth > 0 {
				if name := calleeName(node.Call.Fun); name != "" {
					report(node.Pos(), name)
				}
			}
		}
		return true
	}
	ast.Inspect(root, walk)
}

// childrenOf returns the statement bodies of a loop, so the walker can descend
// without re-entering the loop node itself and recursing forever.
func childrenOf(n ast.Node) []ast.Node {
	switch node := n.(type) {
	case *ast.ForStmt:
		return []ast.Node{node.Body}
	case *ast.RangeStmt:
		return []ast.Node{node.Body}
	}
	return nil
}

func calleeName(e ast.Expr) string {
	switch fn := e.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}

// TestDrainLoopDetectorCatchesTheShapeTheContractNames exercises the detector
// against synthetic sources, following TestMatchForbidden's precedent above --
// "so the gate cannot rot into one that passes everything".
//
// This one needs it more than most: the tree-wide scan currently finds ZERO
// occurrences and will pass for as long as nobody writes the bug. A detector
// that silently stopped working would be indistinguishable from a clean tree.
func TestDrainLoopDetectorCatchesTheShapeTheContractNames(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{"go Drain in a for loop", "for i := 0; i < 4; i++ {\ngo c.Drain(fn)\n}", 1},
		{"go Drain in a range loop", "for range 4 {\ngo c.Drain(fn)\n}", 1},
		{"go Drain nested two loops deep", "for range 2 {\nfor range 2 {\ngo c.Drain(fn)\n}\n}", 1},
		{"a bare Drain call in a loop is not a goroutine", "for range 4 {\nc.Drain(fn)\n}", 0},
		{"go Drain outside any loop is the correct shape", "go c.Drain(fn)", 0},
		{"a different method in a loop is ignored", "for range 4 {\ngo c.Publish(fn)\n}", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := "package p\nvar c struct{}\nvar fn func()\nfunc f() {\n" + tc.body + "\n}\n"
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", src, 0)
			if err != nil {
				t.Fatalf("parse: %v\n%s", err, src)
			}
			got := 0
			forEachGoStmtInLoop(file, func(_ token.Pos, callee string) {
				if callee == "Drain" {
					got++
				}
			})
			if got != tc.want {
				t.Errorf("detector found %d Drain-in-a-loop occurrences, want %d, in:\n%s",
					got, tc.want, tc.body)
			}
		})
	}
}

// TestNoBareQDDropPath encodes the P2-08 exit-checklist line: "grep -r
// TryPublish internal/app shows no bare drop path".
//
// THE DEFECT THIS CATCHES ALREADY HAPPENED. Phase 1's receive loop read
//
//	if !g.qdProducer.TryPublish(&d) { qdSendBlocked.Add(1) }
//
// which silently DISCARDS the datagram -- against req section 10.2's
// "FeedValve discards no unit anywhere", which internal/framing/budget_test.go
// quotes verbatim as "There is no drop path". Producer.PublishWithin,
// Config.StallBudget and Graph.Fatal all existed and had no production caller;
// the full-ring policy had been reimplemented, worse, at the call site. The
// behavioural proof is TestQDBudgetExpiryIsTerminal; this is the structural
// one, and it is cheap.
//
// The rule is deliberately about the FUNCTION rather than the statement: a
// TryPublish used as the fast path of a policy that also calls PublishWithin
// is correct and is exactly what the receiver does. A TryPublish alone in a
// function is the drop path coming back.
func TestNoBareQDDropPath(t *testing.T) {
	for _, f := range scanTree(t) {
		// scanTree normalises rel with filepath.ToSlash, so this is a forward
		// slash on every platform. filepath.Join here would be a backslash on
		// Windows, match nothing, and make the whole gate vacuous -- which is
		// exactly what it did until a break check showed the test passing with
		// the drop path restored.
		if !strings.HasPrefix(f.rel, "internal/app/") {
			continue
		}
		forEachFuncCalling(f.file, "TryPublish", func(pos token.Pos, fn string, callees map[string]bool) {
			if callees["PublishWithin"] {
				return
			}
			t.Errorf("%s: %s calls TryPublish without PublishWithin, at %s.\n"+
				"A failed TryPublish with no budget behind it DISCARDS the datagram, "+
				"which req section 10.2 forbids outright -- and it misreports "+
				"qd_send_blocked, the one counter an operator reads for decoder "+
				"stall, as a drop count rather than a wait count. Use "+
				"PublishWithin(d, t0, cfg.StallBudget()) and return its error.",
				f.rel, fn, f.fset.Position(pos))
		})
	}
}

// forEachFuncCalling reports every top-level function whose body contains a
// call to want, together with the set of every method name that body calls.
//
// Lexical again, and for the same reason the Drain scan is: a policy split
// across two helpers would not be seen. That is acceptable here because the
// behavioural test is the real guard and this one exists to catch the shape at
// review time.
func forEachFuncCalling(file *ast.File, want string,
	report func(token.Pos, string, map[string]bool)) {

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		callees := make(map[string]bool)
		var at token.Pos
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			callees[sel.Sel.Name] = true
			if sel.Sel.Name == want && !at.IsValid() {
				at = call.Pos()
			}
			return true
		})
		if at.IsValid() {
			report(at, fn.Name.Name, callees)
		}
	}
}
