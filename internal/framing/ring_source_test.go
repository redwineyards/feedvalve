package framing

import (
	"io/fs"
	"os"

	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// TestTheWakeSendIsUnconditional is the gate that actually holds the wakeup
// correct, and it is a SOURCE gate because the property is a source property.
//
// The bug it prevents is described at length on TryPublish's send: Vol 0 section
// 9.5.4 says to send "after publishing into an empty ring", and any emptiness
// predicate the producer computes can be stale by exactly one item, because its
// load of head may be ordered before the consumer's store of it. The consumer
// then parks with a datagram queued and sleeps until the next publish.
//
// A runtime test cannot reliably catch that. The interleaving needs the
// producer's head load to race the consumer's head store, and a test that waits
// for the park -- which is the only way to know the consumer reached it --
// thereby guarantees the store is visible and the stale read cannot happen.
// Attempting it that way produces a test that passes against the broken code,
// which is worse than no test. Verified by mutation: implementing the
// conditional send and running the park/wake tests, they all still pass.
//
// So this asserts the implementation property that makes the bug impossible:
// the send is not nested inside any conditional. It costs nothing at runtime,
// unlike a counter on the publish path, and it fails the moment someone
// "optimises" the send back into an `if`.
func TestTheWakeSendIsUnconditional(t *testing.T) {
	requireSourceDir(t)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "ring.go", nil, 0)
	if err != nil {
		t.Fatalf("parse ring.go: %v", err)
	}

	fn := findMethod(file, "TryPublish")
	if fn == nil {
		t.Fatal("TryPublish not found in ring.go; this gate has nothing to check " +
			"and must be updated rather than left passing vacuously")
	}

	sends := findWakeSends(fn.Body, nil)
	if len(sends) == 0 {
		t.Fatal("no send on the wake channel found in TryPublish. Either the wakeup " +
			"was removed -- in which case a parked consumer never wakes -- or it moved, " +
			"and this gate must move with it")
	}
	if len(sends) > 1 {
		t.Errorf("found %d wake sends in TryPublish, want exactly 1", len(sends))
	}
	for _, s := range sends {
		if s.guarded {
			t.Errorf("the wake send at %s is nested inside a conditional.\n"+
				"It MUST be unconditional. Vol 0 section 9.5.4's \"into an empty ring\" "+
				"reading is broken: the producer's view of head can be stale by one "+
				"item, so it skips the token exactly as the consumer parks, and the "+
				"datagram waits for the next publish -- a latency hole of about one "+
				"inter-datagram gap, or at shutdown a datagram that never arrives. "+
				"Section 9.5.4 prices the unconditional send at ~25 ns on a 2,400/s "+
				"feed and says twice that this is the right trade; Vol 2 section 3's "+
				"flush wakeup, which 9.5.4 claims to match, sends unconditionally for "+
				"the same reason.", fset.Position(s.pos))
		}
	}
}

type wakeSend struct {
	pos     token.Pos
	guarded bool
}

// findWakeSends walks a function body for sends on the wake channel, recording
// whether each one sits inside an if, for, switch or select-with-condition.
//
// A send inside the `select { case r.wake <- ...: default: }` idiom is NOT
// guarded: that select is how a non-blocking send is spelled in Go, and its
// default arm filters on the CHANNEL's state, which is the only observable that
// is safe to filter on. What must not appear is a filter on the RING's state.
func findWakeSends(n ast.Node, enclosing []ast.Node) []wakeSend {
	var out []wakeSend
	ast.Inspect(n, func(node ast.Node) bool {
		send, ok := node.(*ast.SendStmt)
		if !ok {
			return true
		}
		sel, ok := send.Chan.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "wake" {
			return true
		}
		out = append(out, wakeSend{
			pos:     send.Pos(),
			guarded: isGuarded(n, send),
		})
		return true
	})
	return out
}

// isGuarded reports whether target sits inside a conditional statement within
// root, ignoring the select that spells the non-blocking send itself.
func isGuarded(root ast.Node, target *ast.SendStmt) bool {
	var stack []ast.Node
	guarded := false
	ast.Inspect(root, func(node ast.Node) bool {
		if node == nil {
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			return true
		}
		if node == target {
			for _, anc := range stack {
				switch anc.(type) {
				case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt,
					*ast.TypeSwitchStmt:
					guarded = true
				}
			}
			return false
		}
		stack = append(stack, node)
		return true
	})
	return guarded
}

func findMethod(file *ast.File, name string) *ast.FuncDecl {
	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if ok && fn.Name.Name == name && fn.Recv != nil {
			return fn
		}
	}
	return nil
}

// TestWakeSendDetectorCatchesAGuardedSend exercises the detector against a
// synthetic source, following the precedent of TestMatchForbidden in
// internal/app/deps_test.go -- "so the gate cannot rot into one that passes
// everything".
//
// Without this, a detector that silently stopped finding sends would report
// success forever, which is the failure mode every source-scanning gate has.
func TestWakeSendDetectorCatchesAGuardedSend(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		wantSends   int
		wantGuarded bool
	}{
		{
			name:      "unconditional non-blocking send",
			body:      "select {\ncase r.wake <- struct{}{}:\ndefault:\n}",
			wantSends: 1,
		},
		{
			name:        "the broken conditional form",
			body:        "if wasEmpty {\nselect {\ncase r.wake <- struct{}{}:\ndefault:\n}\n}",
			wantSends:   1,
			wantGuarded: true,
		},
		{
			name:        "guarded by a loop",
			body:        "for i := 0; i < 2; i++ {\nselect {\ncase r.wake <- struct{}{}:\ndefault:\n}\n}",
			wantSends:   1,
			wantGuarded: true,
		},
		{
			name:      "no send at all",
			body:      "_ = r.tail.Load()",
			wantSends: 0,
		},
		{
			name:      "a send on a different channel is ignored",
			body:      "select {\ncase r.other <- struct{}{}:\ndefault:\n}",
			wantSends: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := "package p\nfunc (p *P) TryPublish() {\n" + tc.body + "\n}\n"
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", src, 0)
			if err != nil {
				t.Fatalf("parse: %v\n%s", err, src)
			}
			fn := findMethod(file, "TryPublish")
			if fn == nil {
				t.Fatal("synthetic TryPublish not found")
			}
			sends := findWakeSends(fn.Body, nil)
			if len(sends) != tc.wantSends {
				t.Fatalf("found %d sends, want %d", len(sends), tc.wantSends)
			}
			if tc.wantSends > 0 && sends[0].guarded != tc.wantGuarded {
				t.Errorf("guarded = %v, want %v", sends[0].guarded, tc.wantGuarded)
			}
		})
	}
}

// TestDrainIsNotStartedInALoopInThisTree is the source half of Vol 0 section
// 9.5.5, which asks Volume 4 for "a source scan that fails the build if the
// consumer is started in a loop".
//
// Scoped to framing's own non-test sources, because that is all this package
// can see: the decoder that will actually call Drain arrives in Phase 2, and
// P1-10 is where the tree-wide version belongs, next to the other gates in
// internal/app. Stated plainly so nobody reads this as the full gate.
//
// The runtime CAS in Drain is the real guard; this is belt and braces.
func TestDrainIsNotStartedInALoopInThisTree(t *testing.T) {
	requireSourceDir(t)
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse framing: %v", err)
	}

	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			forEachGoStmtInLoop(file, func(pos token.Pos, callee string) {
				if callee == "Drain" {
					t.Errorf("%s: Drain is started inside a loop at %s. QD has exactly "+
						"one consumer (Vol 0 section 9.5.5); a pool of them reorders "+
						"datagrams relative to arrival and silently demotes the whole "+
						"ordering story to the sequence guard, which can auto-disable "+
						"itself per transcode", name, fset.Position(pos))
				}
			})
		}
	}
}

// forEachGoStmtInLoop reports every `go f()` that appears lexically inside a
// loop, with the called function's final selector name.
func forEachGoStmtInLoop(root ast.Node, report func(token.Pos, string)) {
	var inLoop int
	var walk func(ast.Node) bool
	walk = func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			inLoop++
			ast.Inspect(n, func(inner ast.Node) bool {
				if inner == n {
					return true
				}
				return walk(inner)
			})
			inLoop--
			return false
		case *ast.GoStmt:
			if inLoop > 0 {
				if name := calleeName(node.Call.Fun); name != "" {
					report(node.Pos(), name)
				}
			}
		}
		return true
	}
	ast.Inspect(root, walk)
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

// requireSourceDir skips when this package's own sources are not beside the
// test binary.
//
// Gate G-AIX-1 ships the cross-compiled test binaries to the LPAR and runs them
// there, where there is no source tree -- so a scan that reads ring.go has
// nothing to read. Skipping says that; the alternatives are both worse.
//
// TestDrainIsNotStartedInALoopInThisTree in particular PASSED VACUOUSLY before
// this guard existed, and that is the defect worth naming: parser.ParseDir on a
// directory with no Go files returns no error and no packages, so the loop ran
// zero times and the gate reported success. A scan that cannot tell "no Drain
// in a loop" from "no files at all" is exactly the rot that TestMatchForbidden
// and the detector self-tests exist to prevent, and on the one host where the
// gate cannot run it was reporting a pass.
func requireSourceDir(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("ring.go"); err != nil {
		t.Skip("no source tree beside the test binary: these two gates read this " +
			"package's own sources, and the cross-compiled binary is running without " +
			"them -- on the LPAR, for gate G-AIX-1. They run on the build host.")
	}
}

// TestTheBuildHostHasThisPackagesSources is the counterweight: skipping is
// correct on the LPAR and wrong here, and a package of skips reports ok exactly
// like a package of passes.
func TestTheBuildHostHasThisPackagesSources(t *testing.T) {
	if _, err := os.Stat("ring.go"); err != nil {
		t.Skip("running without a source tree; the build-host assertion does not apply")
	}
	for _, want := range []string{"ring.go", "consume.go", "budget.go"} {
		if _, err := os.Stat(want); err != nil {
			t.Errorf("%s is missing beside the test: %v. The two source gates in this "+
				"file would both skip, and skips are indistinguishable from passes in "+
				"the summary line.", want, err)
		}
	}
}
