// The source-scan half of Vol 1 section 11.5, alongside gate G-BLD-3's two
// rules in scan_test.go and for the same reason: it can only be enforced by
// looking at the source.
//
// THE RULE. On the receive path, a rate-limited log call must be the body of an
// `if` whose condition calls AppLogger.Allow:
//
//	if r.g.deps.Log.Allow(obs.ClassNetidMismatch) {
//	    r.g.deps.Log.Warnf(obs.ClassNetidMismatch, "...%d...", n)
//	}
//
// WHY A GATE AND NOT A CONVENTION. Warnf takes the bucket before it formats,
// but the CALLER's arguments are evaluated and boxed into the variadic at the
// call site -- so an unguarded site allocates whether or not the line is
// emitted, and the common case on a misframed feed is that it is not. Gate
// G-ALLOC-1 catches this, but only for the branches its fixture happens to
// drive; a WARN on a path the gate does not exercise would allocate in
// production and pass CI. Every one of these call sites was unguarded before
// P2-09 and the most expensive built a 64-byte hex dump per sub-packet.
//
// An AST scan and not a grep, for the reason scan_test.go gives: the words
// appear in the comments explaining the rule.
package app_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// receivePathFiles are the files with code that runs per datagram or per
// sub-packet. Named individually rather than scanned wholesale, because the
// rule is about the receive path and not about the package: run.go's startup
// logging and graph.go's construction are neither rate-limited nor hot.
var receivePathFiles = []string{
	"internal/app/subpacket.go",
	"internal/app/fanout.go",
	"internal/app/counters.go",
	"internal/app/lifecycle.go",
	"internal/app/reload.go",
}

// startupClassIdent is the alias app uses for the one class that is NOT a
// packet-path class (clock.go). Calls carrying it are startup, shutdown and
// read-error-policy lines -- per process or bounded at 64, never per datagram
// -- so they are outside this rule rather than exceptions to it.
const startupClassIdent = "obsClassStartup"

// allowExemptFuncs are the deliberate exceptions, by enclosing function name.
//
// stallFatal runs ONCE, on the way to stopping the process, and its line is the
// operator's only account of why. Guarding it would let a bucket drained by an
// unrelated class suppress exactly that, which is a worse trade than the
// allocation -- and there is no next iteration to protect. The reasoning is
// recorded at the call site too; both places, because a future sweep will find
// one of them.
var allowExemptFuncs = map[string]string{
	"stallFatal": "runs once, on the way out; its line must never be suppressed",
}

// TestRateLimitedLogsOnTheReceivePathAreGuardedByAllow is the rule.
func TestRateLimitedLogsOnTheReceivePathAreGuardedByAllow(t *testing.T) {
	root := moduleRoot(t)

	for _, rel := range receivePathFiles {
		fset := token.NewFileSet()
		path := filepath.Join(root, filepath.FromSlash(rel))
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parsing %s: %v", rel, err)
		}

		guarded := allowGuardedRanges(file)

		// One pass per function, so the enclosing name is available for the
		// exemption and for the failure message.
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if why, exempt := allowExemptFuncs[fn.Name.Name]; exempt {
				t.Logf("%s: %s is exempt -- %s", rel, fn.Name.Name, why)
				continue
			}

			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if sel.Sel.Name != "Warnf" && sel.Sel.Name != "Errorf" {
					return true
				}
				// Warnf/Errorf take the class first. A call carrying the
				// startup alias is not on the rate-limited packet path.
				if len(call.Args) > 0 {
					if id, ok := call.Args[0].(*ast.Ident); ok && id.Name == startupClassIdent {
						return true
					}
				}
				if withinAny(call.Pos(), guarded) {
					return true
				}

				t.Errorf("%s:%d: %s calls %s outside an AppLogger.Allow guard.\n"+
					"Vol 1 section 11.5 is a MUST: the bucket must be tested BEFORE the "+
					"log arguments are built, because the caller's arguments are boxed "+
					"into the variadic at the call site and allocate whether or not the "+
					"line is emitted. Wrap it:\n"+
					"\tif <logger>.Allow(<class>) { <logger>.%s(<class>, ...) }\n"+
					"If the call genuinely runs once per process, add its function to "+
					"allowExemptFuncs with the reason.",
					rel, fset.Position(call.Pos()).Line, fn.Name.Name, sel.Sel.Name, sel.Sel.Name)
				return true
			})
		}
	}
}

// TestTheAllowScanWouldCatchAnUnguardedCall is the anti-vacuity half.
//
// The scan above passes on a clean tree, which is indistinguishable from a scan
// that matches nothing -- a renamed method, a changed AST shape, or a
// receivePathFiles entry whose path stopped resolving would all look like
// success. This parses a source string with one guarded and one unguarded call
// and asserts the detector separates them.
func TestTheAllowScanWouldCatchAnUnguardedCall(t *testing.T) {
	const src = `package app

func guarded(r *receiver) {
	if r.g.deps.Log.Allow(obs.ClassSeqGap) {
		r.g.deps.Log.Warnf(obs.ClassSeqGap, "guarded %d", 1)
	}
}

func unguarded(r *receiver) {
	r.g.deps.Log.Warnf(obs.ClassSeqGap, "unguarded %d", 2)
}

func startup(g *Graph) {
	g.deps.Log.Errorf(obsClassStartup, "not a packet-path class %d", 3)
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, 0)
	if err != nil {
		t.Fatalf("parsing the synthetic source: %v", err)
	}

	guarded := allowGuardedRanges(file)
	if len(guarded) != 1 {
		t.Fatalf("found %d Allow-guarded ranges in the synthetic source, want 1 -- the "+
			"guard detector is not recognising the idiom it governs", len(guarded))
	}

	var flagged []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "Warnf" && sel.Sel.Name != "Errorf") {
				return true
			}
			if len(call.Args) > 0 {
				if id, ok := call.Args[0].(*ast.Ident); ok && id.Name == startupClassIdent {
					return true
				}
			}
			if !withinAny(call.Pos(), guarded) {
				flagged = append(flagged, fn.Name.Name)
			}
			return true
		})
	}

	if got := strings.Join(flagged, ","); got != "unguarded" {
		t.Errorf("the scan flagged %q, want exactly \"unguarded\" -- it must catch the "+
			"unguarded call, pass the guarded one, and leave the startup class alone", got)
	}
}

// --- the guard detector --------------------------------------------------

// posRange is the half-open source range of one guarded block.
type posRange struct{ lo, hi token.Pos }

// allowGuardedRanges returns the body range of every `if` whose condition
// contains a call to a method named Allow.
//
// The CONDITION rather than the whole statement, so that an Allow appearing
// somewhere else in the function does not launder an unguarded call elsewhere
// in it. Any condition shape counts -- a bare Allow, or `!ok &&
// Log.Allow(...)` as countUnresolved uses -- because what the rule requires is
// that the bucket is consulted before the arguments are built, not that the
// expression take one particular form.
func allowGuardedRanges(file *ast.File) []posRange {
	var out []posRange
	ast.Inspect(file, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || ifs.Cond == nil || ifs.Body == nil {
			return true
		}
		found := false
		ast.Inspect(ifs.Cond, func(c ast.Node) bool {
			call, ok := c.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Allow" {
				found = true
			}
			return true
		})
		if found {
			out = append(out, posRange{lo: ifs.Body.Pos(), hi: ifs.Body.End()})
		}
		return true
	})
	return out
}

// withinAny reports whether p falls inside one of the ranges.
func withinAny(p token.Pos, ranges []posRange) bool {
	for _, r := range ranges {
		if p >= r.lo && p < r.hi {
			return true
		}
	}
	return false
}
