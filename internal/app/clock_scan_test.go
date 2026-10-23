package app_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// wallEpochCalls returns the position of every call to a time.Time method that
// turns a reading into a wall-clock epoch number, which is the first half of
// an elapsed-time measurement that an NTP step corrupts.
func wallEpochCalls(fset *token.FileSet, file *ast.File) []string {
	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		// time.Unix(sec, nsec) is the package's constructor, a conversion the
		// other way (an epoch number to a Time); only the methods matter.
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" {
			return true
		}
		switch sel.Sel.Name {
		case "UnixNano", "UnixMilli", "UnixMicro", "Unix":
			out = append(out, fset.Position(call.Pos()).String())
		}
		return true
	})
	return out
}

// TestG_CLOCK_1_NoElapsedTimeIsMeasuredFromTheWallClock is the structural half
// of gate G-CLOCK-1 (Vol 0 section 10.2): ordering, the circuit's thresholds,
// the flush tick, the drain deadline and ConnMaxLifetime all measure time on
// the monotonic clock, so a wall step cannot move them.
//
// SETUP: every non-test Go file under internal/ and cmd/.
// STIMULUS: look for any Unix, UnixNano, UnixMilli or UnixMicro call -- the
// conversion that discards the monotonic reading, which is how
// t1.UnixNano() - t0.UnixNano() is spelled.
// ASSERTION: none. The remaining clock reads are time.Now()/time.Since/
// Timer/Ticker, which carry the monotonic reading, and the stats goroutine's
// one deliberate wall read, which Round(0)s it and compares it to monotonic
// time to find the step.
// WOULD HAVE CAUGHT: an elapsed measurement written as a difference of epoch
// numbers, which across a -3600 s step is negative, and as an unsigned counter
// argument or a timer duration is catastrophic.
// NON-VACUITY: the detector finds the calls in a synthetic source, and the
// scan visited files.
func TestG_CLOCK_1_NoElapsedTimeIsMeasuredFromTheWallClock(t *testing.T) {
	const src = `package x
func f(a, b time.Time) int64 { return a.UnixNano() - b.UnixNano() }
func g(a time.Time) int64    { return time.Now().Unix() }
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(wallEpochCalls(fset, file)); got != 3 {
		t.Fatalf("the detector found %d epoch conversions in the synthetic source, want 3", got)
	}

	files := scanTree(t)
	for _, f := range files {
		for _, pos := range wallEpochCalls(f.fset, f.file) {
			t.Errorf("%s: wall-clock epoch conversion. Elapsed time must be measured on the "+
				"monotonic clock (Vol 0 section 10.2); use time.Since, Sub on two readings "+
				"from time.Now, or processStart", pos)
		}
	}
	if len(files) < 20 {
		t.Errorf("the scan visited only %d files", len(files))
	}
}
