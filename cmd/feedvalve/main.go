// Command feedvalve relays the NSEIL commodity-derivatives NNF broadcast feed
// into Oracle.
//
// main holds nothing but flag parsing and the process exit code; the lifecycle
// lives in run(ctx) error, inside internal/app. The split is not stylistic. A
// fatal logger calls os.Exit, which skips every deferred cleanup, so the
// Fatalf-on-error style could never have closed the database or drained the
// queue. Startup failures return errors instead.
//
// This file is the only place in the tree permitted to call os.Exit or a fatal
// logger (gate G-BLD-3). It currently calls neither.
//
// Scaffolding: P1-01 creates the package tree, P1-09 replaces this body with
// flag parsing, signal handling and the ordered shutdown.
//
// # A note on the timezone database, and OQ-L5
//
// Vol 0 section 7.2 specifies importing time/tzdata here, so that
// liveness.tradingWindow.tz resolves without the host supplying a zoneinfo
// database -- AIX may ship none. That import is NOT present because it does
// not link in this toolchain:
//
//	time/tzdata.init.0: relocation target
//	time.registerLoadFromEmbeddedTZData not defined
//
// Reproduced with go1.23.3 on a two-line program with no other dependency,
// for aix/ppc64, linux/amd64 and the native target alike, and it survives
// "go build -a", so it is neither a stale cache nor specific to the
// cross-compile. The push linkname directive is present in the installed
// time/zoneinfo_read.go, so the source is right and the link step is not.
//
// Consequence, which P1-11's LPAR smoke test must settle: the process relies
// on the HOST for zoneinfo, and config validation currently treats an
// unresolvable timezone as a hard error. If the LPAR has no zoneinfo
// database, the process will refuse to start over a feature that only
// downgrades log severity outside market hours. OQ-L5 notes that failing open
// -- treating every hour as in-window -- is the safe default; adopting it is
// a change to section 7.2's stated rule and is left as a decision rather than
// made here.
package main

func main() {}
