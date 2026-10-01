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
package main

func main() {}
