// Package app builds the whole graph, owns the goroutines, and runs the
// lifecycle: construction order, signal handling, and the ordered drain.
//
// app is the only package allowed to import both halves of the tree -- the
// receive path and the database side. Nothing is forbidden to it, which is
// exactly why every other package's restrictions are enforced by a test rather
// than by convention: app is where a shortcut would be easiest to hide.
//
// Startup binds the socket last, so nothing can arrive before the consumer that
// handles it exists. Shutdown reverses that, with a drain stage between stopping
// the reader and stopping the decoder so the datagram queue empties rather than
// being abandoned.
//
// app does not call os.Exit and does not use a fatal logger. Neither does any
// other package except the command itself: a fatal logger calls os.Exit, which
// skips every deferred cleanup, and with Oracle sessions and in-flight batches
// open that leaves state behind on every terminal condition. The
// proof-of-concept was restructured for exactly this reason
// (poc/gobrdclnt.go:19-28); a source scan now enforces it (gate G-BLD-3).
//
// Carried over in structure from poc/gobrdclnt.go's run and receiveLoop.
//
// Specified by docs/lld/04-lifecycle-ops.md. Built in P1-09.
package app
