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
// THE DRAIN IS IN Run'S BODY AND NOT A DEFER. A defer cannot be sequenced
// against another defer's resource, and the writers will need the Oracle pool
// open while they finish their last batch -- while closing that pool is itself a
// defer registered when it was opened. Putting the drain in the body makes the
// ordering explicit instead of leaving it to LIFO unwind order.
//
// Every acquisition and release is RECORDED as well as performed. Two of the
// lifecycle gates are assertions about order rather than about behaviour -- that
// the socket is bound after the pre-allocation and after the goroutines start,
// and that every resource taken is given back -- and neither is observable from
// outside the process. The record is in addition to real defers, never instead
// of them: only a defer survives a panic.
//
// A terminal condition arrives through Graph.Fatal, which is one-shot and
// delivers to a channel. The first caller wins, because one condition commonly
// causes another -- a database fail-fast stops the writer pool -- and the
// operator needs the one that started it. It never exits.
//
// What P1-09 orders and what it does not yet contain: the loggers, the
// instrument registry, the database handle, QD, the decoder, the stats
// goroutine and the listener are all here. The shard and writer pools are
// Phase 3 and Phase 4, and the slots they take in the startup and shutdown
// sequences are marked in lifecycle.go. The decoder's callback is the seam
// Phase 2 fills with framing and decode; until then it drains and discards,
// which still has to TAKE each datagram or shutdown would abandon a ring with
// items in it.
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
