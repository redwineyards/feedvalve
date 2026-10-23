package app

import (
	"context"
	"errors"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/writer"
)

// maxWriterSlots is the {writer} dimension obs sizes (writers.count's upper
// bound, config V-RANGE 1..32), so the per-writer statement-cache gauges are
// resolved for every slot a Resize could ever reach.
const maxWriterSlots = 32

// resolveWriterInstruments resolves the writer pool's handles. Part of
// resolveInstruments and so before reg.Freeze (G-OBS-1): the writer holds the
// pointers and never looks one up.
func (in *instruments) resolveWriterInstruments(reg *obs.Reg, dests int) {
	w := writer.NewInstruments(reg, dests)
	w.ResolveCache(reg, maxWriterSlots, dests)
	w.ResolvePool(reg, dests)
	in.writer = w
}

// writerParams projects the configuration onto writer.Params -- the one place
// the units are converted, like mirrorParams (ruling D17: loaded once per
// batch by each writer; the SIGHUP driver, reload.go, publishes a new one).
func writerParams(cfg *config.Config) *writer.Params {
	w := &cfg.Writers
	return &writer.Params{
		QueryTimeout:    time.Duration(cfg.DB.QueryTimeoutSec) * time.Second,
		RetryInitial:    time.Duration(w.RetryInitialMs) * time.Millisecond,
		RetryMax:        time.Duration(w.RetryMaxMs) * time.Millisecond,
		ConnIdleRelease: time.Duration(w.ConnIdleReleaseSec) * time.Second,
		ConnHold:        time.Duration(cfg.DB.ConnMaxLifetimeMinutes) * time.Minute,
		StmtCacheSize:   w.StmtCacheSize,

		Time1980UnixOffsetSec: int64(cfg.Runtime.Time1980UnixOffsetSec),

		FailFastWindow:       time.Duration(w.FailFastSeconds) * time.Second,
		FailFastWrites:       w.FailFastWrites,
		StatementFailWriters: w.StatementFailWriters,
		StatementFailWindow:  time.Duration(w.StatementFailWindowSec) * time.Second,
	}
}

// buildPool constructs the pool when a database is configured: allocation
// only, no goroutine and no session (Build's contract).
func (g *Graph) buildPool() error {
	d := g.deps
	if d.DB == nil {
		return nil
	}
	pool, err := writer.NewPool(d.DB, d.Dests,
		writer.Config{Writers: d.Cfg.Writers.Count, Ceiling: d.Cfg.Runtime.MaxBatchSizeCeiling},
		func() *writer.Params { return writerParams(g.loadRuntime().Cfg) },
		d.Inst.writer, d.Log)
	if err != nil {
		return exitf(ExitStartup, "building the writer pool: %w", err)
	}
	g.pool = pool
	g.mark(&g.acquired, "writer-pool")
	return nil
}

// startWriters is startup step 13: start the pool BEFORE the shards and before
// the socket, so the first batch has a consumer. Without a database there is
// no pool and nothing starts -- the production binary has no stub writer.
//
// The pool runs under its own context (D13), not Run's: SIGTERM must not
// cancel a writer that is finishing the drain's last batch. stopWriters cancels
// it at drainExpired and closes the pool after the shards join; teardown runs
// on every Run exit, so a later startup failure (decoder, bind) does not leak
// the writer goroutines and their sessions.
func (g *Graph) startWriters() error {
	if g.pool == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.poolCancel = cancel
	// Q2c as send-only: a writer may return a batch and never read one.
	comp := make([]chan<- *model.Batch, len(g.completionCh))
	for k, c := range g.completionCh {
		comp[k] = c
	}
	if err := g.pool.Start(ctx, g.batchQ, comp); err != nil {
		cancel()
		return exitf(ExitStartup, "starting the writer pool: %w", err)
	}
	g.mark(&g.acquired, "writers")
	g.writersStarted = true
	g.startPoolWatch()
	return nil
}

// stopWriters is shutdown steps 9 and 10. The caller has joined the shards, so
// nothing sends on batchQ and it can be closed: the writers then process
// whatever is queued, return their batches (completionCh has room for every
// one, so a shard that already left cannot block them) and exit.
//
// The pool's context is cancelled only if the shared drain deadline expires
// first (D13): a stalled database must not keep the process alive past
// flush.drainTimeoutMs, and the cancelled batches come back Failed, uncounted,
// their rows already counted by the shard as abandoned. The wait after the
// cancel is unconditional because the database may be closed only once no
// writer can touch it. A no-op without a pool; safe when the pool never
// started (Pool.Close on one is a no-op).
func (g *Graph) stopWriters() {
	if g.batchQ != nil {
		close(g.batchQ)
	}
	if g.pool == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		// Close only errors on its ctx, which is Background here.
		_ = g.pool.Close(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-g.drainExpired:
		if g.poolCancel != nil {
			g.poolCancel()
		}
		<-done
	}
	if g.poolCancel != nil {
		g.poolCancel() // release the context; the writers are gone
	}
	if g.writersStarted {
		g.mark(&g.released, "writers")
	}
}

// startPoolWatch launches the goroutine that turns Pool.Stopped into the fatal
// path. Neither the pool nor app exits the process: Fatal delivers to Run.
func (g *Graph) startPoolWatch() {
	g.poolWatchStop = make(chan struct{})
	g.poolWatchDone = make(chan struct{})
	go func() {
		defer close(g.poolWatchDone)
		select {
		case <-g.pool.Stopped():
			reason, detail := poolStopReason(g.pool.StopReason())
			g.Fatal(reason, detail)
		case <-g.poolWatchStop:
		}
	}()
}

// stopPoolWatch joins the watcher. A no-op when no pool was started.
func (g *Graph) stopPoolWatch() {
	if g.poolWatchStop == nil {
		return
	}
	close(g.poolWatchStop)
	<-g.poolWatchDone
}

// poolStopReason maps a pool stop onto the fatal enumeration (ruling D3): the
// fail-fast rule is F13; the statement rule and an empty pool are F20. Both
// exit 4. The detail is the StopError's text, whose cause is already redacted.
func poolStopReason(err error) (FatalReason, string) {
	if err == nil {
		return FatalWriterPoolStop, "pool stopped"
	}
	var se *writer.StopError
	if errors.As(err, &se) && se.Kind == writer.StopFailFast {
		return FatalDBFailFast, err.Error()
	}
	return FatalWriterPoolStop, err.Error()
}
