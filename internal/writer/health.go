package writer

import (
	"sync"
	"sync/atomic"
	"time"

	"feedvalve/internal/oradb"
)

// HealthState is the pool-wide write health of HLD 9.2 Figure 12.
type HealthState uint8

const (
	HealthHealthy  HealthState = iota // last write committed
	HealthDegraded                    // a transient failure, no commit since
	HealthFailFast                    // budget exhausted or pool-stop fired; terminal
)

// Action is what a writer must do after reporting a failure.
type Action uint8

const (
	Continue     Action = iota // keep taking batches
	RetireWriter               // this writer exits; the pool carries on
	StopPool                   // the pool is in FailFast
)

// healthConfig holds the thresholds, all from writers.* config (V3 9.3, 9.4).
type healthConfig struct {
	failFastWindow      time.Duration // pool-wide clock from first failure
	failFastWrites      int           // consecutive failures on one writer
	statementFailNeed   int           // distinct writers, same identity
	statementFailWindow time.Duration
}

// poolHealth is shared by every writer. OnSuccess is one atomic load while the
// pool is Healthy, so four writers committing do not contend on a mutex.
type poolHealth struct {
	cfg healthConfig
	now func() time.Time

	degraded atomic.Bool // true once not Healthy; the lock-free fast path

	mu            sync.Mutex
	state         HealthState
	degradedSince time.Time
	lastClass     oradb.ErrClass
	spaceOpen     bool // a Space failure no committed batch has cleared yet
	consec        map[int]int
	ledger        *stmtLedger
}

func newPoolHealth(cfg healthConfig, now func() time.Time) *poolHealth {
	return &poolHealth{
		cfg:    cfg,
		now:    now,
		consec: map[int]int{},
		ledger: newStmtLedger(cfg.statementFailNeed, cfg.statementFailWindow),
	}
}

// State returns the current state.
func (h *poolHealth) State() HealthState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}

// OnSuccess records a committed batch: a Degraded pool returns to Healthy and
// every failure count clears. Recovery is pool-wide, so any writer's commit
// recovers it and the id is unused (the signature is kept for the writer
// loop). FailFast is terminal and unaffected.
func (h *poolHealth) OnSuccess(_ int) {
	if !h.degraded.Load() {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state == HealthDegraded {
		h.recover()
	}
}

// recover returns to Healthy. Caller holds mu.
func (h *poolHealth) recover() {
	h.state = HealthHealthy
	h.spaceOpen = false
	clear(h.consec)
	h.degraded.Store(false)
}

// OnFailure records a classified failure of the batch on writerID. key is the
// failing statement identity, used only for ErrStatement. A retried batch that
// then succeeds must not be reported here (D6c).
func (h *poolHealth) OnFailure(writerID int, e *oradb.Error, key stmtKey) Action {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state == HealthFailFast {
		return StopPool
	}
	switch e.Class {
	case oradb.ErrRowData:
		return Continue // a data error never changes writer health
	case oradb.ErrStatement:
		if h.ledger.record(key, writerID, h.now()) {
			h.trip()
			return StopPool
		}
		return RetireWriter
	}
	h.markDegraded(e.Class)
	h.consec[writerID]++
	if h.gateFired(writerID) {
		h.trip()
		return StopPool
	}
	return Continue
}

// OnProbeSuccess records a successful Ping by a degraded writer with no
// batch. It clears that writer's count and restores Healthy only when the
// last failure was a Session class and no Space failure is still open: a
// working listener proves nothing about a full tablespace.
func (h *poolHealth) OnProbeSuccess(writerID int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != HealthDegraded {
		return
	}
	delete(h.consec, writerID)
	if h.lastClass == oradb.ErrSession && !h.spaceOpen {
		h.recover()
	}
}

// OnProbeFailure records a failed Ping. It is how the 30 s clock fires on a
// quiet market where no batch arrives to fail.
func (h *poolHealth) OnProbeFailure(writerID int) Action {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state == HealthFailFast {
		return StopPool
	}
	if h.state != HealthDegraded {
		return Continue
	}
	h.consec[writerID]++
	if h.gateFired(writerID) {
		h.trip()
		return StopPool
	}
	return Continue
}

func (h *poolHealth) markDegraded(c oradb.ErrClass) {
	if h.state == HealthHealthy {
		h.state = HealthDegraded
		h.degradedSince = h.now()
		h.degraded.Store(true)
	}
	h.lastClass = c
	if c == oradb.ErrSpace {
		h.spaceOpen = true
	}
}

// gateFired: the pool-wide clock or this writer's own count. Caller holds mu.
func (h *poolHealth) gateFired(writerID int) bool {
	return h.now().Sub(h.degradedSince) >= h.cfg.failFastWindow ||
		h.consec[writerID] >= h.cfg.failFastWrites
}

func (h *poolHealth) trip() {
	h.state = HealthFailFast
	h.degraded.Store(true)
}
