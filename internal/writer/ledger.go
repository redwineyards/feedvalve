package writer

import (
	"math/bits"
	"time"
)

// stmtFailRecord is one statement identity's entry in the pool-stop ledger
// (V3 section 9.4). writers is a bitmask of writer ids that saw the identity
// fail, so the same writer failing twice does not count twice; first is when
// the window opened.
type stmtFailRecord struct {
	writers uint32
	first   time.Time
}

// stmtLedger decides the pool-stop rule: the same statement identity failing
// on `need` distinct writers within `window`. It is not safe for concurrent
// use; poolHealth serialises access.
//
// Writer ids share 32 bits (id mod 32). A pool is a handful of writers, and
// a collision can only make two writers count as one, which delays the stop
// rather than causing a spurious one.
type stmtLedger struct {
	need   int
	window time.Duration
	m      map[stmtKey]stmtFailRecord
}

func newStmtLedger(need int, window time.Duration) *stmtLedger {
	return &stmtLedger{need: need, window: window, m: map[stmtKey]stmtFailRecord{}}
}

// record notes that writerID saw k fail at now and reports whether the pool
// must stop. Entries whose window has closed are dropped lazily here, so the
// map is bounded by the number of identities and needs no sweeper.
func (l *stmtLedger) record(k stmtKey, writerID int, now time.Time) bool {
	for key, r := range l.m {
		if now.Sub(r.first) > l.window { // the boundary itself is still inside
			delete(l.m, key)
		}
	}
	r, ok := l.m[k]
	if !ok {
		r.first = now
	}
	r.writers |= 1 << (uint(writerID) % 32)
	l.m[k] = r
	return bits.OnesCount32(r.writers) >= l.need
}
