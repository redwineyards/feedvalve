package mirror

import (
	"errors"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/model"
)

// fakeWriter is the writer test double, defined at its consumer (phase 3
// section 4, "The writer test double"). It obeys the shard side of the
// boundary exactly (V2:1851-1854, 2822-2823; V3:52-62):
//
//  1. receive a *model.Batch from batchQ;
//  2. write ONLY Rows[i].Outcome, Rows[i].ErrCode and b.Err;
//  3. send it on completion[b.Shard] and never touch it again (V2:2223-2224);
//  4. B-V3: adjudicate every row -- never Err == nil with a Pending row --
//     except in the scripted unreportedRow mode (G-ROW-3);
//  5. B-V3b: return the batch before any backoff (it has none).
//
// It also records the most batches it held at once, which G-INFL-3/4 assert
// on. Under -race a double that touched anything else would be a detected race
// against the shard, which is the point of keeping it this strict.
type fakeWriter struct {
	batchQ     <-chan *model.Batch
	completion []chan *model.Batch // indexed by Batch.Shard
	mode       fakeMode

	held, maxHeld atomic.Int32
	wg            sync.WaitGroup
}

// fakeMode is how the fake adjudicates a batch: wait (hold or latency), then
// adjudicate.
type fakeMode struct {
	wait       func()
	adjudicate func(b *model.Batch)
}

// errFakeBatch is allFailedErr's batch-level error.
var errFakeBatch = errors.New("fake writer: batch failed")

// commitAll is B-V3's every-row verdict: Committed, no error code.
func commitAll(b *model.Batch) {
	for i := range b.Active() {
		b.Rows[i].Outcome, b.Rows[i].ErrCode = model.OutcomeCommitted, 0
	}
}

// allCommitted commits every row.
func allCommitted() fakeMode { return fakeMode{adjudicate: commitAll} }

// allFailedErr is a batch that never reached the server: Err set and every row
// left Pending, which the completion path must read as Failed (V2:82).
func allFailedErr() fakeMode {
	return fakeMode{adjudicate: func(b *model.Batch) { b.Err = errFakeBatch }}
}

// perRow gives row i outcome[i] and code[i] (e.g. Rejected with 1438).
// Rows past the scripts' ends are Committed.
func perRow(outcome []model.RowOutcome, code []uint16) fakeMode {
	return fakeMode{adjudicate: func(b *model.Batch) {
		commitAll(b)
		for i := range b.Active() {
			if i < len(outcome) {
				b.Rows[i].Outcome = outcome[i]
			}
			if i < len(code) {
				b.Rows[i].ErrCode = code[i]
			}
		}
	}}
}

// unreportedRow commits every row but i, which it leaves Pending with Err nil:
// the B-V3 violation G-ROW-3 scripts on purpose.
func unreportedRow(i int) fakeMode {
	return fakeMode{adjudicate: func(b *model.Batch) {
		commitAll(b)
		if i < int(b.N) {
			b.Rows[i].Outcome = model.OutcomePending
		}
	}}
}

// holdUntil stalls every batch until release is closed, then commits it.
func holdUntil(release <-chan struct{}) fakeMode {
	return fakeMode{wait: func() { <-release }, adjudicate: commitAll}
}

// latency commits every batch after d.
func latency(d time.Duration) fakeMode {
	return fakeMode{wait: func() { time.Sleep(d) }, adjudicate: commitAll}
}

// startFakeWriters runs n writer goroutines in mode, serving batchQ until it is
// closed, returning batches on completion[b.Shard]. stop closes nothing; the
// test closes batchQ and then calls wait.
func startFakeWriters(n int, mode fakeMode, batchQ <-chan *model.Batch, completion []chan *model.Batch) *fakeWriter {
	w := &fakeWriter{batchQ: batchQ, completion: completion, mode: mode}
	w.wg.Add(n)
	for range n {
		go w.serve()
	}
	return w
}

func (w *fakeWriter) serve() {
	defer w.wg.Done()
	for b := range w.batchQ {
		h := w.held.Add(1)
		for m := w.maxHeld.Load(); h > m && !w.maxHeld.CompareAndSwap(m, h); m = w.maxHeld.Load() {
		}
		if w.mode.wait != nil {
			w.mode.wait()
		}
		w.mode.adjudicate(b)
		w.held.Add(-1)
		w.completion[b.Shard] <- b // the last touch
	}
}

// wait blocks until every writer has returned (batchQ closed and drained).
func (w *fakeWriter) wait() { w.wg.Wait() }

// stopper returns stop, which closes release and batchQ once and waits for the
// writers, and registers it with t.Cleanup -- so a t.Fatalf anywhere, waitHeld's
// included, cannot leave a writer blocked on release or on batchQ.
func (w *fakeWriter) stopper(t testing.TB, release chan struct{}, batchQ chan *model.Batch) (stop func()) {
	var once sync.Once
	stop = func() {
		once.Do(func() {
			close(release)
			close(batchQ)
			w.wait()
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitHeld polls until the writers hold n batches, failing after a second.
func (w *fakeWriter) waitHeld(t testing.TB, n int32) {
	t.Helper()
	for deadline := time.Now().Add(time.Second); w.held.Load() != n; {
		if time.Now().After(deadline) {
			t.Fatalf("writers hold %d batches, want %d", w.held.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFakeWriterWritesOnlyItsThreeFields is the double's self-test: every mode,
// on a batch whose every field is random, changes nothing but Rows[i].Outcome,
// Rows[i].ErrCode and Err, returns the batch on its own shard's channel, and
// adjudicates every row (B-V3) except where unreportedRow scripts otherwise.
func TestFakeWriterWritesOnlyItsThreeFields(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	random := func() *model.Batch {
		b := &model.Batch{Shard: 1, Epoch: rng.Uint64(), N: 5, Rows: make([]model.BatchRow, 8)}
		for d := range b.DestStart {
			b.DestStart[d], b.DestCount[d] = rng.Int31(), rng.Int31()
		}
		for i := range b.Rows {
			r := &b.Rows[i]
			r.Row, r.Mask, r.Digest = model.RowKey(rng.Int63()), model.ColMask(rng.Uint64()), rng.Uint64()
			r.Slot, r.Outcome, r.ErrCode = rng.Int31(), model.OutcomePending, uint16(rng.Intn(1<<16))
			rng.Read(r.Image[:])
		}
		return b
	}
	// stripped is b with the three writer-owned fields zeroed, for comparison.
	stripped := func(b *model.Batch) model.Batch {
		c := *b
		c.Err = nil
		c.Rows = append([]model.BatchRow(nil), b.Rows...)
		for i := range c.Rows {
			c.Rows[i].Outcome, c.Rows[i].ErrCode = 0, 0
		}
		return c
	}

	release := make(chan struct{})
	close(release)
	modes := []struct {
		name       string
		mode       fakeMode
		unreported int // the row left Pending with Err nil, or -1
		failed     bool
	}{
		{"allCommitted", allCommitted(), -1, false},
		{"allFailedErr", allFailedErr(), -1, true},
		{"perRow", perRow([]model.RowOutcome{model.OutcomeCommitted, model.OutcomeRejected, model.OutcomeFailed},
			[]uint16{0, 1438, 3113}), -1, false},
		{"unreportedRow", unreportedRow(2), 2, false},
		{"holdUntil", holdUntil(release), -1, false},
		{"latency", latency(time.Millisecond), -1, false},
	}
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			q := make(chan *model.Batch, 1)
			completion := []chan *model.Batch{make(chan *model.Batch, 1), make(chan *model.Batch, 1)}
			w := startFakeWriters(1, tc.mode, q, completion)
			b := random()
			before := stripped(b)
			var tail []uint16 // the error codes past N, which stripped zeroes
			for _, r := range b.Rows[b.N:] {
				tail = append(tail, r.ErrCode)
			}
			q <- b
			got := <-completion[1]
			close(q)
			w.wait()

			if got != b {
				t.Fatal("the writer returned a different batch")
			}
			if !reflect.DeepEqual(stripped(b), before) {
				t.Fatal("the writer wrote outside Rows[i].Outcome, Rows[i].ErrCode and Err")
			}
			if (b.Err != nil) != tc.failed {
				t.Fatalf("Err = %v, want failed %v", b.Err, tc.failed)
			}
			for i := range b.Active() {
				pending := b.Rows[i].Outcome == model.OutcomePending
				if b.Err == nil && pending != (i == tc.unreported) {
					t.Fatalf("row %d Pending %v with Err nil; only row %d may be (B-V3)", i, pending, tc.unreported)
				}
			}
			for i := b.N; i < int32(len(b.Rows)); i++ {
				if b.Rows[i].Outcome != model.OutcomePending || b.Rows[i].ErrCode != tail[i-b.N] {
					t.Fatalf("row %d is past N = %d and was written", i, b.N)
				}
			}
			if tc.name == "perRow" && (b.Rows[1].Outcome != model.OutcomeRejected || b.Rows[1].ErrCode != 1438) {
				t.Fatalf("perRow row 1 = %v/%d, want Rejected/1438", b.Rows[1].Outcome, b.Rows[1].ErrCode)
			}
		})
	}

	// The concurrency record: two writers holding two batches report 2.
	t.Run("maxHeld", func(t *testing.T) {
		hold := make(chan struct{})
		q := make(chan *model.Batch, 2)
		completion := []chan *model.Batch{make(chan *model.Batch, 2)}
		w := startFakeWriters(2, holdUntil(hold), q, completion)
		stop := w.stopper(t, hold, q)
		q <- &model.Batch{Rows: make([]model.BatchRow, 1)}
		q <- &model.Batch{Rows: make([]model.BatchRow, 1)}
		w.waitHeld(t, 2)
		stop()
		<-completion[0]
		<-completion[0]
		if got := w.maxHeld.Load(); got != 2 {
			t.Fatalf("maxHeld = %d, want 2", got)
		}
	})
}
