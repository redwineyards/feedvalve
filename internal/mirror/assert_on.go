//go:build feedvalve_assert

package mirror

import (
	"strconv"
	"sync"

	"feedvalve/internal/model"
)

// loopOwner is gate G-ASSERT-1's shard half (Vol 4 section 7.6): a mutex whose
// TryLock wraps each pass of Run. A failed TryLock means a second goroutine
// entered one shard's state -- the ownership rule the whole lock-free design
// rests on (Vol 0 section 9.1) -- and panics, which is a fatal assertion. No
// goroutine-ID parsing, no runtime.Stack.
//
// Built only under -tags feedvalve_assert; assert_off.go is the production
// build's empty version.
type loopOwner struct{ mu sync.Mutex }

func (o *loopOwner) enter(k model.ShardID) {
	if !o.mu.TryLock() {
		panic("mirror: G-ASSERT-1: a second goroutine entered shard " + strconv.Itoa(int(k)) + "'s loop body")
	}
}

func (o *loopOwner) exit() { o.mu.Unlock() }
