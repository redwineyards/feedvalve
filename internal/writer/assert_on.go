//go:build feedvalve_assert

package writer

import (
	"strconv"
	"sync"
)

// loopOwner is gate G-ASSERT-1's writer half (V4 section 7.6): a mutex whose
// TryLock wraps one writer's whole loop. A failed TryLock means a second
// goroutine entered a loop that is meant to have exactly one owner (the
// session, the statement cache and the binders are all single-goroutine
// state), and it panics. Built only under -tags feedvalve_assert; assert_off.go
// is the production build's empty version. Mirrors internal/mirror.
type loopOwner struct{ mu sync.Mutex }

func (o *loopOwner) enter(id int) {
	if !o.mu.TryLock() {
		panic("writer: G-ASSERT-1: a second goroutine entered writer " + strconv.Itoa(id) + "'s loop")
	}
}

func (o *loopOwner) exit() { o.mu.Unlock() }
