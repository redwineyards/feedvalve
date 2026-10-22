//go:build !feedvalve_assert

package mirror

import (
	"testing"
	"unsafe"
)

// TestG_ASSERT_1_CompiledOutOfTheProductionBuild: without -tags
// feedvalve_assert the shard's ownership assertion has no state -- the
// production build carries no mutex (V4:1763, "compiles out to nothing").
func TestG_ASSERT_1_CompiledOutOfTheProductionBuild(t *testing.T) {
	if n := unsafe.Sizeof(loopOwner{}); n != 0 {
		t.Fatalf("loopOwner is %d bytes in the production build, want 0", n)
	}
}
