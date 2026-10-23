//go:build !feedvalve_assert

package mirror

import "feedvalve/internal/model"

// loopOwner is G-ASSERT-1's ownership assertion compiled out: no state, and
// methods the compiler inlines to nothing. See assert_on.go.
type loopOwner struct{}

func (*loopOwner) enter(model.ShardID) {}
func (*loopOwner) exit()               {}
