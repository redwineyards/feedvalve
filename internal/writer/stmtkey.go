package writer

import (
	"strconv"

	"feedvalve/internal/model"
)

// stmtKey is a statement identity: one destination and one column mask. Two
// batch rows with the same stmtKey run the same MERGE text, so it keys the
// per-writer statement cache and names a Binder (V3 section 4). It is
// comparable and pointer-free so it can be a map key.
type stmtKey struct {
	Dest model.DestID
	Mask model.ColMask
}

// String renders "d<dest>/m<mask hex>", for log lines and test failures.
func (k stmtKey) String() string {
	return "d" + strconv.Itoa(int(k.Dest)) + "/m" + strconv.FormatUint(uint64(k.Mask), 16)
}
