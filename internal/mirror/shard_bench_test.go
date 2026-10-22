package mirror

import (
	"hash/maphash"
	"testing"

	"feedvalve/internal/model"
)

// The shard benchmarks are for numbers only (phase 3 P3-14). Nothing asserts
// on them: the zero-allocation claims are AllocsPerRun tests
// (TestFullCycleOnWarmRowsAllocatesNothing and the per-function ones), and the
// on-target digest assertion is G-AIX-1 assertion 2 in internal/app.
//
//	go test -run '^$' -bench BenchmarkShard -benchmem ./internal/mirror

// BenchmarkShardMerge is one 181-byte 7208 merge onto a warm, dirty row: the
// coalescing path, which is the common case under load (Vol 2 section 4.4's
// ~365 ns estimate).
func BenchmarkShardMerge(b *testing.B) {
	r := newCycleRig(b)
	r.send(0, 1) // dirty, so every measured merge coalesces
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		r.send(0, uint32(i%250)+1)
	}
}

// BenchmarkShardCycle is one full cycle of cycleRig: eight merges, an epoch
// of two batches, four merges in flight and the completions that close it.
func BenchmarkShardCycle(b *testing.B) {
	r := newCycleRig(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.cycle(b)
	}
	b.ReportMetric(float64(cycleRows), "rows/op")
}

// BenchmarkShardDigest is the row digest over destination 0's 181 bytes, in
// MB/s: the number OQ-L3 asks about.
func BenchmarkShardDigest(b *testing.B) {
	seed := maphash.MakeSeed()
	var e model.Entry
	for i := range e.Image {
		e.Image[i] = byte(i * 7)
	}
	var sink uint64
	b.SetBytes(181)
	b.ReportAllocs()
	for range b.N {
		sink += digest(seed, e.Image[:181])
	}
	_ = sink
}
