package mirror

import (
	"hash/maphash"
	"testing"

	"feedvalve/internal/model"
)

// digestMinMBps is G-AIX-1 assertion 2's throughput floor (Vol 4 section 7.5,
// V4:1749): 200 MB/s, against which the merge's ~90 ns digest estimate (Vol 2
// section 4.4) was made.
const digestMinMBps = 200

// TestG_AIX_1_2_TheDigestIsAllocationFreeAndFast is G-AIX-1 assertion 2, and on
// the LPAR it closes OQ-L3. It lives here, not in internal/app/aix_test.go
// where the card first placed it, so that it measures the shipped digest --
// remap included -- rather than a copy of today's implementation: if OQ-L3's
// FNV-1a fallback (ADR-L02) is adopted, this keeps measuring what ships.
// scripts/lpar-smoke.sh runs it from mirror.test. (Not named *_aix_test.go:
// that suffix is a GOOS build constraint and would hide it everywhere else.)
//
// SETUP: one row image of destination 0's 181 bytes and a process seed.
//
// STIMULUS: testing.Benchmark over digest(seed, img[:181]). A Test and not a
// bare Benchmark, because a benchmark reports and cannot fail: run on the
// target, this is the thing that says no.
//
// ASSERTION: 0 allocs/op and >= 200 MB/s; non-vacuity, the benchmark ran and
// hashed bytes at all. The measured figures are logged either way, because on
// the LPAR they are OQ-L3's answer and get recorded.
//
// WOULD HAVE CAUGHT: hash/maphash taking a path on aix/ppc64 that allocates or
// is an order of magnitude slower than assumed (Vol 0 OQ-L3: the AES path is
// not enabled there, so the portable one runs -- assumed, never measured). On
// the build host this is a regression check on the Go toolchain; the assertion
// that matters is the on-target run. A failure there triggers the FNV-1a
// fallback of ADR-L02, which is recorded rather than patched over.
func TestG_AIX_1_2_TheDigestIsAllocationFreeAndFast(t *testing.T) {
	seed := maphash.MakeSeed()
	var img [model.ImageSize]byte
	for i := range img {
		img[i] = byte(i * 7)
	}

	var sink uint64
	r := testing.Benchmark(func(b *testing.B) {
		b.SetBytes(181)
		b.ReportAllocs()
		for range b.N {
			sink += digest(seed, img[:181])
		}
	})
	_ = sink

	if r.N == 0 || r.T <= 0 || r.Bytes != 181 {
		t.Fatalf("non-vacuity: the benchmark did not run (N %d, T %v, bytes/op %d)", r.N, r.T, r.Bytes)
	}
	mbps := float64(r.Bytes) * float64(r.N) / r.T.Seconds() / 1e6
	t.Logf("OQ-L3: digest over 181 bytes: %d ns/op, %.0f MB/s, %d allocs/op, %d B/op (N=%d)",
		r.NsPerOp(), mbps, r.AllocsPerOp(), r.AllocedBytesPerOp(), r.N)

	if a := r.AllocsPerOp(); a != 0 {
		t.Errorf("digest allocated %d times per 181-byte image, want 0: at 20,000 merges/s that is "+
			"garbage on the shard goroutine. OQ-L3 fails; switch to the FNV-1a fallback (ADR-L02) "+
			"and record it", a)
	}
	if mbps < digestMinMBps {
		t.Errorf("digest ran at %.0f MB/s, want >= %d. OQ-L3 fails; switch to the FNV-1a fallback "+
			"(ADR-L02) and record it", mbps, digestMinMBps)
	}
}
