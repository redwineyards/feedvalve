package lzo

import "testing"

// allocs/op is the number that matters, not ns/op -- the convention
// internal/obs/instrument_test.go established for the first benchmarks in this
// repository and internal/framing/bench_test.go followed. The hard assertion
// lives in TestDecompressSafeAllocatesNothing so a regression fails the suite
// rather than waiting for someone to read benchmark output; this prices the
// call.

// BenchmarkDecompressSafe decompresses the 470-byte MBP-shaped fixture into a
// destination sized the way the receiver sizes it, which is the steady state on
// the packet path: one compressed sub-packet, one reused buffer, no setup.
//
// Bytes per second is reported against the DECOMPRESSED size, since that is the
// work done -- the figure to compare against the feed's 7,500 sub-packets/s.
func BenchmarkDecompressSafe(b *testing.B) {
	data := buildSyntheticBroadcastMessage(12345, mbpShapedLen)
	compressed := benchFixture(b, data)
	dst := make([]byte, MaxDecompressedLen)

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := DecompressSafe(dst, compressed)
		if err != nil {
			b.Fatalf("DecompressSafe: %v", err)
		}
		if n != len(data) {
			b.Fatalf("n=%d, want %d", n, len(data))
		}
	}
}

// BenchmarkDecompressSafeRejectTruncated prices the rejection path, because on
// a corrupt or lossy circuit it is the path that runs. A reject that cost an
// allocation or a wildly different amount of work would turn wire corruption
// into a backpressure problem.
func BenchmarkDecompressSafeRejectTruncated(b *testing.B) {
	data := buildSyntheticBroadcastMessage(12345, mbpShapedLen)
	truncated := benchFixture(b, data)
	truncated = truncated[:len(truncated)/2]
	dst := make([]byte, MaxDecompressedLen)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecompressSafe(dst, truncated); err == nil {
			b.Fatal("truncated stream decompressed without error")
		}
	}
}

// benchFixture is compressWithReference for benchmarks: the loader's failure
// mode is a *testing.T Fatalf, and a benchmark holds a *testing.B.
func benchFixture(b *testing.B, data []byte) []byte {
	b.Helper()

	recorded, err := loadLZOFixtures()
	if err != nil {
		b.Fatalf("loading %s: %v", lzoFixtureFile, err)
	}
	compressed, ok := recorded[fixtureKey(data)]
	if !ok {
		b.Fatalf("no recorded LZO fixture for this input (%d bytes); see poc/decompress_test.go", len(data))
	}
	return compressed
}
