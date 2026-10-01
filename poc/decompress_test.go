package main

// This file tests poc/decompress.go's pure-Go LZO1Z decompression
// (Decompress / DecompressSafe) for correctness.
//
// The compressed fixtures it decompresses were produced by an independent
// LZO1Z implementation, github.com/rhnvrm/lzo1z, and are frozen in
// testdata/lzo_fixtures.json. That independence is the whole point: a
// decompressor checked only against a compressor from the same codebase
// agrees with itself even when both are wrong -- the same trap CLAUDE.md
// records for the MBP fixture builder, where a builder walking the same field
// order as the parser kept a 213-byte stride looking correct for months.
//
// Freezing the bytes costs no coverage. Every input here is deterministic:
// fixed strings, bytes.Repeat, zeroed buffers, and math/rand with hardcoded
// seeds. The fixtures never varied between runs. What it buys is a module
// with one fewer dependency, and no compressor of our own to be wrong.
//
// # Regenerating fixtures
//
// Needed only when a test case with a new input is added; such a case fails
// with its own hash and length rather than silently passing.
//
//	go get github.com/rhnvrm/lzo1z@v0.2.1
//	mv poc/lzo_record_test.go1 poc/lzo_record_test.go
//	FEEDVALVE_RECORD_LZO=poc/testdata/lzo_fixtures.jsonl go test ./poc/ -count=1
//	go run ./poc/internal/... # or: fold the .jsonl into lzo_fixtures.json
//	mv poc/lzo_record_test.go poc/lzo_record_test.go1
//	go mod edit -droprequire github.com/rhnvrm/lzo1z && go mod tidy
//
// The recorder lives in lzo_record_test.go1. Files ending .go1 are excluded
// from the build by their extension (see CLAUDE.md, "Disabled files"), so it
// costs nothing while inert and needs no second module to keep around.

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

const lzoFixtureFile = "testdata/lzo_fixtures.json"

// recordHook is nil in the normal build. Activating lzo_record_test.go1 sets
// it from an init, which is what lets regeneration work without editing this
// file -- Go has no way to redefine a function, so the seam has to be a
// variable.
var recordHook func(t *testing.T, data []byte) []byte

// lzoFixture is one recorded (input -> compressed) pair. inputLen is carried
// only so a human reading a diff can tell which case an entry belongs to.
type lzoFixture struct {
	InputLen   int    `json:"inputLen"`
	Compressed string `json:"compressed"` // base64
}

var (
	fixturesOnce sync.Once
	fixtures     map[string][]byte
	fixturesErr  error
)

// loadLZOFixtures reads and decodes the frozen fixture file once.
func loadLZOFixtures() (map[string][]byte, error) {
	fixturesOnce.Do(func() {
		raw, err := os.ReadFile(filepath.FromSlash(lzoFixtureFile))
		if err != nil {
			fixturesErr = err
			return
		}
		var recorded map[string]lzoFixture
		if err := json.Unmarshal(raw, &recorded); err != nil {
			fixturesErr = err
			return
		}
		fixtures = make(map[string][]byte, len(recorded))
		for key, f := range recorded {
			b, err := base64.StdEncoding.DecodeString(f.Compressed)
			if err != nil {
				fixturesErr = err
				return
			}
			fixtures[key] = b
		}
	})
	return fixtures, fixturesErr
}

// compressWithReference returns the LZO1Z-compressed form of data as produced
// by the reference implementation and recorded in testdata.
//
// It keeps the signature of the function that used to call the compressor
// directly, so every call site reads the same as before. Lookup is by hash of
// the input, which means a test can build its fixture however it likes as long
// as it does so deterministically.
func compressWithReference(t *testing.T, data []byte) []byte {
	t.Helper()

	if recordHook != nil {
		return recordHook(t, data)
	}

	recorded, err := loadLZOFixtures()
	if err != nil {
		t.Fatalf("loading %s: %v", lzoFixtureFile, err)
	}

	sum := sha256.Sum256(data)
	key := hex.EncodeToString(sum[:])
	compressed, ok := recorded[key]
	if !ok {
		t.Fatalf("no recorded LZO fixture for this input (%d bytes, sha256 %s).\n"+
			"Fixtures are frozen in %s; see the regeneration procedure at the top of\n"+
			"decompress_test.go. Note the input must be deterministic to be recordable.",
			len(data), key, lzoFixtureFile)
	}
	return compressed
}

// buildSyntheticBroadcastMessage builds a byte slice shaped like a real
// exchange broadcast message: a 40-byte header with a few varying fields
// followed by a mostly-zero-padded body, matching the sparse structure of
// the actual on-wire data these packets carry.
func buildSyntheticBroadcastMessage(token int32, messageLen int) []byte {
	data := make([]byte, messageLen)
	data[10] = 0x1c // transactionCode = 7305
	data[11] = 0x89
	be32 := func(off int, v int32) {
		data[off] = byte(v >> 24)
		data[off+1] = byte(v >> 16)
		data[off+2] = byte(v >> 8)
		data[off+3] = byte(v)
	}
	be32(4, 1700000000) // logTime
	be32(14, 42)        // sequenceNumber
	be32(40, token)     // token, right after the header
	for i := 0; i < 20 && 44+i < messageLen; i++ {
		data[44+i] = byte('A' + i%5) // a short non-zero run so it isn't 100% trivial
	}
	return data
}

func randomBytes(t *testing.T, n int, seed int64) []byte {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	if _, err := r.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

func TestDecompressMatchesReferenceCompressor(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"single byte", []byte{0x42}},
		{"short literal text", []byte("Hello, World! This is a short test string.")},
		{"highly repetitive", bytes.Repeat([]byte("AB"), 500)},
		{"all zeros 1KB", make([]byte, 1024)},
		{"all zeros 512B (typical sub-packet size)", make([]byte, 512)},
		{"random incompressible 2KB", randomBytes(t, 2048, 1)},
		{"random incompressible 64B", randomBytes(t, 64, 2)},
		{"synthetic MSSecurityUpdateInfo-shaped (294B)", buildSyntheticBroadcastMessage(999, 294)},
		{"synthetic MSBroadcastOnlyMBP-shaped (470B)", buildSyntheticBroadcastMessage(12345, 470)},
		{"long run then literal tail", append(bytes.Repeat([]byte{0x00}, 600), []byte("tail-literal-bytes-that-dont-repeat-1234567890")...)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compressed := compressWithReference(t, tc.data)

			t.Run("Decompress", func(t *testing.T) {
				dst := make([]byte, len(tc.data))
				n, err := Decompress(dst, compressed)
				if err != nil {
					t.Fatalf("Decompress: %v", err)
				}
				if n != len(tc.data) {
					t.Fatalf("Decompress returned n=%d, want %d", n, len(tc.data))
				}
				if !bytes.Equal(dst[:n], tc.data) {
					t.Fatalf("Decompress output mismatch:\ngot  %x\nwant %x", dst[:n], tc.data)
				}
			})

			t.Run("DecompressSafe", func(t *testing.T) {
				dst := make([]byte, len(tc.data))
				n, err := DecompressSafe(dst, compressed)
				if err != nil {
					t.Fatalf("DecompressSafe: %v", err)
				}
				if n != len(tc.data) {
					t.Fatalf("DecompressSafe returned n=%d, want %d", n, len(tc.data))
				}
				if !bytes.Equal(dst[:n], tc.data) {
					t.Fatalf("DecompressSafe output mismatch:\ngot  %x\nwant %x", dst[:n], tc.data)
				}
			})
		})
	}
}

// TestDecompressSafeRejectsTruncatedInput verifies DecompressSafe reports an
// error instead of silently producing wrong output when the compressed
// stream is cut short -- important since main() feeds it directly with
// attacker/network-controlled UDP bytes.
func TestDecompressSafeRejectsTruncatedInput(t *testing.T) {
	data := buildSyntheticBroadcastMessage(12345, 470)
	compressed := compressWithReference(t, data)

	for cut := 1; cut < len(compressed); cut *= 2 {
		truncated := compressed[:len(compressed)-cut]
		dst := make([]byte, len(data))
		n, err := DecompressSafe(dst, truncated)
		if err == nil && n == len(data) && bytes.Equal(dst[:n], data) {
			t.Fatalf("truncated input (cut last %d bytes) decompressed successfully to the correct output; expected an error or a detectably wrong result", cut)
		}
	}
}

// TestDecompressSafeRoundTripsAllCases is a lighter-weight rerun of the
// round-trip check across many random sizes, to catch off-by-one bugs in
// literal/match length continuation logic that fixed-size cases might miss.
func TestDecompressSafeRoundTripsRandomSizes(t *testing.T) {
	sizes := []int{0, 1, 2, 3, 15, 16, 17, 31, 32, 33, 255, 256, 257, 511, 512, 513, 1000, 4096}
	for _, size := range sizes {
		if size == 0 {
			continue // LZO requires non-empty input
		}
		t.Run("", func(t *testing.T) {
			data := randomBytes(t, size, int64(size)+100)
			// Bias toward compressibility for some sizes by zeroing half the bytes.
			for i := 0; i < len(data)/2; i++ {
				data[i] = 0
			}

			compressed := compressWithReference(t, data)
			dst := make([]byte, len(data))
			n, err := DecompressSafe(dst, compressed)
			if err != nil {
				t.Fatalf("size=%d DecompressSafe: %v", size, err)
			}
			if !bytes.Equal(dst[:n], data) {
				t.Fatalf("size=%d output mismatch", size)
			}
		})
	}
}
