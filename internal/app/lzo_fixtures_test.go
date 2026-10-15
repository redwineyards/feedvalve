package app

// The frozen LZO oracle, reached from this package because gate G-ALLOC-1's
// fixture must carry a genuinely compressed sub-packet (Vol 1 section 11.4) and
// THERE IS NO COMPRESSOR IN THIS MODULE -- deliberately, and gate G-DEP-3
// (deps_gates_test.go) asserts it stays out.
//
// Same file, same hash-keyed lookup and same failure mode as
// internal/lzo/fixtures_test.go, and pointing at the one shared copy rather
// than taking a second one is the point: two copies of a frozen oracle are two
// things that can silently disagree, and an entry recorded into one and not the
// other would leave two suites proving different claims under the same names.
//
// Adding a case with a genuinely NEW input is the only thing that needs the
// recorder; such a case fails below with its own length and hash rather than
// passing silently. The procedure lives with the fixture file -- see the header
// of poc/decompress_test.go.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// lzoFixtureFile is the shared frozen set. poc stays in the tree through P2-10,
// whose differential validation diffs against its -debug trace, so this path
// does not dangle.
const lzoFixtureFile = "../../poc/testdata/lzo_fixtures.json"

// lzoInputZeros512 is the fixture input this package uses: 512 zero bytes.
//
// Chosen for its COMPRESSED size. Packed data is capped at 512 bytes
// (cPackData[512], bible section 3.2) and the real 7208 consumes 480 of them
// with its length prefix, so a two-sub-packet datagram leaves 32 bytes for the
// compressed one. This input compresses to 13, which fits; most of the frozen
// set does not.
var lzoInputZeros512 = make([]byte, 512)

// frozenLZOStream returns the recorded LZO1Z-compressed form of data.
func frozenLZOStream(t *testing.T, data []byte) []byte {
	t.Helper()

	raw, err := os.ReadFile(filepath.FromSlash(lzoFixtureFile))
	if err != nil {
		t.Fatalf("reading the frozen LZO fixtures at %s: %v", lzoFixtureFile, err)
	}
	var recorded map[string]struct {
		InputLen   int    `json:"inputLen"`
		Compressed string `json:"compressed"`
	}
	if err := json.Unmarshal(raw, &recorded); err != nil {
		t.Fatalf("decoding %s: %v", lzoFixtureFile, err)
	}

	sum := sha256.Sum256(data)
	key := hex.EncodeToString(sum[:])
	f, ok := recorded[key]
	if !ok {
		t.Fatalf("no recorded LZO fixture for this input (%d bytes, sha256 %s).\n"+
			"Fixtures are frozen in %s; see the regeneration procedure at the top of\n"+
			"poc/decompress_test.go. The input must be deterministic to be recordable.",
			len(data), key, lzoFixtureFile)
	}
	stream, err := base64.StdEncoding.DecodeString(f.Compressed)
	if err != nil {
		t.Fatalf("decoding the recorded stream for %s: %v", key, err)
	}
	return stream
}
