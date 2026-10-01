// Package lzo is a pure Go port of LZO1Z decompression, carried over from
// poc/decompress.go and poc/errors.go.
//
// lzo MUST NOT import anything outside the standard library.
//
// DecompressSafe is bounds-checked and returns an error on malformed input; it
// is what the receive path uses, because compressed sub-packets are untrusted
// network input. Decompress is the unchecked variant and may panic on malformed
// input -- it is for tests, where the fixtures are known good.
//
// This package decompresses. It does not compress, and it must stay that way:
// github.com/rhnvrm/lzo1z is a test-only dependency used as an independent
// reference compressor to build fixtures, and importing it from non-test code
// would add an unaudited dependency to the binary for no benefit. Gate G-DEP-3
// asserts it is absent from the non-test build.
//
// The port exists so the binary needs no cgo and no liblzo2, which is what keeps
// CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 working.
//
// Specified by docs/lld/01-receive-decode.md. Built in Phase 2.
package lzo
