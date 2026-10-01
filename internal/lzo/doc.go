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
// This package decompresses. It does not compress, and it must stay that way.
//
// The compressed bytes the tests decompress come from an independent LZO1Z
// implementation and are frozen as recorded fixtures, not produced at test
// time -- see poc/testdata/lzo_fixtures.json and the procedure at the top of
// poc/decompress_test.go. The module therefore has no compression dependency
// at all. Gate G-DEP-3 keeps it that way by asserting github.com/rhnvrm/lzo1z
// is absent from the non-test build; it passes trivially today, which is the
// point -- it is what stops the dependency coming back.
//
// Writing a compressor of our own would be worse than either. A decompressor
// validated only against a compressor from the same codebase agrees with
// itself even when both are wrong.
//
// The port exists so the binary needs no cgo and no liblzo2, which is what keeps
// CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 working.
//
// Specified by docs/lld/01-receive-decode.md. Built in Phase 2.
package lzo
