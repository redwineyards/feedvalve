// The rest of gate G-DIFF-1: the framing-edge cases, 7340's structural
// identity, and the coverage ledger that keeps the gate honest about what it
// does NOT cover.
package app

import (
	"encoding/binary"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"feedvalve/internal/decode"
	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// TestG_DIFF_1_7340IsStructurallyIdenticalTo7305 is 7340's oracle.
//
// poc does not decode 7340 -- it dispatches 7208 and 7305 and nothing else --
// so there is no poc trace to diff against. Vol 1 section 8.6 item 3 names the
// fallback: 7340 carries MS_SECURITY_UPDATE_INFO exactly as 7305 does (bible
// section 4.3), so "reuse the 7305 fixture with the code changed, and assert
// byte-identical field streams".
//
// Setup: the same 302-byte payload twice, once declaring 7305 and once 7340.
// Stimulus: both through the real receive path. Assertion: the two field
// streams are identical except for the TransactionCode field itself.
//
// Would have caught: 7340 resolving to a different decoder, a different body
// length, or the same decoder with a different ColSpec -- any of which would
// write a security master row from the wrong offsets while 7305 stayed correct,
// so every test that only exercises 7305 would still pass.
func TestG_DIFF_1_7340IsStructurallyIdenticalTo7305(t *testing.T) {
	a := ourTrace(t, []int{7305, 7340}, []int{7305, 7340},
		uncompressedDiffSub(sec7305DiffPayload(t, 7305)))
	b := ourTrace(t, []int{7305, 7340}, []int{7305, 7340},
		uncompressedDiffSub(sec7305DiffPayload(t, 7340)))

	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("expected one sub-packet each, got %d and %d", len(a), len(b))
	}
	s305, s340 := a[0], b[0]
	if len(s305) != len(s340) {
		t.Fatalf("7305 traced %d fields and 7340 traced %d; the two carry the same "+
			"structure, so the streams must have the same shape", len(s305), len(s340))
	}
	for i := range s305 {
		x, y := s305[i], s340[i]
		if x.name != y.name || x.lo != y.lo || x.hi != y.hi {
			t.Fatalf("field %d differs in identity: 7305 has %s, 7340 has %s", i, x, y)
		}
		// The transaction code is the one byte pair that SHOULD differ.
		if x.name == "TransactionCode" {
			if x.value == y.value {
				t.Errorf("7305 and 7340 report the same TransactionCode %s; the fixture "+
					"did not actually change the code", x.value)
			}
			continue
		}
		if x.hex != y.hex || x.value != y.value {
			t.Errorf("field %s at %d-%d differs between 7305 and 7340: %s/%s vs %s/%s",
				x.name, x.lo, x.hi, x.hex, x.value, y.hex, y.value)
		}
	}
}

// TestG_DIFF_1_AShortMessageLengthDecodesNoBodyOnEitherSide is section 8.6's
// "a MessageLength of 39" case.
//
// 39 is below the 40-byte broadcast header, so the declared length cannot even
// cover the header it is declared in. Both implementations must refuse to parse
// a body.
//
// THE ASSERTION IS DELIBERATELY NARROWER than stream equality, and the reason
// belongs in the test rather than in a commit message: poc reads the header
// during its length lookup BEFORE rejecting the sub-packet, so the two sides
// legitimately differ in whether a header stream appears at all. The real
// property is that neither fabricates a body from a length that cannot be
// true; asserting full equality here would be asserting a coincidence of
// rejection order, which would break the next time either side reordered a
// check that is otherwise free to move.
func TestG_DIFF_1_AShortMessageLengthDecodesNoBodyOnEitherSide(t *testing.T) {
	// 7208's declared length replaced by 39, payload otherwise intact.
	p := mbp7208DiffPayload(t, 2, false)
	binary.BigEndian.PutUint16(p[absMessageLength:], 39)

	ours := ourTrace(t, []int{7208}, []int{7208}, uncompressedDiffSub(p))
	for _, pass := range ours {
		for _, s := range pass {
			if s.lo >= absHeaderEnd {
				t.Errorf("this design traced a body field %s at %d-%d from a message "+
					"declaring MessageLength=39, which cannot even cover its own 40-byte header",
					s.name, s.lo, s.hi)
			}
		}
	}

	raw := pocRawTrace(t, 8, envelopeOf(uncompressedDiffSub(p)))
	for _, s := range parseSpans(t, fieldLinesFrom(raw)) {
		if s.lo >= absHeaderEnd {
			t.Errorf("poc traced a body field %s at %d-%d from MessageLength=39; "+
				"the oracle itself disagrees with the premise of this case",
				s.name, s.lo, s.hi)
		}
	}
}

// TestG_DIFF_1_AThreeSubPacketBufferAgrees is section 8.6's "section 4.6's
// three-sub-packet buffer exercising both header kinds".
//
// The buffer is a compressed 7305, an uncompressed BCAST code with no decoder,
// and an uncompressed 6541 heartbeat -- 4 + 310 + 50 + 50 = 414 bytes, inside
// the 512-byte cPackData.
//
// THE HEARTBEAT IS NOT COMPARED, and this is the honest statement of a real
// coverage limit rather than a convenience. 6541 and 6503 resolve to a
// MESSAGE_HEADER (Vol 1 section 6.4, gate G-HDR-2), and poc HAS NO HEADER-KIND
// RESOLUTION AT ALL: it always parses the BCAST layout, so for those two codes
// it reads TraderId and a nanosecond Timestamp as reserved fields and a
// sequence number. The two field streams differ by design, poc is simply wrong
// about them, and pretending otherwise would be the "harness that silently
// skips the un-oracled codes and reads as full coverage" that section 8.6
// warns about. Their oracle is G-HDR-2 plus the hand-built offset tables in
// internal/decode, named in the ledger below.
//
// What IS asserted for the heartbeat is the thing that matters here: that it
// does not disturb the framing of the sub-packets around it.
func TestG_DIFF_1_AThreeSubPacketBufferAgrees(t *testing.T) {
	subs := [][]byte{
		compressedDiffSub(t, sec7305DiffPayload(t, 7305)),
		uncompressedDiffSub(unknownBcastDiffPayload()),
		uncompressedDiffSub(heartbeatDiffPayload()),
	}
	raw := envelopeOf(subs...)
	if len(raw) > 4+wire.MaxPackedDataLen {
		t.Fatalf("the three-sub-packet buffer is %d bytes, over the envelope", len(raw))
	}

	theirs := pocTrace(t, 8, raw)
	ours := ourTrace(t, []int{7305}, []int{7305, diffUnknownBcastCode, 6541}, subs...)

	if len(ours) != 3 || len(theirs) != 3 {
		t.Fatalf("this design traced %d sub-packets and poc traced %d; both must frame "+
			"three. A count mismatch means one of them misframed the envelope, after "+
			"which every later sub-packet is at the wrong offset.", len(ours), len(theirs))
	}

	// The two BCAST sub-packets are fully compared.
	assertStreamsAgree(t, "subpacket_1_compressed_7305", ours[0], theirs[0])
	assertStreamsAgree(t, "subpacket_2_unknown_bcast", ours[1], theirs[1])

	// The heartbeat: this design must have resolved it as a MESSAGE_HEADER,
	// which is visible in the trace as the transaction code sitting at payload
	// offset 8 rather than 18.
	var sawCodeAt8 bool
	for _, s := range ours[2] {
		if s.name == "TransactionCode" && s.lo == absTransCodeMsg {
			sawCodeAt8 = true
		}
	}
	if !sawCodeAt8 {
		t.Errorf("the third sub-packet's TransactionCode was not read at payload offset 8.\n"+
			"6541 is a MESSAGE_HEADER, whose code sits at header offset 0; reading it at "+
			"18 means the kind was resolved as a BCAST_HEADER and the whole header is "+
			"misread. Trace:\n%s", spansString(ours[2]))
	}
}

// TestG_DIFF_1_AnOversizeDatagramAgreesOnWhatFits is section 8.6's "a datagram
// of 517 bytes".
//
// cPackData is a fixed 512-byte field, so nothing past offset 4+512 can be
// framed. internal/framing trims rather than rejects -- the sub-packets inside
// those 512 bytes are still well-framed and still real data on a circuit with
// no retransmission -- and poc warns and carries on. The assertion is that both
// agree on the part that fits.
func TestG_DIFF_1_AnOversizeDatagramAgreesOnWhatFits(t *testing.T) {
	subs := [][]byte{uncompressedDiffSub(sec7305DiffPayload(t, 7305))}
	raw := envelopeOf(subs...)

	// Pad to 517 with trailing bytes that are not a valid sub-packet, so the
	// trim is what makes the datagram parse.
	padded := make([]byte, 517)
	copy(padded, raw)
	for i := len(raw); i < len(padded); i++ {
		padded[i] = 0xA5
	}
	if len(raw) >= len(padded) {
		t.Fatalf("the 7305 datagram is already %d bytes; this case needs it under 517", len(raw))
	}

	theirs := pocTrace(t, 8, padded)
	// The internal side gets the same bytes through the same framing path.
	var d model.Datagram
	copy(d.Buf[:], padded)
	d.N = int32(len(padded))
	f := newDiffFixture(t, []int{7305}, []int{7305})
	if err := f.r.decodeDatagram(&d); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}
	ours := splitPasses(parseSpans(t, f.traceLines(t)))

	if len(ours) == 0 || len(theirs) == 0 {
		t.Fatalf("a 517-byte datagram produced %d sub-packets here and %d from poc; the "+
			"first sub-packet is wholly inside the 512-byte cPackData and must still be "+
			"framed", len(ours), len(theirs))
	}
	assertStreamsAgree(t, "subpacket_1_of_an_oversize_datagram", ours[0], theirs[0])
}

func spansString(spans []span) string {
	var b strings.Builder
	for _, s := range spans {
		b.WriteString("  ")
		b.WriteString(s.String())
		b.WriteString("\n")
	}
	return b.String()
}

// oracleKind is how one transcode's field layout is validated.
type oracleKind int

const (
	// oraclePoc: diffed field-by-field against poc's -debug trace by
	// TestG_DIFF_1_FieldStreamsAgreeWithPoc.
	oraclePoc oracleKind = iota
	// oracleStructural: no poc oracle, but byte-identical to a code that has
	// one, asserted here.
	oracleStructural
	// oracleOffsetTable: no poc oracle at all. The oracle is the bible's
	// Part V arithmetic plus an independently written absolute-offset table,
	// per ADR-012's own revisit clause and gate G-FIX-1.
	oracleOffsetTable
)

// diffOracles is the coverage ledger: every decoding transcode against how its
// layout is actually validated.
//
// WHY THIS IS A TEST AND NOT A COMMENT. Vol 1 section 8.6 item 3 says poc "is
// an oracle for two codes only ... and the harness must not pretend otherwise",
// and Vol 1 section 15.1's delta V-D2 records that the HLD's wording hides this
// gap: "a harness that silently skips the un-oracled codes would read as full
// coverage". Seven of the nine decoding codes have no poc oracle. A comment
// saying so rots; a test fails when a tenth decoder arrives without declaring
// how it is checked.
var diffOracles = map[model.TransCode]struct {
	kind  oracleKind
	where string
}{
	7208: {oraclePoc, "TestG_DIFF_1_FieldStreamsAgreeWithPoc"},
	7305: {oraclePoc, "TestG_DIFF_1_FieldStreamsAgreeWithPoc"},
	7340: {oracleStructural, "TestG_DIFF_1_7340IsStructurallyIdenticalTo7305"},

	// Bible section 4.7 plus Appendix B item 2, with its own pad at record
	// offset 17 and a 10-byte ladder re-packed into the 12-byte layout
	// (ADR-L08) -- so it is NOT structurally identical to 7208 and cannot
	// borrow its oracle.
	7200: {oracleOffsetTable, "internal/decode.TestG_FIX_1_MBOFieldsMatchTheHandWrittenOffsets"},

	// Bible section 4.5 plus Appendix B item 7: 320 bytes with a TRAILING pad
	// at 319, and all five codes share one layout.
	6511: {oracleOffsetTable, "internal/decode.TestG_FIX_1_VctFieldsMatchTheHandWrittenOffsets"},
	6521: {oracleOffsetTable, "internal/decode.TestG_FIX_1_VctFieldsMatchTheHandWrittenOffsets"},
	6522: {oracleOffsetTable, "internal/decode.TestG_FIX_1_VctFieldsMatchTheHandWrittenOffsets"},
	6531: {oracleOffsetTable, "internal/decode.TestG_FIX_1_VctFieldsMatchTheHandWrittenOffsets"},
	6571: {oracleOffsetTable, "internal/decode.TestG_FIX_1_VctFieldsMatchTheHandWrittenOffsets"},
}

// TestG_DIFF_1_EveryDecodingTranscodeDeclaresAnOracle is the ledger assertion.
//
// Setup: the real catalogue. Stimulus: none -- this is a structural check.
// Assertion: every catalogued code has a ledger row, every row names a real
// code, and the only codes claiming a poc oracle are the two poc actually
// dispatches.
//
// Would have caught: a tenth decoder landing with no validation at all. The
// Phase 2 exit checklist asserts "the nine decoders are the only nine" and that
// G-DIFF-1 is green "for every supported transcode"; without this, a new
// decoder makes the second claim quietly false while every existing test still
// passes.
func TestG_DIFF_1_EveryDecodingTranscodeDeclaresAnOracle(t *testing.T) {
	catalogued := decode.Catalogue()
	if len(catalogued) == 0 {
		t.Fatal("the catalogue is empty; this ledger would pass vacuously")
	}

	for _, code := range catalogued {
		row, ok := diffOracles[code]
		if !ok {
			t.Errorf("transcode %d is in the catalogue but has no row in diffOracles.\n"+
				"Every decoding code must declare how its layout is validated: against poc's "+
				"trace, by structural identity with a code that is, or by an independently "+
				"written absolute-offset table. Adding a decoder without declaring one is how "+
				"\"G-DIFF-1 green for every supported transcode\" becomes quietly untrue.", code)
			continue
		}
		if row.where == "" {
			t.Errorf("transcode %d declares an oracle with no test named", code)
		}
	}

	for code := range diffOracles {
		if !decode.CanDecode(code) {
			t.Errorf("diffOracles names transcode %d, which the catalogue cannot decode.\n"+
				"A stale row makes the ledger look more complete than it is.", code)
		}
	}

	// The poc oracle covers exactly 7208 and 7305 (poc/gobrdclnt.go dispatches
	// those two and nothing else). Pinned so that a hopeful row claiming a poc
	// oracle for a third code fails here rather than in a subprocess diff
	// nobody reads.
	var viaPoc []int
	for code, row := range diffOracles {
		if row.kind == oraclePoc {
			viaPoc = append(viaPoc, int(code))
		}
	}
	if len(viaPoc) != 2 {
		t.Errorf("%d codes claim a poc oracle (%v); poc decodes exactly 7208 and 7305",
			len(viaPoc), viaPoc)
	}
	for _, code := range viaPoc {
		if code != 7208 && code != 7305 {
			t.Errorf("transcode %d claims a poc oracle, but poc does not dispatch it; "+
				"its trace would be a bare header and the diff would compare nothing", code)
		}
	}
}

// TestG_DIFF_1_TheLedgerNamesTestsThatExist keeps the ledger's references from
// rotting.
//
// A row pointing at a renamed or deleted test is worse than no row: it reads as
// coverage. The decode-package tests are named rather than called -- app must
// not import decode's test code, and the reference is checked by scanning the
// source, which is what the source-scan gates in this package already do.
func TestG_DIFF_1_TheLedgerNamesTestsThatExist(t *testing.T) {
	root := findDiffModuleRoot()
	if root == "" {
		t.Skip("no source tree: this check reads the module's own test sources, which " +
			"the cross-compiled test binary does not have on the LPAR")
	}

	for code, row := range diffOracles {
		name := row.where
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = name[i+1:]
		}
		if !testNameExistsInTree(t, root, name) {
			t.Errorf("transcode %d's oracle names %q, which no test in the tree declares.\n"+
				"A ledger row pointing at a test that does not exist reads as coverage.",
				code, row.where)
		}
	}
}

// testNameExistsInTree reports whether any _test.go file under root declares a
// function with this name.
//
// A source scan rather than a call, because package app must not import
// package decode's test code -- and could not, since test files are not
// importable. The same technique the scan gates in this package already use.
func testNameExistsInTree(t *testing.T, root, name string) bool {
	t.Helper()

	needle := "func " + name + "("
	found := false
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Skip the worktree directory, which holds copies of the tree and
			// would let a stale copy satisfy the ledger.
			if d.Name() == ".git" || d.Name() == ".claude" {
				return fs.SkipDir
			}
			return nil
		}
		if found || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), needle) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scanning %s for %q: %v", root, name, err)
	}
	return found
}
