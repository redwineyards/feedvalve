// Gate G-DIFF-1 — differential validation against poc's -debug trace.
//
// THE GATE AGAINST THE WORST FAILURE IN THE SYSTEM (HLD R1): plausible values
// in the wrong fields, silent in Oracle until somebody reconciles a position
// months later. It works because poc is an INDEPENDENT implementation of the
// same specification, so the oracle is its output.
//
// # How the two streams are compared, and why not line by line
//
// The two implementations partition the same bytes differently, and correctly.
// poc decodes every field for display; this decoder COPIES most of them,
// because Vol 0 section 4.1 R1 keeps the row image in wire form. So poc reads
// 7208's ten-level ladder as forty fields where this decoder takes one 120-byte
// run, and reads ST_INDICATOR as two bytes where this decoder takes one
// two-byte skip. A line-by-line diff reports forty missing fields on a decoder
// that is right.
//
// The comparison is therefore on BYTE INTERVALS, in five assertions. The first
// two need no table at all and are the ones that catch R1:
//
//  1. Contiguity and extent. Each side's spans tile the payload with no gap
//     and no overlap, over an identical range. A wrong stride, a dropped
//     field or a missed pad byte lands here -- this is how a 213-byte stride
//     is caught.
//  2. Byte agreement. The concatenated hex is identical: both read the same
//     bytes in the same order.
//  3. Group alignment. Walk both span lists until their boundaries coincide;
//     per-group hex must match. One-to-one for most fields, one-to-forty for
//     the ladder.
//  4. Value self-consistency. Every scalar a side prints must re-encode, big
//     endian, to that side's own hex at that span's width. This is what makes
//     "decoded value" a real assertion without a hand-written type table that
//     could itself be wrong: a value that round-trips to the same bytes IS
//     the same value, so 2 and 4 together give value agreement.
//  5. Names, through normalisation plus an explicit exceptions table. Every
//     group must be accounted for; an unrecognised pairing FAILS rather than
//     being skipped.
//
// On 4, stated plainly because it is the one place a reader should not be left
// to assume: for a copy-based decoder, hex equality at the right offsets IS
// value equality -- there is no conversion step that could go wrong. Assertion
// 4 earns its place on the fields this decoder genuinely decodes (Token,
// NoOfRecords, the header scalars), where a signedness or endianness error
// would otherwise be invisible, and on poc's side it checks the ORACLE's own
// consistency.
//
// # Reconciling the two volumes, because they disagree
//
// Vol 4's G-DIFF-1 row asserts "identical FIELD NAME, absolute offset range,
// hex and decoded value". Vol 1 section 8.6 item 1 says to diff on
// (offset, hex, value) and explicitly NOT on names, because the rename to the
// bible's spelling is deliberate -- poc prints header.sequenceNumber where this
// design prints BCSeqNo (Vol 0 section 10.5, wire.TestFieldNamesKeepTheBibleSpelling).
//
// Vol 1 is the owning volume: Vol 4 section 7 assigns G-DIFF-1 to Volume 1. So
// the method is Vol 1's, and assertion 5 is how "identical field name" becomes
// a true claim rather than a false one -- names agree THROUGH the table, which
// a reviewer reads. Recorded here rather than silently resolved, because an
// unreconciled conflict between two volumes is how a later reviewer concludes
// the gate is weaker than it is.
package app

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

// padTraceValue is the token both implementations print for an alignment pad.
// Carried verbatim in wire/reader.go's padValue from poc/reader.go:52 so this
// harness sees the same string on both sides.
const padTraceValue = "(alignment padding, ignored)"

// pocPrefixField is the one line poc emits that this design deliberately does
// not.
//
// poc's parseNetIDPrefix logs the 8-byte prefix as a field, with its OWN format
// string and a trailing parenthetical no other line has. This design skips the
// prefix in exactly one place and gives it no line of its own
// (decode/header.go), which is why both streams are compared from offset 8.
// Enumerated rather than filtered loosely, so the exemption cannot quietly
// absorb a real missing field.
const pocPrefixField = "broadcastData.netIdPrefix"

// diffRenames maps a NORMALISED poc field name to this design's name, for the
// fields whose names do not simply correspond.
//
// Vol 1 section 8.6 item 1 requires this table to be explicit and readable:
// "inferring the mapping would make the harness agree with itself". Everything
// not listed here must match after normalisation (see normaliseFieldName),
// which strips poc's struct-path prefixes and array indices and lowercases --
// a rule stated once and applied mechanically, so the table holds only the
// genuine renames.
var diffRenames = map[string]string{
	// Vol 0 section 10.5: protocol identifiers keep the BIBLE's spelling so a
	// grep finds both. These are the three that differ as a result.
	//
	// Note which ones are NOT here: NetPriceChangeFromClosingPrice matches
	// outright, because the FieldID constant is abbreviated
	// (wire.FieldNetPriceChange) while the NAME in the table is the bible's
	// full spelling -- which is the rule working as intended.
	"sequencenumber": "BCSeqNo",
	"recordcount":    "NoOfRecords",
	"alignmentpad":   "pad",

	// 7305's single Reserved byte at payload 179. This design reuses
	// FieldReserved1 for it, so the trace says Reserved1 where the bible and
	// poc say Reserved. Cosmetic -- the offset disambiguates -- but recorded
	// rather than normalised away, because a name that appears twice in one
	// stream is worth a reader knowing about.
	"reserved": "Reserved1",
}

// diffGroupStems are the fields one implementation reads as a SINGLE run where
// poc reads elements, keyed by this design's name.
//
// Each is a deliberate decision, not an accident of style: a group column is
// copied in one move because the destination stores the whole run (Vol 0
// section 4.3), and splitting it would buy nothing but forty reader calls on
// the hot path.
var diffGroupStems = map[string]string{
	"MBP_INFORMATION":               "recordbuffer",          // 7208: 10 levels x 4 fields
	"ST_MBO_INFO":                   "mbobuffer",             // 7200's order-level ladder
	"ST_SEC_ELIGIBILITY_PER_MARKET": "eligibilitypermarket",  // 7305: 4 x {flags, status}
	"ST_ELIGIBILITY_INDICATORS":     "eligibilityindicators", // 7305: flags + reserved
	"ST_PURPOSE":                    "purpose",               // 7305: flags1 + flags2
	"ST_INDICATOR":                  "indicator",             // 7208: flags + reserved
}

// normaliseFieldName reduces a poc field name to a comparable stem.
//
// Three transformations, each covering something real in poc's output:
//
//   - array indices are dropped, so recordBuffer[3] and eligibilityPerMarket[0]
//     reduce to their stems;
//   - everything up to the last '.' is dropped, which removes header.,
//     securityInfo., record[0]. and the DOUBLE DOT in
//     record[0]..recordBuffer[3].quantity. That double dot is a cosmetic defect
//     in poc (its field("") helper yields "record[0]." and the format string
//     adds another), and since this harness must not require modifying poc, it
//     is normalised here;
//   - the result is lowercased, because this design capitalises
//     (LastTradedPrice) where poc does not (lastTradedPrice).
func normaliseFieldName(name string) string {
	var b strings.Builder
	depth := 0
	for _, r := range name {
		switch r {
		case '[':
			depth++
		case ']':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 {
				b.WriteRune(r)
			}
		}
	}
	s := b.String()
	if i := strings.LastIndex(s, "."); i >= 0 {
		s = s[i+1:]
	}
	return strings.ToLower(s)
}

// group is one aligned run: the spans each side read over an identical byte
// range.
type group struct {
	lo, hi int
	ours   []span
	theirs []span
}

// alignSpans pairs the two streams into groups covering identical byte ranges.
//
// Both lists must already be contiguous and cover the same extent (assertion 1
// checks that separately and with a better message). The walk accumulates from
// whichever side is behind until the two high-water marks meet, which is what
// turns one 120-byte Raw against forty poc fields into a single group.
func alignSpans(t *testing.T, ours, theirs []span) []group {
	t.Helper()

	var out []group
	i, j := 0, 0
	for i < len(ours) && j < len(theirs) {
		g := group{lo: ours[i].lo}
		oi, tj := i, j
		oHi, tHi := ours[i].hi, theirs[j].hi
		i++
		j++
		for oHi != tHi {
			if oHi < tHi {
				if i >= len(ours) {
					t.Fatalf("ran out of our spans aligning the group at %d (ours reach %d, theirs %d)",
						g.lo, oHi, tHi)
				}
				oHi = ours[i].hi
				i++
				continue
			}
			if j >= len(theirs) {
				t.Fatalf("ran out of poc spans aligning the group at %d (ours reach %d, theirs %d)",
					g.lo, oHi, tHi)
			}
			tHi = theirs[j].hi
			j++
		}
		g.hi = oHi
		g.ours = ours[oi:i]
		g.theirs = theirs[tj:j]
		out = append(out, g)
	}
	if i != len(ours) || j != len(theirs) {
		t.Fatalf("alignment ended with %d of our spans and %d poc spans unconsumed",
			len(ours)-i, len(theirs)-j)
	}
	return out
}

// assertContiguous is assertion 1 for one stream.
func assertContiguous(t *testing.T, who string, spans []span) {
	t.Helper()
	if len(spans) == 0 {
		t.Fatalf("%s produced no field spans at all", who)
	}
	for k, s := range spans {
		if s.hi <= s.lo {
			t.Errorf("%s span %d (%s) is empty or inverted", who, k, s)
		}
		if k == 0 {
			continue
		}
		if prev := spans[k-1]; s.lo != prev.hi {
			t.Errorf("%s is not contiguous between %s and %s: a %d-byte %s at offset %d.\n"+
				"A gap means a field was never read; an overlap means two fields claim the "+
				"same bytes. Either way the fields after it are at the wrong offsets, which "+
				"is the defect this gate exists for.",
				who, prev.name, s.name,
				abs(s.lo-prev.hi), gapOrOverlap(s.lo, prev.hi), prev.hi)
		}
	}
}

func gapOrOverlap(lo, prevHi int) string {
	if lo > prevHi {
		return "gap"
	}
	return "overlap"
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func concatHex(spans []span) string {
	var b strings.Builder
	for _, s := range spans {
		b.WriteString(s.hex)
	}
	return b.String()
}

// assertValueMatchesItsOwnBytes is assertion 4.
//
// A span's printed value must be consistent with the bytes that span covers. A
// byte-slice rendering ("[255 30]") and the pad token are accepted as-is; a
// scalar must re-encode big endian, at the span's own width, to the span's own
// hex.
//
// Floats go through ParseFloat and Float64bits: Go's %v prints the shortest
// decimal that round-trips, so the original bits are recoverable exactly. That
// matters for 7305's IssuedCapital, a DOUBLE, where a sign-extended
// intermediate is precisely the kind of conversion that turns one value into a
// different plausible one.
func assertValueMatchesItsOwnBytes(t *testing.T, who string, s span) {
	t.Helper()

	switch {
	case s.value == padTraceValue:
		return
	case strings.HasPrefix(s.value, "["):
		// The raw bytes, printed by %v on a []byte. Consistent with the hex by
		// construction, since both come from the same slice.
		return
	}

	width := s.hi - s.lo
	if strings.ContainsAny(s.value, ".eE") && width == 8 {
		f, err := strconv.ParseFloat(s.value, 64)
		if err != nil {
			t.Errorf("%s: %s has a float-shaped value %q that will not parse: %v", who, s.name, s.value, err)
			return
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(f))
		if got := fmt.Sprintf("%x", b); got != s.hex {
			t.Errorf("%s: %s prints %s, which re-encodes to %s, but its bytes are %s.\n"+
				"The decoded DOUBLE does not match the bytes at offset %d-%d.",
				who, s.name, s.value, got, s.hex, s.lo, s.hi)
		}
		return
	}

	n, err := strconv.ParseInt(s.value, 10, 64)
	if err != nil {
		t.Errorf("%s: %s has value %q, which is neither a byte slice, the pad token, "+
			"a float nor an integer. The harness cannot check it against its bytes, and "+
			"an unrecognised rendering must fail rather than be skipped.", who, s.name, s.value)
		return
	}

	var got string
	switch width {
	case 1:
		got = fmt.Sprintf("%x", [1]byte{byte(n)})
	case 2:
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], uint16(int16(n)))
		got = fmt.Sprintf("%x", b)
	case 4:
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(int32(n)))
		got = fmt.Sprintf("%x", b)
	case 8:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		got = fmt.Sprintf("%x", b)
	default:
		t.Errorf("%s: %s prints a scalar %q over %d bytes, which is not a wire width",
			who, s.name, s.value, width)
		return
	}
	if got != s.hex {
		t.Errorf("%s: %s at offset %d-%d prints %s, which re-encodes big endian to %s, "+
			"but its bytes are %s.\n"+
			"The decoded value disagrees with the bytes it came from -- a signedness or "+
			"endianness error, which is invisible in every other assertion.",
			who, s.name, s.lo, s.hi, s.value, got, s.hex)
	}
}

// assertNamesAgree is assertion 5.
func assertNamesAgree(t *testing.T, g group) {
	t.Helper()

	if len(g.ours) != 1 {
		// poc reading a run this design splits. Does not occur in the current
		// inventory; failing is better than inventing a rule for it.
		var names []string
		for _, s := range g.ours {
			names = append(names, s.name)
		}
		t.Errorf("offset %d-%d: this design split a run poc reads whole (%s against %d poc fields). "+
			"The name table has no rule for that direction.", g.lo, g.hi, strings.Join(names, ", "), len(g.theirs))
		return
	}
	ours := g.ours[0].name

	// A grouped run: every poc name in it must share the declared stem.
	if stem, ok := diffGroupStems[ours]; ok {
		for _, s := range g.theirs {
			if !strings.Contains(normaliseGroupStem(s.name), stem) {
				t.Errorf("offset %d-%d: %s is declared as the group for poc's %q, but it covers "+
					"poc field %q, whose stem is %q",
					g.lo, g.hi, ours, stem, s.name, normaliseGroupStem(s.name))
			}
		}
		return
	}

	if len(g.theirs) != 1 {
		t.Errorf("offset %d-%d: %s covers %d poc fields but is not in diffGroupStems.\n"+
			"A one-to-many pairing must be DECLARED: an undeclared one is how a field that "+
			"stopped being read would look like a grouping choice.", g.lo, g.hi, ours, len(g.theirs))
		return
	}

	norm := normaliseFieldName(g.theirs[0].name)
	want := norm
	if r, ok := diffRenames[norm]; ok {
		want = strings.ToLower(r)
	}
	if strings.ToLower(ours) != want {
		t.Errorf("offset %d-%d: poc calls this %q (normalised %q) and this design calls it %q.\n"+
			"If the rename is deliberate, add it to diffRenames with the reason; "+
			"if not, one of the two is reading the wrong field.",
			g.lo, g.hi, g.theirs[0].name, norm, ours)
	}
}

// normaliseGroupStem keeps the path for a grouped field, where the stem is in
// the middle (record[0]..recordBuffer[3].quantity), rather than taking only the
// last segment.
func normaliseGroupStem(name string) string {
	var b strings.Builder
	depth := 0
	for _, r := range name {
		switch r {
		case '[':
			depth++
		case ']':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 {
				b.WriteRune(r)
			}
		}
	}
	return strings.ToLower(b.String())
}

// assertStreamsAgree runs all five assertions over one sub-packet.
func assertStreamsAgree(t *testing.T, label string, ours, theirs []span) {
	t.Helper()

	// The one enumerated exemption: poc's prefix line.
	theirs = dropPrefixSpan(t, theirs)

	t.Run(label, func(t *testing.T) {
		// 1 -- contiguity and extent.
		assertContiguous(t, "this design", ours)
		assertContiguous(t, "poc", theirs)
		if ours[0].lo != theirs[0].lo || ours[len(ours)-1].hi != theirs[len(theirs)-1].hi {
			t.Fatalf("the two streams cover different ranges: this design %d-%d, poc %d-%d.\n"+
				"Both must start at 8 (the broadcast header, after the prefix) and end at the "+
				"same byte, or they are not reading the same message.",
				ours[0].lo, ours[len(ours)-1].hi, theirs[0].lo, theirs[len(theirs)-1].hi)
		}

		// 2 -- byte agreement, the assertion that catches a wrong stride.
		if o, p := concatHex(ours), concatHex(theirs); o != p {
			t.Fatalf("the two streams read DIFFERENT BYTES over %d-%d.\n"+
				"this design: %s\npoc        : %s\n"+
				"%s", ours[0].lo, ours[len(ours)-1].hi, o, p, firstHexDivergence(ours, theirs))
		}

		// 3, 5 -- alignment, per-group hex, and names.
		for _, g := range alignSpans(t, ours, theirs) {
			if o, p := concatHex(g.ours), concatHex(g.theirs); o != p {
				t.Errorf("offset %d-%d: this design read %s, poc read %s", g.lo, g.hi, o, p)
			}
			assertNamesAgree(t, g)
		}

		// 4 -- each side's values against its own bytes.
		for _, s := range ours {
			assertValueMatchesItsOwnBytes(t, "this design", s)
		}
		for _, s := range theirs {
			assertValueMatchesItsOwnBytes(t, "poc", s)
		}
	})
}

// dropPrefixSpan removes poc's netIdPrefix line and asserts it was there.
//
// Asserted rather than assumed: if poc stopped emitting it, this exemption
// would silently start absorbing whatever span happened to be first.
func dropPrefixSpan(t *testing.T, theirs []span) []span {
	t.Helper()
	if len(theirs) == 0 {
		t.Fatal("poc produced no spans")
	}
	if theirs[0].name != pocPrefixField {
		t.Fatalf("poc's first span is %q, expected %q.\n"+
			"This harness exempts exactly one poc line -- the 8-byte prefix, which this "+
			"design deliberately does not trace. If poc's output changed, the exemption "+
			"must be rechecked rather than left to absorb a real field.",
			theirs[0].name, pocPrefixField)
	}
	return theirs[1:]
}

func firstHexDivergence(ours, theirs []span) string {
	o, p := concatHex(ours), concatHex(theirs)
	n := min(len(o), len(p))
	for i := 0; i < n; i++ {
		if o[i] != p[i] {
			// Two hex characters per byte, and the stream starts at the first
			// span's offset.
			return fmt.Sprintf("first divergence at payload offset %d", ours[0].lo+i/2)
		}
	}
	return fmt.Sprintf("one stream is a prefix of the other (%d vs %d hex characters)", len(o), len(p))
}

// ourTrace runs one datagram through the real receive path and returns its
// field stream per sub-packet.
func ourTrace(t *testing.T, configured, traced []int, subs ...[]byte) [][]span {
	t.Helper()
	f := newDiffFixture(t, configured, traced)
	if err := f.r.decodeDatagram(datagramOf(t, subs...)); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}
	return splitPasses(parseSpans(t, f.traceLines(t)))
}

// TestG_DIFF_1_FieldStreamsAgreeWithPoc is gate G-DIFF-1.
//
// SETUP: poc built and run as a subprocess with -debug; the real registry, the
// real decoders and the production trace renderer on this side; fabricated
// inputs whose every offset is a literal from the bible.
//
// STIMULUS: one datagram per case, covering Vol 1 section 8.6 item 4's required
// list -- NoOfRecords at 0, 1 and above the array bound; a record with a
// negative price; a compressed and an uncompressed sub-packet in one datagram;
// a three-sub-packet buffer; and an oversize datagram.
//
// ASSERTION: the two field streams agree on offset range, bytes and decoded
// value for every field, with names reconciled through diffRenames.
//
// WOULD HAVE CAUGHT: HLD R1. A 213-byte stride instead of 214 leaves every
// field before record offset 17 correct and turns everything after it into
// noise; nothing alarms, and the values stay plausible. Assertions 1 and 2
// fail on it immediately.
func TestG_DIFF_1_FieldStreamsAgreeWithPoc(t *testing.T) {
	// Built HERE, before any subtest, and not lazily inside one.
	//
	// The difference is not cosmetic. When the guard fires inside each subtest,
	// every subtest skips and Go reports the PARENT as PASS -- a gate that
	// asserted nothing reading as a gate that held. That is the exact failure
	// testenv_test.go was written against ("a suite of skips reports ok exactly
	// like a suite of passes"), and it would have been this gate's permanent
	// state on the LPAR, where -test.short is how the binaries are run.
	// Skipping at the top makes the whole test report SKIP.
	pocBinary(t)

	for _, tc := range []struct {
		name       string
		configured []int
		traced     []int
		subs       [][]byte
	}{{
		// NoOfRecords 0: the message is still 470 bytes and still has room for
		// two records, and neither side may read them.
		name:       "7208_no_records",
		configured: []int{7208},
		traced:     []int{7208},
		subs:       [][]byte{uncompressedDiffSub(mbp7208DiffPayload(t, 0, false))},
	}, {
		name:       "7208_one_record",
		configured: []int{7208},
		traced:     []int{7208},
		subs:       [][]byte{uncompressedDiffSub(mbp7208DiffPayload(t, 1, false))},
	}, {
		name:       "7208_two_records",
		configured: []int{7208},
		traced:     []int{7208},
		subs:       [][]byte{uncompressedDiffSub(mbp7208DiffPayload(t, 2, false))},
	}, {
		// Above the array bound of 2. Both implementations clamp and both must
		// clamp THE SAME WAY: parsing the third slot would decode the message's
		// trailing bytes into a security that looks entirely real downstream.
		name:       "7208_records_above_the_bound",
		configured: []int{7208},
		traced:     []int{7208},
		subs:       [][]byte{uncompressedDiffSub(mbp7208DiffPayload(t, 3, false))},
	}, {
		// bible section 2.4: prices may be negative. A LONG read as unsigned
		// gives a large positive number that looks plausible in a price column.
		name:       "7208_negative_prices",
		configured: []int{7208},
		traced:     []int{7208},
		subs:       [][]byte{uncompressedDiffSub(mbp7208DiffPayload(t, 2, true))},
	}, {
		// 294 exactly, no padding anywhere, no grouped runs beyond the three
		// bit-field structs -- so nearly every field is a 1:1 comparison.
		name:       "7305_security_update",
		configured: []int{7305},
		traced:     []int{7305},
		subs:       [][]byte{uncompressedDiffSub(sec7305DiffPayload(t, 7305))},
	}, {
		// A compressed and an uncompressed sub-packet in one datagram, with a
		// real decoder running on DECOMPRESSED bytes -- the case P2-09's
		// allocation gate explicitly left to this card.
		name:       "mixed_compression",
		configured: []int{7305},
		traced:     []int{7305, diffUnknownBcastCode},
		subs: [][]byte{
			compressedDiffSub(t, sec7305DiffPayload(t, 7305)),
			uncompressedDiffSub(unknownBcastDiffPayload()),
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			raw := envelopeOf(tc.subs...)
			theirs := pocTrace(t, 8, raw)
			ours := ourTrace(t, tc.configured, tc.traced, tc.subs...)

			if len(ours) != len(theirs) {
				t.Fatalf("this design traced %d sub-packets, poc traced %d.\n"+
					"A datagram's sub-packets are framed FIFO and both must see the same "+
					"number: a mismatch means one of them misframed the envelope, after "+
					"which every later sub-packet is at the wrong offset.",
					len(ours), len(theirs))
			}
			for i := range ours {
				assertStreamsAgree(t, fmt.Sprintf("subpacket_%d", i+1), ours[i], theirs[i])
			}
		})
	}
}
