package obs

import (
	"strings"
	"testing"
)

// TestEveryDimensionIsSizedAndLabelled is the invariant emission relies on:
// a non-dynamic dimension has exactly one pre-rendered label per slot, so
// rendering is an index rather than a format.
func TestEveryDimensionIsSizedAndLabelled(t *testing.T) {
	d := resolveDims(testOptions())
	for id := range numDims {
		got := d[id]
		if got.size == 0 {
			t.Errorf("dimension %d (%s) has no slots; every instrument that uses it "+
				"would be unreachable", id, got.brace)
			continue
		}
		if got.kind == dimDynamic {
			if got.labels != nil {
				t.Errorf("dimension %s is dynamic but carries pre-rendered labels; its "+
					"text comes from a runtime code", got.brace)
			}
			continue
		}
		if uint32(len(got.labels)) != got.size {
			t.Errorf("dimension %s has %d slots but %d labels. A mismatch means some "+
				"slot renders another slot's label against its own number, which reads "+
				"as real data", got.brace, got.size, len(got.labels))
		}
	}
}

// TestCartesianDimensionsAreRowMajor pins the offset arithmetic against the
// labels, which is the only thing that keeps the two consistent. If inner and
// the label order disagreed, dirty_depth{1,quote} would publish shard 0's
// third destination -- plausible, and wrong.
func TestCartesianDimensionsAreRowMajor(t *testing.T) {
	o := testOptions()
	d := resolveDims(o)

	for _, tc := range []struct {
		dim        Dim
		outer      int
		inner      int
		i, j       int
		wantSuffix string
	}{
		{dimShardDest, o.Shards, len(o.Destinations), 2, 1, "{2,md_security_master}"},
		{dimDestReason, len(o.Destinations), len(prevalidateReasons), 1, 3, "{md_security_master,time}"},
		{dimDestColumn, len(o.Destinations), maxColumns, 0, 2, "{md_token_quote,high_price}"},
	} {
		t.Run(dimSpecs[tc.dim].brace, func(t *testing.T) {
			got := d[tc.dim]
			if int(got.inner) != tc.inner {
				t.Fatalf("inner = %d, want %d", got.inner, tc.inner)
			}
			if int(got.size) != tc.outer*tc.inner {
				t.Fatalf("size = %d, want %d*%d", got.size, tc.outer, tc.inner)
			}
			off := tc.i*tc.inner + tc.j
			if label := got.labels[off]; label != tc.wantSuffix {
				t.Errorf("labels[%d] = %q, want %q: the offset arithmetic and the label "+
					"order must agree", off, label, tc.wantSuffix)
			}
		})
	}
}

// TestUndeclaredColumnsAreLabelledByIndex covers the rectangular
// {destination,column} layout.
//
// The slab reserves maxColumns per destination so the offset needs no
// per-destination base table, which leaves slots for columns no destination
// declares. Those are labelled by index rather than left blank, so an
// out-of-range caller produces a line that can be traced back instead of one
// that is malformed. They are sparse, so in practice they never print.
func TestUndeclaredColumnsAreLabelledByIndex(t *testing.T) {
	d := resolveDims(testOptions())
	// md_token_quote declares three columns in testOptions; slot 3 is beyond
	// them.
	if got := d[dimDestColumn].labels[3]; got != "{md_token_quote,col3}" {
		t.Errorf("labels[3] = %q, want {md_token_quote,col3}", got)
	}
}

// TestMarketTypeLabelsAreOneBased is worth its own test because getting it
// wrong is silent. Market types are 1 to 4 (bible Appendix A) and Vol 4 section
// 6.1's example shows session_state{1} for the first, while shard_depth_max{0}
// shows {k} is zero-based. The same code renders both, from a table rather than
// from arithmetic, which is exactly why.
func TestMarketTypeLabelsAreOneBased(t *testing.T) {
	d := resolveDims(testOptions())
	if got := d[dimMarketType].labels[0]; got != "{1}" {
		t.Errorf("the first market type renders %q, want {1}", got)
	}
	if got := d[dimShard].labels[0]; got != "{0}" {
		t.Errorf("the first shard renders %q, want {0}", got)
	}
}

// TestDistinctVocabulariesShareNoDimension is the reason there are twenty
// dimensions for fourteen brace spellings.
//
// {reason} on decompress_failed is a decompression failure mode; {reason} on
// writer_conn_recycled is hold / idle / error. Sharing a Dim would size one
// instrument's array from the other's vocabulary and render the wrong label
// text against a correct number -- worse than a missing counter, because it
// reads as real.
func TestDistinctVocabulariesShareNoDimension(t *testing.T) {
	d := resolveDims(testOptions())

	for _, pair := range [][2]Dim{
		{dimDecompressReason, dimRecycleReason},
		{dimRetryClass, dimLogClass},
		{dimPanicStage, dimPayloadStage},
		{dimTranscode, dimHeaderCode},
	} {
		a, b := d[pair[0]], d[pair[1]]
		if a.brace != b.brace {
			t.Fatalf("test setup: %s and %s do not share a spelling", a.brace, b.brace)
		}
		if a.size == b.size && strings.Join(a.labels, "") == strings.Join(b.labels, "") {
			t.Errorf("two dimensions spelled %s have identical vocabularies; one of "+
				"them is sized from the wrong list", a.brace)
		}
	}
}

// TestClassTableMatchesTheEnum guards the ordering of logClasses, because the
// order IS the slot order: moving a name re-labels a live counter without
// changing any number, so the suppression history of one class would silently
// be reported under another.
func TestClassTableMatchesTheEnum(t *testing.T) {
	if len(logClasses) != int(numClasses) {
		t.Fatalf("logClasses has %d entries but there are %d classes",
			len(logClasses), numClasses)
	}
	for c := range numClasses {
		if logClasses[c] == "" {
			t.Errorf("class %d has no name", c)
		}
	}
	// Spot-check the two ends, so an insertion in the middle that shifts
	// everything is caught by the names rather than only by the count.
	if ClassSeqGap.String() != "seq_gap" {
		t.Errorf("ClassSeqGap = %q, want seq_gap", ClassSeqGap)
	}
	if ClassDrainDeadline.String() != "drain_deadline" {
		t.Errorf("ClassDrainDeadline = %q, want drain_deadline", ClassDrainDeadline)
	}
	// And the two seams P3-04 inserted at: the end of the WARN run and the end
	// of the ERROR run. A name landing one slot off shifts every label after it.
	for c, want := range map[Class]string{
		ClassShardSendWait:    "shard_send_wait",
		ClassEpochStalled:     "epoch_stalled",
		ClassDecompressFailed: "decompress_failed",
		ClassStartupFailed:    "startup_failed",
		ClassBlobLenMismatch:  "blob_len_mismatch",
		ClassGuardDisabled:    "guard_disabled",
	} {
		if c.String() != want {
			t.Errorf("class %d = %q, want %q", c, c, want)
		}
	}
}

// TestStaticVocabularyTablesCoverTheirEnums catches the half-edit: a constant
// added to an enum without a string, which would render an empty label.
func TestStaticVocabularyTablesCoverTheirEnums(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table []string
		last  int
	}{
		{"decompressReasons", decompressReasons, int(ReasonInputNotConsumed)},
		{"recycleReasons", recycleReasons, int(RecycleError)},
		{"prevalidateReasons", prevalidateReasons, int(PrevalidateNonfinite)},
		{"retryClasses", retryClasses, int(RetryDeadlock)},
		{"panicStages", panicStages, int(StageStats)},
		{"payloadStages", payloadStages, int(PayloadBody)},
		{"probeResults", probeResults, int(ProbeFail)},
		{"gapHypotheses", gapHypotheses, int(HypothesisPerTranscode)},
	} {
		if len(tc.table) != tc.last+1 {
			t.Errorf("%s has %d entries but its last constant is %d; an enum value was "+
				"added without a label", tc.name, len(tc.table), tc.last)
		}
		for i, s := range tc.table {
			if s == "" {
				t.Errorf("%s[%d] is empty", tc.name, i)
			}
		}
	}
}
