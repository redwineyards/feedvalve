package obs

import "strconv"

// Dim is a label dimension: the fixed set of slots one instrument's label
// expands into.
//
// "Labels are not a metrics system" (Vol 0 section 8.1). A label set here is a
// fixed-size array indexed at startup, never a map lookup on a hot path, and
// the index arrives as an int the caller already holds -- a shard number, a
// destination slot, a contributor index.
//
// There are TWENTY dimensions for fourteen distinct brace spellings, and that
// is not redundancy. {reason}, {class}, {stage} and {code} each appear on
// several instruments with DIFFERENT vocabularies: decompress_failed{reason} is
// a decompression failure mode while writer_conn_recycled{reason} is hold /
// idle / error, and panics_recovered{stage} is a goroutine class while
// short_payload{stage} is header / body. Sharing a Dim between them would size
// one instrument's array from another's vocabulary and render the wrong label
// text against a correct number, which is worse than a missing counter because
// it reads as real.
type Dim uint8

const (
	// dimNone is the 59 unlabelled instruments: exactly one slot, empty label.
	dimNone Dim = iota

	dimShard     // {k} -- runtime.shards
	dimDest      // {destination}
	dimShardDest // {k,destination}

	// dimTranscode is {code} and {transcode} on every instrument that labels
	// by a CONFIGURED transaction code. Vol 0 section 8.1: the index is
	// "assigned at config load from the configured inventory", so the two
	// spellings are one dimension -- the ledger uses {code} on the decode side
	// and {transcode} on the shard side for the same inventory.
	dimTranscode

	// dimUnknownCode is {code} on unknown_transcode -- THE ONE DYNAMIC LABEL.
	// A 64-slot open-addressed table with CAS insert; its label text is a code
	// nobody configured, so it is rendered at emission, not at freeze.
	dimUnknownCode

	// dimHeaderCode is {code} on message_header_seen, which Vol 0 section 8.2
	// pins to exactly two values: 6541 and 6503. 6503 (bible section 4.15) had
	// no counter at all before gap G2.
	dimHeaderCode

	// dimOracleCode is {code} on oracle_error: a FIXED 24-slot table plus an
	// "other" bucket, fixed because the Oracle error taxonomy a runbook acts
	// on is closed. Which codes occupy the slots is runtime, so like
	// dimUnknownCode its label text is rendered at emission.
	dimOracleCode

	dimMarketType       // {market_type} -- bible Appendix A, 1 to 4
	dimHypothesis       // {hypothesis} -- global | per-transcode
	dimRetryClass       // {class} on write_retry -- session | deadlock | space | unknown
	dimLogClass         // {class} on log_events_suppressed -- the log event classes
	dimPanicStage       // {stage} on panics_recovered -- decode | shard | writer | stats
	dimPayloadStage     // {stage} on short_payload -- header | body
	dimDecompressReason // {reason} on decompress_failed
	dimRecycleReason    // {reason} on writer_conn_recycled -- hold | idle | error
	dimDestReason       // {destination,reason} on prevalidate_rejected
	dimProbeResult      // {result} on write_probe -- ok | fail
	dimWriter           // {writer} -- writers.count
	dimDestColumn       // {destination,column}

	numDims
)

// The static label vocabularies the ledger fixes. Each is a named type so a
// prevalidate reason cannot be passed where a recycle reason belongs, and each
// has a string table whose order IS the slot order -- a value reordered here
// silently re-labels live counters, which is why the tests assert each table's
// length against its dimension's arity.

// DecompressReason indexes decompress_failed{reason} (Vol 0 section 8.2a).
//
// The split earns its place operationally: ReasonOutputOverrun means ASM-14 is
// broken and has a known fix, while the other three mean wire corruption. Two
// different operator actions behind what was one number.
type DecompressReason uint8

const (
	ReasonInputOverrun DecompressReason = iota
	ReasonOutputOverrun
	ReasonLookbehindOverrun
	ReasonInputNotConsumed
)

var decompressReasons = []string{"input_overrun", "output_overrun", "lookbehind_overrun", "input_not_consumed"}

// RecycleReason indexes writer_conn_recycled{reason}. It separates "working as
// designed" from "the database keeps dropping us".
type RecycleReason uint8

const (
	RecycleHold RecycleReason = iota
	RecycleIdle
	RecycleError
)

var recycleReasons = []string{"hold", "idle", "error"}

// PrevalidateReason indexes the reason half of prevalidate_rejected -- which
// kind of value the mirror refused before Oracle had to (Vol 0 section 5A).
type PrevalidateReason uint8

const (
	PrevalidatePrecision PrevalidateReason = iota
	PrevalidateLength
	PrevalidateDomain
	PrevalidateTime
	PrevalidateNonfinite
)

var prevalidateReasons = []string{"precision", "length", "domain", "time", "nonfinite"}

// RetryClass indexes write_retry{class}. A non-zero deadlock count means HLD
// section 8.4's disjoint-ownership invariant is broken -- a configuration
// defect, not load, and the single most important thing the design assumes
// cannot happen.
type RetryClass uint8

const (
	RetrySession RetryClass = iota
	RetryDeadlock
	RetrySpace
	RetryUnknown
)

var retryClasses = []string{"session", "deadlock", "space", "unknown"}

// PanicStage indexes panics_recovered{stage}, canonical for every recovered
// panic. Vol 0 section 8.2a withdraws decode_panic{code} and writer_panic in
// its favour: they counted the same events under narrower names, and the
// offending transcode travels on the ERROR line anyway.
type PanicStage uint8

const (
	StageDecode PanicStage = iota
	StageShard
	StageWriter
	StageStats
)

var panicStages = []string{"decode", "shard", "writer", "stats"}

// PayloadStage indexes short_payload{stage}: which read ran off the end.
type PayloadStage uint8

const (
	PayloadHeader PayloadStage = iota
	PayloadBody
)

var payloadStages = []string{"header", "body"}

// ProbeResult indexes write_probe{result}. Without it the degraded-state probe
// is invisible and "why did it fail-fast against a healthy database?" has no
// answer.
type ProbeResult uint8

const (
	ProbeOK ProbeResult = iota
	ProbeFail
)

var probeResults = []string{"ok", "fail"}

// GapHypothesis indexes the sequence-audit instruments. The audit runs both
// hypotheses simultaneously -- global sequence state and per-transcode state,
// updated in parallel, NEITHER feeding seq_gap_* -- and reports which produced
// zero gaps (Vol 1 section 7.7).
//
// Vol 0 section 8.2 puts {hypothesis} on the row, which carries both
// gap_audit_agree and gap_audit_disagree; Vol 1's prose labels only disagree.
// The ledger is the frozen contract, so both are labelled. Two extra slots cost
// nothing and the alternative is second-guessing the contract.
type GapHypothesis uint8

const (
	HypothesisGlobal GapHypothesis = iota
	HypothesisPerTranscode
)

var gapHypotheses = []string{"global", "per-transcode"}

// headerCodes is dimHeaderCode's two values, from Vol 0 section 8.2's
// "{code} = 6541, 6503".
var headerCodes = []int{6541, 6503}

// MessageHeaderCode indexes message_header_seen{code}.
type MessageHeaderCode uint8

const (
	HeaderHeartbeat MessageHeaderCode = iota // 6541, bible section 4.12
	HeaderAssetRate                          // 6503, bible section 4.15
)

// marketTypeValues is dimMarketType's slots. Market types are 1 to 4 (bible
// Appendix A), so the slot index is the market type minus one and the rendered
// label is the market type itself -- Vol 4 section 6.1's example line shows
// session_state{1}, not session_state{0}.
// Sized by marketTypes, so the G-BLD-2 gate on that constant pins the table
// that actually sizes the dimension: a fifth market type is now a compile
// error here rather than a silently larger slab.
var marketTypeValues = [marketTypes]int{1, 2, 3, 4}

// dimKind decides whether a slot appears on the line when it holds nothing.
//
// Sparsity is not an optimisation, it is what keeps the grouped emission
// readable at the full instrument count. Vol 4 section 6.1 sized the group
// split against "about 200 values, about 5 KB on one line" -- an arithmetic it
// performed BEFORE section 8.2a was merged, and its table does not list
// {destination,column} at all. Three instruments carry that dimension
// (message_truncated, bind_nonfinite, bind_bad_time) at three destinations by
// sixty-four columns, which is 576 slots, nearly all of them in Schema fit. A
// dense Schema fit line would run to roughly 18 KB -- worse than the single
// line the group split exists to avoid, and truncated by the same transports.
//
// So the high-cardinality dimensions print only what carries data. Gate
// G-OBS-3 is unaffected, because its setup touches every instrument before
// emitting, so every name still appears exactly once.
type dimKind uint8

const (
	// dimDense prints every slot, zero or not. This is what makes Vol 4
	// section 6.1 rule 3 meaningful: "a value that has been zero all night is
	// still visible as zero".
	dimDense dimKind = iota

	// dimSparse prints a slot only when it carries data.
	dimSparse

	// dimDynamic is sparse, and its label text is a code nobody configured, so
	// it is rendered during emission from the slot's own code rather than
	// pre-rendered at freeze. An unoccupied dynamic slot has no label at all,
	// which is why dynamic implies sparse.
	dimDynamic
)

// dimSpec is the static half of a dimension: everything knowable without the
// configuration.
type dimSpec struct {
	// brace is the label spelling as the ledger writes it. Used by tests and
	// by the panic text when an accessor is called for the wrong dimension,
	// where naming the expected spelling is what makes the message useful.
	brace string

	kind dimKind
}

var dimSpecs = [numDims]dimSpec{
	dimNone:             {brace: "", kind: dimDense},
	dimShard:            {brace: "{k}", kind: dimDense},
	dimDest:             {brace: "{destination}", kind: dimDense},
	dimShardDest:        {brace: "{k,destination}", kind: dimDense},
	dimTranscode:        {brace: "{code}", kind: dimDense},
	dimUnknownCode:      {brace: "{code}", kind: dimDynamic},
	dimHeaderCode:       {brace: "{code}", kind: dimDense},
	dimOracleCode:       {brace: "{code}", kind: dimDynamic},
	dimMarketType:       {brace: "{market_type}", kind: dimDense},
	dimHypothesis:       {brace: "{hypothesis}", kind: dimDense},
	dimRetryClass:       {brace: "{class}", kind: dimDense},
	dimLogClass:         {brace: "{class}", kind: dimDense},
	dimPanicStage:       {brace: "{stage}", kind: dimDense},
	dimPayloadStage:     {brace: "{stage}", kind: dimDense},
	dimDecompressReason: {brace: "{reason}", kind: dimDense},
	dimRecycleReason:    {brace: "{reason}", kind: dimDense},
	dimDestReason:       {brace: "{destination,reason}", kind: dimDense},
	dimProbeResult:      {brace: "{result}", kind: dimDense},
	// {writer} is sized to the reloadable maximum, so most slots are empty on
	// any given run; sparse is the only sensible kind for it.
	dimWriter:     {brace: "{writer}", kind: dimSparse},
	dimDestColumn: {brace: "{destination,column}", kind: dimSparse},
}

// Options is everything obs needs from the rest of the process, as plain
// scalars and strings.
//
// It is not a *config.Config and it never will be: obs may not import config
// (gate G-DEP-1). The lifecycle reads the configuration and the destination
// table and hands over the resolved shapes at startup step 9, which is also
// what keeps obs testable without either package.
type Options struct {
	// Shards is runtime.shards -- the arity of every {k} label.
	//
	// Safe to freeze a slab against, because runtime.shards is startup-only
	// (rule V-R1, startupOnly() in internal/config/validate.go): changing it
	// re-keys every mirror and strands rows in the old shard, so it cannot
	// move under a running process. The same is true of the destination set
	// (V-R2) and of the transcode inventory (V-R3). obs's entire RELOADABLE
	// surface is four scalars -- stats.intervalSec, stats.logLevel,
	// stats.eventRatePerMin and debug.* -- and not one of them changes a slab
	// size. That is what makes Freeze sound.
	//
	// writers.count is the exception that proves it: it IS reloadable, which
	// is exactly why {writer} is sized to maxWriters rather than to it.
	Shards int

	// Destinations are the destination names in slot order, used as the
	// rendered {destination} label. Names rather than indices because
	// dirty_depth{0,2} tells an operator nothing at 03:00.
	Destinations []string

	// Columns[d] are destination d's column names in ColMask bit order, for
	// the column half of {destination,column}.
	Columns [][]string

	// Transcodes are the configured transaction codes in ascending order. The
	// slot index is the position here, which is the same index the decoder
	// registry and the ordering guard use -- "reusing the counter layer's slot
	// index is what keeps per-transcode from needing a map on the receive
	// path" (Vol 1 section 7.7).
	Transcodes []int
}

// resolvedDim is a dimension once the configuration is known.
type resolvedDim struct {
	dimSpec

	// size is the number of logical slots: the product, for the cartesian
	// dimensions.
	size uint32

	// labels are the pre-rendered label texts, braces included, one per
	// logical slot -- so emission appends bytes and never formats. nil for a
	// dynamic dimension, whose text comes from its code table at emission.
	labels []string

	// inner is the arity of the right-hand label of a cartesian dimension, so
	// the offset is i0*inner + i1. One for the single-label dimensions.
	inner uint32
}

// resolveDims sizes and labels every dimension from the Options the lifecycle
// passes in. Called once, from New.
//
// Label values are DOMAIN values, not slot indices, and the two differ: Vol 4
// section 6.1's example line shows session_state{1} for the first market type,
// because bible Appendix A numbers market types 1 to 4, while shard_depth_max{0}
// shows that {k} is zero-based. A market type rendered zero-based would
// misattribute every session reading by one market, silently, so the rendering
// is a table rather than arithmetic.
//
// {destination} renders the destination NAME, not its slot id, because
// dirty_depth{0,2} tells an operator nothing at 03:00.
func resolveDims(o Options) [numDims]resolvedDim {
	var d [numDims]resolvedDim
	for i := range d {
		d[i].dimSpec = dimSpecs[i]
		d[i].inner = 1
	}

	set := func(id Dim, labels []string) {
		d[id].labels = labels
		d[id].size = uint32(len(labels))
	}
	// A dynamic dimension is sized but not labelled.
	sized := func(id Dim, n int) { d[id].size = uint32(n) }
	// A two-label dimension gets labels, size AND inner in one call: resolve's
	// i*inner+j offset is only correct once inner is set, and an inner left at
	// 1 lands a write on the wrong destination plausibly rather than loudly.
	setCross := func(id Dim, labels []string, inner int) {
		set(id, labels)
		d[id].inner = uint32(inner)
	}

	set(dimNone, []string{""})
	set(dimShard, braceStrings(seqStrings(o.Shards)))
	set(dimDest, braceStrings(o.Destinations))
	set(dimTranscode, braceInts(o.Transcodes))
	set(dimHeaderCode, braceInts(headerCodes))
	set(dimMarketType, braceInts(marketTypeValues[:]))
	set(dimHypothesis, braceStrings(gapHypotheses))
	set(dimRetryClass, braceStrings(retryClasses))
	set(dimLogClass, braceStrings(logClasses))
	set(dimPanicStage, braceStrings(panicStages))
	set(dimPayloadStage, braceStrings(payloadStages))
	set(dimDecompressReason, braceStrings(decompressReasons))
	set(dimRecycleReason, braceStrings(recycleReasons))
	set(dimProbeResult, braceStrings(probeResults))
	set(dimWriter, braceStrings(seqStrings(maxWriters)))

	// The two dynamic tables. oracleErrorSlots+1 reserves the final slot for
	// the overflow bucket, which renders oracle_error{other} -- the same shape
	// as unknown_transcode's named unknown_transcode_other counter, so the
	// asymmetry between the two in the ledger does not reach the output.
	sized(dimUnknownCode, unknownTranscodeSlots)
	sized(dimOracleCode, oracleErrorSlots+1)

	// {destination,reason}: row-major, destination outer.
	setCross(dimDestReason, braceCross(o.Destinations, prevalidateReasons), len(prevalidateReasons))

	// {destination,column}: rectangular at maxColumns per destination, so the
	// offset is dest*maxColumns+col and no per-destination base table is
	// needed. Columns a destination does not declare are labelled by index
	// rather than left blank, so an out-of-range caller produces a line that
	// is traceable instead of one that is malformed. Sparse, so the unused
	// slots never reach the output.
	dc := make([]string, 0, len(o.Destinations)*maxColumns)
	for i, name := range o.Destinations {
		for c := range maxColumns {
			if i < len(o.Columns) && c < len(o.Columns[i]) {
				dc = append(dc, "{"+name+","+o.Columns[i][c]+"}")
				continue
			}
			dc = append(dc, "{"+name+",col"+strconv.Itoa(c)+"}")
		}
	}
	setCross(dimDestColumn, dc, maxColumns)

	// {k,destination}: row-major, shard outer.
	setCross(dimShardDest, braceCross(seqStrings(o.Shards), o.Destinations), len(o.Destinations))

	return d
}

// seqStrings renders 0..n-1, for the dimensions whose values are just an index.
func seqStrings(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = strconv.Itoa(i)
	}
	return out
}

// braceCross renders the row-major cartesian product as {outer,inner} labels.
// The caller passes the inner arity to setCross, which is what keeps the offset
// arithmetic and these labels describing the same table.
func braceCross(outer, inner []string) []string {
	out := make([]string, 0, len(outer)*len(inner))
	for _, o := range outer {
		for _, i := range inner {
			out = append(out, "{"+o+","+i+"}")
		}
	}
	return out
}

func braceInts(v []int) []string {
	out := make([]string, len(v))
	for i, n := range v {
		out[i] = "{" + strconv.Itoa(n) + "}"
	}
	return out
}

func braceStrings(v []string) []string {
	out := make([]string, len(v))
	for i, s := range v {
		out[i] = "{" + s + "}"
	}
	return out
}
