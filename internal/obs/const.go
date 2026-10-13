package obs

// Build-time constants for the label arrays.
//
// Two of these -- maxDestinations and maxColumns -- duplicate model's exported
// constants, and model's own doc comment says re-declaring them per package
// "would defeat gate G-BLD-2, whose entire purpose is that these numbers cannot
// drift apart". That objection is correct and it is overridden here for one
// reason: obs MUST NOT import any other internal package (Vol 0 section 1.3,
// gate G-DEP-1), because obs is imported from everywhere and one edge back
// would make half the tree cyclic.
//
// So the duplication is forced, and the answer is the one destinations already
// uses for its image lengths: two sources of truth are only safe when something
// checks them against each other. TestLocalConstantsMatchModel in
// ledger_test.go asserts each of these equals model's, importing model from the
// external test package -- which gate G-DEP-1 permits, because it reads
// go list -deps WITHOUT -test and documents that test-only imports are
// deliberately excluded.
const (
	// maxDestinations mirrors model.MaxDestinations. It is the arity of every
	// {destination} label.
	maxDestinations = 3

	// maxColumns mirrors model.MaxColumns, the width of ColMask. It bounds the
	// column half of {destination,column}.
	maxColumns = 64

	// marketTypes is the arity of {market_type}: bible Appendix A defines
	// market types 1 to 4, and session_state is published per market type.
	marketTypes = 4

	// unknownTranscodeSlots sizes the ONE dynamic label in the ledger.
	//
	// unknown_transcode{code} cannot use a fixed array because req section
	// 12.1 says unknown codes will occur and nobody can enumerate them in
	// advance. It cannot use a map either: it is on the receive goroutine at
	// ~10,000 messages/s, and a map insert allocates while a mutex at that
	// rate turns an inventory gap into an availability problem. So it is a
	// 64-slot open-addressed table with atomic CAS insert, and overflow lands
	// in unknown_transcode_other (Vol 0 section 8.1).
	unknownTranscodeSlots = 64

	// oracleErrorSlots sizes oracle_error{code}. Vol 0 section 8.2a calls it a
	// "fixed 24-slot table plus other", fixed "because the taxonomy is
	// closed".
	//
	// It is implemented as a SECOND dynamic table rather than a fixed array,
	// and the reason is a dependency, not a performance argument: the 24 codes
	// are oradb's knowledge, and obs may not import oradb. Passing them into
	// New would make a Phase 1 constructor depend on Phase 3 data for no gain,
	// since the probe-and-CAS table sizes and behaves identically. The slot
	// count is the same 24; overflow lands in a reserved slot rendered as
	// oracle_error{other}, so no name is invented.
	oracleErrorSlots = 24

	// maxWriters sizes {writer} on stmt_cache_entries.
	//
	// 32, the V-RANGE upper bound on writers.count, and NOT the count the
	// process started with. writers.count is reloadable -- it is absent from
	// startupOnly() in internal/config/validate.go, and section 9.3 calls
	// raising it "the one lever" for a write path that cannot keep up. Sizing
	// this array to the startup count would leave every writer added by a
	// reload with nowhere to publish its cache occupancy, which is invisible
	// rather than wrong, and therefore worse. The unoccupied slots cost
	// nothing because {writer} is emitted sparsely.
	maxWriters = 32

	// forcedEmitEvery is Vol 4 section 6.1 rule 3: every group is emitted at
	// least once every tenth interval, so a trend never has an unbounded gap
	// and a value that has been zero all night is still visible as zero.
	forcedEmitEvery = 10

	// padStride is the slot stride, in instruments, for the label dimensions
	// several goroutines write concurrently.
	//
	// The ppc64 cache line is 128 bytes, not 64 (Vol 0 section 9.5 sizes QD's
	// head and tail against it), and an atomic.Uint64 is 8 bytes, so sixteen
	// adjacent slots share one line. rows_created{destination} is incremented
	// by all four shard goroutines into three adjacent words at row rate, and
	// dirty_depth{k,destination} and shard_loop_max_ns{k} are written by the
	// shard that owns k. Unpadded, every one of those writes invalidates the
	// line the other three shards are using.
	//
	// This is an addition, not a requirement: no gate asks for it and the
	// contract does not mention instrument padding. It is here because Vol 4
	// section 6.7 is explicit that there is no profiler and no metrics agent
	// on AIX, so a false-sharing stall is the class of problem that never gets
	// found on the machine it happens on. Applied only to the three dimensions
	// with concurrent writers; the cost is about 25 KB against a 189 MB arena
	// budget.
	padStride = 16
)

// Gate G-BLD-2, in the two-sided form of model/const.go and Vol 0 section 2
// rule 7: each line compiles only when the constant holds its declared value,
// because if it moves one of the two terms is negative and converting a
// negative constant to uint is a compile-time error.
//
// These pin the duplicated values at build time. The cross-check against
// model's constants is a test, because a constant expression cannot reach
// another package obs may not import.
const (
	_ = uint(maxDestinations-3) + uint(3-maxDestinations)
	_ = uint(maxColumns-64) + uint(64-maxColumns)
	_ = uint(marketTypes-4) + uint(4-marketTypes)
	_ = uint(unknownTranscodeSlots-64) + uint(64-unknownTranscodeSlots)
	_ = uint(oracleErrorSlots-24) + uint(24-oracleErrorSlots)
	_ = uint(maxWriters-32) + uint(32-maxWriters)
	_ = uint(forcedEmitEvery-10) + uint(10-forcedEmitEvery)
	_ = uint(padStride-16) + uint(16-padStride)
)

// unknownTranscodeSlots must be a power of two: the open-addressed table masks
// the code to find its starting probe, and a mask is only a modulo when the
// size is a power of two. A non-power-of-two size would bias every probe toward
// the low slots and silently lose codes to the overflow bucket.
//
// Negating the popcount residue is what makes this fail rather than merely
// compute: x & (x-1) is zero exactly when x is a power of two, and uint of a
// negative constant is a compile-time error.
const _ = uint(0 - (unknownTranscodeSlots & (unknownTranscodeSlots - 1)))
