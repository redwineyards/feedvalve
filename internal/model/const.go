package model

// Build-time constants. Six struct sizes in this package depend on them, which
// is why they are constants and not configuration: changing one changes
// Entry's size and every memory figure derived from it.
//
// They are exported because the tree needs one source of truth for them.
// destinations checks ImageSize at build time, decode sizes RegistryEntry.Slots
// by MaxDestinations, and mirror sizes its arenas from Entry. Re-declaring them
// per package would defeat gate G-BLD-2 below, whose entire purpose is that
// these numbers cannot drift apart.
const (
	// MaxDestinations is fixed at three by HLD section 8.1: three
	// current-state destinations, no append destination. -selfcheck rejects a
	// configuration declaring any other number.
	MaxDestinations = 3

	// MaxContributors is the number of distinct transaction codes that may
	// contribute columns to one destination. It sizes Entry's three ordering
	// arrays, so it is a build-time constant and contributor membership is
	// startup-only configuration.
	//
	// Eight, not the HLD's four. HLD section 8.2's own MD_MARKET_STATUS
	// destination takes contributions from FIVE transaction codes -- 6511,
	// 6521, 6522, 6531 and 6571 (bible section 4.5) -- so four fails
	// -selfcheck against the schema the HLD itself illustrates. Eight leaves
	// room for one more doubling. See Vol 0 section 12, delta D1.
	MaxContributors = 8

	// ImageSize is the fixed width of a destination row image inside Entry,
	// bounded by ASM-16b: the largest full row image any destination can hold,
	// accumulated across every one of its contributors. Vol 0 section 4.6
	// verifies 256 suffices for HLD section 8.2's instantiation with three
	// bytes to spare in the worst variant.
	ImageSize = 256

	// UnitBlobSize is the packed value area of one UpdateUnit, bounded by
	// ASM-16a: the largest contribution one transaction code makes across all
	// three destinations in one message.
	UnitBlobSize = 256

	// MaxColumns is the number of distinct maskable fields one destination may
	// declare. It is the width of ColMask and is not configurable.
	MaxColumns = 64
)

// Gate G-BLD-2 (Vol 4 section 7.1). Each line compiles only when the constant
// holds its declared value: if it moves, one of the two terms is negative, and
// converting a negative constant to uint is a compile-time error.
//
// The point is not that these are hard to read correctly. It is that
// MaxContributors was once four, and raising it to fix one destination silently
// changed Entry's size and every memory figure in the design. The next change
// has to be as deliberate as that one should have been.
const (
	_ = uint(MaxDestinations-3) + uint(3-MaxDestinations)
	_ = uint(MaxContributors-8) + uint(8-MaxContributors)
	_ = uint(ImageSize-256) + uint(256-ImageSize)
	_ = uint(UnitBlobSize-256) + uint(256-UnitBlobSize)
	_ = uint(MaxColumns-64) + uint(64-MaxColumns)
)
