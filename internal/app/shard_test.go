package app

import (
	"testing"

	"feedvalve/internal/model"
)

// Shard selection: Vol 1 section 10.4's three normative properties and the one
// invariant that is not about distribution at all.
//
// Every test here states the BREAK it is defending against, because each of
// these passes trivially under an implementation that is wrong in a way the
// obvious test would not notice -- a contiguous block of tokens, for instance,
// distributes perfectly evenly under a plain modulo, which is exactly the
// implementation section 10.4 forbids.

// TestShardOfPinsGeneralUnitsToTheGeneralShard is INV-SCOPE, and it is the most
// important test in this file.
//
// A general unit goes to generalShard REGARDLESS of any Token the message
// carries. bible section 4.5's market-status messages do carry a Token field --
// it names a contract -- while the row they write is keyed by MarketType and is
// market-wide. Routing one of those by its Token puts the same MarketType row
// on several shards, which breaks ASM-15's single-writer-per-row and shows up
// as intermittent lost updates with no error anywhere.
//
// BREAK: drop the General() branch from ShardOf. Tokens are chosen here so the
// hash sends each somewhere other than the general shard, so the break is
// caught rather than passed by luck.
func TestShardOfPinsGeneralUnitsToTheGeneralShard(t *testing.T) {
	const (
		shards  = 4
		general = 2
	)
	var pinned, moved int
	for token := int32(1); token <= 400; token++ {
		g := model.UpdateUnit{Token: token, Flags: model.FlagGeneral}
		if got := ShardOf(&g, shards, general); got != general {
			t.Fatalf("general unit with token %d routed to shard %d, want %d",
				token, got, general)
		}
		pinned++

		// The same token WITHOUT the flag must be free to go elsewhere, or the
		// test above would also pass for an implementation that sent
		// everything to shard 2.
		scoped := model.UpdateUnit{Token: token}
		if ShardOf(&scoped, shards, general) != general {
			moved++
		}
	}
	if pinned != 400 {
		t.Fatalf("pinned %d of 400", pinned)
	}
	if moved == 0 {
		t.Error("no token-scoped unit routed away from the general shard, so the " +
			"general-unit assertion above proves nothing")
	}
}

// TestShardOfIsDeterministic is property 1. The mix decides which shard owns a
// row, so an answer that varied between runs or between architectures would
// re-key every mirror and would make -selfcheck's ASM-15 check disagree with
// the running process.
//
// The expected values are not written out as constants: pinning the current
// fmix32 constants would turn section 10.4's "the properties are what is
// normative, not the constants" into a lie the test enforces. What is asserted
// is stability, which is the property.
func TestShardOfIsDeterministic(t *testing.T) {
	const shards = 4
	first := make(map[int32]model.ShardID, 64)
	for token := int32(1); token <= 64; token++ {
		u := model.UpdateUnit{Token: token}
		first[token] = ShardOf(&u, shards, 0)
	}
	for range 100 {
		for token, want := range first {
			u := model.UpdateUnit{Token: token}
			if got := ShardOf(&u, shards, 0); got != want {
				t.Fatalf("token %d routed to shard %d, then to %d", token, want, got)
			}
		}
	}
}

// TestShardOfSpreadsAStridedTokenSeries is property 2, and it is the reason the
// finaliser exists at all.
//
// Section 10.4: "exchange tokens are allocated in contiguous blocks, so a plain
// token % n correlates shard assignment with contract family and puts a whole
// expiry series on one shard". A CONTIGUOUS block is the wrong fixture for
// that -- token % 4 spreads 1,2,3,4 perfectly. The failure is a series whose
// stride shares a factor with n, which is what an expiry ladder of one
// underlying looks like.
//
// BREAK: replace mix32(token) % n with token % n. Every member of the stride-4
// series below then lands on one shard and this fails; the contiguous-block
// test an author would write first would not.
func TestShardOfSpreadsAStridedTokenSeries(t *testing.T) {
	const shards = 4

	hit := make(map[model.ShardID]int, shards)
	for i := range 48 {
		u := model.UpdateUnit{Token: int32(36000 + i*shards)}
		hit[ShardOf(&u, shards, 0)]++
	}
	if len(hit) != shards {
		t.Fatalf("a stride-%d token series reached %d of %d shards (%v); a plain "+
			"modulo would reach exactly one, which is the correlation the mix exists "+
			"to break", shards, len(hit), shards, hit)
	}
}

// TestShardOfHandlesANonPowerOfTwoShardCount is the bound the default hides.
//
// runtime.shards is valid over 1..64 (Vol 0 section 7.2) and only the DEFAULT
// is 4. The code must not assume a power of two.
//
// BREAK: write the reduction as mix32(token) & uint32(n-1). At n == 3 that
// masks with 2, so shard 1 becomes unreachable and a third of the rows pile
// onto shard 2 -- and at n == 4, the configuration everything is tested with,
// it is indistinguishable from correct.
func TestShardOfHandlesANonPowerOfTwoShardCount(t *testing.T) {
	for _, shards := range []int{1, 3, 5, 7, 64} {
		hit := make(map[model.ShardID]int, shards)
		for token := int32(1); token <= 4000; token++ {
			u := model.UpdateUnit{Token: token}
			k := ShardOf(&u, shards, 0)
			if int(k) >= shards {
				t.Fatalf("shards=%d: token %d routed to shard %d, out of range",
					shards, token, k)
			}
			hit[k]++
		}
		if len(hit) != shards {
			t.Errorf("shards=%d: only %d shards reached by 4000 tokens; a mask would "+
				"leave exactly the non-power-of-two slots empty", shards, len(hit))
		}
	}
}

// TestShardOfTreatsNegativeTokensAsRoutable.
//
// Token is a SHORT-derived int32 and the bible's LONG is signed, so a token
// above 2^31 -- or a corrupt one -- arrives negative. uint32(u.Token) is a
// zero-extending reinterpretation, not a conversion, so it routes like any
// other bit pattern rather than panicking or producing a negative index.
func TestShardOfTreatsNegativeTokensAsRoutable(t *testing.T) {
	const shards = 4
	for _, token := range []int32{-1, -2, -2147483648, 2147483647} {
		u := model.UpdateUnit{Token: token}
		if k := ShardOf(&u, shards, 0); int(k) >= shards {
			t.Errorf("token %d routed to shard %d, out of range", token, k)
		}
	}
}

// TestMix32IsABijection checks the property the two odd multipliers buy: no
// input is lost before the modulo.
//
// Two tokens may of course collide on a shard -- there are four shards and
// four billion tokens -- but never because the finaliser threw information
// away. A sweep of the whole 32-bit space is too slow for a unit test; a dense
// window is enough to catch an even multiplier, which collapses the low bit
// immediately.
func TestMix32IsABijection(t *testing.T) {
	seen := make(map[uint32]uint32, 1<<16)
	for i := range uint32(1 << 16) {
		h := mix32(i)
		if prev, dup := seen[h]; dup {
			t.Fatalf("mix32(%d) and mix32(%d) both produce %d; an even multiplier "+
				"collapses the low bits, which are the only ones %%n reads", prev, i, h)
		}
		seen[h] = i
	}
}

// TestMix32AvalanchesTheLowBits is property 2 stated directly rather than
// through ShardOf: flipping the lowest input bit must change the low output
// bits roughly half the time.
//
// This is the property that matters, because % n with a small n reads ONLY the
// low bits. A finaliser that scrambled the high half beautifully and left the
// low half alone would pass a distribution test over contiguous tokens and
// fail in production on a strided one.
func TestMix32AvalanchesTheLowBits(t *testing.T) {
	const (
		trials = 4096
		lowBit = 1
	)
	var changed int
	for i := range uint32(trials) {
		if (mix32(i)^mix32(i^lowBit))&0x7 != 0 {
			changed++
		}
	}
	// Three low bits, so a good mix changes at least one of them 7 times in 8.
	// The bound is loose on purpose: this asserts avalanche, not a constant.
	if changed < trials/2 {
		t.Errorf("flipping the input's low bit changed the output's low 3 bits in "+
			"%d of %d trials; the low bits are the only ones the modulo reads",
			changed, trials)
	}
}
