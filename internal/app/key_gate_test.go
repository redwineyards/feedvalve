package app

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
)

// Gate G-KEY-1: for the SHIPPED mapping, every unit a contributing transcode
// produces is routed to the shard that owns the row it writes.
//
// This is -selfcheck's V-D7 (ASM-15) logic run as a unit test, with one
// difference that matters: V-D7 reasons about the configuration alone, while
// this drives the REAL decoders, so it sees which units a decoder actually
// flags general and which Token it actually routes by. A mapping or decoder
// change that is never -selfchecked still fails here.
//
// Ownership of a row is derived from the DESTINATION'S row-key rule, not from
// the unit under test: a token-keyed row is owned by the shard that routes
// "a unit with that token", a marketType-keyed row by the general shard. The
// unit is then routed by the same ShardOf the fan-out calls.

// keyGateShardings are the (shards, generalShard) pairs swept. One pair could
// agree by luck (a general unit and a hashed token both landing on shard 0);
// the sweep makes a luck-pass impossible for any non-trivial count.
func keyGateShardings() [][2]int {
	var out [][2]int
	for n := 2; n <= 8; n++ {
		for g := range n {
			out = append(out, [2]int{n, g})
		}
	}
	return out
}

// keyGateFillers are two payload fillers, chosen so that every decoder emits
// at least one unit without this test knowing any field offset. SHORT fields
// at even offsets read 1 (a valid BookType, NoOfRecords and MarketType) in the
// first, and 256 in the second; tokens are non-zero and different in each.
var keyGateFillers = []func(i int) byte{
	func(i int) byte { return byte(i & 1) },
	func(i int) byte { return byte(1 - i&1) },
}

// keyGateCounts is what a run exercised, so a pass can be shown non-vacuous.
type keyGateCounts struct {
	units  int
	byRule map[destinations.RowKeyRule]int
	byCode map[model.TransCode]int
}

// ownerProbe is the unit the destination's rule says would own the row.
func ownerProbe(rule destinations.RowKeyRule, row model.RowKey) model.UpdateUnit {
	if rule == destinations.KeyMarketType {
		return model.UpdateUnit{Flags: model.FlagGeneral}
	}
	return model.UpdateUnit{Token: int32(row.Lo())}
}

// checkKeyOwnership decodes one message per contributor of every destination
// in dests and returns each disagreement between where the unit is routed and
// where its row is owned.
func checkKeyOwnership(t *testing.T, dests *destinations.Set) ([]string, keyGateCounts) {
	t.Helper()
	counts := keyGateCounts{
		byRule: map[destinations.RowKeyRule]int{},
		byCode: map[model.TransCode]int{},
	}

	// One TranscodeSpec per code, contributing every column of every
	// destination that lists it.
	byCode := map[int]*decode.TranscodeSpec{}
	var order []int
	for _, d := range dests.All() {
		cols := make([]string, 0, len(d.Columns))
		for i := range d.Columns {
			cols = append(cols, d.Columns[i].Name())
		}
		for i := range d.NContributors {
			code := int(d.Contributors[i])
			if byCode[code] == nil {
				byCode[code] = &decode.TranscodeSpec{Code: code}
				order = append(order, code)
			}
			byCode[code].Contributions = append(byCode[code].Contributions,
				decode.ContribSpec{Destination: d.Name, Columns: cols})
		}
	}
	specs := make([]decode.TranscodeSpec, 0, len(order))
	for _, c := range order {
		specs = append(specs, *byCode[c])
	}
	reg, err := decode.BuildRegistry(specs, dests, decode.NopCounters{})
	if err != nil {
		t.Fatalf("BuildRegistry: %v", err)
	}

	var problems []string
	for _, code := range reg.Codes() {
		e := reg.Lookup(code)
		for fi, fill := range keyGateFillers {
			payload := make([]byte, 8+e.Dec.BodyLen())
			for i := range payload {
				payload[i] = fill(i)
			}
			h := model.MsgHeader{Code: code, MsgLen: int16(e.Dec.BodyLen()), PayloadLen: int32(len(payload))}
			out := make([]model.UpdateUnit, max(e.Dec.MaxUnits(), 1))
			n, err := e.Dec.Decode(h, payload, out)
			if err != nil {
				t.Fatalf("transcode %d filler %d: Decode: %v", code, fi, err)
			}
			for ui := range n {
				u := &out[ui]
				counts.units++
				counts.byCode[code]++
				for _, d := range dests.All() {
					c := u.Contribution(d.ID)
					if !c.Present() {
						continue
					}
					counts.byRule[d.Key]++
					probe := ownerProbe(d.Key, c.Row)
					for _, s := range keyGateShardings() {
						got := ShardOf(u, s[0], s[1])
						want := ShardOf(&probe, s[0], s[1])
						if got != want {
							problems = append(problems, fmt.Sprintf(
								"transcode %d -> %s (keyed by %s): unit routed to shard %d, "+
									"row %#x owned by shard %d (shards=%d general=%d)",
								code, d.Name, d.Key, got, uint64(c.Row), want, s[0], s[1]))
							break
						}
					}
				}
			}
		}
	}
	return problems, counts
}

// TestG_KEY_1_EveryShippedContributionIsRoutedToItsRowsOwner.
//
// SETUP: the embedded mapping.json; a registry of every transcode it lists as
// a contributor, built from the real decoders; two fabricated payloads per
// code.
// STIMULUS: decode each, then ShardOf the unit under every (shards,
// generalShard) pair from 2..8, against the shard the destination's row-key
// rule says owns the row it targets.
// ASSERTION: they are equal in every case.
// WOULD HAVE CAUGHT: ASM-15 / HLD R13 -- two shards writing one row, which
// loses updates intermittently with no error anywhere.
// NON-VACUITY: both row-key rules, and every contributing code, were actually
// exercised -- a decoder emitting no units would otherwise pass for free.
func TestG_KEY_1_EveryShippedContributionIsRoutedToItsRowsOwner(t *testing.T) {
	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	problems, counts := checkKeyOwnership(t, dests)
	for _, p := range problems {
		t.Error(p)
	}

	for _, rule := range []destinations.RowKeyRule{destinations.KeyToken, destinations.KeyMarketType} {
		if counts.byRule[rule] == 0 {
			t.Errorf("no contribution to a %s-keyed destination was exercised, so the "+
				"assertion proves nothing for that rule", rule)
		}
	}
	for _, d := range dests.All() {
		for i := range d.NContributors {
			if code := d.Contributors[i]; counts.byCode[code] == 0 {
				t.Errorf("contributor %d of %s produced no unit in either payload", code, d.Name)
			}
		}
	}
}

// TestG_KEY_1_ACorruptedMappingFails.
//
// SETUP: the shipped mapping.json text with one rule corrupted per case.
// STIMULUS: the same check as above.
// ASSERTION: it reports at least one disagreement.
// WOULD HAVE CAUGHT: the gate passing vacuously -- a check that cannot fail
// guards nothing.
func TestG_KEY_1_ACorruptedMappingFails(t *testing.T) {
	raw, err := os.ReadFile("../destinations/mapping.json")
	if err != nil {
		t.Fatalf("read shipped mapping: %v", err)
	}
	src := string(raw)

	cases := []struct {
		name, from, to string
	}{
		// General units (6511 family) now write a token-keyed row.
		{"marketStatusKeyedByToken",
			`"rowKey": "marketType",`, `"rowKey": "token",`},
		// Token-scoped units now write a market-wide row.
		{"securityMasterKeyedByMarketType",
			`"name": "MD_SECURITY_MASTER",
      "table": "MD_SECURITY_MASTER",
      "rowKey": "token",`,
			`"name": "MD_SECURITY_MASTER",
      "table": "MD_SECURITY_MASTER",
      "rowKey": "marketType",`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := strings.ReplaceAll(src, "\r\n", "\n")
			if !strings.Contains(src, c.from) {
				t.Fatalf("mapping.json no longer contains the text this case corrupts; update the case")
			}
			dests, err := destinations.Parse([]byte(strings.Replace(src, c.from, c.to, 1)))
			if err != nil {
				t.Fatalf("the corrupted mapping must still load (the config check is not what is under test): %v", err)
			}
			problems, _ := checkKeyOwnership(t, dests)
			if len(problems) == 0 {
				t.Fatal("a mapping whose row key disagrees with its contributors' shard key passed the gate")
			}
		})
	}
}
