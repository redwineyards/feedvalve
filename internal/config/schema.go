package config

import (
	"slices"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
)

// Mode says which rules apply. Some are relaxed for the one-shot modes
// precisely so those modes stay usable where the real thing would not run.
type Mode uint8

const (
	// ModeRun is the relay. Everything applies.
	ModeRun Mode = iota
	// ModeSelfCheck is -selfcheck: no network, no password, no database
	// required. It exists to be run from a build host before deployment,
	// and one that quietly required a reachable database would be useless
	// there.
	ModeSelfCheck
	// ModeDBCheck is -dbcheck, which needs the database configured but
	// nothing else about the relay.
	ModeDBCheck
)

// ValidateAll is the complete rule set: everything Validate checks, plus the
// schema rules that need the destination table.
//
// One implementation, three call sites -- startup, -selfcheck, and the SIGHUP
// path -- so the three cannot drift. prev is nil except on reload.
//
// Two rules of Vol 4 section 2.2 are NOT here and cannot be, because they
// need the decoder registry, which arrives in Phase 2: V-T1 (every configured
// transcode resolves to a registered decoder) and V-T6 (each ColSpec.Width
// equals the sum of its mask's declared widths). Unchecked returns their
// identifiers so a caller can say what it has not verified rather than imply
// a clean bill of health.
func ValidateAll(c *Config, dests *destinations.Set, prev *Config, mode Mode) error {
	var p problems
	validateInto(&p, c, prev)
	validateSchema(&p, c, dests)

	// V-DB1. Neither the HLD nor Vol 0 said what a missing db means in
	// production. poc no-ops the write, which is right for a
	// proof-of-concept and wrong for a relay whose only purpose is to
	// persist: it would run, look healthy, and store nothing.
	if mode == ModeRun && !c.HasDB {
		p.add(RuleDBRequired, "no db object is configured. A relay whose purpose is to "+
			"persist must not start without somewhere to persist to; decoder-only "+
			"operation is -selfcheck, never an accident")
	}
	if mode == ModeDBCheck && !c.HasDB {
		p.add(RuleDBRequired, "-dbcheck needs a db object and none is configured")
	}

	return p.err()
}

// Unchecked lists the rule identifiers ValidateAll cannot evaluate, with the
// reason. A pass should never be read as more than it is.
//
// EMPTY since P2-05. V-T1 and V-T6 were the two entries, and both are now
// evaluated -- by decode.BuildRegistry, which is where the catalogue and the
// resolved ColSpecs exist. They report through this package's own taxonomy
// (config.Problem, config.ValidationError, the same V- identifiers), so
// -selfcheck prints them exactly as it prints every other rule and nothing had
// to learn a second error vocabulary.
//
// Kept rather than deleted, and the empty map is the point: the function is the
// contract that a caller may ask "what did you NOT check?", and the answer
// being "nothing" is worth stating explicitly. A future rule that cannot be
// evaluated at load time belongs here on the day it is written, not in a
// comment.
func Unchecked() map[string]string {
	return map[string]string{}
}

func validateSchema(p *problems, c *Config, dests *destinations.Set) {
	if dests == nil {
		return
	}

	// V-D1 and V-D6 are enforced by the destinations loader, which refuses
	// to produce a Set that breaks them. Re-checking here would be a second
	// source of truth; what this does instead is verify the configuration
	// against the map that loaded.

	for i, t := range c.Transcodes {
		where := describeTranscode(i, t.Code)
		validateContributions(p, where, t, dests)
	}
	validateRowKeysDerivable(p, c, dests)
}

func validateContributions(p *problems, where string, t TranscodeConfig, dests *destinations.Set) {
	// V-T5 / ASM-16a: the sum across ALL of one transcode's destinations,
	// because the blob carries every contribution of one message.
	var blobWidth int
	seenDest := map[string]bool{}

	for j, con := range t.Contributions {
		if con.Destination == "" {
			continue // already reported by V-T3's empty-name check
		}

		// V-T3.
		d := dests.ByName(con.Destination)
		if d == nil {
			p.add(RuleContribDest, "%s: contributions[%d] names destination %q, which is "+
				"not declared in the field map", where, j, con.Destination)
			continue
		}
		if seenDest[con.Destination] {
			p.add(RuleContribDest, "%s: destination %q appears twice in its contributions",
				where, con.Destination)
		}
		seenDest[con.Destination] = true

		// A transcode must also be a DECLARED CONTRIBUTOR of the
		// destination it feeds, not merely name it. The contributor list
		// is where the slot comes from, and the slot is what indexes
		// Entry.LastSeq, LastTime and LastRecv. A contribution with no
		// slot has nowhere to record its ordering state, so the guard
		// would compare it against another transcode's values.
		if _, ok := d.Slot(model.TransCode(t.Code)); !ok {
			p.add(RuleContribDest, "%s: contributes to %s but the field map does not list "+
				"it among that destination's contributors, so it has no slot to record "+
				"its ordering state in", where, d.Name)
		}

		// V-T4. This is the rule that makes contribution column lists
		// reloadable at all: they may name only columns the destination
		// already declares, so a reload can never introduce a new bit.
		seenCol := map[string]bool{}
		for _, name := range con.Columns {
			col := findColumn(d, name)
			if col == nil {
				p.add(RuleContribColumn, "%s: contributes %q to %s, which does not declare "+
					"that column", where, name, d.Name)
				continue
			}
			if seenCol[name] {
				p.add(RuleContribColumn, "%s: contributes %q to %s twice",
					where, name, d.Name)
			}
			seenCol[name] = true
			blobWidth += int(col.Width)
		}
	}

	if blobWidth > model.UnitBlobSize {
		p.add(RuleBlobFits, "%s: its contributions total %d bytes across all destinations, "+
			"over UpdateUnit.Blob's %d (ASM-16a). A decoder would overrun the blob",
			where, blobWidth, model.UnitBlobSize)
	}
}

func findColumn(d *destinations.Destination, name string) *destinations.Column {
	for i := range d.Columns {
		if slices.Contains(d.Columns[i].SQL, name) {
			return &d.Columns[i]
		}
	}
	return nil
}

// validateRowKeysDerivable is V-D7, ASM-15 -- the single most valuable rule in
// the set, because the defect it catches is otherwise invisible until
// production.
//
// A destination's row key must be derivable from the shard key of EVERY
// transcode contributing to it. Violate it and two shards write one row:
// updates are intermittently lost because each shard holds its own mirror
// entry and neither sees the other's columns, and HLD section 8.4's deadlock
// argument loses its first layer.
//
// What is checkable HERE, from the configuration and the field map alone: a
// transcode must not contribute to destinations with DIFFERENT row-key rules.
// Units of one transcode are routed by one shard key, so if that transcode
// feeds both a token-keyed and a marketType-keyed destination, one of the two
// is being written from a shard that does not own its rows.
//
// What is NOT checkable here, stated so a pass is not over-read: whether each
// transcode is itself token-scoped or general. That is a property of the
// decoder -- it sets FlagGeneral -- so the complementary half, "a general
// transcode must not feed a token-keyed destination", needs the registry and
// belongs with V-T1 in Phase 2.
func validateRowKeysDerivable(p *problems, c *Config, dests *destinations.Set) {
	for i, t := range c.Transcodes {
		var (
			rules []destinations.RowKeyRule
			names []string
		)
		for _, con := range t.Contributions {
			d := dests.ByName(con.Destination)
			if d == nil {
				continue // V-T3 reported it
			}
			if !slices.Contains(rules, d.Key) {
				rules = append(rules, d.Key)
			}
			names = append(names, d.Name+" keyed by "+d.Key.String())
		}

		if len(rules) > 1 {
			p.add(RuleRowKeyDerivable, "%s: contributes to destinations keyed differently "+
				"(%v). Its units are routed by ONE shard key, so rows of at least one "+
				"of these destinations would be written from a shard that does not own "+
				"them -- two shards writing one row, which loses updates "+
				"intermittently and is invisible until production (ASM-15)",
				describeTranscode(i, t.Code), names)
		}
	}

	// The same defect from the destination's side: every contributor to a
	// destination must be able to produce its row key. With scope
	// unavailable, the checkable part is that a destination's contributors
	// agree with each other, which the loop above establishes transitively
	// -- so what remains is to confirm each declared contributor actually
	// appears in the configuration, since a contributor that never
	// contributes cannot be checked at all.
	configured := map[model.TransCode]bool{}
	for _, t := range c.Transcodes {
		configured[model.TransCode(t.Code)] = true
	}
	for _, d := range dests.All() {
		for i := range d.NContributors {
			code := d.Contributors[i]
			if len(c.Transcodes) > 0 && !configured[code] {
				p.add(RuleRowKeyDerivable, "the field map declares %d as a contributor to %s, "+
					"but no such transcode is configured. Its contributions would never "+
					"arrive and slot %d would stay empty, which silently weakens the "+
					"ordering guard for that row", code, d.Name, i)
			}
		}
	}
}
