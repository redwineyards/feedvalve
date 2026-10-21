package mirror

import (
	"strconv"
	"sync/atomic"

	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// GuardTable is the per-transcode half of the ordering guard (Vol 2 section
// 5.5): one entry per configured transaction code, holding that code's
// auto-disable bit and its two guard counters.
//
// It lives here, outside the decoder registry (phase 3 ruling D2).
// decode.RegistryEntry has no GuardOff, and the registry is immutable and
// swapped whole on a reload, so a bit stored there would be reset by the swap --
// re-enabling a guard the session had switched off, which section 5.5 point 4
// forbids. Built once by app, shared by every shard, never rebuilt.
//
// Lookup is a linear scan: there are at most eleven configured codes, so a scan
// is a handful of compares and needs no map, hash or allocation.
type GuardTable struct {
	entries []guardEntry
}

// guardEntry is one configured transcode's guard state.
//
// off is written only by the guard evaluator on the stats goroutine,
// Store(true) once and never back (section 5.5 points 3-4); every shard reads
// it. offered and superseded are written by every shard and read only by the
// evaluator (section 12.1): Add is an atomic read-modify-write, so concurrent
// increments from S shards are exact.
type guardEntry struct {
	code       model.TransCode
	off        atomic.Bool
	offered    *obs.Counter // guard_offered{transcode}
	superseded *obs.Counter // guard_superseded{transcode}
}

// NewGuardTable zips the configured codes with their pre-resolved counters.
// All three slices are indexed by configured position -- ascending transaction
// code, the order reg.Codes() returns and TranscodeInstruments is resolved in
// (ruling D2). The counters must already be resolved: resolving them here would
// be after reg.Freeze(), which panics (gate G-OBS-1).
//
// A length mismatch, a nil counter or a repeated code is a wiring defect at
// startup, not a runtime condition, so it panics here rather than leaving a
// nil counter for the first comparison to dereference on a shard goroutine, or
// a second entry whose counters lookup would silently never reach.
func NewGuardTable(codes []model.TransCode, offered, superseded []*obs.Counter) *GuardTable {
	if len(offered) != len(codes) || len(superseded) != len(codes) {
		panic("mirror: NewGuardTable given " + strconv.Itoa(len(codes)) + " codes, " +
			strconv.Itoa(len(offered)) + " guard_offered and " + strconv.Itoa(len(superseded)) +
			" guard_superseded handles; they must be zipped by configured position")
	}
	t := &GuardTable{entries: make([]guardEntry, len(codes))}
	for i, code := range codes {
		if offered[i] == nil || superseded[i] == nil {
			panic("mirror: NewGuardTable given a nil guard counter for transcode " + strconv.Itoa(int(code)) +
				"; the counters must be resolved before the table is built")
		}
		for _, prev := range codes[:i] {
			if prev == code {
				panic("mirror: NewGuardTable given transcode " + strconv.Itoa(int(code)) +
					" twice; lookup would never reach the second entry's counters")
			}
		}
		t.entries[i].code = code
		t.entries[i].offered = offered[i]
		t.entries[i].superseded = superseded[i]
	}
	return t
}

// lookup returns code's entry, or nil if code is not configured. Called once
// per unit and reused across its contributions, which all share u.Code
// (section 5.3, "Cost").
func (t *GuardTable) lookup(code model.TransCode) *guardEntry {
	for i := range t.entries {
		if t.entries[i].code == code {
			return &t.entries[i]
		}
	}
	return nil
}
