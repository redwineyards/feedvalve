package app

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/mirror"
	"feedvalve/internal/obs"
)

// mirrorInstrumentFixture is the configuration every test here resolves
// against: the defaults plus two configured transcodes, so the per-transcode
// loop body actually runs. With none configured -- the lifecycle tests' case --
// it never would, which is exactly how the recorded Build-after-Freeze defect
// went unnoticed (instruments.go, resolveInstruments' comment).
func mirrorInstrumentFixture(t *testing.T) (*config.Config, *destinations.Set) {
	t.Helper()
	cfg := config.Defaults()
	cfg.Transcodes = []config.TranscodeConfig{{Code: 7208}, {Code: 7305}}
	dests, err := destinations.Default()
	if err != nil {
		t.Fatalf("destinations.Default: %v", err)
	}
	return cfg, dests
}

// nilHandles walks a ShardInstruments and returns the path of every nil
// instrument pointer, descending through arrays, slices and the nested struct.
func nilHandles(v reflect.Value, path string) []string {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return []string{path}
		}
	case reflect.Struct:
		var out []string
		for i := range v.NumField() {
			out = append(out, nilHandles(v.Field(i), path+"."+v.Type().Field(i).Name)...)
		}
		return out
	case reflect.Array, reflect.Slice:
		if v.Kind() == reflect.Slice && v.Len() == 0 {
			return []string{path + " (empty)"}
		}
		var out []string
		for i := range v.Len() {
			out = append(out, nilHandles(v.Index(i), path+"["+strconv.Itoa(i)+"]")...)
		}
		return out
	}
	return nil
}

// handle is one resolved instrument pointer and the field path it sits at.
type handle struct {
	path string
	ptr  uintptr
}

// handles walks a ShardInstruments the way nilHandles does and returns every
// non-nil instrument pointer with its path.
func handles(v reflect.Value, path string) []handle {
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			return []handle{{path, v.Pointer()}}
		}
	case reflect.Struct:
		var out []handle
		for i := range v.NumField() {
			out = append(out, handles(v.Field(i), path+"."+v.Type().Field(i).Name)...)
		}
		return out
	case reflect.Array, reflect.Slice:
		var out []handle
		for i := range v.Len() {
			out = append(out, handles(v.Index(i), path+"["+strconv.Itoa(i)+"]")...)
		}
		return out
	}
	return nil
}

// sharedFields are ShardInstruments' top-level fields whose handles are the
// SAME pointers in every shard's set: the unlabelled, {destination} and
// {transcode} ones, and flush_wake_dropped{j}, which every shard indexes by the
// shard it failed to wake (mirror/instruments.go). Every other field is {k} or
// {k,destination} -- the shard's own slot -- so a field added to the struct is
// held to the per-shard rule until it is listed here.
var sharedFields = map[string]bool{
	"UnitsMerged": true, "UnitsApplied": true, "RowsDirtied": true,
	"UnitsSupersededOutOfOrder": true, "ChangeSuppressed": true,
	"FlushTriggered": true, "FlushCoalesced": true, "RedirtyTotal": true,
	"FlushWakeDropped": true,

	"RowsCreated": true, "MergeIntoQuarantined": true, "RowQuarantined": true,
	"RowsQuarantinedCurrent": true, "QuarantineReleased": true,
	"QuarantineSkipped": true, "DrainAbandonedRows": true,
	"RowsRejectedByOracleUnpredicted": true, "PrevalidateRejected": true,
	"Transcode": true,
}

// topField is the top-level field name of a handles path such as
// ".Transcode.GuardOffered[1]" or ".RowsCreated[2]".
func topField(path string) string {
	f := strings.TrimPrefix(path, ".")
	if i := strings.IndexAny(f, ".["); i >= 0 {
		f = f[:i]
	}
	return f
}

// slotProblems checks the pointer identity of every shard's handle set:
//
//	(a) within one shard, no two paths share a pointer -- a wrong ID, or a
//	    fixed index inside a {j}, {transcode}, {destination} or {reason} loop,
//	    aliases two slots;
//	(b) a per-shard path has a different pointer in every shard -- a {k}
//	    handle resolved at a fixed k merges every shard into one slot;
//	(c) a shared path has the same pointer in every shard.
func slotProblems(sets []mirror.ShardInstruments) []string {
	var out []string
	byPath := map[string][]uintptr{}
	var order []string
	for k := range sets {
		seen := map[uintptr]string{}
		for _, h := range handles(reflect.ValueOf(sets[k]), "") {
			if prev, dup := seen[h.ptr]; dup {
				out = append(out, "shard "+strconv.Itoa(k)+": "+prev+" and "+h.path+" are one slot")
			}
			seen[h.ptr] = h.path
			if _, ok := byPath[h.path]; !ok {
				order = append(order, h.path)
			}
			byPath[h.path] = append(byPath[h.path], h.ptr)
		}
	}
	for _, path := range order {
		ptrs := byPath[path]
		if sharedFields[topField(path)] {
			for k := 1; k < len(ptrs); k++ {
				if ptrs[k] != ptrs[0] {
					out = append(out, path+" is shared but differs between shards 0 and "+strconv.Itoa(k))
				}
			}
			continue
		}
		owner := map[uintptr]int{}
		for k, p := range ptrs {
			if j, dup := owner[p]; dup {
				out = append(out, path+" is per-shard but shards "+strconv.Itoa(j)+" and "+
					strconv.Itoa(k)+" share it")
			}
			owner[p] = k
		}
	}
	return out
}

// TestMirrorShardInstrumentsAreResolvedBeforeFreeze: every handle a shard
// writes is resolved by resolveInstruments, which runs before reg.Freeze(), and
// every handle is the slot its labels say it is.
//
// SETUP: the default configuration (4 shards, 3 destinations) with two
// transcodes configured.
//
// STIMULUS: resolveInstruments, then Freeze, as run.go does.
//
// ASSERTION: every pointer in every shard's ShardInstruments is non-nil; within
// a shard no two handles share a slot; every {k} and {k,destination} handle
// differs between shards; every shared handle is the same in every shard.
//
// WOULD HAVE CAUGHT: a handle added to ShardInstruments and never resolved --
// a nil dereference on the shard goroutine on its first increment; a {k}
// handle resolved at a fixed index, which would merge every shard's count into
// one slot; a {j}, {transcode} or {destination} handle resolved at a fixed
// index, or a handle resolved with a neighbour's ID, either of which aliases
// two metrics.
func TestMirrorShardInstrumentsAreResolvedBeforeFreeze(t *testing.T) {
	cfg, dests := mirrorInstrumentFixture(t)
	reg := obs.New(obsOptions(cfg, dests))
	inst := resolveInstruments(reg, cfg, dests)
	reg.Freeze()

	if len(inst.mirrorShards) != cfg.Runtime.Shards || cfg.Runtime.Shards < 2 || dests.Len() != 3 {
		t.Fatalf("setup: %d shard sets for %d shards, %d destinations",
			len(inst.mirrorShards), cfg.Runtime.Shards, dests.Len())
	}
	// Non-vacuity: the walk does find an unresolved handle when there is one.
	if len(nilHandles(reflect.ValueOf(mirror.ShardInstruments{}), "")) == 0 {
		t.Fatal("nilHandles reports nothing for a zero ShardInstruments; the check below is blind")
	}
	for k := range inst.mirrorShards {
		for _, p := range nilHandles(reflect.ValueOf(inst.mirrorShards[k]), "ShardInstruments") {
			t.Errorf("shard %d: %s is not resolved", k, p)
		}
		if n := len(inst.mirrorShards[k].Transcode.GuardOffered); n != len(cfg.Transcodes) {
			t.Errorf("shard %d: %d guard_offered handles, want %d", k, n, len(cfg.Transcodes))
		}
	}

	for _, p := range slotProblems(inst.mirrorShards) {
		t.Error(p)
	}

	// Non-vacuity: each check reports the defect it exists for. The copies are
	// deep where they are mutated, so inst.mirrorShards is left untouched.
	mutated := func(f func(s []mirror.ShardInstruments)) []string {
		sets := make([]mirror.ShardInstruments, len(inst.mirrorShards))
		copy(sets, inst.mirrorShards)
		for k := range sets {
			sets[k].FlushWakeDropped = append([]*obs.Counter(nil), sets[k].FlushWakeDropped...)
		}
		f(sets)
		return slotProblems(sets)
	}
	if len(mutated(func(s []mirror.ShardInstruments) { s[1].FlushDeferred = s[1].FlushOnTick })) == 0 {
		t.Error("check (a) misses a handle resolved with a neighbour's ID")
	}
	if len(mutated(func(s []mirror.ShardInstruments) {
		for k := range s {
			s[k].FlushWakeDropped[1] = s[k].FlushWakeDropped[0]
		}
	})) == 0 {
		t.Error("check (a) misses a {j} handle resolved at a fixed index")
	}
	if len(mutated(func(s []mirror.ShardInstruments) { s[1].EpochOpened = s[0].EpochOpened })) == 0 {
		t.Error("check (b) misses a {k} handle resolved at a fixed index")
	}
	if len(mutated(func(s []mirror.ShardInstruments) { s[1].RowsCreated[0] = new(obs.Counter) })) == 0 {
		t.Error("check (c) misses a shared handle that differs between shards")
	}
}

// TestMirrorShardInstrumentsCannotBeResolvedAfterFreeze pins the defect
// recorded at instruments.go (resolveInstruments' comment): a resolution done
// after reg.Freeze() panics, by gate G-OBS-1's design. The mirror's instruments
// must therefore be resolved inside resolveInstruments and not, say, in
// NewShard or Build -- both of which run after the freeze.
//
// SETUP: a frozen registry. STIMULUS: resolveMirrorShards alone, so the panic
// can only come from a mirror handle's accessor, not from the per-shard
// counters resolveShardInstruments resolves before it.
// ASSERTION: it panics. WOULD HAVE CAUGHT: moving the resolution after Freeze
// believing it harmless -- this is the panic production would hit.
func TestMirrorShardInstrumentsCannotBeResolvedAfterFreeze(t *testing.T) {
	cfg, dests := mirrorInstrumentFixture(t)
	reg := obs.New(obsOptions(cfg, dests))

	// Non-vacuity: the same call on the unfrozen registry succeeds and does
	// resolve the mirror's handles, so the panic below is the freeze's -- not a
	// bad index, and not a function that has stopped resolving anything.
	var ok instruments
	ok.resolveTranscodeInstruments(reg, len(cfg.Transcodes))
	ok.resolveMirrorShards(reg, cfg.Runtime.Shards, dests.Len())
	if len(ok.mirrorShards) != cfg.Runtime.Shards || ok.mirrorShards[0].EpochOpened == nil {
		t.Fatal("resolveMirrorShards resolved no mirror handles; the panic below would prove nothing")
	}

	reg.Freeze()
	defer func() {
		if recover() == nil {
			t.Error("resolving the mirror's instruments after Freeze did not panic")
		}
	}()
	late := instruments{mirrorTranscode: ok.mirrorTranscode}
	late.resolveMirrorShards(reg, cfg.Runtime.Shards, dests.Len())
}
