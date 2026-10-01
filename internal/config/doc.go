// Package config owns the Config root, JSON loading with pointer-optional
// fields, validation, and the atomic.Pointer swap that publishes a new
// generation.
//
// config MUST NOT import framing, decode, mirror or writer.
//
// Fields are pointers so that absent is distinguishable from set to zero.
// Unknown keys are rejected rather than ignored: a typo must fail the load
// loudly, not silently leave a default in place.
//
// Every key carries a reload class -- startup-only, reloadable, or reloadable
// downward only. A reload that violates any rule, including the cross-field
// ones, is rejected wholesale and the previous configuration keeps running. A
// silently clamped safety threshold is worse than a refused file.
//
// A goroutine loads the config pointer once at a declared boundary and uses that
// pointer for the whole unit of work. Re-reading mid-unit lets one Config govern
// the first half of an operation and another the second -- for a shard, two
// different batch size limits inside one epoch.
//
// The database password is not a key here. It comes only from
// FEEDVALVE_DB_PASSWORD; see internal/oradb.
//
// Carried over from poc/config.go.
//
// Specified by docs/lld/00-foundations.md section 7. Built in P1-05.
package config
