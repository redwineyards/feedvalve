package mirror

import (
	"hash/maphash"
	"time"
)

// The configuration seam (phase 3 ruling D1).
//
// mirror may not import config: config carries oradb.DBSettings, so the edge
// would make the shard reach database/sql transitively, which gate G-DEP-1
// forbids. So mirror declares the two halves of the configuration it reads, as
// plain values, and app projects config onto them -- the same move obsOptions
// and transcodeSpecs make for obs and decode. Only fields mirror reads are here.

// Sizing is the startup-only half: everything that sizes a structure NewShard
// allocates. It cannot change for the life of the process (Vol 0 section 7.2),
// which is why it is a value handed over once rather than something loaded.
type Sizing struct {
	Shards              int // runtime.shards
	ExpectedRows        int // runtime.expectedRows; Index, Accum and Cohort are pre-sized at ExpectedRows/Shards
	ArenaChunkRows      int // runtime.arenaChunkRows; entries per arena chunk
	MaxInFlightPerShard int // runtime.maxInFlightPerShard; batches in Free, m
	MaxBatchSizeCeiling int // runtime.maxBatchSizeCeiling; len(Batch.Rows), and the failedRows scratch

	// Seed is the one process-wide digest seed (Vol 0 section 5.5). Every
	// shard must hash with the same one, or a row's digest would depend on
	// which shard computed it.
	Seed maphash.Seed
}

// Params is the reloadable half. A published *Params is immutable: a reload
// builds a new one. The shard loads it once at epoch open and once at each tick
// (ruling D3) and holds that pointer for the unit of work, so one epoch never
// sees two MaxBatchSize values.
type Params struct {
	// Gen is the configuration generation (config.Config.Gen). Generations
	// start at 1, so the shard initialises cfgGen from the first Params it
	// loads; a cfgGen of 0 would make the first epoch "release" a quarantine
	// that cannot yet hold anything.
	Gen uint64

	Window         time.Duration // flush.windowMs
	MaxBatchSize   int           // flush.maxBatchSize, within Sizing.MaxBatchSizeCeiling
	EpochStallWarn time.Duration // flush.epochStallWarnMs
	DrainTimeout   time.Duration // flush.drainTimeoutMs; one shared budget (ruling D4)

	Guard                 GuardMode     // ordering.guard
	GuardSuppressionRatio float64       // ordering.guardSuppressionRatio
	GuardMinSamples       int           // ordering.guardMinSamples
	GuardEvalInterval     time.Duration // ordering.guardEvalIntervalSec
}

// GuardMode is ordering.guard, projected from config's string by app so the
// shard compares an integer on the merge path rather than a string.
type GuardMode uint8

const (
	GuardAuto GuardMode = iota // the default: on, and auto-disabled per transcode (Vol 2 section 5.5)
	GuardOn                    // on, never auto-disabled
	GuardOff                   // bypassed and not counted; safe by construction (Vol 2 section 5.1)
)
