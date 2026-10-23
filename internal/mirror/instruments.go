package mirror

import (
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// numPrevalidateReasons is the size of prevalidate_rejected's {reason}
// vocabulary, which obs owns. Written against its last constant so a reason
// added there resizes this array instead of leaving a hole.
const numPrevalidateReasons = int(obs.PrevalidateNonfinite) + 1

// ShardInstruments is every instrument handle one shard writes (Vol 2 section
// 13.3, A11-A25; the Vol 0 set it lists after them; shard_loop_max_ns,
// shard_depth and shard_depth_max; and the three Vol 0 section 5A
// pre-validation instruments).
//
// A bag of resolved pointers and nothing else. app fills it in
// resolveInstruments, BEFORE reg.Freeze(), because every registry accessor
// panics afterwards (gate G-OBS-1) -- the defect recorded at
// internal/app/instruments.go, where a resolution done later than that would
// have panicked on first use. The shard never resolves anything: an increment
// is one atomic on a field (Vol 0 section 8.4).
//
// One per shard. The {k} and {k,destination} fields are that shard's own slots;
// the unlabelled, {destination} and {transcode} fields are the SAME pointers in
// every shard's copy, which is safe because Add is an atomic read-modify-write
// (Vol 2 section 12.1).
//
// Not here, deliberately: row_rejected{destination}, which is the writer's
// (phase 3 ruling D11 -- the shard counts row_quarantined), and
// message_truncated, which is the decoder's (Vol 1 A-07).
type ShardInstruments struct {
	// ---- unlabelled, shared by every shard ----
	UnitsMerged               *obs.Counter
	UnitsApplied              *obs.Counter
	RowsDirtied               *obs.Counter
	UnitsSupersededOutOfOrder *obs.Counter
	ChangeSuppressed          *obs.Counter
	FlushTriggered            *obs.Counter
	FlushCoalesced            *obs.Counter
	RedirtyTotal              *obs.Counter

	// ---- {k}, this shard's own slot ----
	FlushOnTick          *obs.Counter
	FlushDeferred        *obs.Counter
	FlushWakeReceived    *obs.Counter // A12
	TickSendDropped      *obs.Counter // A13 -- written by the ticker, which sends on this shard's tickCh
	EpochOpened          *obs.Counter
	EpochEmpty           *obs.Counter
	EpochRows            *obs.Counter
	EpochBatches         *obs.Counter
	EpochNsTotal         *obs.Counter
	TickSkippedEpochOpen *obs.Counter
	RetryRequeued        *obs.Counter
	BatchRowUnreported   *obs.Counter
	InflightDesync       *obs.Counter // A23
	CohortNotEmpty       *obs.Counter // A24
	EpochStalled         *obs.Counter // A25
	EpochDurationMaxNs   *obs.Max
	ShardLoopMaxNs       *obs.Max
	CompletionLagMaxNs   *obs.Max
	ShardDepthMax        *obs.Max
	ShardDepth           *obs.Gauge
	RowsInflight         *obs.Gauge

	// FlushWakeDropped is A11 for EVERY shard j, indexed by j: Trigger counts a
	// dropped wake against the shard it failed to wake, not the caller.
	FlushWakeDropped []*obs.Counter

	// ---- {k,destination}, this shard's row ----
	DirtyDepth  [model.MaxDestinations]*obs.Gauge
	CohortDepth [model.MaxDestinations]*obs.Gauge
	ArenaChunks [model.MaxDestinations]*obs.Gauge   // A15: STORES the chunk count
	RingGrown   [model.MaxDestinations]*obs.Counter // A16

	// ---- {destination}, shared ----
	RowsCreated                     [model.MaxDestinations]*obs.Counter // A14
	MergeIntoQuarantined            [model.MaxDestinations]*obs.Counter // A18
	RowQuarantined                  [model.MaxDestinations]*obs.Counter // A19
	RowsQuarantinedCurrent          [model.MaxDestinations]*obs.Gauge   // A20
	QuarantineReleased              [model.MaxDestinations]*obs.Counter // A21
	QuarantineSkipped               [model.MaxDestinations]*obs.Counter // A22
	DrainAbandonedRows              [model.MaxDestinations]*obs.Counter
	RowsRejectedByOracleUnpredicted [model.MaxDestinations]*obs.Counter
	PrevalidateRejected             [model.MaxDestinations][numPrevalidateReasons]*obs.Counter

	// ---- {transcode}, shared ----
	Transcode TranscodeInstruments
}

// TranscodeInstruments are the per-transcode handles, each slice indexed by the
// CONFIGURED position -- ascending transaction code, the index obs's
// {transcode} dimension and the decoder registry share (phase 3 ruling D2).
// Resolved once, in app's resolveTranscodeInstruments, and shared by every
// shard.
type TranscodeInstruments struct {
	GuardOffered          []*obs.Counter
	GuardSuperseded       []*obs.Counter
	GuardDisabled         []*obs.Gauge   // ordering_guard_disabled, written by the evaluator
	BlobLenMismatch       []*obs.Counter // A17
	PrevalidateRejectedTC []*obs.Counter // prevalidate_rejected_transcode
}
