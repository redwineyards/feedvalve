package obs

// ID identifies one instrument. Its value is its index into the ledger, and
// into the per-row base table that locates its slots in a slab.
//
// Index constants rather than name lookups, because Vol 0 section 8.4 is a
// MUST: "instruments are resolved ONCE, at startup, into fields or arrays the
// hot path holds directly. Registry.Counter(...) is never called from the
// receive, shard or writer path -- a per-event map lookup and a []Label
// allocation on the receive goroutine is exactly the cost ADR-006 exists to
// avoid."
//
// The ledger in ledger.go is an array literal keyed BY these constants, so a
// constant and its row cannot drift apart: inserting one here without a row
// leaves a zero row, which TestEveryIDHasARow rejects, and there is no
// positional correspondence to maintain by hand.
type ID uint16

// The 131 instruments of Vol 0 section 8.2 and section 8.2a, in ledger order.
//
// 131, not the 129 the task card and section 2 of the task breakdown say. Both
// numbers are right about different things: section 8.2 and section 8.2a hold
// exactly 129 table ROWS (69 and 60), but two rows name several instruments
// each -- gap_audit_agree / gap_audit_disagree, and writes_attempted /
// writes_succeeded / writes_failed -- giving 132 names, of which
// decompress_failed appears in both tables. Section 8.2a's preamble says it
// supersedes the sibling volumes' addition sections, so its {reason}-labelled
// form wins and those two rows are ONE instrument. 129 rows, 131 instruments.
const (
	// Ingress -- section 8.2.
	IDDatagramsReceived ID = iota
	IDDatagramsOversize
	IDDatagramsUnexpectedSource
	IDSubpacketsTotal
	IDSubpacketsCompressed
	IDSubpacketsUncompressed
	IDDecompressedLenMax

	// Decode -- section 8.2.
	IDPacketsByTranscode
	IDUnknownTranscode
	IDUnknownTranscodeOther
	IDMessageHeaderSeen
	IDDecompressFailed
	IDFramingAbandoned
	IDNetidMismatch
	IDErrorResponse
	IDRecordCountClamped
	IDBodyLenMismatch
	IDUnitsEmitted
	IDUnitsPerPacketMax

	// Loss -- section 8.2.
	IDSeqGapEvents
	IDSeqGapMessages
	IDGapAuditAgree
	IDGapAuditDisagree

	// Coalescing -- section 8.2.
	IDUnitsMerged
	IDUnitsApplied
	IDRowsDirtied
	IDUnitsSupersededOutOfOrder
	IDChangeSuppressed

	// Ordering -- section 8.2.
	IDGuardOffered
	IDGuardSuperseded
	IDOrderingGuardDisabled

	// Flush -- section 8.2.
	IDFlushTriggered
	IDFlushCoalesced
	IDFlushOnTick
	IDFlushDeferred

	// Epoch -- section 8.2.
	IDEpochOpened
	IDEpochEmpty
	IDEpochRows
	IDEpochBatches
	IDEpochDurationMaxNs
	IDEpochNsTotal
	IDTickSkippedEpochOpen
	IDRetryRequeued
	IDBatchRowUnreported

	// Stall -- section 8.2.
	IDShardSendBlocked
	IDStallBudgetUsedMaxNs
	IDShardLoopMaxNs

	// Queues -- section 8.2.
	IDShardDepth
	IDDirtyDepth
	IDCohortDepth
	IDBatchqDepth
	IDCompletionLagMaxNs

	// Write pool -- section 8.2.
	IDWritersConfigured
	IDWritersBusy
	IDWritersStopped
	IDRowsInflight
	IDRedirtyTotal

	// Writes -- section 8.2.
	IDWritesAttempted
	IDWritesSucceeded
	IDWritesFailed
	IDBatchesWritten
	IDRowsWritten
	IDRowRejected
	IDWriteRetry

	// Liveness -- section 8.2.
	IDHeartbeatSeen
	IDCircuitState
	IDSessionState

	// Active set -- section 8.2.
	IDActiveTokens1sMax

	// Schema fit -- section 8.2.
	IDMessageTruncated

	// Configuration and lifecycle -- section 8.2.
	IDConfigReloads
	IDConfigReloadRejected
	IDDrainAbandonedRows

	// Receive and decode -- section 8.2a, merged from Vol 1.
	IDDecompressedLenMismatch
	IDSeqRegression
	IDSeqResync
	IDUnconfiguredTranscode
	IDBooktypeSkipped
	IDBlobOverrun
	IDShortPayload
	IDUDPReadErrors
	IDLastDatagramMonoNs

	// Datagram ring and pre-validation -- section 8.2a, new in that merge.
	IDQDDepthMax
	IDQDSendBlocked
	IDQDWakeParked
	IDDecoderLoopMaxNs
	IDPrevalidateRejected
	IDPrevalidateRejectedTranscode
	IDRowsRejectedByOracleUnpredicted

	// Shard and mirror -- section 8.2a, merged from Vol 2.
	IDFlushWakeDropped
	IDFlushWakeReceived
	IDTickSendDropped
	IDRowsCreated
	IDArenaChunks
	IDRingGrown
	IDBlobLenMismatch
	IDMergeIntoQuarantined
	IDRowQuarantined
	IDRowsQuarantinedCurrent
	IDQuarantineReleased
	IDQuarantineSkipped
	IDInflightDesync
	IDCohortNotEmpty
	IDEpochStalled
	IDShardDepthMax

	// Writer and persistence -- section 8.2a, merged from Vol 3.
	IDWritersRunning
	IDCommitDurationMaxNs
	IDWriterBatchDurationMaxNs
	IDArrayBindRowsMax
	IDStmtCacheEntries
	IDStmtCacheUncached
	IDBatchFallbackPerrow
	IDRowsAffectedMismatch
	IDBatchRowEmptyMask
	IDWriterConnRecycled
	IDWriteProbe
	IDOracleError
	IDBindNonfinite
	IDBindBadTime

	// Lifecycle, liveness and observability -- section 8.2a, merged from Vol 4.
	IDPanicsRecovered
	IDCircuitQuietEvents
	IDCircuitDownEvents
	IDCircuitOutageMaxNs
	IDSessionTransitions
	IDSessionAnomalies
	IDSessionEventsDropped
	IDWriterHealthState
	IDLogEventsSuppressed
	IDClockStepDetected
	IDReadbufEffectiveBytes
	IDUptimeSeconds
	IDConfigGeneration

	// numInstruments is the count, and it is asserted against both the
	// declared 131 and the 129-row provenance in ledger_test.go.
	numInstruments
)
