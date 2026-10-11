package obs

// shape is the instrument shape of Vol 0 section 8.1. Three, and no others.
type shape uint8

const (
	shapeCounter shape = iota // C -- monotonic total, published with its delta
	shapeMax                  // M -- CAS high-water mark, RESET at emission
	shapeGauge                // G -- published by the owner of the state
)

func (s shape) String() string {
	switch s {
	case shapeCounter:
		return "C"
	case shapeMax:
		return "M"
	case shapeGauge:
		return "G"
	}
	return "?"
}

// row is one ledger entry: everything about an instrument that is frozen by
// the contract.
type row struct {
	// name is the instrument name as the ledger spells it. This is what is
	// emitted, and gate G-OBS-3 asserts the emitted name set equals the set
	// of names here, in both directions.
	name string

	// shape is C, M or G.
	shape shape

	// unit is section 8.2's Unit column. It is documentation and is never
	// emitted -- the line carries no unit token. Section 8.2a dropped the
	// column when it was merged, so the units of those sixty are derived from
	// the instrument's semantics rather than quoted.
	unit string

	// group is the home group: exactly one per instrument, naming the line it
	// is emitted on. The health line (group.go) is a separate projection.
	group Group

	// writer is the goroutine class permitted to write this instrument, from
	// section 8.2's Writer column and section 9.1's ownership table. Also
	// documentation -- obs cannot enforce it -- but it is the record of which
	// goroutine a gauge's Store belongs to, which is the whole reason shape G
	// exists on a platform with no race detector.
	writer string

	// dim is the label dimension, dimNone for the 59 unlabelled instruments.
	dim Dim
}

// ledger is the instrument ledger of Vol 0 section 8.2 and section 8.2a.
//
// Keyed by the ID constants rather than written positionally, so a constant
// cannot drift from its row: there is no ordering to maintain, and an ID
// declared without a row here leaves a zero row that TestEveryIDHasARow
// rejects by name.
var ledger = [numInstruments]row{
	// ---- Ingress (section 8.2) ----
	IDDatagramsReceived:         {"datagrams_received", shapeCounter, "datagrams", GroupIngress, "receiver", dimNone},
	IDDatagramsOversize:         {"datagrams_oversize", shapeCounter, "datagrams", GroupIngress, "receiver", dimNone},
	IDDatagramsUnexpectedSource: {"datagrams_unexpected_source", shapeCounter, "datagrams", GroupIngress, "receiver", dimNone},
	IDSubpacketsTotal:           {"subpackets_total", shapeCounter, "sub-packets", GroupIngress, "framing", dimNone},
	IDSubpacketsCompressed:      {"subpackets_compressed", shapeCounter, "sub-packets", GroupIngress, "framing", dimNone},
	IDSubpacketsUncompressed:    {"subpackets_uncompressed", shapeCounter, "sub-packets", GroupIngress, "framing", dimNone},
	IDDecompressedLenMax:        {"decompressed_len_max", shapeMax, "bytes", GroupIngress, "framing", dimNone},

	// ---- Decode (section 8.2) ----
	IDPacketsByTranscode: {"packets_by_transcode", shapeCounter, "messages", GroupDecode, "decode", dimTranscode},
	IDUnknownTranscode:   {"unknown_transcode", shapeCounter, "messages", GroupDecode, "decode", dimUnknownCode},
	IDUnknownTranscodeOther: {"unknown_transcode_other", shapeCounter, "messages", GroupDecode, "decode",
		dimNone},
	IDMessageHeaderSeen: {"message_header_seen", shapeCounter, "messages", GroupDecode, "decode", dimHeaderCode},
	// decompress_failed is the one instrument named in BOTH tables: unlabelled
	// in section 8.2, refined with {reason} in section 8.2a. The labelled form
	// wins, per section 8.2a's preamble.
	IDDecompressFailed:   {"decompress_failed", shapeCounter, "sub-packets", GroupDecode, "framing", dimDecompressReason},
	IDFramingAbandoned:   {"framing_abandoned", shapeCounter, "datagrams", GroupDecode, "framing", dimNone},
	IDNetidMismatch:      {"netid_mismatch", shapeCounter, "payloads", GroupDecode, "framing", dimNone},
	IDErrorResponse:      {"error_response", shapeCounter, "messages", GroupDecode, "decode", dimNone},
	IDRecordCountClamped: {"record_count_clamped", shapeCounter, "messages", GroupDecode, "decode", dimTranscode},
	IDBodyLenMismatch:    {"body_len_mismatch", shapeCounter, "messages", GroupDecode, "decode", dimTranscode},
	IDUnitsEmitted:       {"units_emitted", shapeCounter, "units", GroupDecode, "decode", dimNone},
	IDUnitsPerPacketMax:  {"units_per_packet_max", shapeMax, "units", GroupDecode, "decode", dimNone},

	// ---- Loss (section 8.2) ----
	IDSeqGapEvents:     {"seq_gap_events", shapeCounter, "events", GroupLoss, "receiver", dimNone},
	IDSeqGapMessages:   {"seq_gap_messages", shapeCounter, "messages", GroupLoss, "receiver", dimNone},
	IDGapAuditAgree:    {"gap_audit_agree", shapeCounter, "messages", GroupLoss, "receiver", dimHypothesis},
	IDGapAuditDisagree: {"gap_audit_disagree", shapeCounter, "messages", GroupLoss, "receiver", dimHypothesis},

	// ---- Coalescing (section 8.2) ----
	IDUnitsMerged:               {"units_merged", shapeCounter, "contributions", GroupCoalescing, "shard", dimNone},
	IDUnitsApplied:              {"units_applied", shapeCounter, "units", GroupCoalescing, "shard", dimNone},
	IDRowsDirtied:               {"rows_dirtied", shapeCounter, "rows", GroupCoalescing, "shard", dimNone},
	IDUnitsSupersededOutOfOrder: {"units_superseded_out_of_order", shapeCounter, "contributions", GroupCoalescing, "shard", dimNone},
	IDChangeSuppressed:          {"change_suppressed", shapeCounter, "rows", GroupCoalescing, "shard", dimNone},

	// ---- Ordering (section 8.2) ----
	IDGuardOffered:          {"guard_offered", shapeCounter, "comparisons", GroupOrdering, "shard", dimTranscode},
	IDGuardSuperseded:       {"guard_superseded", shapeCounter, "comparisons", GroupOrdering, "shard", dimTranscode},
	IDOrderingGuardDisabled: {"ordering_guard_disabled", shapeGauge, "0/1", GroupOrdering, "stats", dimTranscode},

	// ---- Flush (section 8.2) ----
	IDFlushTriggered: {"flush_triggered", shapeCounter, "requests", GroupFlush, "shard", dimNone},
	IDFlushCoalesced: {"flush_coalesced", shapeCounter, "requests", GroupFlush, "shard", dimNone},
	IDFlushOnTick:    {"flush_on_tick", shapeCounter, "epochs", GroupFlush, "shard", dimShard},
	IDFlushDeferred:  {"flush_deferred", shapeCounter, "sends", GroupFlush, "shard", dimShard},

	// ---- Epoch (section 8.2) ----
	IDEpochOpened:          {"epoch_opened", shapeCounter, "epochs", GroupEpoch, "shard", dimShard},
	IDEpochEmpty:           {"epoch_empty", shapeCounter, "attempts", GroupEpoch, "shard", dimShard},
	IDEpochRows:            {"epoch_rows", shapeCounter, "rows", GroupEpoch, "shard", dimShard},
	IDEpochBatches:         {"epoch_batches", shapeCounter, "batches", GroupEpoch, "shard", dimShard},
	IDEpochDurationMaxNs:   {"epoch_duration_max_ns", shapeMax, "ns", GroupEpoch, "shard", dimShard},
	IDEpochNsTotal:         {"epoch_ns_total", shapeCounter, "ns", GroupEpoch, "shard", dimShard},
	IDTickSkippedEpochOpen: {"tick_skipped_epoch_open", shapeCounter, "ticks", GroupEpoch, "shard", dimShard},
	IDRetryRequeued:        {"retry_requeued", shapeCounter, "rows", GroupEpoch, "shard", dimShard},
	IDBatchRowUnreported:   {"batch_row_unreported", shapeCounter, "rows", GroupEpoch, "shard", dimShard},

	// ---- Stall (section 8.2) ----
	IDShardSendBlocked:     {"shard_send_blocked", shapeCounter, "sends", GroupStall, "receiver", dimShard},
	IDStallBudgetUsedMaxNs: {"stall_budget_used_max_ns", shapeMax, "ns", GroupStall, "receiver", dimNone},
	IDShardLoopMaxNs:       {"shard_loop_max_ns", shapeMax, "ns", GroupStall, "shard", dimShard},

	// ---- Queues (section 8.2) ----
	IDShardDepth:         {"shard_depth", shapeGauge, "units", GroupQueues, "shard", dimShard},
	IDDirtyDepth:         {"dirty_depth", shapeGauge, "rows", GroupQueues, "shard", dimShardDest},
	IDCohortDepth:        {"cohort_depth", shapeGauge, "rows", GroupQueues, "shard", dimShardDest},
	IDBatchqDepth:        {"batchq_depth", shapeGauge, "batches", GroupQueues, "stats", dimNone},
	IDCompletionLagMaxNs: {"completion_lag_max_ns", shapeMax, "ns", GroupQueues, "shard", dimShard},

	// ---- Write pool (section 8.2) ----
	IDWritersConfigured: {"writers_configured", shapeGauge, "writers", GroupWritePool, "lifecycle", dimNone},
	IDWritersBusy:       {"writers_busy", shapeGauge, "writers", GroupWritePool, "writer", dimNone},
	IDWritersStopped:    {"writers_stopped", shapeCounter, "writers", GroupWritePool, "writer", dimNone},
	IDRowsInflight:      {"rows_inflight", shapeGauge, "rows", GroupWritePool, "shard", dimShard},
	IDRedirtyTotal:      {"redirty_total", shapeCounter, "rows", GroupWritePool, "shard", dimNone},

	// ---- Writes (section 8.2) ----
	IDWritesAttempted: {"writes_attempted", shapeCounter, "batches", GroupWrites, "writer", dimNone},
	IDWritesSucceeded: {"writes_succeeded", shapeCounter, "batches", GroupWrites, "writer", dimNone},
	IDWritesFailed:    {"writes_failed", shapeCounter, "batches", GroupWrites, "writer", dimNone},
	IDBatchesWritten:  {"batches_written", shapeCounter, "batches", GroupWrites, "writer", dimNone},
	IDRowsWritten:     {"rows_written", shapeCounter, "rows", GroupWrites, "writer", dimDest},
	IDRowRejected:     {"row_rejected", shapeCounter, "rows", GroupWrites, "writer", dimDest},
	IDWriteRetry:      {"write_retry", shapeCounter, "batches", GroupWrites, "writer", dimRetryClass},

	// ---- Liveness (section 8.2) ----
	IDHeartbeatSeen: {"heartbeat_seen", shapeCounter, "messages", GroupLiveness, "decode", dimNone},
	IDCircuitState:  {"circuit_state", shapeGauge, "enum 0-3", GroupLiveness, "health", dimNone},
	IDSessionState:  {"session_state", shapeGauge, "enum 0-5", GroupLiveness, "health", dimMarketType},

	// ---- Active set (section 8.2) ----
	IDActiveTokens1sMax: {"active_tokens_1s_max", shapeMax, "tokens", GroupActiveSet, "receiver", dimNone},

	// ---- Schema fit (section 8.2) ----
	// Writer corrected from shard to decode (Vol 1 A-07): the decoder is the
	// only component that ever sees the unclamped length.
	IDMessageTruncated: {"message_truncated", shapeCounter, "values", GroupSchemaFit, "decode", dimDestColumn},

	// ---- Configuration and lifecycle (section 8.2) ----
	IDConfigReloads:        {"config_reloads", shapeCounter, "reloads", GroupConfigLifecycle, "lifecycle", dimNone},
	IDConfigReloadRejected: {"config_reload_rejected", shapeCounter, "reloads", GroupConfigLifecycle, "lifecycle", dimNone},
	IDDrainAbandonedRows:   {"drain_abandoned_rows", shapeCounter, "rows", GroupConfigLifecycle, "shard", dimDest},

	// ---- Receive and decode (section 8.2a, Vol 1) ----
	IDDecompressedLenMismatch: {"decompressed_len_mismatch", shapeCounter, "messages", GroupDecode, "decode", dimTranscode},
	IDSeqRegression:           {"seq_regression", shapeCounter, "datagrams", GroupLoss, "receiver", dimNone},
	IDSeqResync:               {"seq_resync", shapeCounter, "events", GroupLoss, "receiver", dimNone},
	IDUnconfiguredTranscode:   {"unconfigured_transcode", shapeCounter, "messages", GroupDecode, "decode", dimTranscode},
	IDBooktypeSkipped:         {"booktype_skipped", shapeCounter, "messages", GroupDecode, "decode", dimTranscode},
	IDBlobOverrun:             {"blob_overrun", shapeCounter, "messages", GroupDecode, "decode", dimTranscode},
	IDShortPayload:            {"short_payload", shapeCounter, "messages", GroupDecode, "decode", dimPayloadStage},
	IDUDPReadErrors:           {"udp_read_errors", shapeCounter, "errors", GroupIngress, "receiver", dimNone},
	IDLastDatagramMonoNs:      {"last_datagram_mono_ns", shapeGauge, "ns", GroupLiveness, "receiver", dimNone},

	// ---- Datagram ring and pre-validation (section 8.2a) ----
	IDQDDepthMax:                      {"qd_depth_max", shapeMax, "datagrams", GroupQueues, "receiver", dimNone},
	IDQDSendBlocked:                   {"qd_send_blocked", shapeCounter, "publishes", GroupStall, "receiver", dimNone},
	IDQDWakeParked:                    {"qd_wake_parked", shapeCounter, "parks", GroupQueues, "decoder", dimNone},
	IDDecoderLoopMaxNs:                {"decoder_loop_max_ns", shapeMax, "ns", GroupStall, "decoder", dimNone},
	IDPrevalidateRejected:             {"prevalidate_rejected", shapeCounter, "values", GroupSchemaFit, "shard", dimDestReason},
	IDPrevalidateRejectedTranscode:    {"prevalidate_rejected_transcode", shapeCounter, "values", GroupSchemaFit, "shard", dimTranscode},
	IDRowsRejectedByOracleUnpredicted: {"rows_rejected_by_oracle_unpredicted", shapeCounter, "rows", GroupSchemaFit, "shard", dimDest},

	// ---- Shard and mirror (section 8.2a, Vol 2) ----
	IDFlushWakeDropped:       {"flush_wake_dropped", shapeCounter, "wakeups", GroupFlush, "shard", dimShard},
	IDFlushWakeReceived:      {"flush_wake_received", shapeCounter, "wakeups", GroupFlush, "shard", dimShard},
	IDTickSendDropped:        {"tick_send_dropped", shapeCounter, "ticks", GroupEpoch, "ticker", dimShard},
	IDRowsCreated:            {"rows_created", shapeCounter, "rows", GroupCoalescing, "shard", dimDest},
	IDArenaChunks:            {"arena_chunks", shapeGauge, "chunks", GroupQueues, "shard", dimShardDest},
	IDRingGrown:              {"ring_grown", shapeCounter, "rings", GroupQueues, "shard", dimShardDest},
	IDBlobLenMismatch:        {"blob_len_mismatch", shapeCounter, "units", GroupSchemaFit, "shard", dimTranscode},
	IDMergeIntoQuarantined:   {"merge_into_quarantined", shapeCounter, "merges", GroupSchemaFit, "shard", dimDest},
	IDRowQuarantined:         {"row_quarantined", shapeCounter, "rows", GroupSchemaFit, "shard", dimDest},
	IDRowsQuarantinedCurrent: {"rows_quarantined_current", shapeGauge, "rows", GroupSchemaFit, "shard", dimDest},
	IDQuarantineReleased:     {"quarantine_released", shapeCounter, "rows", GroupSchemaFit, "shard", dimDest},
	IDQuarantineSkipped:      {"quarantine_skipped", shapeCounter, "rows", GroupSchemaFit, "shard", dimDest},
	IDInflightDesync:         {"inflight_desync", shapeCounter, "events", GroupWritePool, "shard", dimShard},
	IDCohortNotEmpty:         {"cohort_not_empty", shapeCounter, "events", GroupEpoch, "shard", dimShard},
	IDEpochStalled:           {"epoch_stalled", shapeCounter, "epochs", GroupEpoch, "shard", dimShard},
	IDShardDepthMax:          {"shard_depth_max", shapeMax, "units", GroupStall, "shard", dimShard},

	// ---- Writer and persistence (section 8.2a, Vol 3) ----
	IDWritersRunning:           {"writers_running", shapeGauge, "writers", GroupWritePool, "writer", dimNone},
	IDCommitDurationMaxNs:      {"commit_duration_max_ns", shapeMax, "ns", GroupWrites, "writer", dimNone},
	IDWriterBatchDurationMaxNs: {"writer_batch_duration_max_ns", shapeMax, "ns", GroupWrites, "writer", dimNone},
	IDArrayBindRowsMax:         {"array_bind_rows_max", shapeMax, "rows", GroupWrites, "writer", dimDest},
	IDStmtCacheEntries:         {"stmt_cache_entries", shapeGauge, "statements", GroupWritePool, "writer", dimWriter},
	IDStmtCacheUncached:        {"stmt_cache_uncached", shapeCounter, "executions", GroupWritePool, "writer", dimNone},
	IDBatchFallbackPerrow:      {"batch_fallback_perrow", shapeCounter, "batches", GroupWrites, "writer", dimNone},
	IDRowsAffectedMismatch:     {"rows_affected_mismatch", shapeCounter, "rows", GroupWrites, "writer", dimDest},
	IDBatchRowEmptyMask:        {"batch_row_empty_mask", shapeCounter, "rows", GroupWrites, "writer", dimDest},
	IDWriterConnRecycled:       {"writer_conn_recycled", shapeCounter, "connections", GroupWritePool, "writer", dimRecycleReason},
	IDWriteProbe:               {"write_probe", shapeCounter, "probes", GroupWritePool, "writer", dimProbeResult},
	IDOracleError:              {"oracle_error", shapeCounter, "errors", GroupWrites, "writer", dimOracleCode},
	IDBindNonfinite:            {"bind_nonfinite", shapeCounter, "values", GroupSchemaFit, "writer", dimDestColumn},
	IDBindBadTime:              {"bind_bad_time", shapeCounter, "values", GroupSchemaFit, "writer", dimDestColumn},

	// ---- Lifecycle, liveness and observability (section 8.2a, Vol 4) ----
	IDPanicsRecovered:       {"panics_recovered", shapeCounter, "panics", GroupConfigLifecycle, "decode/shard/writer/stats", dimPanicStage},
	IDCircuitQuietEvents:    {"circuit_quiet_events", shapeCounter, "events", GroupLiveness, "health", dimNone},
	IDCircuitDownEvents:     {"circuit_down_events", shapeCounter, "events", GroupLiveness, "health", dimNone},
	IDCircuitOutageMaxNs:    {"circuit_outage_max_ns", shapeMax, "ns", GroupLiveness, "health", dimNone},
	IDSessionTransitions:    {"session_transitions", shapeCounter, "transitions", GroupLiveness, "health", dimMarketType},
	IDSessionAnomalies:      {"session_anomalies", shapeCounter, "events", GroupLiveness, "health", dimMarketType},
	IDSessionEventsDropped:  {"session_events_dropped", shapeCounter, "events", GroupLiveness, "decoder", dimNone},
	IDWriterHealthState:     {"writer_health_state", shapeGauge, "enum 0-2", GroupWritePool, "writer", dimNone},
	IDLogEventsSuppressed:   {"log_events_suppressed", shapeCounter, "events", GroupConfigLifecycle, "obs", dimLogClass},
	IDClockStepDetected:     {"clock_step_detected", shapeCounter, "events", GroupConfigLifecycle, "stats", dimNone},
	IDReadbufEffectiveBytes: {"readbuf_effective_bytes", shapeGauge, "bytes", GroupIngress, "lifecycle", dimNone},
	IDUptimeSeconds:         {"uptime_seconds", shapeGauge, "s", GroupConfigLifecycle, "stats", dimNone},
	IDConfigGeneration:      {"config_generation", shapeGauge, "generation", GroupConfigLifecycle, "lifecycle", dimNone},
}

// ledgerRows is the number of TABLE ROWS in Vol 0 section 8.2 plus section
// 8.2a -- 69 and 60 -- which is the 129 the task breakdown quotes. It is not
// the instrument count; see the comment on the ID constants.
const ledgerRows = 129

// multiNameRows is the number of extra names contributed by rows that name
// several instruments: gap_audit_agree / gap_audit_disagree gives one extra,
// and writes_attempted / writes_succeeded / writes_failed gives two.
const multiNameRows = 3

// duplicatedNames is decompress_failed, which appears in both tables.
const duplicatedNames = 1

// The arithmetic that reconciles the two counts, pinned so neither can be
// edited without the other being reconsidered: 129 rows + 3 extra names - 1
// duplicate = 131 instruments.
const _ = uint(ledgerRows+multiNameRows-duplicatedNames-numInstruments) +
	uint(numInstruments-(ledgerRows+multiNameRows-duplicatedNames))
