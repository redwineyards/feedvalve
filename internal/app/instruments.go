package app

import (
	"errors"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/mirror"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
	"feedvalve/internal/writer"
)

// instruments are the lifecycle's own instrument handles, resolved once and
// held as pointers.
//
// Resolved BEFORE obs.Freeze, because gate G-OBS-1 makes every accessor panic
// afterwards -- which is the whole point of that gate: Vol 0 section 8.4's MUST
// is that instruments reach a struct field and never a map lookup on any path
// that runs per datagram. So the resolution happens here, at startup step 9,
// and the goroutines hold the pointers.
//
// Only the instruments the LIFECYCLE writes are here. The ledger assigns owners
// per instrument (internal/obs/ledger.go), and the receiver's, decoder's, shard's
// and writer's own handles belong to those components as they arrive.
type instruments struct {
	// lifecycle-owned
	configGeneration     *obs.Gauge
	configReloads        *obs.Counter
	configReloadRejected *obs.Counter
	readbufEffective     *obs.Gauge
	writersConfigured    *obs.Gauge

	// receiver-owned, held here until Phase 2 gives the receiver a home
	datagramsReceived      *obs.Counter
	datagramsOversize      *obs.Counter
	datagramsUnexpectedSrc *obs.Counter
	qdSendBlocked          *obs.Counter
	qdDepthMax             *obs.Max
	udpReadErrors          *obs.Counter
	lastDatagramMonoNs     *obs.Gauge

	// decoder-owned, likewise
	qdWakeParked *obs.Counter

	// The fan-out's two (P2-08). The ledger assigns both to the receiver, which
	// in this topology is the decoder goroutine -- the same reconciliation
	// loadRuntime records, and for the same reason: the contract draws read,
	// decode and route as one goroutine, and QD was inserted between the first
	// and the second.
	//
	// shardSendBlocked is indexed by shard, resolved by resolveShardInstruments
	// once the arity is known. Against a shard the design computes as 0.08%
	// busy, ANY non-zero value here is a defect rather than load -- it is the
	// only early warning before the fatal path (Vol 4 F24, then F9).
	shardSendBlocked     []*obs.Counter
	stallBudgetUsedMaxNs *obs.Max

	// mirrorShards is every handle shard k writes, one struct per shard
	// (P3-04), resolved here for the same reason as everything else in this
	// struct: Freeze ends resolution, so a shard that resolved its own would
	// panic on its first increment. mirrorTranscode is the per-transcode part,
	// resolved in resolveTranscodeInstruments by configured position (phase 3
	// ruling D2) and shared by every shard's copy.
	mirrorShards    []mirror.ShardInstruments
	mirrorTranscode mirror.TranscodeInstruments

	// writer is every handle the writer pool writes (P4-10), resolved here for
	// the same reason. Nil only if the registry is not an *obs.Reg, which the
	// writer's concrete-typed resolvers need; NewPool accepts nil.
	writer *writer.Instruments

	// The sub-packet path (P2-05). Resolved here, before Freeze, because
	// gate G-OBS-1's whole point is that an instrument reaches a struct field
	// and never a map lookup on a path that runs per datagram -- and these run
	// per SUB-PACKET, three to four times per datagram.
	//
	// The labelled ones are held as arrays indexed by the label's own type, so
	// the call site reads in.shortPayload[obs.PayloadBody] rather than
	// resolving a slot per message.
	subpacketsTotal        *obs.Counter
	subpacketsCompressed   *obs.Counter
	subpacketsUncompressed *obs.Counter
	decompressedLenMax     *obs.Max
	decompressFailed       [numDecompressReasons]*obs.Counter
	framingAbandoned       *obs.Counter
	netidMismatch          *obs.Counter
	errorResponse          *obs.Counter
	shortPayload           [numPayloadStages]*obs.Counter
	panicsRecovered        *obs.Counter
	messageHeaderSeen      [numMessageHeaderCodes]*obs.Counter
	heartbeatSeen          *obs.Counter

	// Per-transcode, indexed by the REGISTRY's slot. The registry's codes and
	// obs's {code} dimension are both the configured inventory in ascending
	// order, so one index serves both -- which is what keeps per-transcode
	// counting off a map on the receive path (Vol 1 section 7.7).
	packetsByTranscode      []*obs.Counter
	bodyLenMismatch         []*obs.Counter
	decompressedLenMismatch []*obs.Counter
	recordCountClamped      []*obs.Counter
	booktypeSkipped         []*obs.Counter
	blobOverrun             []*obs.Counter

	// The gap detector's counters (P2-07). seq_gap_messages is the number to
	// trend: it is the only loss figure this system can produce, because
	// everything after the socket is lossless by construction.
	seqGapEvents     *obs.Counter
	seqGapMessages   *obs.Counter
	seqRegression    *obs.Counter
	seqResync        *obs.Counter
	gapAuditAgree    [numGapHypotheses]*obs.Counter
	gapAuditDisagree [numGapHypotheses]*obs.Counter

	// The decoders' own reports (P2-06), reached through decodeCounters.
	unitsEmitted   *obs.Counter
	unitsPerPacket *obs.Max

	// activeTokens1sMax closes req section 12.4: the distinct tokens seen in
	// one second, which is the figure the requirement asks to be DERIVED and
	// this measures directly. Declared in the ledger since P1-07 and resolved
	// by nothing until P2-09 -- an allocated slab slot that could never carry a
	// number.
	activeTokens1sMax *obs.Max

	// messageTruncated is message_truncated{destination,column}, RAGGED: one
	// row per destination, each as long as that destination's own column list
	// (16, 9 and 6). Ragged rather than rectangular because the arity differs,
	// and rectangular would either waste slots or need index arithmetic of its
	// own -- but resolved at STARTUP either way.
	//
	// It used to be a func that called reg.CounterAt2 at the moment of use, on
	// the reasoning that a two-dimensional label is awkward to pre-resolve and
	// that truncation is not the hot path. Both true, and both beside the
	// point: resolution panics once the registry is frozen (gate G-OBS-1), and
	// the call site sits inside the decode recover boundary -- so the first
	// truncated value in production would have panicked, been swallowed as
	// panics_recovered{decode}, cost that message its units, and never
	// incremented the counter an operator was reading to find out which column
	// is being cut. Vol 0 section 8.4's MUST is not a performance note.
	messageTruncated [][]*obs.Counter

	// log is how a counter that needs a line gets one. Held here rather than
	// passed, because decodeCounters is constructed before the graph.
	log *obs.AppLogger

	// unknownTranscode is a FUNCTION, not a handle, because it is the one
	// dynamic label in the ledger: the code is by definition not knowable at
	// startup, so the slot is claimed by a CAS at emission. It is also the
	// documented exception to G-OBS-1, which is why it is reached through the
	// registry rather than resolved here.
	unknownTranscode func(code int) (*obs.Counter, bool)

	// unconfiguredTranscode is the sibling branch of ADR-011's distinction,
	// and it shares the same dynamic table -- so a code claims one slot and
	// both counters index it.
	unconfiguredTranscode func(code int) (*obs.Counter, bool)

	// stats-owned. batchqDepth is len(batchQ), sampled at emission -- race-free
	// on a channel, which is why the ledger lets the stats goroutine own it.
	clockStepDetected *obs.Counter
	batchqDepth       *obs.Gauge

	// panics_recovered{stage=shard}: F26's observable (Vol 4 section 5),
	// counted by the shard goroutine's recover in runShard.
	panicsRecoveredShard *obs.Counter

	// panics_recovered{stage=stats}: F28's observable (P5-13).
	panicsRecoveredStats *obs.Counter

	// health-owned (P5-11): the circuit machine's instruments, written only by
	// the stats goroutine. The session ones are indexed by market type minus
	// one, the same slot rule obs's {market_type} dimension uses; the machine
	// is fed by channel H (P5-12), and session_events_dropped is the one the
	// decoder side writes (ruling D5).
	circuitState         *obs.Gauge
	circuitQuietEvents   *obs.Counter
	circuitDownEvents    *obs.Counter
	circuitOutageMaxNs   *obs.Max
	sessionState         [numMarketTypes]*obs.Gauge
	sessionTransitions   [numMarketTypes]*obs.Counter
	sessionAnomalies     [numMarketTypes]*obs.Counter
	sessionEventsDropped *obs.Counter
}

// numMarketTypes is the {market_type} dimension: 1 Normal, 2 Odd Lot, 3 Spot,
// 4 Auction (bible Appendix A). Slot = market type minus one.
const numMarketTypes = 4

// resolveInstruments performs the resolution half of startup step 9. The caller
// freezes the registry afterwards.
//
// EVERY instrument this process resolves is resolved here, including the two
// labelled families whose arity comes from the configuration. It takes the
// *config.Config for exactly that: len(cfg.Transcodes) and
// cfg.Runtime.Shards are the arities, and they are the same two numbers
// obsOptions gave the registry, so the resolution cannot ask for an index obs
// never allocated.
//
// It is ONE function because the alternative was tried and was wrong.
// resolveTranscodeInstruments used to be called from Build, on the reasoning
// that the arity belongs with the decoder registry it must agree with -- but
// Build runs AFTER reg.Freeze() (run.go step 9), so every accessor in it
// panics by gate G-OBS-1's design. No test caught it: the lifecycle tests
// build with no transcodes configured, so the loop body never ran, and the
// sub-packet tests construct a Graph directly and resolve these themselves
// before freezing. The first configuration to declare a transcode would have
// panicked at startup. The agreement Build was protecting is still asserted
// there, where the decoder registry exists; only the resolution moved to the
// one place that is allowed to resolve.
// It takes the destination set for the same reason it takes the config: the
// {destination,column} label's arity is the destination table's own shape, and
// it is the same shape obsOptions gave the registry, so the resolution cannot
// ask for an index obs never allocated.
func resolveInstruments(reg obs.Registry, cfg *config.Config, dests *destinations.Set) instruments {
	in := resolveStaticInstruments(reg)
	in.resolveTranscodeInstruments(reg, len(cfg.Transcodes))
	in.resolveShardInstruments(reg, cfg.Runtime.Shards, dests.Len())
	in.resolveDestinationInstruments(reg, dests)
	in.resolveWriterInstruments(reg, dests.Len())
	in.resolveSessionInstruments(reg)
	return in
}

// resolveSessionInstruments resolves the {market_type} family of the session
// machine.
func (in *instruments) resolveSessionInstruments(reg obs.Registry) {
	for i := range numMarketTypes {
		in.sessionState[i] = reg.GaugeAt(obs.IDSessionState, i)
		in.sessionTransitions[i] = reg.CounterAt(obs.IDSessionTransitions, i)
		in.sessionAnomalies[i] = reg.CounterAt(obs.IDSessionAnomalies, i)
	}
}

// resolveDestinationInstruments resolves the {destination,column} family, whose
// arity is ragged: each destination has its own column count.
func (in *instruments) resolveDestinationInstruments(reg obs.Registry, dests *destinations.Set) {
	all := dests.All()
	in.messageTruncated = make([][]*obs.Counter, len(all))
	for d := range all {
		cols := all[d].Columns
		row := make([]*obs.Counter, len(cols))
		for c := range cols {
			row[c] = reg.CounterAt2(obs.IDMessageTruncated, d, c)
		}
		in.messageTruncated[d] = row
	}
}

// resolveStaticInstruments resolves everything whose arity is a vocabulary obs
// owns rather than a number the configuration chose.
func resolveStaticInstruments(reg obs.Registry) instruments {
	return instruments{
		configGeneration:     reg.Gauge(obs.IDConfigGeneration),
		configReloads:        reg.Counter(obs.IDConfigReloads),
		configReloadRejected: reg.Counter(obs.IDConfigReloadRejected),
		readbufEffective:     reg.Gauge(obs.IDReadbufEffectiveBytes),
		writersConfigured:    reg.Gauge(obs.IDWritersConfigured),

		datagramsReceived:      reg.Counter(obs.IDDatagramsReceived),
		datagramsOversize:      reg.Counter(obs.IDDatagramsOversize),
		datagramsUnexpectedSrc: reg.Counter(obs.IDDatagramsUnexpectedSource),
		qdSendBlocked:          reg.Counter(obs.IDQDSendBlocked),
		qdDepthMax:             reg.Max(obs.IDQDDepthMax),
		udpReadErrors:          reg.Counter(obs.IDUDPReadErrors),
		lastDatagramMonoNs:     reg.Gauge(obs.IDLastDatagramMonoNs),

		qdWakeParked: reg.Counter(obs.IDQDWakeParked),

		clockStepDetected:    reg.Counter(obs.IDClockStepDetected),
		batchqDepth:          reg.Gauge(obs.IDBatchqDepth),
		panicsRecoveredShard: reg.CounterAt(obs.IDPanicsRecovered, int(obs.StageShard)),
		panicsRecoveredStats: reg.CounterAt(obs.IDPanicsRecovered, int(obs.StageStats)),

		circuitState:         reg.Gauge(obs.IDCircuitState),
		circuitQuietEvents:   reg.Counter(obs.IDCircuitQuietEvents),
		circuitDownEvents:    reg.Counter(obs.IDCircuitDownEvents),
		circuitOutageMaxNs:   reg.Max(obs.IDCircuitOutageMaxNs),
		sessionEventsDropped: reg.Counter(obs.IDSessionEventsDropped),

		subpacketsTotal:        reg.Counter(obs.IDSubpacketsTotal),
		subpacketsCompressed:   reg.Counter(obs.IDSubpacketsCompressed),
		subpacketsUncompressed: reg.Counter(obs.IDSubpacketsUncompressed),
		decompressedLenMax:     reg.Max(obs.IDDecompressedLenMax),
		framingAbandoned:       reg.Counter(obs.IDFramingAbandoned),
		netidMismatch:          reg.Counter(obs.IDNetidMismatch),
		errorResponse:          reg.Counter(obs.IDErrorResponse),
		panicsRecovered:        reg.CounterAt(obs.IDPanicsRecovered, int(obs.StageDecode)),
		heartbeatSeen:          reg.Counter(obs.IDHeartbeatSeen),

		decompressFailed: [numDecompressReasons]*obs.Counter{
			obs.ReasonInputOverrun:      reg.CounterAt(obs.IDDecompressFailed, int(obs.ReasonInputOverrun)),
			obs.ReasonOutputOverrun:     reg.CounterAt(obs.IDDecompressFailed, int(obs.ReasonOutputOverrun)),
			obs.ReasonLookbehindOverrun: reg.CounterAt(obs.IDDecompressFailed, int(obs.ReasonLookbehindOverrun)),
			obs.ReasonInputNotConsumed:  reg.CounterAt(obs.IDDecompressFailed, int(obs.ReasonInputNotConsumed)),
		},
		shortPayload: [numPayloadStages]*obs.Counter{
			obs.PayloadHeader: reg.CounterAt(obs.IDShortPayload, int(obs.PayloadHeader)),
			obs.PayloadBody:   reg.CounterAt(obs.IDShortPayload, int(obs.PayloadBody)),
		},
		messageHeaderSeen: [numMessageHeaderCodes]*obs.Counter{
			obs.HeaderHeartbeat: reg.CounterAt(obs.IDMessageHeaderSeen, int(obs.HeaderHeartbeat)),
			obs.HeaderAssetRate: reg.CounterAt(obs.IDMessageHeaderSeen, int(obs.HeaderAssetRate)),
		},

		unknownTranscode:      unknownTranscodeFunc(reg),
		unconfiguredTranscode: unconfiguredTranscodeFunc(reg),

		seqGapEvents:   reg.Counter(obs.IDSeqGapEvents),
		seqGapMessages: reg.Counter(obs.IDSeqGapMessages),
		seqRegression:  reg.Counter(obs.IDSeqRegression),
		seqResync:      reg.Counter(obs.IDSeqResync),
		gapAuditAgree: [numGapHypotheses]*obs.Counter{
			obs.HypothesisGlobal: reg.CounterAt(obs.IDGapAuditAgree,
				int(obs.HypothesisGlobal)),
			obs.HypothesisPerTranscode: reg.CounterAt(obs.IDGapAuditAgree,
				int(obs.HypothesisPerTranscode)),
		},
		gapAuditDisagree: [numGapHypotheses]*obs.Counter{
			obs.HypothesisGlobal: reg.CounterAt(obs.IDGapAuditDisagree,
				int(obs.HypothesisGlobal)),
			obs.HypothesisPerTranscode: reg.CounterAt(obs.IDGapAuditDisagree,
				int(obs.HypothesisPerTranscode)),
		},
		unitsEmitted:      reg.Counter(obs.IDUnitsEmitted),
		unitsPerPacket:    reg.Max(obs.IDUnitsPerPacketMax),
		activeTokens1sMax: reg.Max(obs.IDActiveTokens1sMax),

		stallBudgetUsedMaxNs: reg.Max(obs.IDStallBudgetUsedMaxNs),
	}
}

// resolveShardInstruments resolves the per-shard counters, whose arity is
// runtime.shards -- the same number obsOptions gave the registry -- and then
// the mirror's per-shard instrument sets. Runs after
// resolveTranscodeInstruments, whose per-transcode handles every shard shares.
func (in *instruments) resolveShardInstruments(reg obs.Registry, shards, dests int) {
	in.shardSendBlocked = make([]*obs.Counter, shards)
	for k := range shards {
		in.shardSendBlocked[k] = reg.CounterAt(obs.IDShardSendBlocked, k)
	}
	in.resolveMirrorShards(reg, shards, dests)
}

// resolveMirrorShards resolves one mirror.ShardInstruments per shard. The
// {destination} arity is the destination table's, the number obsOptions gave
// the registry. Its own function so a test can resolve the mirror's handles,
// and only those, against a frozen registry.
func (in *instruments) resolveMirrorShards(reg obs.Registry, shards, dests int) {
	// flush_wake_dropped{j} for every j, shared: Trigger counts a dropped wake
	// against the shard it failed to wake, whichever shard called it.
	wakeDropped := make([]*obs.Counter, shards)
	for j := range shards {
		wakeDropped[j] = reg.CounterAt(obs.IDFlushWakeDropped, j)
	}

	in.mirrorShards = make([]mirror.ShardInstruments, shards)
	for k := range shards {
		m := mirror.ShardInstruments{
			UnitsMerged:               reg.Counter(obs.IDUnitsMerged),
			UnitsApplied:              reg.Counter(obs.IDUnitsApplied),
			RowsDirtied:               reg.Counter(obs.IDRowsDirtied),
			UnitsSupersededOutOfOrder: reg.Counter(obs.IDUnitsSupersededOutOfOrder),
			ChangeSuppressed:          reg.Counter(obs.IDChangeSuppressed),
			FlushTriggered:            reg.Counter(obs.IDFlushTriggered),
			FlushCoalesced:            reg.Counter(obs.IDFlushCoalesced),
			RedirtyTotal:              reg.Counter(obs.IDRedirtyTotal),

			FlushOnTick:          reg.CounterAt(obs.IDFlushOnTick, k),
			FlushDeferred:        reg.CounterAt(obs.IDFlushDeferred, k),
			FlushWakeReceived:    reg.CounterAt(obs.IDFlushWakeReceived, k),
			TickSendDropped:      reg.CounterAt(obs.IDTickSendDropped, k),
			EpochOpened:          reg.CounterAt(obs.IDEpochOpened, k),
			EpochEmpty:           reg.CounterAt(obs.IDEpochEmpty, k),
			EpochRows:            reg.CounterAt(obs.IDEpochRows, k),
			EpochBatches:         reg.CounterAt(obs.IDEpochBatches, k),
			EpochNsTotal:         reg.CounterAt(obs.IDEpochNsTotal, k),
			TickSkippedEpochOpen: reg.CounterAt(obs.IDTickSkippedEpochOpen, k),
			RetryRequeued:        reg.CounterAt(obs.IDRetryRequeued, k),
			BatchRowUnreported:   reg.CounterAt(obs.IDBatchRowUnreported, k),
			InflightDesync:       reg.CounterAt(obs.IDInflightDesync, k),
			CohortNotEmpty:       reg.CounterAt(obs.IDCohortNotEmpty, k),
			EpochStalled:         reg.CounterAt(obs.IDEpochStalled, k),
			EpochDurationMaxNs:   reg.MaxAt(obs.IDEpochDurationMaxNs, k),
			ShardLoopMaxNs:       reg.MaxAt(obs.IDShardLoopMaxNs, k),
			CompletionLagMaxNs:   reg.MaxAt(obs.IDCompletionLagMaxNs, k),
			ShardDepthMax:        reg.MaxAt(obs.IDShardDepthMax, k),
			ShardDepth:           reg.GaugeAt(obs.IDShardDepth, k),
			RowsInflight:         reg.GaugeAt(obs.IDRowsInflight, k),

			FlushWakeDropped: wakeDropped,
			Transcode:        in.mirrorTranscode,
		}
		for d := range dests {
			m.DirtyDepth[d] = reg.GaugeAt2(obs.IDDirtyDepth, k, d)
			m.CohortDepth[d] = reg.GaugeAt2(obs.IDCohortDepth, k, d)
			m.ArenaChunks[d] = reg.GaugeAt2(obs.IDArenaChunks, k, d)
			m.RingGrown[d] = reg.CounterAt2(obs.IDRingGrown, k, d)

			m.RowsCreated[d] = reg.CounterAt(obs.IDRowsCreated, d)
			m.MergeIntoQuarantined[d] = reg.CounterAt(obs.IDMergeIntoQuarantined, d)
			m.RowQuarantined[d] = reg.CounterAt(obs.IDRowQuarantined, d)
			m.RowsQuarantinedCurrent[d] = reg.GaugeAt(obs.IDRowsQuarantinedCurrent, d)
			m.QuarantineReleased[d] = reg.CounterAt(obs.IDQuarantineReleased, d)
			m.QuarantineSkipped[d] = reg.CounterAt(obs.IDQuarantineSkipped, d)
			m.DrainAbandonedRows[d] = reg.CounterAt(obs.IDDrainAbandonedRows, d)
			m.RowsRejectedByOracleUnpredicted[d] = reg.CounterAt(obs.IDRowsRejectedByOracleUnpredicted, d)
			for r := range m.PrevalidateRejected[d] {
				m.PrevalidateRejected[d][r] = reg.CounterAt2(obs.IDPrevalidateRejected, d, r)
			}
		}
		in.mirrorShards[k] = m
	}
}

// The label arities, named rather than written as 2 and 4 at the array
// declarations: each is the size of a vocabulary obs owns, and a value added
// there must not silently leave a hole here.
const (
	numDecompressReasons  = 4 // input | output | lookbehind | not-consumed
	numPayloadStages      = 2 // header | body
	numMessageHeaderCodes = 2 // 6541 | 6503
	numGapHypotheses      = 2 // global | per-transcode
)

// resolveTranscodeInstruments resolves the per-transcode counters.
//
// The arity is the CONFIGURED inventory's size, which the instrument registry
// learns from obsOptions and the decoder registry learns from the same
// configuration -- so the two agree by construction rather than by
// coincidence, and Build asserts it once both exist.
func (in *instruments) resolveTranscodeInstruments(reg obs.Registry, n int) {
	in.packetsByTranscode = make([]*obs.Counter, n)
	in.bodyLenMismatch = make([]*obs.Counter, n)
	in.decompressedLenMismatch = make([]*obs.Counter, n)
	in.recordCountClamped = make([]*obs.Counter, n)
	in.booktypeSkipped = make([]*obs.Counter, n)
	in.blobOverrun = make([]*obs.Counter, n)
	for i := range n {
		in.packetsByTranscode[i] = reg.CounterAt(obs.IDPacketsByTranscode, i)
		in.bodyLenMismatch[i] = reg.CounterAt(obs.IDBodyLenMismatch, i)
		in.decompressedLenMismatch[i] = reg.CounterAt(obs.IDDecompressedLenMismatch, i)
		in.recordCountClamped[i] = reg.CounterAt(obs.IDRecordCountClamped, i)
		in.booktypeSkipped[i] = reg.CounterAt(obs.IDBooktypeSkipped, i)
		in.blobOverrun[i] = reg.CounterAt(obs.IDBlobOverrun, i)
	}

	// The mirror's, shared by every shard (P3-04, ruling D2).
	in.mirrorTranscode = mirror.TranscodeInstruments{
		GuardOffered:          make([]*obs.Counter, n),
		GuardSuperseded:       make([]*obs.Counter, n),
		GuardDisabled:         make([]*obs.Gauge, n),
		BlobLenMismatch:       make([]*obs.Counter, n),
		PrevalidateRejectedTC: make([]*obs.Counter, n),
	}
	mt := &in.mirrorTranscode
	for i := range n {
		mt.GuardOffered[i] = reg.CounterAt(obs.IDGuardOffered, i)
		mt.GuardSuperseded[i] = reg.CounterAt(obs.IDGuardSuperseded, i)
		mt.GuardDisabled[i] = reg.GaugeAt(obs.IDOrderingGuardDisabled, i)
		mt.BlobLenMismatch[i] = reg.CounterAt(obs.IDBlobLenMismatch, i)
		mt.PrevalidateRejectedTC[i] = reg.CounterAt(obs.IDPrevalidateRejectedTranscode, i)
	}
}

// unconfiguredTranscodeFunc adapts obs's second dynamic accessor, the sibling
// of unknownTranscodeFunc below and for the same reason.
func unconfiguredTranscodeFunc(reg obs.Registry) func(int) (*obs.Counter, bool) {
	type unconfiguredResolver interface {
		UnconfiguredTranscode(code int) (*obs.Counter, bool)
	}
	if r, ok := reg.(unconfiguredResolver); ok {
		return r.UnconfiguredTranscode
	}
	var sink obs.Counter
	return func(int) (*obs.Counter, bool) { return &sink, true }
}

// unknownTranscodeFunc adapts the one dynamic accessor obs exposes. A closure
// rather than holding the *obs.Reg, so instruments stays a bag of handles and
// the receive path cannot reach anything else on the registry.
func unknownTranscodeFunc(reg obs.Registry) func(int) (*obs.Counter, bool) {
	type unknownResolver interface {
		UnknownTranscode(code int) (*obs.Counter, bool)
	}
	if r, ok := reg.(unknownResolver); ok {
		return r.UnknownTranscode
	}
	// A registry without the accessor cannot count unknown codes; counting
	// them under a nil pointer would panic on the receive goroutine, so the
	// fallback is a no-op counter that is discarded.
	var sink obs.Counter
	return func(int) (*obs.Counter, bool) { return &sink, true }
}

// transcodeSpecs projects the configured inventory into the form decode takes.
//
// The SAME translation obsOptions below performs, for the same reason and with
// the same justification: decode may not import config, because config carries
// oradb.DBSettings and that would make the receive path reach database/sql --
// forbidden by Vol 0 section 1.3 and machine-checked by gate G-DEP-1. Vol 1
// section 8.2 writes BuildRegistry as taking a *config.Config; the import rule
// is the part of the contract that is enforced, so the projection lives here,
// in the one package allowed to see both halves.
func transcodeSpecs(cfg *config.Config) []decode.TranscodeSpec {
	out := make([]decode.TranscodeSpec, 0, len(cfg.Transcodes))
	for _, t := range cfg.Transcodes {
		spec := decode.TranscodeSpec{
			Code:          t.Code,
			Class:         parseClass(t.Class),
			Contributions: make([]decode.ContribSpec, 0, len(t.Contributions)),
		}
		for _, c := range t.Contributions {
			spec.Contributions = append(spec.Contributions, decode.ContribSpec{
				Destination: c.Destination,
				Columns:     c.Columns,
			})
		}
		out = append(out, spec)
	}
	return out
}

// parseClass maps the configured class string onto model.Class.
//
// config's V-T2 has already restricted it to the two legal spellings, so the
// fallback is the branch no validated Config reaches. Rate-limited is the
// conservative default: it applies the window tick and digest suppression
// rather than flushing every message immediately.
func parseClass(s string) model.Class {
	if s == "non-rate-limited" {
		return model.ClassNonRateLimited
	}
	return model.ClassRateLimited
}

// registryProblemRules maps decode's problem kinds onto config's V- rule
// identifiers.
//
// The identifiers are declared once, in config/problem.go, and decode does not
// know them -- it reports KINDS. This table is the join, and it is in app for
// the same reason the projection above is: app is where both vocabularies are
// legal. V-T1 and V-T6 are the two rules config.Unchecked() listed as
// un-evaluable from P1-06 until this task.
var registryProblemRules = map[decode.ProblemKind]string{
	decode.ProblemNoDecoder:          config.RuleDecoderExists, // V-T1
	decode.ProblemColSpecWidth:       config.RuleColSpecWidth,  // V-T6
	decode.ProblemUnknownDestination: config.RuleContribDest,   // V-T3
	decode.ProblemNotAContributor:    config.RuleContribDest,   // V-T3
	decode.ProblemUnknownColumn:      config.RuleContribColumn, // V-T4
	decode.ProblemDuplicateColumn:    config.RuleContribColumn, // V-T4
	decode.ProblemTooManyTranscodes:  config.RuleRange,         // V-RANGE
}

// registryError turns a decode.BuildError into the config.ValidationError the
// rest of the system already knows how to print, so a registry rejection reads
// exactly like every other configuration rejection and -selfcheck needed no
// new case.
func registryError(err error) error {
	var be *decode.BuildError
	if !errors.As(err, &be) {
		return err
	}
	ve := &config.ValidationError{Problems: make([]config.Problem, 0, len(be.Problems))}
	for _, p := range be.Problems {
		rule, ok := registryProblemRules[p.Kind]
		if !ok {
			rule = config.RuleRange
		}
		ve.Problems = append(ve.Problems, config.Problem{Rule: rule, Msg: p.Msg})
	}
	return ve
}

// obsOptions translates the resolved configuration and destination table into
// the plain scalars and label strings obs needs.
//
// This translation exists because obs MUST NOT import config or destinations
// (gate G-DEP-1): it is imported from everywhere, so one edge back would make
// half the tree cyclic. app is the package allowed to see both halves, so the
// translation lives here -- which is also why obs.Options is scalars and
// strings rather than a *config.Config.
func obsOptions(cfg *config.Config, dests *destinations.Set) obs.Options {
	all := dests.All()

	names := make([]string, 0, len(all))
	cols := make([][]string, 0, len(all))
	for i := range all {
		names = append(names, all[i].Name)
		// Column names in ColMask bit order, which is also the image order --
		// the same order the {destination,column} label indexes by.
		cn := make([]string, 0, len(all[i].Columns))
		for j := range all[i].Columns {
			cn = append(cn, all[i].Columns[j].Name())
		}
		cols = append(cols, cn)
	}

	// Ascending transaction code, which is the index the decoder registry and
	// the ordering guard also use -- Vol 1 section 7.7: "reusing the counter
	// layer's slot index is what keeps per-transcode from needing a map on the
	// receive path". config validates the inventory, so this only projects it.
	codes := make([]int, 0, len(cfg.Transcodes))
	for i := range cfg.Transcodes {
		codes = append(codes, cfg.Transcodes[i].Code)
	}

	return obs.Options{
		Shards:       cfg.Runtime.Shards,
		Destinations: names,
		Columns:      cols,
		Transcodes:   codes,
	}
}
