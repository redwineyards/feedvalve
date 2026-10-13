package obs

// Class is a log event class: the unit the WARN and ERROR token buckets are
// keyed by, and the label values of log_events_suppressed{class}.
//
// Why a closed enumeration rather than a string the caller passes in: the rate
// limit is PER CLASS, so the bucket has to be found without allocating on the
// path that is already in trouble. A misframed feed produces one WARN per
// sub-packet, and at ~10,000 messages/s an uncapped logger is both the loudest
// and the slowest thing in the process -- it converts a data problem into an
// availability problem, and it does so on the receive goroutine, which is the
// one NFR4 protects. A map keyed by string would allocate the key's interface
// box and hash it, per event, exactly when the process can least afford it.
//
// The classes are Vol 4 section 6.2's WARN and ERROR rows, one per distinct
// event the table names. They live here rather than at the call sites because
// obs owns the buckets; without a shared vocabulary each Phase 2 call site
// would invent its own spelling and the suppression counters would fragment
// into near-duplicates.
type Class uint8

const (
	// WARN classes -- Vol 4 section 6.2.
	ClassSeqGap             Class = iota // F1  -- a hole in the sequence
	ClassNetidMismatch                   // F5  -- the free frame-alignment oracle
	ClassOversizeDatagram                // F6  -- the 516-byte envelope assumption broke
	ClassRecordCountClamped              // F8  -- NoOfRecords out of range
	ClassUnexpectedSource                // F19 -- ASM-12; never a drop by default
	ClassTruncation                      // F29 -- a value cut at its declared width
	ClassClockStep                       // F23 -- the wall clock moved under us
	ClassReloadRejected                  // F15 -- the operator's change did NOT take effect
	ClassSessionAnomaly                  // the exchange sent something unexpected
	ClassShardSendWait                   // a Q1 send that had to wait AT ALL

	// ERROR classes -- Vol 4 section 6.2.
	ClassDecompressFailed // F3  -- DecompressSafe refused the input
	ClassFramingAbandoned // F4  -- costs the remainder of the datagram
	ClassErrorResponse    // F7  -- non-zero ErrorCode
	ClassWriteFailed      // F12, F13
	ClassRowRejected      // F14 -- ORA-00001, ORA-01438, ORA-12899
	ClassWriterStopped    // F22 -- a pool member retired
	ClassPanicRecovered   // F25, F26, F28
	ClassStartupFailed    // a failed startup step

	numClasses
)

// logClasses are the {class} label values, in Class order. The order IS the
// slot order, so reordering this list silently re-labels live counters -- which
// is what TestClassTableMatchesEnum exists to prevent.
var logClasses = []string{
	ClassSeqGap:             "seq_gap",
	ClassNetidMismatch:      "netid_mismatch",
	ClassOversizeDatagram:   "oversize_datagram",
	ClassRecordCountClamped: "record_count_clamped",
	ClassUnexpectedSource:   "unexpected_source",
	ClassTruncation:         "truncation",
	ClassClockStep:          "clock_step",
	ClassReloadRejected:     "reload_rejected",
	ClassSessionAnomaly:     "session_anomaly",
	ClassShardSendWait:      "shard_send_wait",
	ClassDecompressFailed:   "decompress_failed",
	ClassFramingAbandoned:   "framing_abandoned",
	ClassErrorResponse:      "error_response",
	ClassWriteFailed:        "write_failed",
	ClassRowRejected:        "row_rejected",
	ClassWriterStopped:      "writer_stopped",
	ClassPanicRecovered:     "panic_recovered",
	ClassStartupFailed:      "startup_failed",
}

func (c Class) String() string {
	if int(c) < len(logClasses) {
		return logClasses[c]
	}
	return "class(?)"
}
