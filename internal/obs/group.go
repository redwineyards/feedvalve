package obs

// Group is one emission line.
//
// Vol 4 section 6.1 refines Vol 0 section 8.1's "one structured line", because
// taken literally at the full instrument count that line is about 200 values
// and 5 KB -- unreadable at 03:00 and truncated by several log transports. The
// emission stays ONE event, with one timestamp and one generation, but is
// written as a small fixed set of group lines, one per Vol 0 section 8.2 group
// heading.
//
// The fifteen groups below are exactly section 8.2's bold heading rows.
// Section 8.2a, merged later from the sibling volumes, carries no Group column
// at all -- its sixty instruments are organised by source volume instead -- so
// every one of them was assigned a home group during P1-07 and the column added
// to the document in the same commit. Where Vol 4 gave evidence it won over
// tidiness: its own section 6.1 example line shows shard_depth_max{0} on the
// stall line, so shard_depth_max is Stall rather than Queues beside
// shard_depth.
type Group uint8

const (
	GroupIngress Group = iota
	GroupDecode
	GroupLoss
	GroupCoalescing
	GroupOrdering
	GroupFlush
	GroupEpoch
	GroupStall
	GroupQueues
	GroupWritePool
	GroupWrites
	GroupLiveness
	GroupActiveSet
	GroupSchemaFit
	GroupConfigLifecycle

	numGroups
)

// groupNames are the group= values in the emitted line, lower-cased and
// underscored. Three are shortened from their section 8.2 headings -- "Write
// pool", "Active set", "Schema fit" -- and "Configuration and lifecycle"
// becomes config_lifecycle, because the group token is read in a grep pattern
// and not in prose.
var groupNames = [numGroups]string{
	GroupIngress:         "ingress",
	GroupDecode:          "decode",
	GroupLoss:            "loss",
	GroupCoalescing:      "coalescing",
	GroupOrdering:        "ordering",
	GroupFlush:           "flush",
	GroupEpoch:           "epoch",
	GroupStall:           "stall",
	GroupQueues:          "queues",
	GroupWritePool:       "write_pool",
	GroupWrites:          "writes",
	GroupLiveness:        "liveness",
	GroupActiveSet:       "active_set",
	GroupSchemaFit:       "schema_fit",
	GroupConfigLifecycle: "config_lifecycle",
}

func (g Group) String() string {
	if g < numGroups {
		return groupNames[g]
	}
	return "group(?)"
}

// healthLine is the SIXTEENTH group, and it is a projection rather than an
// ownership group.
//
// Vol 4 section 6.1's example leads with group=health, which is not one of
// section 8.2's fifteen headings: it is a cross-cutting selection. So every
// instrument keeps exactly ONE home group, and health is this explicit ordered
// list, emitted first, every interval. An instrument named here is therefore
// emitted twice in one emission -- once on health, once in its home group --
// and that is the intent. Section 6.1 rule 1 exists so that "an operator who
// reads one line reads that one".
//
// Membership is the union of section 6.1's example line and section 6.6's
// questions Q1 to Q3, which disagreed: the example omits seq_gap_messages,
// which Q3 names, and Q1 to Q3 omit config_reload_rejected, session_state and
// writer_health_state, which the example carries. Taking the union gives ten
// instruments; session_state expands to four market types, which is section
// 6.1's "the dozen values section 6.6 Q1 to Q3 read".
//
// The order is Q1 first, then Q2, then Q3 -- the order the runbook asks the
// questions in, so reading the line top to bottom is reading the runbook.
var healthLine = []ID{
	IDUptimeSeconds,        // Q1 -- a value that keeps resetting is a restart loop
	IDConfigGeneration,     // Q1 -- back at 1 confirms a restart rather than a reload
	IDConfigReloadRejected, // Q10 -- absence of an error is not confirmation
	IDCircuitState,         // Q2 -- silence is a circuit-down signal, not a quiet market
	IDDatagramsReceived,    // Q2 -- the denominator of everything
	IDHeartbeatSeen,        // Q2 -- 6541, bible section 4.12
	IDSessionState,         // the free cross-check against MD_MARKET_STATUS in Oracle
	IDSeqGapEvents,         // Q3 -- is it us or the wire
	IDSeqGapMessages,       // Q3 -- the size of the hole
	IDWriterHealthState,    // Q8 -- without it, writes_failed rising reads as retrying
}
