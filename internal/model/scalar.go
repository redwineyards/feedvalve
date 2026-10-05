package model

import "strconv"

// TransCode is an NNF transaction code -- the dispatch key of the whole feed
// (bible section 2.8: BCAST_HEADER offset 10, payload offset 18).
//
// Unsigned, because a code is an identifier and not a quantity, and read from
// the wire with binary.BigEndian.Uint16. This widens poc's int16
// (poc/exchange_communication.go, BroadcastHeader.transactionCode).
type TransCode uint16

func (c TransCode) String() string { return strconv.FormatUint(uint64(c), 10) }

// DestID indexes the three destinations, 0..MaxDestinations-1.
//
// It is the array index of every [MaxDestinations] field in this package, and
// it is also the fixed write order inside a transaction: destination 0, then 1,
// then 2. That ordering is the second layer of the deadlock argument
// (HLD section 8.3, 8.4), so it is a property of the type, not a convention.
type DestID uint8

func (d DestID) String() string { return strconv.FormatUint(uint64(d), 10) }

// ShardID indexes the shard goroutines, 0..shards-1.
type ShardID uint8

func (s ShardID) String() string { return strconv.FormatUint(uint64(s), 10) }

// Class decides WHEN a shard flushes, and nothing else (HLD section 6.8). It
// never decides where a unit goes.
type Class uint8

const (
	// ClassRateLimited is flushed on the window tick; digest suppression applies.
	ClassRateLimited Class = iota
	// ClassNonRateLimited raises the flush flag after the merge; suppression is skipped.
	ClassNonRateLimited
)

func (c Class) String() string {
	switch c {
	case ClassRateLimited:
		return "rate-limited"
	case ClassNonRateLimited:
		return "non-rate-limited"
	}
	return "Class(" + strconv.FormatUint(uint64(c), 10) + ")"
}

// HeaderKind is the outcome of header resolution (HLD section 6.4).
//
// It is decided WITHOUT consulting the decoder registry, so that adding a
// transaction code to the inventory can never change how a message's header is
// classified.
type HeaderKind uint8

const (
	// HeaderBcast is BCAST_HEADER (bible section 2.8) -- everything except the two below.
	HeaderBcast HeaderKind = iota
	// HeaderMessage is MESSAGE_HEADER (bible section 2.8): 6541 at payload length 48,
	// 6503 at 60.
	HeaderMessage
)

func (k HeaderKind) String() string {
	switch k {
	case HeaderBcast:
		return "bcast"
	case HeaderMessage:
		return "message"
	}
	return "HeaderKind(" + strconv.FormatUint(uint64(k), 10) + ")"
}

// UnitFlags is a bitset of per-unit booleans, packed into one byte so
// UpdateUnit stays 8-byte aligned without three separate bools.
type UnitFlags uint8

const (
	// FlagGeneral marks a non-token-scoped unit, routed to the designated shard (FR8).
	FlagGeneral UnitFlags = 1 << iota
	// FlagNonRateLimited mirrors ClassNonRateLimited (HLD section 6.8).
	FlagNonRateLimited
	// FlagOrderingUnusable means the decoder could not obtain a usable ordering
	// value, so the guard is bypassed for this unit (HLD section 6.5).
	FlagOrderingUnusable
)

// Has reports whether every flag in f is set.
func (u UnitFlags) Has(f UnitFlags) bool { return u&f == f }

// RowOutcome is the writer's verdict on ONE row of a batch.
//
// Three terminal values, because HLD section 9.2 classifies row-level data
// errors (ORA-00001, ORA-01438) as "skip the row, continue the batch, stay
// Healthy" while session-level errors (ORA-03113, ORA-12541) require the row to
// be retried. Collapsing them into success and failure makes failure mode F14
// inexpressible.
type RowOutcome uint8

const (
	// OutcomePending is the zero value and the state a batch is reset to. A
	// batch returned with any row still Pending is a WRITER DEFECT, not a load
	// condition: the shard counts it and stops loudly.
	OutcomePending RowOutcome = iota
	// OutcomeCommitted means the row's columns are in Oracle.
	OutcomeCommitted
	// OutcomeRejected is a permanent data error; retry cannot help (HLD section 9.2).
	OutcomeRejected
	// OutcomeFailed is transient; the row must be retried (HLD section 9.2).
	OutcomeFailed
)

func (o RowOutcome) String() string {
	switch o {
	case OutcomePending:
		return "pending"
	case OutcomeCommitted:
		return "committed"
	case OutcomeRejected:
		return "rejected"
	case OutcomeFailed:
		return "failed"
	}
	return "RowOutcome(" + strconv.FormatUint(uint64(o), 10) + ")"
}
