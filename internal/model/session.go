package model

// SessionEvent is one market-session transition, crossing from decode to health
// BY VALUE.
//
// By value, and on a channel rather than as shared state, so the session
// machine observes every transition rather than a sampled end state: a same-day
// re-open can pass through 6531, 6571 and 6511 inside a single second
// (bible section 4.5), and anything that sampled would see only the last.
type SessionEvent struct {
	MarketType uint16    // bible section 4.5 message offset 74: 1 Normal, 2 Odd Lot, 3 Spot, 4 Auction
	Code       TransCode // 6511, 6521, 6522, 6531 or 6571
	LogTime    int32     // BCAST_HEADER.LogTime, seconds since 1 Jan 1980 (bible section 2.5, 2.8)
	RecvMono   int64     // ns since process start -- the ordering the health machine applies
}
