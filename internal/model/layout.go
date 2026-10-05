package model

import "unsafe"

// Gate G-LAY-2 (Vol 4 section 7.1): the frozen memory layout of every
// fixed-size type in this package, in the order Vol 0 section 3 declares them.
//
// Each line below is a constant expression that compiles only when the two
// values are equal; if one moves, the subtraction underflows uintptr and the
// BUILD fails. Nothing here runs, and importing unsafe for Sizeof and Offsetof
// does not make the build unsafe or cgo-dependent -- both are compile-time
// constants and go vet is content with them.
//
// Sizes are asserted because the design's memory arithmetic divides and
// multiplies by them. Offsets are asserted as well, and that is not
// redundancy: a struct can keep its total while two fields swap, and the merge
// loop, the pump and the completion path all depend on Entry's field order.
//
// Blank padding fields cannot be named in Offsetof, so they are checked
// implicitly -- the following named field's offset only lands where it does if
// the padding before it is the declared width.

const (
	contributionSize = 24
	updateUnitSize   = 360
	msgHeaderSize    = 32
	entrySize        = 424
	batchRowSize     = 288
	datagramSize     = 1040
	sessionEventSize = 16
)

// Contribution -- align 8, size 24 (Vol 0 section 3.4).
const (
	_ = uint(unsafe.Sizeof(Contribution{})-contributionSize) + uint(contributionSize-unsafe.Sizeof(Contribution{}))

	_ = uint(unsafe.Offsetof(Contribution{}.Row)-0) + uint(0-unsafe.Offsetof(Contribution{}.Row))
	_ = uint(unsafe.Offsetof(Contribution{}.Mask)-8) + uint(8-unsafe.Offsetof(Contribution{}.Mask))
	_ = uint(unsafe.Offsetof(Contribution{}.BlobOff)-16) + uint(16-unsafe.Offsetof(Contribution{}.BlobOff))
	_ = uint(unsafe.Offsetof(Contribution{}.BlobLen)-18) + uint(18-unsafe.Offsetof(Contribution{}.BlobLen))
	_ = uint(unsafe.Offsetof(Contribution{}.Slot)-20) + uint(20-unsafe.Offsetof(Contribution{}.Slot))
)

// UpdateUnit -- align 8, size 360 (Vol 0 section 3.5).
//
// HLD section 6.6 and ADR-006 quote about 384 B. The re-derivation gives 360,
// which is smaller, so every memory figure computed from 384 remains valid as
// an upper bound. Q1 is 4 x 4096 x 360 = 5.9 MB.
const (
	_ = uint(unsafe.Sizeof(UpdateUnit{})-updateUnitSize) + uint(updateUnitSize-unsafe.Sizeof(UpdateUnit{}))

	_ = uint(unsafe.Offsetof(UpdateUnit{}.Token)-0) + uint(0-unsafe.Offsetof(UpdateUnit{}.Token))
	_ = uint(unsafe.Offsetof(UpdateUnit{}.Code)-4) + uint(4-unsafe.Offsetof(UpdateUnit{}.Code))
	_ = uint(unsafe.Offsetof(UpdateUnit{}.Flags)-6) + uint(6-unsafe.Offsetof(UpdateUnit{}.Flags))
	_ = uint(unsafe.Offsetof(UpdateUnit{}.Seq)-8) + uint(8-unsafe.Offsetof(UpdateUnit{}.Seq))
	_ = uint(unsafe.Offsetof(UpdateUnit{}.LogTime)-12) + uint(12-unsafe.Offsetof(UpdateUnit{}.LogTime))
	_ = uint(unsafe.Offsetof(UpdateUnit{}.RecvMono)-16) + uint(16-unsafe.Offsetof(UpdateUnit{}.RecvMono))
	_ = uint(unsafe.Offsetof(UpdateUnit{}.Contribs)-24) + uint(24-unsafe.Offsetof(UpdateUnit{}.Contribs))
	_ = uint(unsafe.Offsetof(UpdateUnit{}.BlobLen)-96) + uint(96-unsafe.Offsetof(UpdateUnit{}.BlobLen))
	_ = uint(unsafe.Offsetof(UpdateUnit{}.Blob)-104) + uint(104-unsafe.Offsetof(UpdateUnit{}.Blob))
)

// MsgHeader -- align 8, size 32 (Vol 0 section 3.6).
const (
	_ = uint(unsafe.Sizeof(MsgHeader{})-msgHeaderSize) + uint(msgHeaderSize-unsafe.Sizeof(MsgHeader{}))

	_ = uint(unsafe.Offsetof(MsgHeader{}.TimestampNs)-0) + uint(0-unsafe.Offsetof(MsgHeader{}.TimestampNs))
	_ = uint(unsafe.Offsetof(MsgHeader{}.LogTime)-8) + uint(8-unsafe.Offsetof(MsgHeader{}.LogTime))
	_ = uint(unsafe.Offsetof(MsgHeader{}.BCSeqNo)-12) + uint(12-unsafe.Offsetof(MsgHeader{}.BCSeqNo))
	_ = uint(unsafe.Offsetof(MsgHeader{}.Code)-16) + uint(16-unsafe.Offsetof(MsgHeader{}.Code))
	_ = uint(unsafe.Offsetof(MsgHeader{}.ErrorCode)-18) + uint(18-unsafe.Offsetof(MsgHeader{}.ErrorCode))
	_ = uint(unsafe.Offsetof(MsgHeader{}.MsgLen)-20) + uint(20-unsafe.Offsetof(MsgHeader{}.MsgLen))
	_ = uint(unsafe.Offsetof(MsgHeader{}.Kind)-22) + uint(22-unsafe.Offsetof(MsgHeader{}.Kind))
	_ = uint(unsafe.Offsetof(MsgHeader{}.PayloadLen)-24) + uint(24-unsafe.Offsetof(MsgHeader{}.PayloadLen))
)

// Entry -- align 8, size 424 at MaxContributors 8 and ImageSize 256
// (Vol 0 section 3.7).
//
// Quarantined and SlotSeen cost nothing: both land inside the five pad bytes
// this struct already declared at offset 163, which is why Entry is still 424
// and no arena figure moved. Three bytes remain for the next flag.
const (
	_ = uint(unsafe.Sizeof(Entry{})-entrySize) + uint(entrySize-unsafe.Sizeof(Entry{}))

	_ = uint(unsafe.Offsetof(Entry{}.Row)-0) + uint(0-unsafe.Offsetof(Entry{}.Row))
	_ = uint(unsafe.Offsetof(Entry{}.ColMask)-8) + uint(8-unsafe.Offsetof(Entry{}.ColMask))
	_ = uint(unsafe.Offsetof(Entry{}.Written)-16) + uint(16-unsafe.Offsetof(Entry{}.Written))
	_ = uint(unsafe.Offsetof(Entry{}.Digest)-24) + uint(24-unsafe.Offsetof(Entry{}.Digest))
	_ = uint(unsafe.Offsetof(Entry{}.LastSeq)-32) + uint(32-unsafe.Offsetof(Entry{}.LastSeq))
	_ = uint(unsafe.Offsetof(Entry{}.LastTime)-64) + uint(64-unsafe.Offsetof(Entry{}.LastTime))
	_ = uint(unsafe.Offsetof(Entry{}.LastRecv)-96) + uint(96-unsafe.Offsetof(Entry{}.LastRecv))
	_ = uint(unsafe.Offsetof(Entry{}.Dirty)-160) + uint(160-unsafe.Offsetof(Entry{}.Dirty))
	_ = uint(unsafe.Offsetof(Entry{}.InFlight)-161) + uint(161-unsafe.Offsetof(Entry{}.InFlight))
	_ = uint(unsafe.Offsetof(Entry{}.ReDirty)-162) + uint(162-unsafe.Offsetof(Entry{}.ReDirty))
	_ = uint(unsafe.Offsetof(Entry{}.Quarantined)-163) + uint(163-unsafe.Offsetof(Entry{}.Quarantined))
	_ = uint(unsafe.Offsetof(Entry{}.SlotSeen)-164) + uint(164-unsafe.Offsetof(Entry{}.SlotSeen))
	_ = uint(unsafe.Offsetof(Entry{}.Image)-168) + uint(168-unsafe.Offsetof(Entry{}.Image))
)

// BatchRow -- align 8, size 288 at ImageSize 256 (Vol 0 section 3.8).
//
// ErrCode is free as well: it occupies two of the three pad bytes already
// declared at offset 29.
const (
	_ = uint(unsafe.Sizeof(BatchRow{})-batchRowSize) + uint(batchRowSize-unsafe.Sizeof(BatchRow{}))

	_ = uint(unsafe.Offsetof(BatchRow{}.Row)-0) + uint(0-unsafe.Offsetof(BatchRow{}.Row))
	_ = uint(unsafe.Offsetof(BatchRow{}.Mask)-8) + uint(8-unsafe.Offsetof(BatchRow{}.Mask))
	_ = uint(unsafe.Offsetof(BatchRow{}.Digest)-16) + uint(16-unsafe.Offsetof(BatchRow{}.Digest))
	_ = uint(unsafe.Offsetof(BatchRow{}.Slot)-24) + uint(24-unsafe.Offsetof(BatchRow{}.Slot))
	_ = uint(unsafe.Offsetof(BatchRow{}.Outcome)-28) + uint(28-unsafe.Offsetof(BatchRow{}.Outcome))
	_ = uint(unsafe.Offsetof(BatchRow{}.ErrCode)-30) + uint(30-unsafe.Offsetof(BatchRow{}.ErrCode))
	_ = uint(unsafe.Offsetof(BatchRow{}.Image)-32) + uint(32-unsafe.Offsetof(BatchRow{}.Image))
)

// Datagram -- align 8, size 1040 (Vol 0 section 9.5.2).
const (
	_ = uint(unsafe.Sizeof(Datagram{})-datagramSize) + uint(datagramSize-unsafe.Sizeof(Datagram{}))

	_ = uint(unsafe.Offsetof(Datagram{}.RecvMono)-0) + uint(0-unsafe.Offsetof(Datagram{}.RecvMono))
	_ = uint(unsafe.Offsetof(Datagram{}.N)-8) + uint(8-unsafe.Offsetof(Datagram{}.N))
	_ = uint(unsafe.Offsetof(Datagram{}.Buf)-16) + uint(16-unsafe.Offsetof(Datagram{}.Buf))
)

// SessionEvent -- align 8, size 16 (Vol 4 section 4).
const (
	_ = uint(unsafe.Sizeof(SessionEvent{})-sessionEventSize) + uint(sessionEventSize-unsafe.Sizeof(SessionEvent{}))

	_ = uint(unsafe.Offsetof(SessionEvent{}.MarketType)-0) + uint(0-unsafe.Offsetof(SessionEvent{}.MarketType))
	_ = uint(unsafe.Offsetof(SessionEvent{}.Code)-2) + uint(2-unsafe.Offsetof(SessionEvent{}.Code))
	_ = uint(unsafe.Offsetof(SessionEvent{}.LogTime)-4) + uint(4-unsafe.Offsetof(SessionEvent{}.LogTime))
	_ = uint(unsafe.Offsetof(SessionEvent{}.RecvMono)-8) + uint(8-unsafe.Offsetof(SessionEvent{}.RecvMono))
)
