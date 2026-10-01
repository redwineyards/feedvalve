package main

type BroadcastPackedData struct {
	netID       [2]byte
	packetCount int16
	packedData  [maxPackedDataLen]byte
}

// maxPackedDataLen is the size of cPackData in the BcastPackData envelope --
// the buffer holding all sequentially-packed sub-packets.
const maxPackedDataLen = 512

// maxDatagramLen is the full wire size of BcastPackData: cNetId[2] +
// iNoPackets + cPackData. A UDP datagram larger than this cannot be a
// well-formed envelope.
const maxDatagramLen = 2 + 2 + maxPackedDataLen

// udpReadBufferLen is deliberately far larger than maxDatagramLen: ReadFrom
// silently discards whatever does not fit, reporting no error, so reading into
// an exactly-sized buffer would make an oversized datagram indistinguishable
// from a full-size one. Reading into a buffer bigger than any possible UDP
// payload lets main detect and report the oversize instead.
const udpReadBufferLen = 65536

const maxDecompressedPacketLen = maxPackedDataLen

// broadcastDataPrefixLen is the 8-byte prefix that precedes the broadcast
// header inside every broadcast data payload, compressed or not. Byte 0
// repeats the segment netId; bytes 1-7 are ignored. The broadcast header
// therefore starts at the 9th byte (spec Ch.10, p.143 and p.145).
const broadcastDataPrefixLen = 8

// broadcastHeaderSize is the wire size of BroadcastHeader, excluding the
// broadcastDataPrefixLen bytes that precede it.
const broadcastHeaderSize = 40

// netId values identifying the segment a broadcast originates from
// (spec Ch.10, p.143).
const (
	netIDEquityDerivative    = 2
	netIDEquity              = 4
	netIDCurrencyDerivative  = 6
	netIDCommodityDerivative = 8
)

type BroadcastHeader struct {
	reserved1       [2]byte
	reserved2       [2]byte
	logTime         int32
	alphaChar       [2]byte
	transactionCode int16
	errorCode       int16
	sequenceNumber  int32
	reserved3       byte
	reserved4       [3]byte
	timestamp2      [8]byte
	filler2         [8]byte
	messageLength   int16
}

type STIndicator struct {
	flags    byte
	reserved byte
}

type MBPInformation struct {
	quantity       int32
	price          int32
	numberOfOrders int16
	bbBuySellFlag  int16
}

// InteractiveOnlyMBPData is the wire layout of INTERACTIVE_ONLY_MBP_DATA. It
// is mbpRecordSize bytes on the wire, not the sum of the field sizes below:
// pack(2) inserts one pad byte after netChangeIndicator, so
// netPriceChangeFromClosingPrice and everything after it sit one byte later
// than spec Table 81's offset column claims. parseInteractiveOnlyMBPData reads
// that pad explicitly.
type InteractiveOnlyMBPData struct {
	token                          int32
	bookType                       int16
	tradingStatus                  int16
	volumeTradedToday              int32
	lastTradedPrice                int32
	netChangeIndicator             byte
	netPriceChangeFromClosingPrice int32
	lastTradeQuantity              int32
	lastTradeTime                  int32
	averageTradePrice              int32
	auctionNumber                  int16
	auctionStatus                  int16
	initiatorType                  int16
	initiatorPrice                 int32
	initiatorQuantity              int32
	auctionPrice                   int32
	auctionQuantity                int32
	recordBuffer                   [10]MBPInformation
	bbTotalBuyFlag                 int16
	bbTotalSellFlag                int16
	totalBuyQuantity               float64
	totalSellQuantity              float64
	indicator                      STIndicator
	closingPrice                   int32
	openPrice                      int32
	highPrice                      int32
	lowPrice                       int32
}

// mbpMaxRecords is the number of INTERACTIVE_ONLY_MBP_DATA records the 7208
// wire message has room for. The message is fixed-size, so the array always
// has this many slots; NoOfRecords says how many of them carry real data.
const mbpMaxRecords = 2

// mbpRecordSize is the wire stride of one INTERACTIVE_ONLY_MBP_DATA. The
// fields sum to 213 bytes, but the structs are pack(2) -- "structures of odd
// size are padded to an even number of bytes" (spec Ch.2, "Guidelines for
// Designers" item 3) -- so each array element occupies 214. That is what makes
// the parent's declared length come out exactly:
//
//	broadcastHeaderSize(40) + NoOfRecords(2) + 2*214 = 470 = mbpMessageBodySize
//
// With an unpadded 213 the total is 468, and the 2-byte shortfall is not
// trailing slack: it means the second record starts a byte early and every
// field of the first record after netChangeIndicator is off by one.
const mbpRecordSize = 214

// mbpMessageBodySize is the declared packet length of MS_BCAST_ONLY_MBP,
// measured from the broadcast header onward (the 8-byte prefix sits outside
// it). mbpLayoutCheck below fails to compile if the field layout stops
// accounting for it exactly.
const mbpMessageBodySize = 470

// mbpLayoutCheck is a compile-time assertion that the record stride still adds
// up to the declared message size. Converting both the difference and its
// negation to uint only succeeds when the difference is zero, so a future edit
// to the record layout that breaks the 470-byte total fails the build instead
// of silently reintroducing a one-byte field shift.
const mbpLayoutSlack = mbpMessageBodySize - (broadcastHeaderSize + 2 + mbpMaxRecords*mbpRecordSize)
const mbpLayoutCheck = uint(mbpLayoutSlack) + uint(-mbpLayoutSlack)

type MSBroadcastOnlyMBP struct {
	header BroadcastHeader
	// recordCount is NoOfRecords, clamped by the parser to [0, mbpMaxRecords]
	// so that validRecords is always in range. The unclamped wire value is
	// reported by the parser's warning when clamping happens.
	recordCount int16
	records     [mbpMaxRecords]InteractiveOnlyMBPData
}

// validRecords returns only the records the message actually declared. The
// remaining array slots are left zeroed by the parser -- reading them would
// surface the message's trailing padding as if it were a real security.
func (m MSBroadcastOnlyMBP) validRecords() []InteractiveOnlyMBPData {
	return m.records[:m.recordCount]
}

type SecurityInfo struct {
	instrumentName [6]byte
	symbol         [10]byte
	series         [2]byte
	expiryDate     int32
	strikePrice    int32
	optionType     [2]byte
	caLevel        int16
}

type SecurityEligibilityPerMarket struct {
	flags  byte
	status int16
}

type EligibilityIndicators struct {
	flags    byte
	reserved byte
}

type Purpose struct {
	flags1 byte
	flags2 byte
}

type MSSecurityUpdateInfo struct {
	header                BroadcastHeader
	token                 int32
	securityInfo          SecurityInfo
	permittedToTrade      int16
	issuedCapital         float64
	warningQuantity       int32
	freezeQuantity        int32
	creditRating          [12]byte
	eligibilityPerMarket  [4]SecurityEligibilityPerMarket
	issueRate             int16
	issueStartDate        int32
	interestPaymentDate   int32
	issueMaturityDate     int32
	marginPercentage      int32
	minimumLotQuantity    int32
	boardLotQuantity      int32
	tickSize              int32
	name                  [25]byte
	reserved              byte
	listingDate           int32
	expulsionDate         int32
	reAdmissionDate       int32
	recordDate            int32
	lowPriceRange         int32
	highPriceRange        int32
	expiryDate            int32
	noDeliveryStartDate   int32
	noDeliveryEndDate     int32
	eligibilityIndicators EligibilityIndicators
	bookClosureStartDate  int32
	bookClosureEndDate    int32
	exerciseStartDate     int32
	exerciseEndDate       int32
	multiplier            int32
	assetInstrument       [6]byte
	assetName             [10]byte
	underlyingToken       int32
	intrinsicValue        int32
	extrinsicValue        int32
	purpose               Purpose
	localUpdateDateTime   int32
	deleteFlag            byte
	remark                [25]byte
	basePrice             int32
}
