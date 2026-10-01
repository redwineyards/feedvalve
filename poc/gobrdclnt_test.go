package main

// End-to-end tests for the broadcast envelope -> decompression -> parse
// pipeline. Compressed fixtures come from compressWithReference (see
// decompress_test.go), which reads bytes recorded from an independent LZO1Z
// implementation and frozen in testdata/lzo_fixtures.json. The repo has no
// compressor of its own and no compression dependency.
//
// Every broadcast data payload here carries the 8-byte prefix the protocol
// spec mandates (byte 0 = netId, bytes 1-7 ignored, broadcast header starts
// at the 9th byte).

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"testing"
)

func mustWrite(t *testing.T, buf *bytes.Buffer, v any) {
	t.Helper()
	if err := binary.Write(buf, binary.BigEndian, v); err != nil {
		t.Fatalf("write field %T: %v", v, err)
	}
}

// captureAppLogger redirects appLogger for the duration of the test.
func captureAppLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	appLogger.SetOutput(&buf)
	t.Cleanup(func() { appLogger.SetOutput(io.Discard) })
	return &buf
}

// capturePacketLogger redirects packetLogger for the duration of the test.
func capturePacketLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	initPacketLogger(&buf)
	t.Cleanup(func() { initPacketLogger(io.Discard) })
	return &buf
}

// captureDebugLogger enables per-field debug logging for the duration of the
// test, restoring the (disabled) default afterwards.
func captureDebugLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	initDebugLogger(&buf)
	t.Cleanup(func() {
		debugLogger.SetOutput(io.Discard)
		debugEnabled = false
	})
	return &buf
}

// setExpectedNetID overrides the expected segment netId for one test.
func setExpectedNetID(t *testing.T, netID int) {
	t.Helper()
	previous := expectedNetID
	expectedNetID = netID
	t.Cleanup(func() { expectedNetID = previous })
}

// buildBroadcastPrefix builds the 8 bytes that precede every broadcast
// header: byte 0 repeats the segment netId, bytes 1-7 are ignored. The
// ignored bytes are deliberately non-zero so that any code mistakenly
// parsing the header at offset 0 decodes obvious garbage instead of
// accidentally-plausible values.
func buildBroadcastPrefix(netID byte) []byte {
	return []byte{netID, 0xde, 0xad, 0xbe, 0xef, 0xba, 0xdc, 0x0f}
}

func buildBroadcastHeader(t *testing.T, transactionCode, messageLength int16) []byte {
	t.Helper()

	buf := new(bytes.Buffer)
	mustWrite(t, buf, [2]byte{})         // reserved1
	mustWrite(t, buf, [2]byte{})         // reserved2
	mustWrite(t, buf, int32(1700000000)) // logTime
	mustWrite(t, buf, [2]byte{'N', 'C'}) // alphaChar
	mustWrite(t, buf, transactionCode)   // transactionCode
	mustWrite(t, buf, int16(0))          // errorCode
	mustWrite(t, buf, int32(42))         // sequenceNumber
	mustWrite(t, buf, byte(0))           // reserved3
	mustWrite(t, buf, [3]byte{})         // reserved4
	mustWrite(t, buf, [8]byte{})         // timestamp2
	mustWrite(t, buf, [8]byte{})         // filler2
	mustWrite(t, buf, messageLength)     // messageLength
	return buf.Bytes()
}

// buildMSBroadcastOnlyMBPMessage builds a transaction-code-7208 broadcast
// data payload: the 8-byte prefix followed by a 470-byte message.
func buildMSBroadcastOnlyMBPMessage(t *testing.T, netID byte, token int32) []byte {
	t.Helper()

	const recordCount = 2

	buf := new(bytes.Buffer)
	buf.Write(buildBroadcastPrefix(netID))
	buf.Write(buildBroadcastHeader(t, 7208, 470))
	mustWrite(t, buf, int16(recordCount)) // recordCount

	for i := 0; i < recordCount; i++ {
		mustWrite(t, buf, token+int32(i)) // token
		mustWrite(t, buf, int16(1))       // bookType
		mustWrite(t, buf, int16(1))       // tradingStatus
		mustWrite(t, buf, int32(1000))    // volumeTradedToday
		mustWrite(t, buf, int32(2500))    // lastTradedPrice
		mustWrite(t, buf, byte(0))        // netChangeIndicator
		mustWrite(t, buf, byte(0))        // pack(2) alignment pad
		mustWrite(t, buf, int32(0))       // netPriceChangeFromClosingPrice
		mustWrite(t, buf, int32(10))      // lastTradeQuantity
		mustWrite(t, buf, int32(0))       // lastTradeTime
		mustWrite(t, buf, int32(2500))    // averageTradePrice
		mustWrite(t, buf, int16(0))       // auctionNumber
		mustWrite(t, buf, int16(0))       // auctionStatus
		mustWrite(t, buf, int16(0))       // initiatorType
		mustWrite(t, buf, int32(0))       // initiatorPrice
		mustWrite(t, buf, int32(0))       // initiatorQuantity
		mustWrite(t, buf, int32(0))       // auctionPrice
		mustWrite(t, buf, int32(0))       // auctionQuantity

		for j := 0; j < 10; j++ {
			mustWrite(t, buf, int32(100+j)) // quantity
			mustWrite(t, buf, int32(2500))  // price
			mustWrite(t, buf, int16(1))     // numberOfOrders
			mustWrite(t, buf, int16(1))     // bbBuySellFlag
		}

		mustWrite(t, buf, int16(0))      // bbTotalBuyFlag
		mustWrite(t, buf, int16(0))      // bbTotalSellFlag
		mustWrite(t, buf, float64(1000)) // totalBuyQuantity
		mustWrite(t, buf, float64(1000)) // totalSellQuantity
		mustWrite(t, buf, byte(0))       // indicator.flags
		mustWrite(t, buf, byte(0))       // indicator.reserved
		mustWrite(t, buf, int32(2400))   // closingPrice
		mustWrite(t, buf, int32(2450))   // openPrice
		mustWrite(t, buf, int32(2550))   // highPrice
		mustWrite(t, buf, int32(2400))   // lowPrice
	}

	// No trailing padding: with the pack(2) 214-byte record stride,
	// header(40) + recordCount(2) + 2*214 is exactly the declared 470 bytes.

	if got, want := buf.Len(), broadcastDataPrefixLen+470; got != want {
		t.Fatalf("built 7208 payload is %d bytes, want %d", got, want)
	}
	return buf.Bytes()
}

// buildMSSecurityUpdateInfoMessage builds a transaction-code-7305 broadcast
// data payload: the 8-byte prefix followed by a 294-byte message.
func buildMSSecurityUpdateInfoMessage(t *testing.T, netID byte, token int32, symbol string) []byte {
	t.Helper()

	var symbolField [10]byte
	copy(symbolField[:], symbol)

	buf := new(bytes.Buffer)
	buf.Write(buildBroadcastPrefix(netID))
	buf.Write(buildBroadcastHeader(t, 7305, 294))
	mustWrite(t, buf, token) // token

	// securityInfo
	mustWrite(t, buf, [6]byte{})         // instrumentName
	mustWrite(t, buf, symbolField)       // symbol
	mustWrite(t, buf, [2]byte{'E', 'Q'}) // series
	mustWrite(t, buf, int32(0))          // expiryDate
	mustWrite(t, buf, int32(0))          // strikePrice
	mustWrite(t, buf, [2]byte{})         // optionType
	mustWrite(t, buf, int16(0))          // caLevel

	mustWrite(t, buf, int16(1))     // permittedToTrade
	mustWrite(t, buf, float64(1e6)) // issuedCapital
	mustWrite(t, buf, int32(0))     // warningQuantity
	mustWrite(t, buf, int32(0))     // freezeQuantity
	mustWrite(t, buf, [12]byte{})   // creditRating

	for i := 0; i < 4; i++ {
		mustWrite(t, buf, byte(0))  // eligibilityPerMarket[i].flags
		mustWrite(t, buf, int16(0)) // eligibilityPerMarket[i].status
	}

	mustWrite(t, buf, int16(0))    // issueRate
	mustWrite(t, buf, int32(0))    // issueStartDate
	mustWrite(t, buf, int32(0))    // interestPaymentDate
	mustWrite(t, buf, int32(0))    // issueMaturityDate
	mustWrite(t, buf, int32(0))    // marginPercentage
	mustWrite(t, buf, int32(1))    // minimumLotQuantity
	mustWrite(t, buf, int32(1))    // boardLotQuantity
	mustWrite(t, buf, int32(5))    // tickSize
	mustWrite(t, buf, [25]byte{})  // name
	mustWrite(t, buf, byte(0))     // reserved
	mustWrite(t, buf, int32(0))    // listingDate
	mustWrite(t, buf, int32(0))    // expulsionDate
	mustWrite(t, buf, int32(0))    // reAdmissionDate
	mustWrite(t, buf, int32(0))    // recordDate
	mustWrite(t, buf, int32(0))    // lowPriceRange
	mustWrite(t, buf, int32(0))    // highPriceRange
	mustWrite(t, buf, int32(0))    // expiryDate (message-level)
	mustWrite(t, buf, int32(0))    // noDeliveryStartDate
	mustWrite(t, buf, int32(0))    // noDeliveryEndDate
	mustWrite(t, buf, byte(0))     // eligibilityIndicators.flags
	mustWrite(t, buf, byte(0))     // eligibilityIndicators.reserved
	mustWrite(t, buf, int32(0))    // bookClosureStartDate
	mustWrite(t, buf, int32(0))    // bookClosureEndDate
	mustWrite(t, buf, int32(0))    // exerciseStartDate
	mustWrite(t, buf, int32(0))    // exerciseEndDate
	mustWrite(t, buf, int32(1))    // multiplier
	mustWrite(t, buf, [6]byte{})   // assetInstrument
	mustWrite(t, buf, [10]byte{})  // assetName
	mustWrite(t, buf, int32(0))    // underlyingToken
	mustWrite(t, buf, int32(0))    // intrinsicValue
	mustWrite(t, buf, int32(0))    // extrinsicValue
	mustWrite(t, buf, byte(0))     // purpose.flags1
	mustWrite(t, buf, byte(0))     // purpose.flags2
	mustWrite(t, buf, int32(0))    // localUpdateDateTime
	mustWrite(t, buf, byte(0))     // deleteFlag
	mustWrite(t, buf, [25]byte{})  // remark
	mustWrite(t, buf, int32(2400)) // basePrice

	if got, want := buf.Len(), broadcastDataPrefixLen+294; got != want {
		t.Fatalf("built 7305 payload is %d bytes, want %d", got, want)
	}
	return buf.Bytes()
}

// subPacket is one sequentially-packed packet inside a BcastPackData buffer.
type subPacket struct {
	data       []byte // full broadcast data, including the 8-byte prefix
	compressed bool
}

// buildEnvelope wraps sub-packets in the BcastPackData envelope: cNetId[2],
// iNoPackets, then each packet prefixed by its compressed length (0 meaning
// uncompressed), in FIFO order.
func buildEnvelope(t *testing.T, netID byte, packets []subPacket) []byte {
	t.Helper()

	buf := new(bytes.Buffer)
	mustWrite(t, buf, [2]byte{0, netID})   // cNetId
	mustWrite(t, buf, int16(len(packets))) // iNoPackets

	for _, p := range packets {
		if !p.compressed {
			mustWrite(t, buf, int16(0)) // iCompLen == 0 -> uncompressed
			buf.Write(p.data)
			continue
		}
		compressed := compressWithReference(t, p.data)
		mustWrite(t, buf, int16(len(compressed)))
		buf.Write(compressed)
	}
	return buf.Bytes()
}

// runPipeline mirrors main's per-sub-packet handling exactly: unpack the
// envelope and hand each sub-packet to handleSubPacket, which decompresses
// (via DecompressSafe) and parses it.
func runPipeline(t *testing.T, envelope []byte) {
	t.Helper()

	from := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
	decompressBuf := make([]byte, maxDecompressedPacketLen)

	err := processBroadcastPackData(envelope, func(i, packetCount int, compressedLength int, compressedData []byte, compressed bool) {
		handleSubPacket(from, i, packetCount, compressedLength, compressedData, compressed, decompressBuf)
	})
	if err != nil {
		t.Fatalf("processBroadcastPackData: %v", err)
	}
}

// TestParseBroadcastHeaderSkipsPrefix is the regression test for the bug this
// change fixes: the broadcast header starts at the 9th byte, not the 1st.
func TestParseBroadcastHeaderSkipsPrefix(t *testing.T) {
	payload := buildMSSecurityUpdateInfoMessage(t, netIDCommodityDerivative, 999, "TESTSYM")

	header, err := parseBroadcastHeader(payload)
	if err != nil {
		t.Fatalf("parseBroadcastHeader: %v", err)
	}
	if header.transactionCode != 7305 {
		t.Errorf("transactionCode = %d, want 7305 (header must be read at offset %d, not 0)",
			header.transactionCode, broadcastDataPrefixLen)
	}
	if header.messageLength != 294 {
		t.Errorf("messageLength = %d, want 294", header.messageLength)
	}
	if header.sequenceNumber != 42 {
		t.Errorf("sequenceNumber = %d, want 42", header.sequenceNumber)
	}
	if string(header.alphaChar[:]) != "NC" {
		t.Errorf("alphaChar = %q, want %q", header.alphaChar, "NC")
	}

	message, err := parseMSSecurityUpdateInfo(payload)
	if err != nil {
		t.Fatalf("parseMSSecurityUpdateInfo: %v", err)
	}
	if message.token != 999 {
		t.Errorf("token = %d, want 999", message.token)
	}
	if got := strings.TrimRight(string(message.securityInfo.symbol[:]), "\x00"); got != "TESTSYM" {
		t.Errorf("symbol = %q, want %q", got, "TESTSYM")
	}
	if message.basePrice != 2400 {
		t.Errorf("basePrice = %d, want 2400", message.basePrice)
	}
}

// TestPayloadTooShortForPrefixPlusHeader checks the length guard accounts for
// the prefix: 40 bytes alone is no longer enough for a header.
func TestPayloadTooShortForPrefixPlusHeader(t *testing.T) {
	if _, err := parseBroadcastHeader(make([]byte, broadcastHeaderSize)); err == nil {
		t.Fatalf("expected an error for a %d-byte payload (needs %d prefix + %d header)",
			broadcastHeaderSize, broadcastDataPrefixLen, broadcastHeaderSize)
	}
}

// TestDebugOffsetsAreWireTrue asserts the -debug field offsets are reported
// relative to the start of the broadcast data, so they can be compared
// directly against a hex dump of a real capture.
func TestDebugOffsetsAreWireTrue(t *testing.T) {
	debugBuf := captureDebugLogger(t)

	payload := buildMSSecurityUpdateInfoMessage(t, netIDCommodityDerivative, 999, "TESTSYM")
	if _, err := parseMSSecurityUpdateInfo(payload); err != nil {
		t.Fatalf("parseMSSecurityUpdateInfo: %v", err)
	}

	out := debugBuf.String()
	for _, want := range []string{
		"field=broadcastData.netIdPrefix", // the ignored prefix stays visible
		"offset=18-20",                    // header.transactionCode: 8 + 10
		"offset=46-48",                    // header.messageLength:   8 + 38
		"offset=48-52",                    // token:                  8 + 40
	} {
		if !strings.Contains(out, want) {
			t.Errorf("debug output missing %q, got:\n%s", want, out)
		}
	}
}

// TestMBPRecordWireOffsets pins the pack(2) layout of
// INTERACTIVE_ONLY_MBP_DATA against a payload built from an independent table
// of absolute byte offsets, rather than from
// buildMSBroadcastOnlyMBPMessage -- a fixture builder that walks the same
// field order as the parser agrees with it even when both are wrong.
//
// The record stride is 214, not the 213 the spec's Table 81 offset column
// implies: NetChangeIndicator is the only odd-sized member, so a pad byte
// follows it and everything from NetPriceChangeFromClosingPrice onward shifts
// by one. Getting this wrong leaves Token readable while OpenPrice/HighPrice/
// LowPrice decode garbage, and starts the second record a byte early.
func TestMBPRecordWireOffsets(t *testing.T) {
	captureAppLogger(t)
	debugBuf := captureDebugLogger(t)

	const (
		recordsStart = broadcastDataPrefixLen + broadcastHeaderSize + 2 // 50
		record0      = recordsStart                                     // 50
		record1      = recordsStart + mbpRecordSize                     // 264
	)

	payload := make([]byte, broadcastDataPrefixLen+mbpMessageBodySize)
	copy(payload, buildBroadcastPrefix(netIDCommodityDerivative))
	copy(payload[broadcastDataPrefixLen:], buildBroadcastHeader(t, 7208, mbpMessageBodySize))

	put16 := func(off int, v int16) { binary.BigEndian.PutUint16(payload[off:], uint16(v)) }
	put32 := func(off int, v int32) { binary.BigEndian.PutUint32(payload[off:], uint32(v)) }

	put16(broadcastDataPrefixLen+broadcastHeaderSize, 2) // NoOfRecords

	// Record-relative offsets, straight from the corrected layout table.
	for i, base := range []int{record0, record1} {
		seq := int32((i + 1) * 1000)
		put32(base+0, 111+seq)   // Token
		put16(base+4, 1)         // BookType
		put16(base+6, 2)         // TradingStatus
		put32(base+8, 222+seq)   // VolumeTradedToday
		put32(base+12, 333+seq)  // LastTradedPrice
		payload[base+16] = '+'   // NetChangeIndicator
		payload[base+17] = 0xff  // the pad byte -- must not leak into any field
		put32(base+18, 444+seq)  // NetPriceChangeFromClosingPrice
		put32(base+22, 555+seq)  // LastTradeQuantity
		put32(base+52, 666+seq)  // AuctionQuantity
		put32(base+56, 777+seq)  // RecordBuffer[0].Quantity
		put16(base+176, 888)     // BbTotalBuyFlag
		put32(base+198, 999+seq) // ClosingPrice
		put32(base+202, 1+seq)   // OpenPrice
		put32(base+206, 2+seq)   // HighPrice
		put32(base+210, 3+seq)   // LowPrice
	}

	message, err := parseMSBroadcastOnlyMBP(payload)
	if err != nil {
		t.Fatalf("parseMSBroadcastOnlyMBP: %v", err)
	}
	if got := len(message.validRecords()); got != 2 {
		t.Fatalf("validRecords() = %d records, want 2", got)
	}

	for i, record := range message.validRecords() {
		seq := int32((i + 1) * 1000)
		for _, tc := range []struct {
			field string
			got   int32
			want  int32
		}{
			{"token", record.token, 111 + seq},
			{"volumeTradedToday", record.volumeTradedToday, 222 + seq},
			{"lastTradedPrice", record.lastTradedPrice, 333 + seq},
			{"netPriceChangeFromClosingPrice", record.netPriceChangeFromClosingPrice, 444 + seq},
			{"lastTradeQuantity", record.lastTradeQuantity, 555 + seq},
			{"auctionQuantity", record.auctionQuantity, 666 + seq},
			{"recordBuffer[0].quantity", record.recordBuffer[0].quantity, 777 + seq},
			{"closingPrice", record.closingPrice, 999 + seq},
			{"openPrice", record.openPrice, 1 + seq},
			{"highPrice", record.highPrice, 2 + seq},
			{"lowPrice", record.lowPrice, 3 + seq},
		} {
			if tc.got != tc.want {
				t.Errorf("record[%d].%s = %d, want %d", i, tc.field, tc.got, tc.want)
			}
		}
		if record.netChangeIndicator != '+' {
			t.Errorf("record[%d].netChangeIndicator = %q, want '+'", i, record.netChangeIndicator)
		}
		if record.bookType != 1 || record.tradingStatus != 2 || record.bbTotalBuyFlag != 888 {
			t.Errorf("record[%d]: bookType=%d tradingStatus=%d bbTotalBuyFlag=%d, want 1/2/888",
				i, record.bookType, record.tradingStatus, record.bbTotalBuyFlag)
		}
	}

	// The debug trace reports absolute offsets within the broadcast data, so it
	// lines up with a wire capture. These are the ones a 213-byte stride gets
	// wrong.
	out := debugBuf.String()
	for _, want := range []string{
		"offset=67-68",   // record[0].alignmentPad:                   50 + 17
		"offset=68-72",   // record[0].netPriceChangeFromClosingPrice: 50 + 18
		"offset=252-256", // record[0].openPrice:                      50 + 202
		"offset=264-268", // record[1].token:                          264 + 0
		"offset=474-478", // record[1].lowPrice:                       264 + 210, ending at 8+470
	} {
		if !strings.Contains(out, want) {
			t.Errorf("debug output missing %q", want)
		}
	}
}

// buildHeaderOnlyPayload builds a minimal broadcast data payload: the 8-byte
// prefix plus a header whose messageLength covers the header alone. Two of
// these fit comfortably inside the 512-byte cPackData buffer, unlike two full
// 7305 messages.
func buildHeaderOnlyPayload(t *testing.T, netID byte, sequenceNumber int32) []byte {
	t.Helper()

	buf := new(bytes.Buffer)
	buf.Write(buildBroadcastPrefix(netID))
	mustWrite(t, buf, [2]byte{})                  // reserved1
	mustWrite(t, buf, [2]byte{})                  // reserved2
	mustWrite(t, buf, int32(1700000000))          // logTime
	mustWrite(t, buf, [2]byte{'N', 'C'})          // alphaChar
	mustWrite(t, buf, int16(7202))                // transactionCode (Ticker, not parsed here)
	mustWrite(t, buf, int16(0))                   // errorCode
	mustWrite(t, buf, sequenceNumber)             // sequenceNumber
	mustWrite(t, buf, byte(0))                    // reserved3
	mustWrite(t, buf, [3]byte{})                  // reserved4
	mustWrite(t, buf, [8]byte{})                  // timestamp2
	mustWrite(t, buf, [8]byte{})                  // filler2
	mustWrite(t, buf, int16(broadcastHeaderSize)) // messageLength: header only
	return buf.Bytes()
}

// TestUncompressedSubPacketStride packs two uncompressed sub-packets into one
// buffer and checks the exact byte boundaries handed to the callback. The
// second packet is only framed correctly if the first advanced the cursor by
// prefix + messageLength.
func TestUncompressedSubPacketStride(t *testing.T) {
	captureAppLogger(t)

	first := buildHeaderOnlyPayload(t, netIDCommodityDerivative, 101)
	second := buildHeaderOnlyPayload(t, netIDCommodityDerivative, 102)
	envelope := buildEnvelope(t, netIDCommodityDerivative, []subPacket{
		{data: first},
		{data: second},
	})

	var got [][]byte
	err := processBroadcastPackData(envelope, func(i, packetCount int, packetLen int, payload []byte, compressed bool) {
		if compressed {
			t.Errorf("packet %d reported as compressed, want uncompressed", i+1)
		}
		if packetCount != 2 {
			t.Errorf("packetCount = %d, want 2", packetCount)
		}
		got = append(got, append([]byte(nil), payload...))
	})
	if err != nil {
		t.Fatalf("processBroadcastPackData: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d packets, want 2", len(got))
	}
	wantLen := broadcastDataPrefixLen + broadcastHeaderSize
	for i, payload := range got {
		if len(payload) != wantLen {
			t.Errorf("packet %d length = %d, want %d (prefix + messageLength)", i+1, len(payload), wantLen)
		}
	}
	if !bytes.Equal(got[0], first) {
		t.Errorf("packet 1 payload mismatch:\ngot  %x\nwant %x", got[0], first)
	}
	if !bytes.Equal(got[1], second) {
		t.Errorf("packet 2 payload mismatch (stride is wrong):\ngot  %x\nwant %x", got[1], second)
	}

	// Sequence numbers confirm the two packets were not swapped or shifted.
	for i, wantSeq := range []int32{101, 102} {
		header, err := parseBroadcastHeader(got[i])
		if err != nil {
			t.Fatalf("parseBroadcastHeader packet %d: %v", i+1, err)
		}
		if header.sequenceNumber != wantSeq {
			t.Errorf("packet %d sequenceNumber = %d, want %d", i+1, header.sequenceNumber, wantSeq)
		}
	}
}

// TestUncompressedFullMessageEndToEnd runs a single uncompressed 7305 packet
// through the whole pipeline (7305 is not on the spec's compressed list, so
// this is how it arrives in practice).
func TestUncompressedFullMessageEndToEnd(t *testing.T) {
	captureAppLogger(t)
	packetBuf := capturePacketLogger(t)

	envelope := buildEnvelope(t, netIDCommodityDerivative, []subPacket{
		{data: buildMSSecurityUpdateInfoMessage(t, netIDCommodityDerivative, 111, "FIRST")},
	})

	runPipeline(t, envelope)

	out := packetBuf.String()
	for _, want := range []string{"MSSecurityUpdateInfo=", "token:111", "packet=1/1"} {
		if !strings.Contains(out, want) {
			t.Errorf("packetLogger output missing %q, got:\n%s", want, out)
		}
	}
}

// TestCompressedSubPacketRoundTrip runs a compressed 7208 packet through the
// full envelope -> Decompress -> parse path.
func TestCompressedSubPacketRoundTrip(t *testing.T) {
	captureAppLogger(t)
	packetBuf := capturePacketLogger(t)

	envelope := buildEnvelope(t, netIDCommodityDerivative, []subPacket{
		{data: buildMSBroadcastOnlyMBPMessage(t, netIDCommodityDerivative, 12345), compressed: true},
	})

	runPipeline(t, envelope)

	out := packetBuf.String()
	for _, want := range []string{"MSBroadcastOnlyMBP=", "token:12345", "token:12346"} {
		if !strings.Contains(out, want) {
			t.Errorf("packetLogger output missing %q, got:\n%s", want, out)
		}
	}
}

// TestMixedCompressedAndUncompressedBuffer covers the spec's note that the
// packets within one buffer may be an admixture of compressed and
// uncompressed packets, processed FIFO.
func TestMixedCompressedAndUncompressedBuffer(t *testing.T) {
	captureAppLogger(t)
	packetBuf := capturePacketLogger(t)

	envelope := buildEnvelope(t, netIDCommodityDerivative, []subPacket{
		{data: buildMSBroadcastOnlyMBPMessage(t, netIDCommodityDerivative, 5000), compressed: true},
		{data: buildMSSecurityUpdateInfoMessage(t, netIDCommodityDerivative, 777, "MIXED")},
	})

	runPipeline(t, envelope)

	out := packetBuf.String()
	for _, want := range []string{"MSBroadcastOnlyMBP=", "token:5000", "MSSecurityUpdateInfo=", "token:777"} {
		if !strings.Contains(out, want) {
			t.Errorf("packetLogger output missing %q, got:\n%s", want, out)
		}
	}

	// FIFO order: the 7208 packet was packed first, so it must be logged first.
	if mbp, sec := strings.Index(out, "MSBroadcastOnlyMBP="), strings.Index(out, "MSSecurityUpdateInfo="); mbp > sec {
		t.Errorf("packets logged out of order: MSBroadcastOnlyMBP at %d should precede MSSecurityUpdateInfo at %d", mbp, sec)
	}
}

// TestNetIDMismatchWarnsButKeepsParsing verifies a netId mismatch is reported
// without dropping the packet -- the encoding assumption is unverified, so a
// wrong guess must never discard real data.
func TestNetIDMismatchWarnsButKeepsParsing(t *testing.T) {
	setExpectedNetID(t, netIDCommodityDerivative)
	appBuf := captureAppLogger(t)
	packetBuf := capturePacketLogger(t)

	// Envelope and payload both carry the equity netId instead.
	envelope := buildEnvelope(t, netIDEquity, []subPacket{
		{data: buildMSSecurityUpdateInfoMessage(t, netIDEquity, 314, "WRONGNET")},
	})

	runPipeline(t, envelope)

	appOut := appBuf.String()
	if !strings.Contains(appOut, "WARN envelope net_id mismatch") {
		t.Errorf("expected an envelope net_id warning, got:\n%s", appOut)
	}
	if !strings.Contains(appOut, "WARN broadcast data net_id mismatch") {
		t.Errorf("expected a broadcast data net_id warning, got:\n%s", appOut)
	}
	if !strings.Contains(packetBuf.String(), "token:314") {
		t.Errorf("packet was dropped on net_id mismatch; it should still parse, got:\n%s", packetBuf.String())
	}
}

// TestNetIDCheckDisabled verifies -netid=0 silences the comparison.
func TestNetIDCheckDisabled(t *testing.T) {
	setExpectedNetID(t, 0)
	appBuf := captureAppLogger(t)
	capturePacketLogger(t)

	envelope := buildEnvelope(t, netIDEquity, []subPacket{
		{data: buildMSSecurityUpdateInfoMessage(t, netIDEquity, 314, "ANYNET")},
	})

	runPipeline(t, envelope)

	if strings.Contains(appBuf.String(), "net_id mismatch") {
		t.Errorf("expected no mismatch warnings when the check is disabled, got:\n%s", appBuf.String())
	}
}

// TestNetIDAcceptsAsciiEncoding covers the ambiguity in the spec: cNetId is
// declared CHAR but documented with numeric values, so an ASCII digit must
// not be reported as a mismatch.
func TestNetIDAcceptsAsciiEncoding(t *testing.T) {
	setExpectedNetID(t, netIDCommodityDerivative)
	appBuf := captureAppLogger(t)
	capturePacketLogger(t)

	envelope := buildEnvelope(t, '8', []subPacket{
		{data: buildMSSecurityUpdateInfoMessage(t, '8', 555, "ASCIINET")},
	})

	runPipeline(t, envelope)

	if strings.Contains(appBuf.String(), "net_id mismatch") {
		t.Errorf("ASCII-encoded netId should be accepted, got:\n%s", appBuf.String())
	}
}

// TestCorruptCompressedPacketDoesNotCrash verifies that truncated or
// bit-flipped compressed data produces an error log line instead of
// panicking -- the regression test for switching main from Decompress (the
// unchecked variant, which can panic on malformed input) to DecompressSafe.
func TestCorruptCompressedPacketDoesNotCrash(t *testing.T) {
	appBuf := captureAppLogger(t)
	packetBuf := capturePacketLogger(t)

	valid := buildMSBroadcastOnlyMBPMessage(t, netIDCommodityDerivative, 12345)
	compressed := compressWithReference(t, valid)

	cases := []struct {
		name string
		data []byte // raw bytes injected directly as the "already compressed" sub-packet content
	}{
		{"truncated", compressed[:len(compressed)/2]},
		{"all 0xFF garbage", bytes.Repeat([]byte{0xFF}, len(compressed))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			appBuf.Reset()
			packetBuf.Reset()

			// Build the envelope by hand: buildEnvelope's compressed=true path
			// would re-compress tc.data via the reference compressor, but we
			// need these exact malformed bytes injected as the sub-packet's
			// already-compressed content.
			buf := new(bytes.Buffer)
			mustWrite(t, buf, [2]byte{0, netIDCommodityDerivative})
			mustWrite(t, buf, int16(1))
			mustWrite(t, buf, int16(len(tc.data)))
			buf.Write(tc.data)

			// This must not panic; if it does, the test fails with the panic's
			// stack trace rather than silently crashing, which is exactly what
			// distinguishes "handled" from "unhandled" here.
			runPipeline(t, buf.Bytes())

			if strings.Contains(packetBuf.String(), "MSBroadcastOnlyMBP=") {
				t.Errorf("corrupt packet should not have produced a parsed message, got:\n%s", packetBuf.String())
			}
		})
	}
}

// setDeclaredRecordCount overwrites the NoOfRecords field of a 7208 payload in
// place, leaving both records' bytes populated. This is the case that matters:
// a message that declares fewer records than it has room for.
func setDeclaredRecordCount(payload []byte, count int16) {
	const offset = broadcastDataPrefixLen + broadcastHeaderSize // NoOfRecords sits at message offset 40
	binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(count))
}

// TestRecordCountLimitsParsedRecords is the regression test for 7208 parsing
// all mbpMaxRecords slots regardless of NoOfRecords. The 470-byte message
// always has room for two records, so a message declaring fewer used to
// publish the leftover bytes as a second, entirely fabricated security.
func TestRecordCountLimitsParsedRecords(t *testing.T) {
	for _, tc := range []struct {
		declared  int16
		wantCount int16
		wantWarn  bool
	}{
		{declared: 0, wantCount: 0},
		{declared: 1, wantCount: 1},
		{declared: 2, wantCount: 2},
		{declared: 9, wantCount: 2, wantWarn: true},  // corrupt: clamped down
		{declared: -1, wantCount: 0, wantWarn: true}, // corrupt: clamped up to zero
	} {
		t.Run(fmt.Sprintf("declared=%d", tc.declared), func(t *testing.T) {
			appBuf := captureAppLogger(t)

			payload := buildMSBroadcastOnlyMBPMessage(t, netIDCommodityDerivative, 12345)
			setDeclaredRecordCount(payload, tc.declared)

			message, err := parseMSBroadcastOnlyMBP(payload)
			if err != nil {
				t.Fatalf("parseMSBroadcastOnlyMBP: %v", err)
			}

			if message.recordCount != tc.wantCount {
				t.Errorf("recordCount = %d, want %d", message.recordCount, tc.wantCount)
			}
			if got := len(message.validRecords()); got != int(tc.wantCount) {
				t.Errorf("len(validRecords()) = %d, want %d", got, tc.wantCount)
			}

			// Every slot past the declared count must stay zeroed, even though
			// the wire bytes behind it hold a plausible-looking record.
			for i := int(tc.wantCount); i < mbpMaxRecords; i++ {
				if message.records[i] != (InteractiveOnlyMBPData{}) {
					t.Errorf("record[%d] was parsed despite recordCount=%d: token=%d lastTradedPrice=%d",
						i, tc.wantCount, message.records[i].token, message.records[i].lastTradedPrice)
				}
			}
			// ...and every slot within it must still be real.
			for i := 0; i < int(tc.wantCount); i++ {
				if want := int32(12345 + i); message.records[i].token != want {
					t.Errorf("record[%d].token = %d, want %d", i, message.records[i].token, want)
				}
			}

			if warned := strings.Contains(appBuf.String(), "record count out of range"); warned != tc.wantWarn {
				t.Errorf("out-of-range warning present = %v, want %v; log:\n%s", warned, tc.wantWarn, appBuf.String())
			}
		})
	}
}

// TestParsedMessageLogOmitsUndeclaredRecords checks the fabricated record does
// not reach the packet log either -- the parser zeroing it is only half the
// fix if the logger prints the whole fixed-size array anyway.
func TestParsedMessageLogOmitsUndeclaredRecords(t *testing.T) {
	captureAppLogger(t)
	packetBuf := capturePacketLogger(t)

	payload := buildMSBroadcastOnlyMBPMessage(t, netIDCommodityDerivative, 12345)
	setDeclaredRecordCount(payload, 1)

	processPacket(nil, 0, 1, len(payload), payload)

	out := packetBuf.String()
	if !strings.Contains(out, "token:12345") {
		t.Errorf("packet log missing the declared record, got:\n%s", out)
	}
	if strings.Contains(out, "token:12346") {
		t.Errorf("packet log contains the undeclared second record, got:\n%s", out)
	}
}

// TestUncompressedRejectsShortMessageLength covers the framing hazard in the
// uncompressed path: messageLength counts from the broadcast header onward, so
// a value below the header size cannot be real. Accepting one advances the
// cursor too little and misframes every later sub-packet in the same buffer.
func TestUncompressedRejectsShortMessageLength(t *testing.T) {
	captureAppLogger(t)

	const messageLengthOffset = broadcastDataPrefixLen + 38

	for _, messageLength := range []int16{-294, -1, 0, 1, 8, broadcastHeaderSize - 1} {
		t.Run(fmt.Sprintf("messageLength=%d", messageLength), func(t *testing.T) {
			payload := buildHeaderOnlyPayload(t, netIDCommodityDerivative, 101)
			binary.BigEndian.PutUint16(payload[messageLengthOffset:messageLengthOffset+2], uint16(messageLength))
			envelope := buildEnvelope(t, netIDCommodityDerivative, []subPacket{{data: payload}})

			called := false
			err := processBroadcastPackData(envelope, func(int, int, int, []byte, bool) { called = true })
			if err == nil {
				t.Fatalf("messageLength=%d was accepted, want an error", messageLength)
			}
			if called {
				t.Errorf("messageLength=%d delivered a sub-packet before failing", messageLength)
			}
			if !strings.Contains(err.Error(), "below the") {
				t.Errorf("unexpected error for messageLength=%d: %v", messageLength, err)
			}
		})
	}

	// The boundary value is legitimate: a header-only message.
	t.Run("messageLength=broadcastHeaderSize", func(t *testing.T) {
		envelope := buildEnvelope(t, netIDCommodityDerivative, []subPacket{
			{data: buildHeaderOnlyPayload(t, netIDCommodityDerivative, 101)},
		})
		if err := processBroadcastPackData(envelope, func(int, int, int, []byte, bool) {}); err != nil {
			t.Fatalf("header-only message rejected: %v", err)
		}
	})
}

// TestHeaderParseFailureStopsDispatch verifies processPacket returns instead of
// running the transaction-code switch against a zero-valued header, which used
// to report every unparseable payload as "transaction code not handled 0".
func TestHeaderParseFailureStopsDispatch(t *testing.T) {
	appBuf := captureAppLogger(t)
	packetBuf := capturePacketLogger(t)

	processPacket(nil, 0, 1, 9, make([]byte, 9)) // too short for prefix + header

	if !strings.Contains(appBuf.String(), "ERROR parse broadcast header") {
		t.Errorf("expected a header parse error, got:\n%s", appBuf.String())
	}
	for _, unwanted := range []string{"transaction code not handled", "INFO wrote reply_bytes"} {
		if strings.Contains(packetBuf.String()+appBuf.String(), unwanted) {
			t.Errorf("dispatch continued past the header failure: found %q in:\n%s%s", unwanted, appBuf.String(), packetBuf.String())
		}
	}
}

// TestOversizedPackedDataIsReported checks that trimming to the 512-byte
// cPackData buffer is announced. Silently dropping the tail turns a broken
// size assumption into unexplained framing errors much further downstream.
func TestOversizedPackedDataIsReported(t *testing.T) {
	appBuf := captureAppLogger(t)

	envelope := buildEnvelope(t, netIDCommodityDerivative, []subPacket{
		{data: buildMSSecurityUpdateInfoMessage(t, netIDCommodityDerivative, 111, "FIRST")},
	})
	envelope = append(envelope, bytes.Repeat([]byte{0xAA}, maxDatagramLen)...)

	var delivered int
	if err := processBroadcastPackData(envelope, func(int, int, int, []byte, bool) { delivered++ }); err != nil {
		t.Fatalf("processBroadcastPackData: %v", err)
	}
	if delivered != 1 {
		t.Errorf("delivered %d sub-packets, want 1 (the packets that do fit must still be processed)", delivered)
	}
	if !strings.Contains(appBuf.String(), "exceeding the 512-byte cPackData buffer") {
		t.Errorf("oversized packed data was trimmed without a warning, got:\n%s", appBuf.String())
	}
}

func TestCappedWriterStopsAtLimit(t *testing.T) {
	captureAppLogger(t)

	var sink bytes.Buffer
	w := newCappedWriter(&sink, 10)

	// A write that fits.
	if n, err := w.Write([]byte("12345")); n != 5 || err != nil {
		t.Fatalf("Write = (%d, %v), want (5, nil)", n, err)
	}
	// A write that would cross the limit is dropped whole, and reports success
	// so the logger does not spam stderr for every subsequent packet.
	if n, err := w.Write([]byte("678901")); n != 6 || err != nil {
		t.Fatalf("over-limit Write = (%d, %v), want (6, nil)", n, err)
	}
	if n, err := w.Write([]byte("x")); n != 1 || err != nil {
		t.Fatalf("post-limit Write = (%d, %v), want (1, nil)", n, err)
	}
	if got := sink.String(); got != "12345" {
		t.Errorf("sink = %q, want %q", got, "12345")
	}

	// Zero means unlimited.
	var unbounded bytes.Buffer
	u := newCappedWriter(&unbounded, 0)
	big := bytes.Repeat([]byte{'z'}, 4096)
	if _, err := u.Write(big); err != nil {
		t.Fatalf("unlimited Write: %v", err)
	}
	if unbounded.Len() != len(big) {
		t.Errorf("unlimited writer wrote %d bytes, want %d", unbounded.Len(), len(big))
	}
}

func init() {
	// Keep test output readable: individual tests opt into capturing these.
	appLogger.SetOutput(io.Discard)
	appLogger.SetFlags(log.LstdFlags)
	initPacketLogger(io.Discard)
}
