package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// SIGINT/SIGTERM cancel this context, which unblocks the receive loop and
	// lets run's deferred cleanup -- notably closing the database connection --
	// actually happen. Everything below returns errors rather than calling
	// appLogger.Fatalf, because Fatalf calls os.Exit and os.Exit skips defers.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		appLogger.Printf("ERROR %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	udpAddr := flag.String("udp", ":9000", "UDP listen address")
	outPath := flag.String("out", "tcp_reply.log", "file to append decompressed packet data to")
	debug := flag.Bool("debug", false, "log per-field offset/hex/value while parsing, plus a hex dump of each decompressed payload")
	configPath := flag.String("config", "", "path to a JSON config file providing defaults for the other flags (command-line flags take precedence)")
	netID := flag.Int("netid", netIDCommodityDerivative, "expected segment netId (equity 4, equity derivative 2, currency derivative 6, commodity derivative 8); mismatches are logged, never dropped; 0 disables the check")
	maxOut := flag.Int64("maxout", defaultMaxOutBytes, "stop appending to the -out file after this many bytes, so a busy feed cannot fill the disk; 0 disables the limit")
	dbCheck := flag.Bool("dbcheck", false, "connect to the database configured under \"db\" in -config, verify a round trip, report the server version, and exit without starting the relay")
	flag.Parse()

	var dbCfg DBConfig
	dbConfigured := false

	if *configPath != "" {
		cfg, err := loadConfig(*configPath)
		if err != nil {
			return fmt.Errorf("load config path=%s: %w", *configPath, err)
		}

		explicit := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

		if cfg.UDP != nil && !explicit["udp"] {
			*udpAddr = *cfg.UDP
		}
		if cfg.Out != nil && !explicit["out"] {
			*outPath = *cfg.Out
		}
		if cfg.Debug != nil && !explicit["debug"] {
			*debug = *cfg.Debug
		}
		if cfg.NetID != nil && !explicit["netid"] {
			*netID = *cfg.NetID
		}
		if cfg.MaxOut != nil && !explicit["maxout"] {
			*maxOut = *cfg.MaxOut
		}
		if cfg.DB != nil {
			dbCfg, err = cfg.DB.toDBConfig()
			if err != nil {
				return fmt.Errorf("load config path=%s: %w", *configPath, err)
			}
			dbConfigured = true
		}
	}

	if *debug {
		initDebugLogger(os.Stderr)
	}
	expectedNetID = *netID

	if *dbCheck {
		if !dbConfigured {
			return fmt.Errorf("-dbcheck needs a \"db\" object in -config (and the password in %s)", dbPasswordEnv)
		}
		if err := runDBCheck(ctx, dbCfg); err != nil {
			return fmt.Errorf("database check failed: %w", err)
		}
		return nil
	}

	// The connection is opened once here and lives for the whole process.
	// Defers run last-registered-first: clear the packet-path pointer first,
	// then close the database.
	if dbConfigured {
		db, err := OpenDB(ctx, dbCfg)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				appLogger.Printf("ERROR close database err=%v", err)
			} else {
				appLogger.Printf("INFO database connection closed")
			}
		}()

		setPacketDB(db)
		defer setPacketDB(nil)
	} else {
		appLogger.Printf("INFO no \"db\" object in -config; per-packet database updates are disabled")
	}

	udpConn, err := net.ListenPacket("udp", *udpAddr)
	if err != nil {
		return fmt.Errorf("listen udp addr=%s: %w", *udpAddr, err)
	}
	defer udpConn.Close()

	outFile, err := os.OpenFile(*outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open output file path=%s: %w", *outPath, err)
	}
	defer outFile.Close()
	initPacketLogger(newCappedWriter(outFile, *maxOut))

	appLogger.Printf("INFO listening udp=%s output=%s max_output_bytes=%d", *udpAddr, *outPath, *maxOut)

	receiveLoop(ctx, udpConn)
	appLogger.Printf("INFO shutting down")
	return nil
}

// receiveLoop reads datagrams until ctx is cancelled. Cancellation closes the
// connection, which unblocks the in-flight ReadFrom -- there is no other way to
// interrupt it -- so a read error while ctx is done means "shutting down"
// rather than a real failure.
func receiveLoop(ctx context.Context, udpConn net.PacketConn) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			appLogger.Printf("INFO shutdown signal received, closing listener")
			udpConn.Close()
		case <-done:
		}
	}()

	buf := make([]byte, udpReadBufferLen)
	decompressBuf := make([]byte, maxDecompressedPacketLen)
	for {
		n, from, err := udpConn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			appLogger.Printf("ERROR read udp err=%v", err)
			continue
		}
		if n > maxDatagramLen {
			appLogger.Printf("WARN datagram from=%s is %d bytes, larger than the %d-byte BcastPackData envelope -- the feed does not match the assumed layout",
				from, n, maxDatagramLen)
		}

		if err := processBroadcastPackData(buf[:n], func(i, packetCount int, compressedLength int, compressedData []byte, compressed bool) {
			handleSubPacket(from, i, packetCount, compressedLength, compressedData, compressed, decompressBuf)
		}); err != nil {
			appLogger.Printf("ERROR parse udp packet from=%s err=%v", from, err)
			continue
		}
	}
}

// handleSubPacket decompresses (if needed) and parses one sub-packet. It
// recovers from panics so a single malformed or corrupt packet logs an error
// and the relay keeps running, instead of the whole process crashing.
// Decompression uses DecompressSafe (bounds-checked) rather than Decompress
// (the unchecked variant, documented to potentially panic on malformed
// input) since compressedData here is untrusted network input.
func handleSubPacket(from net.Addr, i, packetCount int, compressedLength int, compressedData []byte, compressed bool, decompressBuf []byte) {
	defer func() {
		if r := recover(); r != nil {
			appLogger.Printf("ERROR panic handling packet packet=%d/%d from=%s recovered=%v", i+1, packetCount, from, r)
		}
	}()

	if !compressed {
		processPacket(from, i, packetCount, compressedLength, compressedData)
		return
	}

	n, err := DecompressSafe(decompressBuf, compressedData)
	if err != nil {
		appLogger.Printf("ERROR decompress packet packet=%d/%d from=%s err=%v", i+1, packetCount, from, err)
		return
	}

	processPacket(from, i, packetCount, compressedLength, decompressBuf[:n])
}

// expectedNetID is the segment netId the feed is expected to carry. Zero
// disables comparison (observed values are still logged). Defaults to the
// Commodity Derivatives segment, matching the protocol spec this decoder
// targets; override with -netid.
var expectedNetID = netIDCommodityDerivative

// netIDMatches reports whether an observed byte matches expectedNetID. The
// spec gives netId as a small number but declares it CHAR, so accept both the
// binary encoding and the ASCII digit rather than guessing which one the feed
// uses.
func netIDMatches(observed byte) bool {
	return int(observed) == expectedNetID || (expectedNetID >= 0 && expectedNetID <= 9 && observed == byte('0'+expectedNetID))
}

// checkEnvelopeNetID warns when the envelope's cNetId does not carry the
// expected segment. cNetId is CHAR[2] holding a small number, so a match in
// either byte is accepted. Never drops data -- the encoding assumption is
// itself unverified.
func checkEnvelopeNetID(netID []byte) {
	if expectedNetID == 0 {
		appLogger.Printf("INFO envelope net_id=%x (check disabled)", netID)
		return
	}
	if netIDMatches(netID[0]) || netIDMatches(netID[1]) {
		return
	}
	appLogger.Printf("WARN envelope net_id mismatch expected=%d observed=%x (as ascii %q) -- wrong segment, or cNetId is encoded differently than assumed",
		expectedNetID, netID, netID)
}

// checkPacketNetID warns when the netId repeated in byte 0 of a broadcast
// payload does not match the expected segment. On packet 2 or later of a
// buffer this most often means the previous sub-packet's length was computed
// wrongly, leaving the cursor misaligned.
func checkPacketNetID(from net.Addr, packetIndex, packetCount int, observed byte) {
	if expectedNetID == 0 || netIDMatches(observed) {
		return
	}
	hint := "wrong segment, or the netId prefix is encoded differently than assumed"
	if packetIndex > 0 {
		hint = "likely frame misalignment: the previous sub-packet's length was probably computed wrongly"
	}
	appLogger.Printf("WARN broadcast data net_id mismatch from=%s packet=%d/%d expected=%d observed=%d -- %s",
		from, packetIndex+1, packetCount, expectedNetID, observed, hint)
}

func processPacket(from net.Addr, packetIndex, packetCount int, sentBytes int, reply []byte) {
	packetLogger.Printf("INFO from=%s packet=%d/%d sent_bytes=%d reply_bytes=%d reply=%q",
		from, packetIndex+1, packetCount, sentBytes, len(reply), reply)

	if debugEnabled {
		debugLogger.Printf("DEBUG from=%s packet=%d/%d broadcast data (%d bytes):\n%s",
			from, packetIndex+1, packetCount, len(reply), hexDump(reply))
	}

	if len(reply) >= broadcastDataPrefixLen {
		checkPacketNetID(from, packetIndex, packetCount, reply[0])
	}

	header, err := parseBroadcastHeader(reply)
	if err != nil {
		// Without a header there is no transaction code to dispatch on;
		// continuing would run the switch against a zero-valued header and
		// report transaction code 0 as "not handled".
		appLogger.Printf("ERROR parse broadcast header from=%s packet=%d/%d err=%v", from, packetIndex+1, packetCount, err)
		return
	}
	appLogger.Printf("INFO from=%s packet=%d/%d header_reserved1=%x header_reserved2=%x header_log_time=%d header_alpha_char=%q header_transaction_code=%d header_error_code=%d header_sequence_number=%d header_reserved3=%x header_reserved4=%x header_timestamp2=%q header_filler2=%x header_message_length=%d",
		from, packetIndex+1, packetCount, header.reserved1, header.reserved2, header.logTime, header.alphaChar, header.transactionCode, header.errorCode, header.sequenceNumber, header.reserved3, header.reserved4, header.timestamp2, header.filler2, header.messageLength)

	switch header.transactionCode {
	case 7208:
		message, err := parseMSBroadcastOnlyMBP(reply)
		if err != nil {
			appLogger.Printf("ERROR parse MSBroadcastOnlyMBP from=%s packet=%d/%d err=%v", from, packetIndex+1, packetCount, err)
			break
		}
		packetLogger.Printf("INFO MSBroadcastOnlyMBP={header:%+v recordCount:%d records:%+v}", message.header, message.recordCount, message.validRecords())
		updateDummyRowForPacket(from, packetIndex, packetCount, header.transactionCode, len(reply))
	case 7305:
		message, err := parseMSSecurityUpdateInfo(reply)
		if err != nil {
			appLogger.Printf("ERROR parse MSSecurityUpdateInfo from=%s packet=%d/%d err=%v", from, packetIndex+1, packetCount, err)
			break
		}
		packetLogger.Printf("INFO MSSecurityUpdateInfo=%+v", message)
		updateDummyRowForPacket(from, packetIndex, packetCount, header.transactionCode, len(reply))
	default:
		packetLogger.Printf("INFO transaction code not handled in processPacket transaction_code=%d", header.transactionCode)
		updateDummyRowForPacket(from, packetIndex, packetCount, header.transactionCode, len(reply))
	}

	appLogger.Printf("INFO wrote reply_bytes=%d packet=%d/%d from=%s", len(reply), packetIndex+1, packetCount, from)
}

// parseBroadcastHeader parses the broadcast header out of a full broadcast
// data payload -- that is, data still carrying the broadcastDataPrefixLen
// prefix bytes. The header starts at the 9th byte; the prefix is skipped but
// reported by parseNetIDPrefix for diagnostics.
func parseBroadcastHeader(data []byte) (BroadcastHeader, error) {
	const headerEnd = broadcastDataPrefixLen + broadcastHeaderSize

	var header BroadcastHeader
	if len(data) < headerEnd {
		return header, fmt.Errorf("broadcast header too short: got %d bytes, need %d (%d prefix + %d header)",
			len(data), headerEnd, broadcastDataPrefixLen, broadcastHeaderSize)
	}

	parseNetIDPrefix(data)

	r := newFieldReader(data[broadcastDataPrefixLen:headerEnd], broadcastDataPrefixLen)
	copy(header.reserved1[:], r.bytes("header.reserved1", 2))
	copy(header.reserved2[:], r.bytes("header.reserved2", 2))
	header.logTime = r.i32("header.logTime")
	copy(header.alphaChar[:], r.bytes("header.alphaChar", 2))
	header.transactionCode = r.i16("header.transactionCode")
	header.errorCode = r.i16("header.errorCode")
	header.sequenceNumber = r.i32("header.sequenceNumber")
	header.reserved3 = r.byte1("header.reserved3")
	copy(header.reserved4[:], r.bytes("header.reserved4", 3))
	copy(header.timestamp2[:], r.bytes("header.timestamp2", 8))
	copy(header.filler2[:], r.bytes("header.filler2", 8))
	header.messageLength = r.i16("header.messageLength")
	return header, nil
}

// parseNetIDPrefix returns the netId repeated in byte 0 of a broadcast data
// payload, logging the whole ignored prefix so it stays visible when
// diagnosing framing problems. data must already be length-checked.
func parseNetIDPrefix(data []byte) byte {
	if debugEnabled {
		debugLogger.Printf("DEBUG field=%-30s offset=%d-%d hex=%x value=%d (bytes 1-7 ignored per spec)",
			"broadcastData.netIdPrefix", 0, broadcastDataPrefixLen, data[:broadcastDataPrefixLen], data[0])
	}
	return data[0]
}

func parseMSBroadcastOnlyMBP(data []byte) (MSBroadcastOnlyMBP, error) {
	const (
		bodySize    = mbpMessageBodySize
		messageSize = broadcastDataPrefixLen + bodySize
		headerEnd   = broadcastDataPrefixLen + broadcastHeaderSize
	)

	var message MSBroadcastOnlyMBP
	if len(data) < messageSize {
		return message, fmt.Errorf("MSBroadcastOnlyMBP too short: got %d bytes, need %d", len(data), messageSize)
	}

	header, err := parseBroadcastHeader(data[:headerEnd])
	if err != nil {
		return message, err
	}
	message.header = header

	r := newFieldReader(data[headerEnd:], headerEnd)

	// Only the first NoOfRecords records carry real data. The message is
	// fixed-size, so parsing all mbpMaxRecords slots unconditionally would
	// decode the trailing padding into a second, entirely fabricated security
	// -- indistinguishable downstream from a real one.
	declaredRecords := r.i16("recordCount")
	message.recordCount = declaredRecords
	if declaredRecords < 0 || int(declaredRecords) > mbpMaxRecords {
		message.recordCount = min(max(declaredRecords, 0), mbpMaxRecords)
		appLogger.Printf("WARN MSBroadcastOnlyMBP record count out of range declared=%d clamped=%d max=%d -- corrupt message or frame misalignment",
			declaredRecords, message.recordCount, mbpMaxRecords)
	}
	for i := 0; i < int(message.recordCount); i++ {
		message.records[i] = parseInteractiveOnlyMBPData(r, i)
	}

	return message, nil
}

func parseInteractiveOnlyMBPData(r *fieldReader, recordIndex int) InteractiveOnlyMBPData {
	field := func(name string) string { return fmt.Sprintf("record[%d].%s", recordIndex, name) }

	var record InteractiveOnlyMBPData
	record.token = r.i32(field("token"))
	record.bookType = r.i16(field("bookType"))
	record.tradingStatus = r.i16(field("tradingStatus"))
	record.volumeTradedToday = r.i32(field("volumeTradedToday"))
	record.lastTradedPrice = r.i32(field("lastTradedPrice"))
	record.netChangeIndicator = r.byte1(field("netChangeIndicator"))
	// The wire structs are pack(2): CHAR members are byte aligned, everything
	// else is word aligned (spec Ch.2, "Guidelines for Designers" item 3).
	// NetChangeIndicator is the only odd-sized member, so this is the one place
	// a pad byte falls -- the LONG that follows sits at record offset 18, not
	// 17. Spec Table 81's offset column omits it and so does not add up to the
	// declared 470-byte parent; see mbpRecordSize.
	r.pad(field("alignmentPad"), 1)
	record.netPriceChangeFromClosingPrice = r.i32(field("netPriceChangeFromClosingPrice"))
	record.lastTradeQuantity = r.i32(field("lastTradeQuantity"))
	record.lastTradeTime = r.i32(field("lastTradeTime"))
	record.averageTradePrice = r.i32(field("averageTradePrice"))
	record.auctionNumber = r.i16(field("auctionNumber"))
	record.auctionStatus = r.i16(field("auctionStatus"))
	record.initiatorType = r.i16(field("initiatorType"))
	record.initiatorPrice = r.i32(field("initiatorPrice"))
	record.initiatorQuantity = r.i32(field("initiatorQuantity"))
	record.auctionPrice = r.i32(field("auctionPrice"))
	record.auctionQuantity = r.i32(field("auctionQuantity"))

	for i := range record.recordBuffer {
		entry := func(name string) string { return fmt.Sprintf("%s.recordBuffer[%d].%s", field(""), i, name) }
		record.recordBuffer[i] = MBPInformation{
			quantity:       r.i32(entry("quantity")),
			price:          r.i32(entry("price")),
			numberOfOrders: r.i16(entry("numberOfOrders")),
			bbBuySellFlag:  r.i16(entry("bbBuySellFlag")),
		}
	}

	record.bbTotalBuyFlag = r.i16(field("bbTotalBuyFlag"))
	record.bbTotalSellFlag = r.i16(field("bbTotalSellFlag"))
	record.totalBuyQuantity = r.f64(field("totalBuyQuantity"))
	record.totalSellQuantity = r.f64(field("totalSellQuantity"))
	record.indicator.flags = r.byte1(field("indicator.flags"))
	record.indicator.reserved = r.byte1(field("indicator.reserved"))
	record.closingPrice = r.i32(field("closingPrice"))
	record.openPrice = r.i32(field("openPrice"))
	record.highPrice = r.i32(field("highPrice"))
	record.lowPrice = r.i32(field("lowPrice"))
	return record
}

func parseMSSecurityUpdateInfo(data []byte) (MSSecurityUpdateInfo, error) {
	const (
		bodySize    = 294
		messageSize = broadcastDataPrefixLen + bodySize
		headerEnd   = broadcastDataPrefixLen + broadcastHeaderSize
	)

	var message MSSecurityUpdateInfo
	if len(data) < messageSize {
		return message, fmt.Errorf("MSSecurityUpdateInfo too short: got %d bytes, need %d", len(data), messageSize)
	}

	header, err := parseBroadcastHeader(data[:headerEnd])
	if err != nil {
		return message, err
	}
	message.header = header

	r := newFieldReader(data[headerEnd:], headerEnd)
	message.token = r.i32("token")
	message.securityInfo = parseSecurityInfo(r)
	message.permittedToTrade = r.i16("permittedToTrade")
	message.issuedCapital = r.f64("issuedCapital")
	message.warningQuantity = r.i32("warningQuantity")
	message.freezeQuantity = r.i32("freezeQuantity")
	copy(message.creditRating[:], r.bytes("creditRating", 12))
	for i := range message.eligibilityPerMarket {
		message.eligibilityPerMarket[i] = parseSecurityEligibilityPerMarket(r, i)
	}
	message.issueRate = r.i16("issueRate")
	message.issueStartDate = r.i32("issueStartDate")
	message.interestPaymentDate = r.i32("interestPaymentDate")
	message.issueMaturityDate = r.i32("issueMaturityDate")
	message.marginPercentage = r.i32("marginPercentage")
	message.minimumLotQuantity = r.i32("minimumLotQuantity")
	message.boardLotQuantity = r.i32("boardLotQuantity")
	message.tickSize = r.i32("tickSize")
	copy(message.name[:], r.bytes("name", 25))
	message.reserved = r.byte1("reserved")
	message.listingDate = r.i32("listingDate")
	message.expulsionDate = r.i32("expulsionDate")
	message.reAdmissionDate = r.i32("reAdmissionDate")
	message.recordDate = r.i32("recordDate")
	message.lowPriceRange = r.i32("lowPriceRange")
	message.highPriceRange = r.i32("highPriceRange")
	message.expiryDate = r.i32("expiryDate")
	message.noDeliveryStartDate = r.i32("noDeliveryStartDate")
	message.noDeliveryEndDate = r.i32("noDeliveryEndDate")
	message.eligibilityIndicators.flags = r.byte1("eligibilityIndicators.flags")
	message.eligibilityIndicators.reserved = r.byte1("eligibilityIndicators.reserved")
	message.bookClosureStartDate = r.i32("bookClosureStartDate")
	message.bookClosureEndDate = r.i32("bookClosureEndDate")
	message.exerciseStartDate = r.i32("exerciseStartDate")
	message.exerciseEndDate = r.i32("exerciseEndDate")
	message.multiplier = r.i32("multiplier")
	copy(message.assetInstrument[:], r.bytes("assetInstrument", 6))
	copy(message.assetName[:], r.bytes("assetName", 10))
	message.underlyingToken = r.i32("underlyingToken")
	message.intrinsicValue = r.i32("intrinsicValue")
	message.extrinsicValue = r.i32("extrinsicValue")
	message.purpose.flags1 = r.byte1("purpose.flags1")
	message.purpose.flags2 = r.byte1("purpose.flags2")
	message.localUpdateDateTime = r.i32("localUpdateDateTime")
	message.deleteFlag = r.byte1("deleteFlag")
	copy(message.remark[:], r.bytes("remark", 25))
	message.basePrice = r.i32("basePrice")
	return message, nil
}

func parseSecurityInfo(r *fieldReader) SecurityInfo {
	var securityInfo SecurityInfo
	copy(securityInfo.instrumentName[:], r.bytes("securityInfo.instrumentName", 6))
	copy(securityInfo.symbol[:], r.bytes("securityInfo.symbol", 10))
	copy(securityInfo.series[:], r.bytes("securityInfo.series", 2))
	securityInfo.expiryDate = r.i32("securityInfo.expiryDate")
	securityInfo.strikePrice = r.i32("securityInfo.strikePrice")
	copy(securityInfo.optionType[:], r.bytes("securityInfo.optionType", 2))
	securityInfo.caLevel = r.i16("securityInfo.caLevel")
	return securityInfo
}

func parseSecurityEligibilityPerMarket(r *fieldReader, index int) SecurityEligibilityPerMarket {
	return SecurityEligibilityPerMarket{
		flags:  r.byte1(fmt.Sprintf("eligibilityPerMarket[%d].flags", index)),
		status: r.i16(fmt.Sprintf("eligibilityPerMarket[%d].status", index)),
	}
}

func processBroadcastPackData(data []byte, process func(int, int, int, []byte, bool)) error {
	const (
		netIDLen       = 2
		packetCountLen = 2
		packetLenSize  = 2
		headerLen      = netIDLen + packetCountLen
	)

	if len(data) < headerLen {
		return fmt.Errorf("packet too short: got %d bytes, need at least %d", len(data), headerLen)
	}

	checkEnvelopeNetID(data[0:netIDLen])

	packetCount := int(int16(binary.BigEndian.Uint16(data[netIDLen:headerLen])))
	if packetCount < 0 {
		return fmt.Errorf("negative packet count: %d", packetCount)
	}

	packedDataLen := len(data) - headerLen
	if packedDataLen > maxPackedDataLen {
		// cPackData is a fixed 512 bytes, so anything past it cannot be
		// framed. Trim rather than drop the whole buffer -- the sub-packets
		// that do fit are still good -- but say so, because silently
		// discarding the tail turns a size-assumption breakage into a stream
		// of unexplained framing errors further down.
		appLogger.Printf("WARN packed data is %d bytes, exceeding the %d-byte cPackData buffer; trailing %d bytes ignored",
			packedDataLen, maxPackedDataLen, packedDataLen-maxPackedDataLen)
		packedDataLen = maxPackedDataLen
	}
	packedData := data[headerLen : headerLen+packedDataLen]

	offset := 0
	for i := 0; i < packetCount; i++ {
		if offset+packetLenSize > len(packedData) {
			return fmt.Errorf("packet %d length missing at offset %d", i+1, offset)
		}

		packetLen := int(int16(binary.BigEndian.Uint16(packedData[offset : offset+packetLenSize])))
		offset += packetLenSize
		compressed := true
		if packetLen < 0 {
			return fmt.Errorf("packet %d has negative length: %d", i+1, packetLen)
		}
		if packetLen == 0 {
			// Uncompressed: the sub-packet carries no length of its own, so it
			// comes from the header's messageLength, which covers the message
			// from the broadcast header onward -- the prefix sits outside it.
			compressed = false
			header, err := parseBroadcastHeader(packedData[offset:])
			if err != nil {
				return fmt.Errorf("packet %d header length lookup: %w", i+1, err)
			}
			// messageLength covers the message from the broadcast header
			// onward, so it can never be smaller than the header itself. A
			// short value would not just yield an unparseable sub-packet: it
			// advances the cursor too little and misframes every sub-packet
			// after it in this buffer.
			if header.messageLength < broadcastHeaderSize {
				return fmt.Errorf("packet %d header message length %d is below the %d-byte broadcast header", i+1, header.messageLength, broadcastHeaderSize)
			}
			packetLen = broadcastDataPrefixLen + int(header.messageLength)
		}
		if offset+packetLen > len(packedData) {
			return fmt.Errorf("packet %d length %d exceeds packed data remaining %d", i+1, packetLen, len(packedData)-offset)
		}

		process(i, packetCount, packetLen, packedData[offset:offset+packetLen], compressed)
		offset += packetLen
	}

	return nil
}
