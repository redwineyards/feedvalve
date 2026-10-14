package app

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// The read loop's decisions (Vol 1 section 3): the source-address check
// (section 3.5), the oversize rule (section 3.8) and the read-error policy
// (section 3.7).
//
// These drive a real loopback socket rather than a fake connection, for the
// same reason the lifecycle tests do: the thing under test is what the loop
// does with what the KERNEL hands it, and ReadFromUDPAddrPort's behaviour on a
// short read is the whole premise of the oversize rule.

// receiverFixture is a running graph plus a sender aimed at it.
type receiverFixture struct {
	g         *Graph
	buf       *syncBuf
	sender    net.Conn
	delivered *atomic.Int64
	stop      func()
}

// startReceiver runs a graph and returns once the socket is bound and a sender
// is pointed at it. The decode callback counts datagrams, which is how the
// tests tell "processed" from "discarded".
func startReceiver(t *testing.T, tune func(*config.Config)) *receiverFixture {
	t.Helper()

	var delivered atomic.Int64
	g, buf := testGraphWith(t, "127.0.0.1:0", tune, func(*model.Datagram) error {
		delivered.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "the listener to be bound", func() bool { return g.LocalAddr() != nil })

	sender, err := net.Dial("udp", g.LocalAddr().String())
	if err != nil {
		cancel()
		<-done
		t.Fatalf("dialling the listener: %v", err)
	}

	f := &receiverFixture{g: g, buf: buf, sender: sender, delivered: &delivered}
	f.stop = func() {
		sender.Close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run returned %v, want nil for a cancelled context", err)
		}
	}
	t.Cleanup(f.stop)
	return f
}

// send writes one datagram of n bytes. The content does not matter to the read
// loop -- framing is the decoder's half and arrives with P2-05 -- so the bytes
// are recognisable filler with a plausible envelope header.
func (f *receiverFixture) send(t *testing.T, n int) {
	t.Helper()
	d := make([]byte, n)
	d[0] = 8 // cNetId: commodity derivatives
	if n >= 4 {
		d[3] = 1 // iNoPackets = 1
	}
	for i := 4; i < n; i++ {
		d[i] = byte(i)
	}
	if _, err := f.sender.Write(d); err != nil {
		t.Fatalf("sending a %d-byte datagram: %v", n, err)
	}
}

// TestG_FRM_3_OversizeDatagramIsTrimmedLoudly is gate G-FRM-3 (Vol 4
// section 7.3): a 600-byte datagram must produce a WARN, increment
// datagrams_oversize, be trimmed to 512 -- "and the sub-packets that fit are
// still processed".
//
// Trim-and-process rather than discard, because the sub-packets inside the
// first 512 bytes are well-framed and good, and on a circuit with no
// retransmission (bible section 1) discarding them costs real information to
// punish the sender for a framing surprise. But it is logged LOUDLY: HLD F6's
// operator action is "the envelope assumption has changed -- escalate to the
// exchange before trusting any decode", and an operator cannot act on a line
// that does not exist.
//
// The defect the gate names is an exactly-516-byte read buffer, which would
// make this datagram indistinguishable from a full one, since a recvfrom-style
// read discards the excess and reports no error.
func TestG_FRM_3_OversizeDatagramIsTrimmedLoudly(t *testing.T) {
	f := startReceiver(t, nil)

	const oversize = 600
	f.send(t, oversize)
	waitUntil(t, "the oversize datagram to be counted", func() bool {
		return f.g.inst().datagramsOversize.Load() == 1
	})
	waitUntil(t, "the oversize datagram to be processed anyway", func() bool {
		return f.delivered.Load() == 1
	})

	log := f.buf.String()
	if !strings.Contains(log, "oversize datagram") {
		t.Errorf("no oversize WARN in the log:\n%s", log)
	}
	// The TRUE length, not the trimmed one. This is the whole point of doing
	// the check before the copy into the ring's fixed slot: 600 is the number
	// an operator takes to the exchange.
	if !strings.Contains(log, "600 bytes") {
		t.Errorf("the WARN does not carry the true 600-byte length:\n%s", log)
	}
	if !strings.Contains(log, "escalate to the exchange") {
		t.Errorf("the WARN does not carry F6's operator action:\n%s", log)
	}
}

// TestOversizeBoundaryIsTheEnvelopeCap pins the comparison to 516.
//
// The counter means "the envelope assumption broke" (bible section 3.2: 516 is
// cNetId[2] + iNoPackets[2] + cPackData[512]). One off-by-one either way and it
// either fires on every full datagram -- which would make it permanently
// non-zero and therefore ignored, the worst outcome for an assumption check --
// or stops firing on a real break.
//
// The two sends are ordered so neither assertion can pass vacuously: the 516
// case is only checked once that datagram has actually arrived.
func TestOversizeBoundaryIsTheEnvelopeCap(t *testing.T) {
	f := startReceiver(t, nil)

	f.send(t, wire.MaxDatagramLen) // exactly 516
	waitUntil(t, "the full-size datagram to arrive", func() bool { return f.delivered.Load() == 1 })
	if got := f.g.inst().datagramsOversize.Load(); got != 0 {
		t.Fatalf("a datagram of exactly %d counted as oversize (%d)", wire.MaxDatagramLen, got)
	}

	f.send(t, wire.MaxDatagramLen+1) // 517
	waitUntil(t, "the one-byte-over datagram to be counted", func() bool {
		return f.g.inst().datagramsOversize.Load() == 1
	})
	waitUntil(t, "it to be processed anyway", func() bool { return f.delivered.Load() == 2 })
}

// TestSourceCheckWarnsButDoesNotDropByDefault is Vol 1 section 3.5 with
// sourceEnforce at its default false.
//
// The datagram is still processed, and that mirrors the netId philosophy
// deliberately: a wrong allow-list silently blinds the feed, and there is no
// retransmission to recover what it swallowed (HLD F19, bible section 1). The
// counter and the WARN are how an operator finds out the list is wrong --
// before arming it.
func TestSourceCheckWarnsButDoesNotDropByDefault(t *testing.T) {
	f := startReceiver(t, func(c *config.Config) {
		// Loopback is not in 10/8, so every datagram this test sends is
		// "unexpected".
		c.UDP.SourceAllowList = []string{"10.0.0.0/8"}
		c.UDP.SourceEnforce = false
	})

	f.send(t, 64)
	waitUntil(t, "the unexpected source to be counted", func() bool {
		return f.g.inst().datagramsUnexpectedSrc.Load() == 1
	})
	waitUntil(t, "the datagram to be processed anyway", func() bool {
		return f.delivered.Load() == 1
	})

	if log := f.buf.String(); !strings.Contains(log, "unexpected source") {
		t.Errorf("no unexpected-source WARN in the log:\n%s", log)
	}
}

// TestSourceCheckDropsWhenEnforced is the same input with the operator's switch
// armed.
//
// This is THE ONE PLACE FeedValve discards data on purpose (section 3.5), which
// is why it is off by default and why Vol 0 section 9.2's "there is no drop
// policy" has to name it explicitly rather than leaving it to be discovered.
//
// The counter is waited on FIRST so that "not delivered" cannot pass because
// the datagram was simply lost on loopback: reaching the counter proves it
// arrived and was checked.
func TestSourceCheckDropsWhenEnforced(t *testing.T) {
	f := startReceiver(t, func(c *config.Config) {
		c.UDP.SourceAllowList = []string{"10.0.0.0/8"}
		c.UDP.SourceEnforce = true
	})

	f.send(t, 64)
	waitUntil(t, "the unexpected source to be counted", func() bool {
		return f.g.inst().datagramsUnexpectedSrc.Load() == 1
	})

	// It arrived, it was counted, and it must not have reached the decoder.
	if got := f.delivered.Load(); got != 0 {
		t.Errorf("%d datagrams were processed with sourceEnforce set", got)
	}
	if got := f.g.inst().datagramsReceived.Load(); got != 1 {
		t.Errorf("datagrams_received = %d, want 1: an enforced drop still counts the read", got)
	}
}

// TestEmptyAllowListSkipsTheCheck is the default configuration, and the
// distinction that matters: an empty list means "skip the check", never
// "matched against nothing". Read the second way it would reject every datagram
// the exchange ever sent.
func TestEmptyAllowListSkipsTheCheck(t *testing.T) {
	f := startReceiver(t, nil)

	f.send(t, 64)
	waitUntil(t, "the datagram to be processed", func() bool { return f.delivered.Load() == 1 })

	if got := f.g.inst().datagramsUnexpectedSrc.Load(); got != 0 {
		t.Errorf("datagrams_unexpected_source = %d with an empty allow-list", got)
	}
	if log := f.buf.String(); strings.Contains(log, "unexpected source") {
		t.Errorf("an empty allow-list produced a source WARN:\n%s", log)
	}

	// Step 5 of section 3.4, while a datagram is known to have arrived: the
	// liveness FSM reads this gauge, and a receiver that never stores it looks
	// like a dead feed.
	if got := f.g.inst().lastDatagramMonoNs.Load(); got <= 0 {
		t.Errorf("last_datagram_mono_ns = %d after a datagram arrived", got)
	}
}

// TestSourceAllowedMatching covers the matching rule away from the socket.
func TestSourceAllowedMatching(t *testing.T) {
	prefixes := func(t *testing.T, entries ...string) []netip.Prefix {
		t.Helper()
		cfg := config.UDPConfig{SourceAllowList: entries}
		p, err := cfg.SourcePrefixes()
		if err != nil {
			t.Fatalf("SourcePrefixes(%v): %v", entries, err)
		}
		return p
	}

	for _, tc := range []struct {
		name    string
		entries []string
		src     string
		want    bool
	}{
		{"bare IP matches itself", []string{"10.1.2.3"}, "10.1.2.3:9000", true},
		{"bare IP does not match a neighbour", []string{"10.1.2.3"}, "10.1.2.4:9000", false},
		{"CIDR matches inside", []string{"10.0.0.0/8"}, "10.9.9.9:9000", true},
		{"CIDR does not match outside", []string{"10.0.0.0/8"}, "11.0.0.1:9000", false},
		{"second entry matches", []string{"10.0.0.0/8", "192.168.0.0/16"}, "192.168.4.4:9000", true},
		{"loopback against 10/8", []string{"10.0.0.0/8"}, "127.0.0.1:9000", false},

		// An operator writing "10.1.2.3/8" means the 10/8 network. Masked at
		// parse time, so the hot path does no masking.
		{"host bits in the entry still match the network", []string{"10.1.2.3/8"}, "10.9.9.9:9000", true},

		// A v4-mapped v6 source must compare against a v4 prefix the way an
		// operator expects, which is what the Unmap is for.
		{"v4-mapped v6 source", []string{"10.0.0.0/8"}, "[::ffff:10.1.1.1]:9000", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, err := netip.ParseAddrPort(tc.src)
			if err != nil {
				t.Fatalf("parsing %q: %v", tc.src, err)
			}
			if got := sourceAllowed(prefixes(t, tc.entries...), src); got != tc.want {
				t.Errorf("sourceAllowed(%v, %s) = %v, want %v", tc.entries, tc.src, got, tc.want)
			}
		})
	}
}

// TestReadErrorPolicy is Vol 1 section 3.7, tested as the policy it is rather
// than through an injected connection -- which would mean an interface with one
// production implementation, and YAGNI overrules that.
func TestReadErrorPolicy(t *testing.T) {
	live := context.Background()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("a read error during shutdown is shutdown", func(t *testing.T) {
		var p readErrorPolicy
		if got := p.classify(errors.New("whatever"), cancelled); got != readShutdown {
			t.Errorf("classify with a cancelled context = %v, want readShutdown", got)
		}
		// poc logged this as a failure, and an ERROR line at every clean stop
		// trains operators to ignore the level.
		if p.consecutive != 0 {
			t.Errorf("a shutdown read error counted toward the bound (%d)", p.consecutive)
		}
	})

	t.Run("ErrClosed is shutdown even with a live context", func(t *testing.T) {
		// The lifecycle closes the socket; the read returns ErrClosed. The
		// context cancellation that caused it may not have been observed yet.
		var p readErrorPolicy
		if got := p.classify(net.ErrClosed, live); got != readShutdown {
			t.Errorf("classify(net.ErrClosed) = %v, want readShutdown", got)
		}
	})

	t.Run("errors below the bound keep reading", func(t *testing.T) {
		var p readErrorPolicy
		for i := 1; i < maxConsecutiveReadErrors; i++ {
			if got := p.classify(errors.New("transient"), live); got != readContinue {
				t.Fatalf("error %d of %d = %v, want readContinue", i, maxConsecutiveReadErrors, got)
			}
		}
		if got := p.classify(errors.New("transient"), live); got != readFatal {
			t.Fatalf("error %d = %v, want readFatal", maxConsecutiveReadErrors, got)
		}
	})

	t.Run("a successful read resets the count", func(t *testing.T) {
		// Without this the bound would be "64 errors ever" rather than "64 in
		// a row", and a session with 64 unrelated transients spread over a day
		// would stop the process at market close for no reason.
		var p readErrorPolicy
		for range maxConsecutiveReadErrors - 1 {
			p.classify(errors.New("transient"), live)
		}
		p.ok()
		if p.consecutive != 0 {
			t.Fatalf("consecutive = %d after a good read", p.consecutive)
		}
		if got := p.classify(errors.New("transient"), live); got != readContinue {
			t.Errorf("the first error after a good read = %v, want readContinue", got)
		}
	})
}

// TestWaitTreatsAReaderErrorAsTerminal closes the defect P1-09 left: the reader
// returned after maxConsecutiveReadErrors and NOTHING woke the lifecycle, so
// the process sat in wait with no receiver, no terminal condition and no
// shutdown -- holding an Oracle pool open and reading no feed. Silent, and
// invisible to every existing test.
func TestWaitTreatsAReaderErrorAsTerminal(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", nil)

	g.readerStarted = true
	g.readerErr = errors.New("persistent read failure")
	close(g.readerDone)

	ev, terminal := g.wait(context.Background())
	if !terminal {
		t.Fatal("a reader that stopped on an error was not treated as terminal")
	}
	if ev.reason != FatalUDPReadFailure {
		t.Errorf("reason = %v, want FatalUDPReadFailure", ev.reason)
	}
	if ev.reason.String() != "V7-udp-read-failure" {
		t.Errorf("reason names itself %q; Vol 1 section 12 calls it V-7", ev.reason.String())
	}
	if !strings.Contains(ev.detail, "persistent read failure") {
		t.Errorf("detail = %q, which does not carry the error", ev.detail)
	}
}

// TestPersistentReadErrorsAreTerminal drives the whole path the two tests
// above only cover in halves: a real socket failing reads, the policy reaching
// its bound, the reader recording the error, wait observing it, and Run
// returning non-nil so main maps it to exit code 4.
//
// The stimulus is a read deadline in the past, which makes every read return
// os.ErrDeadlineExceeded immediately -- not ErrClosed, and with the context
// still alive, so it is a genuine failure by section 3.7's rule. Note what
// this is NOT: section 3.6 rejects SetReadDeadline as a shutdown mechanism for
// three stated reasons. Using it here as a way to make reads fail on demand is
// a test stimulus, not an endorsement.
//
// Before P2-04 this scenario produced a silent zombie: the reader returned and
// the lifecycle kept waiting forever with no receiver.
func TestPersistentReadErrorsAreTerminal(t *testing.T) {
	g, buf := testGraph(t, "127.0.0.1:0", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	waitUntil(t, "the listener to be bound", func() bool { return g.LocalAddr() != nil })

	// Every subsequent read fails at once.
	if err := g.getConn().SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("setting a past read deadline: %v", err)
	}

	// Bounded, and the bound is the assertion. If the reader's error goes
	// unrecorded, or wait stops observing the reader, Run does not return at
	// all -- so the failure mode of this test without its fix is a HANG. A
	// hanging test reports as a ten-minute timeout with no message, which is
	// nearly useless, so the wait is explicit and says what it means.
	var err error
	select {
	case err = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after persistent read failures: the lifecycle is still " +
			"waiting with no receiver. This is the silent zombie -- an idle process holding " +
			"its resources open and reading no feed")
	}
	if err == nil {
		t.Fatal("Run returned nil after persistent read failures; the process would have " +
			"kept running with no receiver")
	}

	var exit *ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("Run returned %T (%v), want an *ExitError", err, err)
	}
	if exit.Code != ExitTerminal {
		t.Errorf("exit code = %d, want ExitTerminal (%d)", exit.Code, ExitTerminal)
	}
	if !strings.Contains(err.Error(), "V7-udp-read-failure") {
		t.Errorf("the terminal error does not name V-7: %v", err)
	}
	if got := g.inst().udpReadErrors.Load(); got < uint64(maxConsecutiveReadErrors) {
		t.Errorf("udp_read_errors = %d, want at least %d", got, maxConsecutiveReadErrors)
	}
	if log := buf.String(); !strings.Contains(log, "consecutive UDP read errors") {
		t.Errorf("no giving-up ERROR in the log:\n%s", lastLines(log, 6))
	}
}

// lastLines keeps a failure message readable when the log holds 63 error lines.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// TestWaitIgnoresACleanReaderExit is the other half: the reader returning
// WITHOUT an error means the socket was closed under it, i.e. a shutdown
// already under way. That must not be reported as a terminal condition, and
// wait must not spin on the closed channel either -- which is what nilling the
// local copy is for.
func TestWaitIgnoresACleanReaderExit(t *testing.T) {
	g, _ := testGraph(t, "127.0.0.1:0", nil)

	g.readerStarted = true
	close(g.readerDone) // no readerErr: a clean stop

	ctx, cancel := context.WithCancel(context.Background())
	go cancel()

	ev, terminal := g.wait(ctx)
	if terminal {
		t.Errorf("a clean reader exit was reported as terminal (%v)", ev.reason)
	}
}

// TestPerDatagramChecksAllocateNothing is the receiver's share of G-ALLOC-1 for
// the work P2-04 adds. ReadFromUDPAddrPort is what makes it possible:
// PacketConn.ReadFrom would hand back a heap-allocated *net.UDPAddr per
// datagram, 2,400 times a second, and no amount of care further down would
// recover it.
func TestPerDatagramChecksAllocateNothing(t *testing.T) {
	cfg := config.UDPConfig{SourceAllowList: []string{"10.0.0.0/8", "192.168.0.0/16"}}
	prefixes, err := cfg.SourcePrefixes()
	if err != nil {
		t.Fatalf("SourcePrefixes: %v", err)
	}
	src := netip.MustParseAddrPort("127.0.0.1:9000")
	n := 600

	if allocs := testing.AllocsPerRun(1000, func() {
		if sourceAllowed(prefixes, src) {
			t.Fatal("loopback matched 10/8")
		}
		if n <= wire.MaxDatagramLen {
			t.Fatal("600 is not over the envelope cap")
		}
	}); allocs != 0 {
		t.Fatalf("the per-datagram checks allocate %v times, want 0", allocs)
	}
}
