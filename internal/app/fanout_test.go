package app

import (
	"strings"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/model"
)

// The output boundary: Vol 1 section 10.5's send discipline, section 10.6's
// fatal path, and the two stamps section 10.2 gives the receiver rather than
// the decoder.
//
// The property under test throughout is the one the requirement states in four
// words -- "FeedValve discards no unit anywhere". Every path below either
// delivers the unit or stops the process; there is no third outcome, and a
// test that accepted one would be accepting the defect this card exists to
// remove.

// The fixture message is deliberately SMALL. Real envelopes cap packed data at
// 512 bytes (cPackData[512], bible section 3.2), so framing takes
// min(n-4, 512) -- and a fixture built from several 470-byte messages silently
// loses every sub-packet after the first, which reads as a fan-out bug rather
// than as an unrepresentative fixture.
const (
	fanoutBodyLen    = 60
	fanoutPayloadLen = 8 + fanoutBodyLen // the prefix, then the declared body
)

// fanoutFixture is a receiver with Q1 sized by the test.
type fanoutFixture struct {
	*subPacketFixture
	g *Graph
}

// newFanoutFixture builds a receiver whose decoder emits units units per
// message, with Q1 at the given shape.
//
// shards is 1 unless a test says otherwise, which makes INV-ORDER directly
// observable: every unit lands in one channel, so the order units come out in
// IS the order they were sent in.
func newFanoutFixture(t *testing.T, units, shards, capacity, budgetMs int) *fanoutFixture {
	t.Helper()

	const code = 7208
	f := newSubPacketFixture(t, &fakeRegistry{
		code: code,
		entry: &decode.RegistryEntry{Code: code, Dec: &scriptedDecoder{
			code: code, bodyLen: fanoutBodyLen, maxUnits: units,
			onDecode: func(h model.MsgHeader, _ []byte, out []model.UpdateUnit) (int, error) {
				for i := range units {
					// A distinct Token per unit so the order assertion can tell
					// them apart, and Code from the header so the fatal line
					// has something true to report.
					out[i] = model.UpdateUnit{Token: int32(i + 1), Code: h.Code}
				}
				return units, nil
			},
		}},
	}, func(c *config.Config) {
		c.Runtime.Shards = shards
		c.Runtime.GeneralShard = 0
		c.Runtime.ShardQueueCapacity = capacity
		c.UDP.StallBudgetMs = budgetMs
	})
	return &fanoutFixture{subPacketFixture: f, g: f.r.g}
}

// prime does what decodeDatagram does to the per-datagram state, for the tests
// that call route directly rather than driving a whole datagram.
func (f *fanoutFixture) prime(recvMono int64) {
	f.r.t0 = time.Now()
	f.r.recvMono = recvMono
	f.r.datagramBytes = 512
	f.r.unitsInDatagram = 0
	f.r.fatalErr = nil
}

// freezeSince replaces the fan-out's clock for the duration of one test.
func freezeSince(t *testing.T, d time.Duration) {
	t.Helper()
	prev := since
	since = func(time.Time) time.Duration { return d }
	t.Cleanup(func() { since = prev })
}

// drain reads n units off a shard channel, failing rather than hanging.
func drain(t *testing.T, ch chan model.UpdateUnit, n int) []model.UpdateUnit {
	t.Helper()
	out := make([]model.UpdateUnit, 0, n)
	for range n {
		select {
		case u := <-ch:
			out = append(out, u)
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d units reached the shard", len(out), n)
		}
	}
	return out
}

// TestFanOutDeliversEveryUnitOfEverySubPacketInOrder is the card's first "done
// when" and INV-ORDER (section 10.7).
//
// Two sub-packets, two units each, one shard: all four must arrive, in
// datagram order then FIFO sub-packet order then record order. The guarantee
// holds structurally -- one goroutine does all of it with no queue in between
// (ADR-001) -- so what this pins is that nothing in the fan-out reorders or
// loses what the structure already ordered.
func TestFanOutDeliversEveryUnitOfEverySubPacketInOrder(t *testing.T) {
	f := newFanoutFixture(t, 2, 1, 4096, 50)

	d := datagramOf(t,
		uncompressedSub(bcastPayload(7208, fanoutBodyLen, fanoutPayloadLen)),
		uncompressedSub(bcastPayload(7208, fanoutBodyLen, fanoutPayloadLen)),
	)
	if err := f.r.decodeDatagram(d); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}

	got := drain(t, f.g.shardCh[0], 4)
	want := []int32{1, 2, 1, 2} // record order within each of two sub-packets
	for i, u := range got {
		if u.Token != want[i] {
			t.Errorf("unit %d has token %d, want %d -- fan-out order is not wire order",
				i, u.Token, want[i])
		}
	}
	if n := len(f.g.shardCh[0]); n != 0 {
		t.Errorf("%d units left queued; four were produced and four should have been read", n)
	}
}

// TestFanOutStampsRecvMonoOnEveryUnit is section 10.3.
//
// The receipt clock is read ONCE per datagram and stamped on every unit it
// produces -- 2,400 clock reads a second instead of 20,000 -- and it is
// recorded unconditionally, never as a fallback. The case that forces it:
// destination 2's five codes share one row per MarketType, and a same-day
// re-open sends 6531 -> 6571 -> 6511 inside one second. On LogTime alone the
// last two are indistinguishable from the first and the ordering guard
// discards them, so the market never reaches Open in the database.
//
// BREAK: leave RecvMono unset. Every unit then carries zero, the guard's final
// tiebreak compares 0 > 0, and the re-open is lost silently.
func TestFanOutStampsRecvMonoOnEveryUnit(t *testing.T) {
	f := newFanoutFixture(t, 2, 1, 4096, 50)

	const recv = 1234567890
	var d model.Datagram
	d.Buf[0] = 8
	d.Buf[3] = 1
	copy(d.Buf[4:], uncompressedSub(bcastPayload(7208, fanoutBodyLen, fanoutPayloadLen)))
	d.N = 4 + 2 + fanoutPayloadLen
	d.RecvMono = recv

	if err := f.r.decodeDatagram(&d); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}
	for i, u := range drain(t, f.g.shardCh[0], 2) {
		if u.RecvMono != recv {
			t.Errorf("unit %d has RecvMono %d, want the datagram's %d", i, u.RecvMono, recv)
		}
	}
}

// TestFanOutFlagsAGeneralUnitWithNoLogTime is the FlagOrderingUnusable rule of
// section 10.3.
//
// A general unit's primary ordering component is LogTime (Vol 2 section 5.3's
// guard compares it plainly and never looks at Seq for a general unit), so one
// with no LogTime has nothing for the guard to compare and the guard must be
// bypassed for it. A token-scoped unit is never flagged: its primary component
// is Seq, which a BCAST_HEADER always carries.
func TestFanOutFlagsAGeneralUnitWithNoLogTime(t *testing.T) {
	f := newFanoutFixture(t, 1, 1, 4096, 50)
	f.prime(99)

	cases := []struct {
		name string
		u    model.UpdateUnit
		want bool
	}{
		{"general with no LogTime", model.UpdateUnit{Flags: model.FlagGeneral}, true},
		{"general with a LogTime", model.UpdateUnit{Flags: model.FlagGeneral, LogTime: 1}, false},
		{"token-scoped with no LogTime", model.UpdateUnit{Token: 7}, false},
	}
	for _, c := range cases {
		units := []model.UpdateUnit{c.u}
		if err := f.r.route(units, 0, 1); err != nil {
			t.Fatalf("%s: route: %v", c.name, err)
		}
		got := drain(t, f.g.shardCh[0], 1)[0]
		if has := got.Flags.Has(model.FlagOrderingUnusable); has != c.want {
			t.Errorf("%s: FlagOrderingUnusable = %v, want %v", c.name, has, c.want)
		}
	}
}

// TestFanOutCountsUnitsEmitted is section 10.1. n == 0 is a normal outcome and
// not a reject, so it must not be counted as anything else.
func TestFanOutCountsUnitsEmitted(t *testing.T) {
	f := newFanoutFixture(t, 1, 1, 4096, 50)
	f.prime(1)

	if err := f.r.route(nil, 0, 1); err != nil {
		t.Fatalf("route of zero units: %v", err)
	}
	if n := f.in.unitsEmitted.Load(); n != 0 {
		t.Errorf("units_emitted = %d after a zero-unit message, want 0", n)
	}

	for _, n := range []int{1, 3, 2} {
		units := make([]model.UpdateUnit, n)
		if err := f.r.route(units, 0, 1); err != nil {
			t.Fatalf("route of %d units: %v", n, err)
		}
		drain(t, f.g.shardCh[0], n)
	}
	if got := f.in.unitsEmitted.Load(); got != 6 {
		t.Errorf("units_emitted = %d, want 6", got)
	}
	if got := f.in.unitsPerPacket.Load(); got != 3 {
		t.Errorf("units_per_packet_max = %d, want 3 -- the high-water mark, not the last", got)
	}
}

// TestFanOutFastPathCountsNothingAndReadsNoClock is section 10.5 property 3.
//
// At the shard occupancy HLD section 6.6 computes -- 0.08% busy -- essentially
// every send takes the default arm, so a time.Since there would add 20,000
// clock reads a second to measure a quantity that is zero. The clock is
// replaced with one that panics, which is a stronger assertion than a
// benchmark: the fast path does not read it AT ALL.
//
// shard_send_blocked is the counter that says so from the other side. Vol 4 Q4
// makes any non-zero value a defect signal rather than a load signal, so a
// fast send must leave it at zero.
func TestFanOutFastPathCountsNothingAndReadsNoClock(t *testing.T) {
	f := newFanoutFixture(t, 1, 1, 4096, 50)

	prev := since
	since = func(time.Time) time.Duration {
		panic("the fan-out fast path read the clock")
	}
	t.Cleanup(func() { since = prev })

	f.prime(1)
	units := make([]model.UpdateUnit, 8)
	if err := f.r.route(units, 0, 1); err != nil {
		t.Fatalf("route: %v", err)
	}
	drain(t, f.g.shardCh[0], 8)

	if n := f.in.shardSendBlocked[0].Load(); n != 0 {
		t.Errorf("shard_send_blocked{0} = %d after eight unobstructed sends, want 0", n)
	}
	if n := f.in.stallBudgetUsedMaxNs.Load(); n != 0 {
		t.Errorf("stall_budget_used_max_ns = %d with nothing ever blocked, want 0", n)
	}
}

// TestFanOutBlockedSendThatSucceedsCountsTheWait is the middle outcome: the
// channel was full, the send waited, and it got through inside the budget.
//
// Nothing is lost and the process continues. What must be visible afterwards
// is that it HAPPENED -- shard_send_blocked is F24, the only early warning
// there is before the fatal path, and stall_budget_used_max_ns is how an
// operator sees the distance to the cliff.
func TestFanOutBlockedSendThatSucceedsCountsTheWait(t *testing.T) {
	f := newFanoutFixture(t, 1, 1, 1, 100)
	ch := f.g.shardCh[0]
	ch <- model.UpdateUnit{Token: 999} // fill it

	// Freed shortly after the send blocks, well inside the budget.
	go func() {
		time.Sleep(10 * time.Millisecond)
		<-ch
	}()

	f.prime(1)
	if err := f.r.route([]model.UpdateUnit{{Token: 1}}, 0, 1); err != nil {
		t.Fatalf("route should have succeeded inside the budget: %v", err)
	}
	if u := drain(t, ch, 1)[0]; u.Token != 1 {
		t.Errorf("the waiting unit arrived as token %d, want 1", u.Token)
	}
	if n := f.in.shardSendBlocked[0].Load(); n != 1 {
		t.Errorf("shard_send_blocked{0} = %d, want exactly 1 -- it counts sends that "+
			"had to WAIT", n)
	}
	if n := f.in.stallBudgetUsedMaxNs.Load(); n == 0 {
		t.Error("stall_budget_used_max_ns is zero after a send that waited 10 ms")
	}
	select {
	case ev := <-f.g.fatal:
		t.Fatalf("a send that succeeded reported a terminal condition: %v", ev.reason)
	default:
	}
}

// TestFanOutBudgetExpiryIsFatal is the card's second "done when" and section
// 10.6.
//
// req section 10.2 permits exactly one response to a critical-path queue
// overflowing and Vol 0 section 9.2 states the alternative in one line --
// "there is none". The unit is NOT dropped, the process stops.
//
// Both arms of the discipline are exercised: the timer actually firing, and
// the budget already being spent by an earlier unit of the same datagram.
func TestFanOutBudgetExpiryIsFatal(t *testing.T) {
	t.Run("the timer fires", func(t *testing.T) {
		f := newFanoutFixture(t, 1, 1, 1, 1)
		f.g.shardCh[0] <- model.UpdateUnit{} // full, and nothing drains it
		f.prime(1)

		err := f.r.route([]model.UpdateUnit{{Token: 1, Code: 7208}}, 0, 1)
		if err != model.ErrStallBudgetExpired {
			t.Fatalf("route returned %v, want ErrStallBudgetExpired", err)
		}
		assertQ1Fatal(t, f)
	})

	t.Run("an earlier unit already spent it", func(t *testing.T) {
		f := newFanoutFixture(t, 1, 1, 1, 50)
		f.g.shardCh[0] <- model.UpdateUnit{}
		f.prime(1)
		// Past the budget before this unit was even offered, which is the
		// shape section 10.5 checks BEFORE arming the timer -- Reset with a
		// negative duration fires immediately and would reach the same place
		// by a more confusing route.
		freezeSince(t, time.Hour)

		err := f.r.route([]model.UpdateUnit{{Token: 1, Code: 7208}}, 0, 1)
		if err != model.ErrStallBudgetExpired {
			t.Fatalf("route returned %v, want ErrStallBudgetExpired", err)
		}
		assertQ1Fatal(t, f)
	})
}

// assertQ1Fatal checks that the terminal condition was reported once, under
// the right reason, and that the log line carries section 10.6's table.
func assertQ1Fatal(t *testing.T, f *fanoutFixture) {
	t.Helper()

	select {
	case ev := <-f.g.fatal:
		if ev.reason != FatalQ1Overflow {
			t.Errorf("terminal reason %v, want FatalQ1Overflow (F9)", ev.reason)
		}
	default:
		t.Fatal("no terminal condition reported; the process would have carried on " +
			"with a saturated queue, which is the outcome req 10.2 forbids")
	}

	// Section 10.6's table, field by field. The line has to be diagnosable on
	// its own: HLD section 10.2 requires a terminal message to be readable
	// from the log alone, with no live process left to query.
	line := f.buf.String()
	for _, want := range []string{
		"shard=0", "depth=1", "cap=1", "code=7208", "token=1",
		"unit_index=0/1", "units_in_datagram=1", "subpacket=1/1",
		"datagram_bytes=512", "budget_ms=", "used_ns=", "depths=0:1",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("the terminal line does not carry %q:\n%s", want, line)
		}
	}
}

// TestFanOutBudgetIsPerDatagramNotPerUnit is section 10.5 property 1, and HLD
// section 6.3 states the failure it prevents: "a per-unit deadline would let a
// single datagram stall for 35 x 50 ms and satisfy its own rule the whole way
// down".
//
// Two units into a channel with room for one, against the REAL clock. A
// frozen clock cannot test this: freezing since() makes every t0 look equally
// expired, so resetting t0 per unit changes nothing and the break walks
// straight through. That is how this test was first written, and it passed
// against the defect it was meant to catch.
//
// What is asserted is the BOUND ITSELF -- how long the whole datagram was
// allowed to stall -- rather than a side effect of it, because that is the
// quantity NFR4 cares about and the only one a per-unit deadline changes.
//
// The timeline, budget 100 ms, one slot, one drain and no second:
//
//	t=0    unit 1 offered, channel full, blocks
//	t=60   a slot frees; unit 1 goes through having used 60 of the 100
//	t=60   unit 2 offered, channel full again, blocks with 40 ms left
//	t=100  the DATAGRAM's budget is gone -> FATAL
//
// BREAK: take t0 inside send, per unit. Unit 2 gets a fresh 100 ms from t=60
// and the datagram does not stop until t=160 -- still fatal, so an
// error-only assertion passes the defect. It is the 60 extra milliseconds
// that are the bug, and at 35 units they are HLD section 6.3's "a single
// datagram stalls for 35 x 50 ms and satisfies its own rule the whole way
// down".
func TestFanOutBudgetIsPerDatagramNotPerUnit(t *testing.T) {
	f := newFanoutFixture(t, 2, 1, 1, 100)
	ch := f.g.shardCh[0]
	ch <- model.UpdateUnit{} // full, and freed exactly once

	go func() {
		time.Sleep(60 * time.Millisecond)
		<-ch
	}()

	f.prime(1)
	start := time.Now()
	err := f.r.route([]model.UpdateUnit{{Token: 1}, {Token: 2}}, 0, 1)
	elapsed := time.Since(start)

	if err != model.ErrStallBudgetExpired {
		t.Fatalf("route returned %v, want ErrStallBudgetExpired", err)
	}
	// Both units blocked, which is what says unit 1 got through and unit 2 is
	// the casualty. A 1 here means unit 1 never made it -- a very slow
	// machine -- and the timing assertion below would then be measuring the
	// wrong thing, so this guards it.
	if n := f.in.shardSendBlocked[0].Load(); n != 2 {
		t.Fatalf("shard_send_blocked{0} = %d, want 2: unit 1 waited and got through, "+
			"unit 2 waited and did not. This run proves nothing about the shared "+
			"budget", n)
	}
	// Halfway between the shared bound (100 ms) and the per-unit one (160 ms).
	const limit = 130 * time.Millisecond
	if elapsed >= limit {
		t.Errorf("the datagram stalled for %v before stopping, over the %v bound.\n"+
			"One budget is shared by every unit a datagram fans out (section 10.5 "+
			"property 1); %v means the second unit was handed a fresh one, and 35 "+
			"units would stall for 35 budgets while satisfying the rule at every step",
			elapsed, limit, elapsed)
	}
}

// TestFanOutStopsTheRestOfTheDatagramAfterExpiry.
//
// framing.Split's handler returns nothing, so a stall-budget expiry is sticky
// on the receiver and short-circuits the sub-packets that follow it in the
// same datagram. Decoding them would produce units with nowhere to go, and
// counting them would overstate what the process actually handled before it
// stopped.
func TestFanOutStopsTheRestOfTheDatagramAfterExpiry(t *testing.T) {
	f := newFanoutFixture(t, 1, 1, 1, 1)
	f.g.shardCh[0] <- model.UpdateUnit{} // full, never drained

	d := datagramOf(t,
		uncompressedSub(bcastPayload(7208, fanoutBodyLen, fanoutPayloadLen)),
		uncompressedSub(bcastPayload(7208, fanoutBodyLen, fanoutPayloadLen)),
		uncompressedSub(bcastPayload(7208, fanoutBodyLen, fanoutPayloadLen)),
	)
	if err := f.r.decodeDatagram(d); err != model.ErrStallBudgetExpired {
		t.Fatalf("decodeDatagram returned %v, want ErrStallBudgetExpired -- the error "+
			"must survive Split unwinding and reach the drain", err)
	}
	if n := f.in.subpacketsTotal.Load(); n != 1 {
		t.Errorf("subpackets_total = %d, want 1: the two after the expiry must be "+
			"skipped, not decoded into units with nowhere to go", n)
	}
}

// TestFanOutResetsPerDatagramStateBetweenDatagrams.
//
// fatalErr is sticky WITHIN a datagram, which is the point -- and it must not
// be sticky ACROSS them, or one expiry would silently suppress every datagram
// after it rather than stopping the process. The lifecycle stops the process;
// the receiver must not pretend to.
func TestFanOutResetsPerDatagramStateBetweenDatagrams(t *testing.T) {
	f := newFanoutFixture(t, 2, 1, 4096, 50)
	f.r.fatalErr = model.ErrStallBudgetExpired

	d := datagramOf(t, uncompressedSub(bcastPayload(7208, fanoutBodyLen, fanoutPayloadLen)))
	if err := f.r.decodeDatagram(d); err != nil {
		t.Fatalf("decodeDatagram: %v", err)
	}
	drain(t, f.g.shardCh[0], 2)
	if f.r.unitsInDatagram != 2 {
		t.Errorf("unitsInDatagram = %d after one two-unit datagram, want 2", f.r.unitsInDatagram)
	}
}

// TestFanOutAllocatesNothing is the forward defence for G-ALLOC-1 (P2-09).
//
// Run now rather than at the end of the phase: it is far cheaper to keep at
// zero than to get back to zero, and finding which of ten tasks introduced an
// allocation is work that need not exist. A 360-byte channel send copies into
// the ring buffer and must not escape to the heap.
func TestFanOutAllocatesNothing(t *testing.T) {
	f := newFanoutFixture(t, 1, 4, 4096, 50)
	units := make([]model.UpdateUnit, 2)
	units[1].Flags = model.FlagGeneral

	// Primed once: the measurement is of route, not of the per-datagram reset,
	// which decodeDatagram does and which is three field stores.
	f.prime(1)

	got := testing.AllocsPerRun(1000, func() {
		if err := f.r.route(units, 0, 1); err != nil {
			t.Fatalf("route: %v", err)
		}
		for k := range f.g.shardCh {
			for len(f.g.shardCh[k]) > 0 {
				<-f.g.shardCh[k]
			}
		}
	})
	if got != 0 {
		t.Errorf("the fan-out allocated %v times per run, want 0 (G-ALLOC-1)", got)
	}
}
