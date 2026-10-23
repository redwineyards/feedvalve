package app

import (
	"testing"

	"feedvalve/internal/config"
)

// changeSuppressed is change_suppressed summed over the shards.
func changeSuppressed(g *Graph) (s uint64) {
	for k := range g.inst().mirrorShards {
		s += g.inst().mirrorShards[k].ChangeSuppressed.Load()
	}
	return s
}

// repeatStream sends the SAME datagrams (two rows per shard) n times, each
// round only after the previous one was merged and, when tick is false, after
// its flush completed. It returns the rows the first round wrote. With tick
// true the test stands in for the class trigger by sending a flush tick.
func repeatStream(t *testing.T, g *Graph, n int, tick bool) (perRound uint64) {
	t.Helper()
	var payloads [][]byte
	for _, toks := range tokensPerShard(g.deps.Cfg.Runtime.Shards, 2) {
		payloads = append(payloads, mbpPayloadTokens(toks...))
	}
	perRoundRows := uint64(2 * g.deps.Cfg.Runtime.Shards)
	for i := 1; i <= n; i++ {
		suppressedBefore := changeSuppressed(g)
		for _, p := range payloads {
			sendOnce(t, g, p)
		}
		// Hand-written: 7208 feeds one destination, one row per token, so a round
		// of two tokens per shard creates 2 x shards rows, once (repeats re-use them).
		waitUntil(t, "the round to be merged", func() bool { return sum3(createdByDest(g)) >= perRoundRows })
		if tick {
			g.sendTicks()
		}
		if i == 1 {
			// Every row of the first round was created by now (it was merged);
			// the flush is complete when all of them are written.
			perRound = perRoundRows
			waitUntil(t, "the first flush", func() bool { return perRound > 0 && sum3(writtenByDest(g)) >= perRound })
			continue
		}
		if !tick {
			want := uint64(i) * perRound
			waitUntil(t, "this round's flush to complete", func() bool { return sum3(writtenByDest(g)) >= want })
		} else {
			// A tick-driven repeat writes nothing; the positive signal that its flush
			// ran is the suppression of the repeated rows.
			waitUntil(t, "the repeat to be suppressed", func() bool { return changeSuppressed(g) > suppressedBefore })
		}
	}
	return perRound
}

// TestG_UNT_1_EndToEnd_NonRateLimitedUnitsAreNeverDiscarded is gate G-UNT-1 with
// the real writers.
//
// SETUP: 7208 configured non-rate-limited, a one-minute window so only the
// class's own trigger can flush, a healthy database. STIMULUS: the SAME
// datagrams 4 times, each round sent only after the previous flush completed
// (so every row's image is identical each time and digest suppression, were it
// applied, would swallow every repeat). ASSERTION: rows_written equals 4 times
// the rows of one round; change_suppressed is 0; nothing abandoned.
// Non-vacuity, a control graph: the same stream with the class rate-limited and
// a tick standing in for the trigger writes the first round's rows and
// suppresses the repeats (change_suppressed > 0), so the comparison tells the
// two apart; and the main run flushed with no tick at all.
//
// WOULD HAVE CAUGHT: the suppression predicate applied to non-rate-limited
// units (an FR7 regression: repeats silently dropped), and a trigger that never
// reaches the writers.
func TestG_UNT_1_EndToEnd_NonRateLimitedUnitsAreNeverDiscarded(t *testing.T) {
	const n = 4

	t.Run("control: rate-limited suppresses repeats", func(t *testing.T) {
		g, _, _ := dbGraph(t, func(c *config.Config) { c.Flush.WindowMs = 60000 })
		cancel, done := runGraph(t, g)
		per := repeatStream(t, g, n, true)
		cancel()
		if err := awaitRun(t, done); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if per == 0 || changeSuppressed(g) == 0 {
			t.Errorf("control is blind: first flush wrote %d, change_suppressed = %d", per, changeSuppressed(g))
		}
		if got := sum3(writtenByDest(g)); got != per {
			t.Errorf("control wrote %d rows, want only the first round's %d", got, per)
		}
	})

	g, buf, _ := dbGraph(t, func(c *config.Config) {
		c.Flush.WindowMs = 60000
		for i := range c.Transcodes {
			c.Transcodes[i].Class = "non-rate-limited"
		}
	})
	cancel, done := runGraph(t, g)
	per := repeatStream(t, g, n, false)
	if per == 0 {
		t.Fatal("non-vacuity: the first flush wrote no rows")
	}
	if g.inst().mirrorShards[0].EpochOpened.Load() == 0 {
		t.Fatal("non-vacuity: no epoch opened")
	}
	cancel()
	if err := awaitRun(t, done); err != nil {
		t.Fatalf("Run: %v\n%s", err, buf.String())
	}
	if got := sum3(writtenByDest(g)); got != n*per {
		t.Errorf("rows_written = %d, want %d (%d rounds x %d rows): units were discarded", got, n*per, n, per)
	}
	if s := changeSuppressed(g); s != 0 {
		t.Errorf("change_suppressed = %d for a non-rate-limited stream, want 0", s)
	}
	if got := abandonedByDest(g); got != [3]uint64{} {
		t.Errorf("drain_abandoned_rows = %v, want zeros", got)
	}
}
