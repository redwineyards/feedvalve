package writer

import "testing"

// TestG_CFG_8_TheTimeEpochOffsetReachesTheBinderAndFollowsAReload is the writer
// half of the runtime.time1980UnixOffsetSec pickup (Vol 3 A8: OQ-W2 turns a
// rebuild into a reload).
//
// SETUP: a runner (as a writer's loop builds it) and a Params carrying an
// offset; a binder for one statement identity is built under the default.
// STIMULUS: the offset the batch's Params carry changes, as after a reload, and
// the runner is asked for the SAME statement's binder again.
// ASSERTION: Params.time1980 is the carried value, and the default only for the
// zero value; the binder handed back is the cached one and now carries the new
// offset.
// WOULD HAVE CAUGHT: the offset never leaving the Params (every timestamp keeps
// the compiled default after a reload) and a binder cache that freezes the
// offset at first use (the reload takes effect only for statements not yet seen).
// NON-VACUITY: the first lookup carries the default, so the cached binder really
// did exist before the change.
func TestG_CFG_8_TheTimeEpochOffsetReachesTheBinderAndFollowsAReload(t *testing.T) {
	for _, c := range []struct{ in, want int64 }{
		{0, DefaultTime1980UnixOffsetSec},
		{315552600, 315552600},
	} {
		if got := (&Params{Time1980UnixOffsetSec: c.in}).time1980(); got != c.want {
			t.Errorf("Params{%d}.time1980() = %d, want %d", c.in, got, c.want)
		}
	}

	g := newRig(t)
	k := stmtKey{Dest: 0, Mask: 3}
	first, err := g.r.binder(k)
	if err != nil {
		t.Fatal(err)
	}
	if first.Time1980UnixOffsetSec != DefaultTime1980UnixOffsetSec {
		t.Fatalf("fresh binder offset = %d, want the default", first.Time1980UnixOffsetSec)
	}

	g.r.time1980 = (&Params{Time1980UnixOffsetSec: 315552600}).time1980()
	again, err := g.r.binder(k)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Error("the binder was rebuilt instead of reused")
	}
	if again.Time1980UnixOffsetSec != 315552600 {
		t.Errorf("cached binder offset = %d after the reload, want 315552600", again.Time1980UnixOffsetSec)
	}
}

// TestG_CFG_8_ServeAppliesTheParamsOffsetToTheRunner is the link between the
// two halves above: that a running writer's serve copies the offset its Params
// carry onto its runner.
//
// SETUP: a started pool whose Params carry an offset other than the default.
// STIMULUS: one batch is served and its completion received.
// ASSERTION: the writer's runner holds the Params offset.
// WOULD HAVE CAUGHT: serve no longer applying Params.time1980 to the runner
// (the helper and the binder refresh stay green while the reload path is dead).
// NON-VACUITY: the runner is built with the default, so the assertion can only
// hold if serve moved the value.
func TestG_CFG_8_ServeAppliesTheParamsOffsetToTheRunner(t *testing.T) {
	const want = int64(315552600)
	r := newPoolRig(t, rigOpts{par: func(p *Params) { p.Time1980UnixOffsetSec = want }})
	r.start()
	r.batchQ <- r.batch(0)
	r.recv(0)

	r.p.mu.Lock()
	defer r.p.mu.Unlock()
	if len(r.p.live) != 1 {
		t.Fatalf("live writers = %d, want 1", len(r.p.live))
	}
	for _, w := range r.p.live {
		if w.r.time1980 != want {
			t.Errorf("runner offset = %d after serve, want %d", w.r.time1980, want)
		}
	}
	if want == DefaultTime1980UnixOffsetSec {
		t.Fatal("vacuous: the test offset equals the default")
	}
}
