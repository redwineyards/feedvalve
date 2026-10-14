package app

import (
	"feedvalve/internal/config"
	"feedvalve/internal/destinations"
	"feedvalve/internal/obs"
)

// instruments are the lifecycle's own instrument handles, resolved once and
// held as pointers.
//
// Resolved BEFORE obs.Freeze, because gate G-OBS-1 makes every accessor panic
// afterwards -- which is the whole point of that gate: Vol 0 section 8.4's MUST
// is that instruments reach a struct field and never a map lookup on any path
// that runs per datagram. So the resolution happens here, at startup step 9,
// and the goroutines hold the pointers.
//
// Only the instruments the LIFECYCLE writes are here. The ledger assigns owners
// per instrument (internal/obs/ledger.go), and the receiver's, decoder's, shard's
// and writer's own handles belong to those components as they arrive.
type instruments struct {
	// lifecycle-owned
	configGeneration     *obs.Gauge
	configReloads        *obs.Counter
	configReloadRejected *obs.Counter
	readbufEffective     *obs.Gauge
	writersConfigured    *obs.Gauge

	// receiver-owned, held here until Phase 2 gives the receiver a home
	datagramsReceived      *obs.Counter
	datagramsOversize      *obs.Counter
	datagramsUnexpectedSrc *obs.Counter
	qdSendBlocked          *obs.Counter
	qdDepthMax             *obs.Max
	udpReadErrors          *obs.Counter
	lastDatagramMonoNs     *obs.Gauge

	// decoder-owned, likewise
	qdWakeParked *obs.Counter

	// stats-owned
	clockStepDetected *obs.Counter
}

// resolveInstruments performs the resolution half of startup step 9. The caller
// freezes the registry afterwards.
func resolveInstruments(reg obs.Registry) instruments {
	return instruments{
		configGeneration:     reg.Gauge(obs.IDConfigGeneration),
		configReloads:        reg.Counter(obs.IDConfigReloads),
		configReloadRejected: reg.Counter(obs.IDConfigReloadRejected),
		readbufEffective:     reg.Gauge(obs.IDReadbufEffectiveBytes),
		writersConfigured:    reg.Gauge(obs.IDWritersConfigured),

		datagramsReceived:      reg.Counter(obs.IDDatagramsReceived),
		datagramsOversize:      reg.Counter(obs.IDDatagramsOversize),
		datagramsUnexpectedSrc: reg.Counter(obs.IDDatagramsUnexpectedSource),
		qdSendBlocked:          reg.Counter(obs.IDQDSendBlocked),
		qdDepthMax:             reg.Max(obs.IDQDDepthMax),
		udpReadErrors:          reg.Counter(obs.IDUDPReadErrors),
		lastDatagramMonoNs:     reg.Gauge(obs.IDLastDatagramMonoNs),

		qdWakeParked: reg.Counter(obs.IDQDWakeParked),

		clockStepDetected: reg.Counter(obs.IDClockStepDetected),
	}
}

// obsOptions translates the resolved configuration and destination table into
// the plain scalars and label strings obs needs.
//
// This translation exists because obs MUST NOT import config or destinations
// (gate G-DEP-1): it is imported from everywhere, so one edge back would make
// half the tree cyclic. app is the package allowed to see both halves, so the
// translation lives here -- which is also why obs.Options is scalars and
// strings rather than a *config.Config.
func obsOptions(cfg *config.Config, dests *destinations.Set) obs.Options {
	all := dests.All()

	names := make([]string, 0, len(all))
	cols := make([][]string, 0, len(all))
	for i := range all {
		names = append(names, all[i].Name)
		// Column names in ColMask bit order, which is also the image order --
		// the same order the {destination,column} label indexes by.
		cn := make([]string, 0, len(all[i].Columns))
		for j := range all[i].Columns {
			cn = append(cn, all[i].Columns[j].Name())
		}
		cols = append(cols, cn)
	}

	// Ascending transaction code, which is the index the decoder registry and
	// the ordering guard also use -- Vol 1 section 7.7: "reusing the counter
	// layer's slot index is what keeps per-transcode from needing a map on the
	// receive path". config validates the inventory, so this only projects it.
	codes := make([]int, 0, len(cfg.Transcodes))
	for i := range cfg.Transcodes {
		codes = append(codes, cfg.Transcodes[i].Code)
	}

	return obs.Options{
		Shards:       cfg.Runtime.Shards,
		Destinations: names,
		Columns:      cols,
		Transcodes:   codes,
	}
}
