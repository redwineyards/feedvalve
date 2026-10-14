package app

import (
	"errors"

	"feedvalve/internal/config"
	"feedvalve/internal/decode"
	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
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

	// The sub-packet path (P2-05). Resolved here, before Freeze, because
	// gate G-OBS-1's whole point is that an instrument reaches a struct field
	// and never a map lookup on a path that runs per datagram -- and these run
	// per SUB-PACKET, three to four times per datagram.
	//
	// The labelled ones are held as arrays indexed by the label's own type, so
	// the call site reads in.shortPayload[obs.PayloadBody] rather than
	// resolving a slot per message.
	subpacketsTotal        *obs.Counter
	subpacketsCompressed   *obs.Counter
	subpacketsUncompressed *obs.Counter
	decompressedLenMax     *obs.Max
	decompressFailed       [numDecompressReasons]*obs.Counter
	framingAbandoned       *obs.Counter
	netidMismatch          *obs.Counter
	errorResponse          *obs.Counter
	shortPayload           [numPayloadStages]*obs.Counter
	panicsRecovered        *obs.Counter
	messageHeaderSeen      [numMessageHeaderCodes]*obs.Counter
	heartbeatSeen          *obs.Counter

	// Per-transcode, indexed by the REGISTRY's slot. The registry's codes and
	// obs's {code} dimension are both the configured inventory in ascending
	// order, so one index serves both -- which is what keeps per-transcode
	// counting off a map on the receive path (Vol 1 section 7.7).
	packetsByTranscode      []*obs.Counter
	bodyLenMismatch         []*obs.Counter
	decompressedLenMismatch []*obs.Counter

	// unknownTranscode is a FUNCTION, not a handle, because it is the one
	// dynamic label in the ledger: the code is by definition not knowable at
	// startup, so the slot is claimed by a CAS at emission. It is also the
	// documented exception to G-OBS-1, which is why it is reached through the
	// registry rather than resolved here.
	unknownTranscode func(code int) (*obs.Counter, bool)

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

		subpacketsTotal:        reg.Counter(obs.IDSubpacketsTotal),
		subpacketsCompressed:   reg.Counter(obs.IDSubpacketsCompressed),
		subpacketsUncompressed: reg.Counter(obs.IDSubpacketsUncompressed),
		decompressedLenMax:     reg.Max(obs.IDDecompressedLenMax),
		framingAbandoned:       reg.Counter(obs.IDFramingAbandoned),
		netidMismatch:          reg.Counter(obs.IDNetidMismatch),
		errorResponse:          reg.Counter(obs.IDErrorResponse),
		panicsRecovered:        reg.CounterAt(obs.IDPanicsRecovered, int(obs.StageDecode)),
		heartbeatSeen:          reg.Counter(obs.IDHeartbeatSeen),

		decompressFailed: [numDecompressReasons]*obs.Counter{
			obs.ReasonInputOverrun:      reg.CounterAt(obs.IDDecompressFailed, int(obs.ReasonInputOverrun)),
			obs.ReasonOutputOverrun:     reg.CounterAt(obs.IDDecompressFailed, int(obs.ReasonOutputOverrun)),
			obs.ReasonLookbehindOverrun: reg.CounterAt(obs.IDDecompressFailed, int(obs.ReasonLookbehindOverrun)),
			obs.ReasonInputNotConsumed:  reg.CounterAt(obs.IDDecompressFailed, int(obs.ReasonInputNotConsumed)),
		},
		shortPayload: [numPayloadStages]*obs.Counter{
			obs.PayloadHeader: reg.CounterAt(obs.IDShortPayload, int(obs.PayloadHeader)),
			obs.PayloadBody:   reg.CounterAt(obs.IDShortPayload, int(obs.PayloadBody)),
		},
		messageHeaderSeen: [numMessageHeaderCodes]*obs.Counter{
			obs.HeaderHeartbeat: reg.CounterAt(obs.IDMessageHeaderSeen, int(obs.HeaderHeartbeat)),
			obs.HeaderAssetRate: reg.CounterAt(obs.IDMessageHeaderSeen, int(obs.HeaderAssetRate)),
		},

		unknownTranscode: unknownTranscodeFunc(reg),
	}
}

// The label arities, named rather than written as 2 and 4 at the array
// declarations: each is the size of a vocabulary obs owns, and a value added
// there must not silently leave a hole here.
const (
	numDecompressReasons  = 4 // input | output | lookbehind | not-consumed
	numPayloadStages      = 2 // header | body
	numMessageHeaderCodes = 2 // 6541 | 6503
)

// resolveTranscodeInstruments resolves the per-transcode counters once the
// registry is known, which is after resolveInstruments but still before Freeze.
//
// Separate from resolveInstruments because the arity is the CONFIGURED
// inventory's size, which the instrument registry learns from obsOptions and
// the decoder registry learns from the same configuration -- so the two agree
// by construction rather than by coincidence, and the test asserts it.
func (in *instruments) resolveTranscodeInstruments(reg obs.Registry, n int) {
	in.packetsByTranscode = make([]*obs.Counter, n)
	in.bodyLenMismatch = make([]*obs.Counter, n)
	in.decompressedLenMismatch = make([]*obs.Counter, n)
	for i := range n {
		in.packetsByTranscode[i] = reg.CounterAt(obs.IDPacketsByTranscode, i)
		in.bodyLenMismatch[i] = reg.CounterAt(obs.IDBodyLenMismatch, i)
		in.decompressedLenMismatch[i] = reg.CounterAt(obs.IDDecompressedLenMismatch, i)
	}
}

// unknownTranscodeFunc adapts the one dynamic accessor obs exposes. A closure
// rather than holding the *obs.Reg, so instruments stays a bag of handles and
// the receive path cannot reach anything else on the registry.
func unknownTranscodeFunc(reg obs.Registry) func(int) (*obs.Counter, bool) {
	type unknownResolver interface {
		UnknownTranscode(code int) (*obs.Counter, bool)
	}
	if r, ok := reg.(unknownResolver); ok {
		return r.UnknownTranscode
	}
	// A registry without the accessor cannot count unknown codes; counting
	// them under a nil pointer would panic on the receive goroutine, so the
	// fallback is a no-op counter that is discarded.
	var sink obs.Counter
	return func(int) (*obs.Counter, bool) { return &sink, true }
}

// transcodeSpecs projects the configured inventory into the form decode takes.
//
// The SAME translation obsOptions below performs, for the same reason and with
// the same justification: decode may not import config, because config carries
// oradb.DBSettings and that would make the receive path reach database/sql --
// forbidden by Vol 0 section 1.3 and machine-checked by gate G-DEP-1. Vol 1
// section 8.2 writes BuildRegistry as taking a *config.Config; the import rule
// is the part of the contract that is enforced, so the projection lives here,
// in the one package allowed to see both halves.
func transcodeSpecs(cfg *config.Config) []decode.TranscodeSpec {
	out := make([]decode.TranscodeSpec, 0, len(cfg.Transcodes))
	for _, t := range cfg.Transcodes {
		spec := decode.TranscodeSpec{
			Code:          t.Code,
			Class:         parseClass(t.Class),
			Contributions: make([]decode.ContribSpec, 0, len(t.Contributions)),
		}
		for _, c := range t.Contributions {
			spec.Contributions = append(spec.Contributions, decode.ContribSpec{
				Destination: c.Destination,
				Columns:     c.Columns,
			})
		}
		out = append(out, spec)
	}
	return out
}

// parseClass maps the configured class string onto model.Class.
//
// config's V-T2 has already restricted it to the two legal spellings, so the
// fallback is the branch no validated Config reaches. Rate-limited is the
// conservative default: it applies the window tick and digest suppression
// rather than flushing every message immediately.
func parseClass(s string) model.Class {
	if s == "non-rate-limited" {
		return model.ClassNonRateLimited
	}
	return model.ClassRateLimited
}

// registryProblemRules maps decode's problem kinds onto config's V- rule
// identifiers.
//
// The identifiers are declared once, in config/problem.go, and decode does not
// know them -- it reports KINDS. This table is the join, and it is in app for
// the same reason the projection above is: app is where both vocabularies are
// legal. V-T1 and V-T6 are the two rules config.Unchecked() listed as
// un-evaluable from P1-06 until this task.
var registryProblemRules = map[decode.ProblemKind]string{
	decode.ProblemNoDecoder:          config.RuleDecoderExists, // V-T1
	decode.ProblemColSpecWidth:       config.RuleColSpecWidth,  // V-T6
	decode.ProblemUnknownDestination: config.RuleContribDest,   // V-T3
	decode.ProblemNotAContributor:    config.RuleContribDest,   // V-T3
	decode.ProblemUnknownColumn:      config.RuleContribColumn, // V-T4
	decode.ProblemDuplicateColumn:    config.RuleContribColumn, // V-T4
	decode.ProblemTooManyTranscodes:  config.RuleRange,         // V-RANGE
}

// registryError turns a decode.BuildError into the config.ValidationError the
// rest of the system already knows how to print, so a registry rejection reads
// exactly like every other configuration rejection and -selfcheck needed no
// new case.
func registryError(err error) error {
	var be *decode.BuildError
	if !errors.As(err, &be) {
		return err
	}
	ve := &config.ValidationError{Problems: make([]config.Problem, 0, len(be.Problems))}
	for _, p := range be.Problems {
		rule, ok := registryProblemRules[p.Kind]
		if !ok {
			rule = config.RuleRange
		}
		ve.Problems = append(ve.Problems, config.Problem{Rule: rule, Msg: p.Msg})
	}
	return ve
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
