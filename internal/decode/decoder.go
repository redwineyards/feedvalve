package decode

import (
	"errors"

	"feedvalve/internal/model"
	"feedvalve/internal/wire"
)

// Decoder decodes one broadcast message body into zero or more update units
// (Vol 0 section 3.9).
//
// payload is the WHOLE broadcast data including the 8-byte prefix (bible
// section 3.4); implementations read through a wire.FieldReader cursor based so
// that debug offsets line up 1:1 with a hex dump and with a wire capture.
//
// out is caller-owned and reused per packet. A Decoder writes at most
// MaxUnits() entries into it and returns how many it wrote. It MUST NOT retain
// any slice of payload or of out -- payload aliases the receiver's
// decompression buffer, which the next compressed sub-packet in the same
// datagram overwrites (Vol 1 section 5.2), and out is overwritten by the next
// message.
//
// A Decoder produces field values only. Which destination rows and columns
// those values land in is the destinations package's job.
//
// # Allocation rule (MUST)
//
// A Decoder returns only the package-level sentinel errors below. It never
// calls fmt.Errorf, errors.New or any wrapping helper on the decode path: the
// receive path is allocation-free in steady state (ADR-006), and a wrapped
// error at 10,000 packets/s during a misframe is both the loudest and the
// slowest thing in the process. Context belongs in the counter label, not in
// the error string.
type Decoder interface {
	Code() model.TransCode

	// BodyLen is the declared packet length from the bible's Part V table,
	// measured from the broadcast header onward -- 470 for 7208, 294 for
	// 7305/7340, 320 for the 6511 family. ZERO means variable-length.
	//
	// The dispatcher compares it against MsgHeader.MsgLen and rejects a
	// mismatch BEFORE the decoder runs, which is the runtime half of the
	// compile-time layout gate: the gate proves our arithmetic reconciles with
	// the bible, and this proves the wire agrees with the bible.
	BodyLen() int

	// MaxUnits is the largest fan-out this message can produce: 2 for 7208,
	// 1 for 7305/7340 and the 6511 family, 0 for the two header-only codes.
	// It bounds the caller's fan-out buffer, so a decoder that returns more
	// than it declares would write past it.
	MaxUnits() int

	Decode(h model.MsgHeader, payload []byte, out []model.UpdateUnit) (n int, err error)
}

// The sentinels a Decoder may return. Package-level values, compared with
// errors.Is, allocating nothing.
//
// They are deliberately few. A decoder that wants to say more than these can
// say it in a counter label; one that wants to say it in prose would allocate
// on the path ADR-006 keeps allocation-free.
var (
	// ErrShortBody means the payload ends before a field the declared layout
	// requires. The dispatcher's length checks make this unreachable for a
	// fixed-length transcode, which is why reaching it is worth its own
	// sentinel rather than a panic.
	ErrShortBody = errors.New("decode: payload shorter than the declared body")

	// ErrFieldRange means a field held a value outside what its type can
	// represent -- a count that cannot be clamped, a length longer than the
	// field holding it.
	ErrFieldRange = errors.New("decode: field value out of range")
)

// ColSpec is one transcode's declared contribution to one destination: the mask
// of fields it supplies, the total packed width of their values, and where each
// field sits inside that packed run.
//
// Width is PRECOMPUTED, so a decoder bound-checks its blob write in one
// comparison rather than walking the mask per message. Keeping the sum next to
// the mask is also what makes rule V-T6 checkable: the two must agree, and
// BuildRegistry computes them together so they cannot drift.
type ColSpec struct {
	Mask  model.ColMask
	Width uint16 // sum of the declared widths of Mask's set bits; 0 means no contribution

	// Off is each mask bit's byte offset within the packed run, and Len its
	// declared width. Vol 0 section 3.4 packs values in ASCENDING BIT ORDER
	// with no padding, so bit b's offset is the sum of the widths of every
	// LOWER set bit.
	//
	// Resolved at config load (Vol 1 section 10.2's "per-(transcode,
	// destination) blobOffset[bit] array") and NOT as decoder constants, which
	// is the subtle part: the run is packed over the CONFIGURED mask, so a
	// configuration that declares a subset of a transcode's columns shifts
	// every offset above the one it dropped. A decoder with hardcoded offsets
	// would keep writing to the full-mask layout and silently mis-place every
	// field after the gap.
	//
	// Indexed by bit, so a decoder writes at Off[bit] with no arithmetic on
	// the hot path. Entries for unset bits are zero and must not be read; the
	// mask is the authority on which are live.
	Off [model.MaxColumns]uint16
	Len [model.MaxColumns]uint16
}

// Counters is what a decoder reports that is neither a unit nor an error.
//
// Four events, each rare and each already in the instrument ledger. They are
// counted INSIDE the decoder because that is where the information exists: only
// the decoder sees the unclamped record count, the gating BookType, the
// unclamped message length (Vol 1 section 9.5, addition A-07 moving
// message_truncated's owner from shard to decode) and its own blob arithmetic.
//
// An interface defined HERE, by the consumer, and implemented by app over its
// resolved obs handles. decode therefore does not import obs, and the
// alternative -- a package-level counter variable set at startup -- would be
// global mutable state on the one path that must stay testable in isolation.
//
// Every method is called at most once per message, so the interface dispatch is
// free next to the decode itself.
type Counters interface {
	// RecordCountClamped is a declared record count outside [0, max]: bible
	// section 4.8's NoOfRecords, clamped rather than trusted.
	RecordCountClamped(code model.TransCode)

	// BooktypeSkipped is 7200's BookType outside {1,2} (bible section 4.7), a
	// PROCESSING decision rather than a parse failure: the message framed and
	// parsed correctly and we are choosing not to store it.
	BooktypeSkipped(code model.TransCode)

	// BlobOverrun is a decoder whose writes exceeded its declared
	// ColSpec.Width -- a code defect, not bad input. The decoder emits ZERO
	// units for that message rather than a partial one.
	BlobOverrun(code model.TransCode)

	// MessageTruncated is a stored value cut at its declared width, by
	// destination and column bit (Vol 0 section 4.5's 128-byte cap).
	MessageTruncated(dest model.DestID, bit uint8)
}

// NopCounters discards every report. For tests that are not asserting on
// counters, and for nothing else: a production path that reached this would
// lose the only signal that a clamp happened.
type NopCounters struct{}

func (NopCounters) RecordCountClamped(model.TransCode)   {}
func (NopCounters) BooktypeSkipped(model.TransCode)      {}
func (NopCounters) BlobOverrun(model.TransCode)          {}
func (NopCounters) MessageTruncated(model.DestID, uint8) {}

// RegistryEntry is what the registry stores per code: the decoder, its class,
// and the per-destination contribution descriptors resolved at config load so
// the decode path does no lookups at all.
//
// Contributor slots are assigned ONCE, at startup, by ascending transaction
// code within each destination. The slot indexes Entry.LastSeq, LastTime and
// LastRecv, so a reload that added or reordered a contributor would silently
// reassign every stored ordering value to the wrong transcode -- which is why
// the transcode set is startup-only configuration.
// Traceable is the optional half of Decoder: a decoder whose body can emit a
// field trace.
//
// Separate from Decoder, for two reasons. The two recognised-but-not-decoded
// codes have no body to trace and no Decoder at all, and a one-method interface
// defined at the point of use is the shape this tree uses rather than widening
// an interface every implementation must then satisfy.
//
// The sink is stored on the decoder rather than passed to Decode because Decode
// is the hot path and its signature is fixed (section 8.2): a fifth parameter
// that is nil in all normal operation would be paid for on every message.
type Traceable interface{ SetTrace(wire.TraceSink) }

type RegistryEntry struct {
	Dec   Decoder
	Code  model.TransCode
	Class model.Class

	// Trace is Dec asserted to Traceable, or nil when Dec cannot trace.
	//
	// RESOLVED AT BUILD TIME, deliberately. Asserting at the call site would
	// put a type assertion on the receive path for every sub-packet, and
	// gate G-ALLOC-1 measures that path -- the same reason messageTruncated
	// stopped being resolved lazily in P2-09.
	Trace Traceable

	// Slots is the contributor slot of Code within each destination, and
	// Contrib which columns of that destination Code supplies. Both indexed by
	// model.DestID. A destination this code does not contribute to has
	// Contrib[d].Width == 0, which is the test the decode path uses.
	Slots   [model.MaxDestinations]uint8
	Contrib [model.MaxDestinations]ColSpec
}

// Contributes reports whether this transcode writes anything to destination d.
func (e *RegistryEntry) Contributes(d model.DestID) bool {
	return int(d) < len(e.Contrib) && e.Contrib[d].Width > 0
}

// assertDecoderShape keeps the interface honest about what the dispatcher
// assumes. A decoder declaring more units than the fan-out buffer can hold
// would write past it, and the buffer is sized from Vol 0 section 9.1's 40
// rather than from this -- so the relationship is stated where a new decoder
// author will see it.
//
// FanoutUnits sizes the receiver's fan-out buffer, [FanoutUnits]UpdateUnit:
// Vol 0 section 9.1's 40. Declared here, next to the gate below, and used by app
// so the buffer and the gate cannot name two different numbers.
const FanoutUnits = 40

// The one-sided form of the layout gate: compiles only while every decoder's
// declared fan-out fits the buffer. maxDecoderUnits is raised deliberately when
// a decoder with a larger fan-out is added -- 35 for 7320/7210, 25 for 7220 --
// and raising it past FanoutUnits must break the build rather than overflow
// the buffer at market open.
const maxDecoderUnits = 35

const _ = uint(FanoutUnits - maxDecoderUnits)

// wireReaderIsReady is a compile-time assertion that the cursor the decoders
// read through exists with the shape section 8.4 specifies. It costs nothing
// and it fails loudly if the reader's API moves under the decoders.
var _ = func(r *wire.FieldReader) {
	_ = r.I16(wire.FieldInvalid)
	_ = r.I32(wire.FieldInvalid)
	_ = r.I64(wire.FieldInvalid)
	_ = r.F64(wire.FieldInvalid)
	_ = r.Byte(wire.FieldInvalid)
	r.Skip(wire.FieldInvalid, 0)
	r.Pad(wire.FieldInvalid, 0)
	r.Copy(wire.FieldInvalid, nil)
	_ = r.Remaining()
}
