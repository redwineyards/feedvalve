package mirror

import (
	"encoding/binary"
	"math"
	"math/bits"

	"feedvalve/internal/destinations"
	"feedvalve/internal/model"
	"feedvalve/internal/obs"
)

// Vol 0 section 5A: pre-validation. A value the DDL is certain to refuse is
// refused here, at merge, per contribution -- before it is written into the
// image, so the rest of the row stays writable (V0:1677-1682). The facts are
// the column's own (SQLPrecision, SQLMaxBytes, Domain); a column without the
// fact a check needs is unchecked.
//
// Defence in depth only (V0 section 5A.4): Oracle stays authoritative, and the
// completion path's quarantine stays in full. NotNull is advisory and is not
// checked here (phase 3 section 6, seam 7): at merge the mirror cannot tell an
// insert from an update.

// prevalidate is merge step 4's per-field check. It returns the subset of
// c.Mask whose incoming values the destination's DDL facts accept -- the
// contribution's EFFECTIVE mask, the only bits step 4 copies and step 5 ORs
// into ColMask -- and counts each refusal against d and u's transcode.
//
// c must already have passed fieldsFit, so every slice below is in bounds.
func (s *Shard) prevalidate(d model.DestID, pos int, dst *destinations.Destination,
	e *model.Entry, c *model.Contribution, u *model.UpdateUnit) model.ColMask {
	ok := c.Mask
	cur := int(c.BlobOff)
	for m := c.Mask; m != 0; m &= m - 1 {
		b := uint8(bits.TrailingZeros64(uint64(m)))
		col := &dst.Columns[b]
		v := u.Blob[cur : cur+int(col.Width)]
		cur += int(col.Width)
		if r, bad := checkField(col, v); bad {
			ok = ok.Clear(b)
			s.countRefused(d, pos, r)
		}
	}

	// An explicit-length text field and its length field are one logical value
	// in two fields (Vol 0 section 4.5): the writer binds min(length, width)
	// bytes of the text, so neither half can be judged -- or land -- without
	// the other. Each half is the incoming value when c carries it, else the
	// image's. Either half refused withdraws both: a length landing over a text
	// it was not checked against, or a text under a length that never landed,
	// binds a value nothing validated. Counted once, against the half refused.
	// The walk is over the destination's columns, not c.Mask, so that a
	// contribution carrying only the length is judged against the image's text.
	for b := range dst.Columns {
		col := &dst.Columns[b]
		if col.Kind != destinations.KindText || !col.HasExplicitLength() || col.SQLMaxBytes == 0 {
			continue
		}
		tb, lb := uint8(b), uint8(col.LenBit)
		pair := model.Bit(tb) | model.Bit(lb)
		if !c.Mask.Intersects(pair) {
			continue
		}
		if ok&pair != c.Mask&pair { // the length was refused by the first walk
			ok = ok.Without(pair)
			continue
		}
		if textLen(halfOf(dst, e, c, u, tb), lengthValue(halfOf(dst, e, c, u, lb))) > int(col.SQLMaxBytes) {
			ok = ok.Without(pair)
			s.countRefused(d, pos, obs.PrevalidateLength)
		}
	}
	return ok
}

// countRefused counts one refused field: prevalidate_rejected{d,reason} and,
// when u's code is configured, prevalidate_rejected_transcode{code}.
func (s *Shard) countRefused(d model.DestID, pos int, r obs.PrevalidateReason) {
	s.inst.PrevalidateRejected[d][r].Add(1)
	if pos >= 0 {
		s.inst.Transcode.PrevalidateRejectedTC[pos].Add(1)
	}
}

// checkField checks one incoming wire-form value v against col's DDL facts,
// returning the reason it would be refused. Explicit-length text is not
// checked here: its length is another field (see prevalidate).
func checkField(col *destinations.Column, v []byte) (obs.PrevalidateReason, bool) {
	switch col.Kind {
	case destinations.KindLong:
		return obs.PrevalidatePrecision, !fitsPrecision(int64(int32(binary.BigEndian.Uint32(v))), col.SQLPrecision)
	case destinations.KindShort:
		return obs.PrevalidatePrecision, !fitsPrecision(int64(int16(binary.BigEndian.Uint16(v))), col.SQLPrecision)
	case destinations.KindDouble:
		f := math.Float64frombits(binary.BigEndian.Uint64(v))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return obs.PrevalidateNonfinite, true
		}
		return obs.PrevalidatePrecision, !doubleFitsPrecision(f, col.SQLPrecision)
	case destinations.KindTime1980:
		// Seconds since 1980 as a signed LONG. Zero is R6's "never set" and
		// binds NULL; a negative value is pre-1980 and cannot be real (Vol 3
		// section 5.4). Vol 3's upper bound, 3,786,912,000, exceeds an int32,
		// so no wire value can reach it.
		return obs.PrevalidateTime, int32(binary.BigEndian.Uint32(v)) < 0
	case destinations.KindChar1:
		return obs.PrevalidateDomain, !inDomain(v[0], col.Domain)
	case destinations.KindText:
		if col.HasExplicitLength() || col.SQLMaxBytes == 0 {
			return 0, false
		}
		if allZero(v) {
			return 0, false // binds NULL (Vol 3 section 5.2's KindTextFixed row)
		}
		return obs.PrevalidateLength, textLen(v, len(v)) > int(col.SQLMaxBytes)
	}
	return 0, false // KindGroup: an opaque run, no facts
}

// pow10 is 10^i for every i an int64 can hold.
var pow10 = [...]int64{1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18}

// fitsPrecision reports whether the raw integer v fits NUMBER(p,s), where s is
// the column's Scale for a scaled column and 0 otherwise. The image holds the
// value UNSCALED (R1) and the writer divides by 10^Scale at bind time (Vol 3
// section 5.3), so v already carries the s fraction digits: the test is
// |v| < 10^p either way. p == 0 is "not a NUMBER" -- unchecked -- and a p past
// pow10 cannot be exceeded by an int32.
//
// The column carries no SQL scale, so this ASSUMES the DDL's scale is the
// column's Scale (0 when unscaled). A DDL scale larger than that leaves fewer
// integer digits than assumed, and the check under-refuses -- the safe
// direction, since Oracle stays authoritative (V0 section 5A.4). A DDL scale
// SMALLER than Scale would make it over-refuse; no mapped column has one.
func fitsPrecision(v int64, p uint8) bool {
	if p == 0 || int(p) >= len(pow10) {
		return true
	}
	if v < 0 {
		v = -v
	}
	return v < pow10[p]
}

// doubleFitsPrecision is fitsPrecision for a double, under the same scale
// assumption: Oracle rounds the bound value f/10^s to s places, which fits
// NUMBER(p,s) exactly when round(|f|) < 10^p -- scaled or not, since the image
// value is unscaled.
func doubleFitsPrecision(f float64, p uint8) bool {
	return p == 0 || math.Round(math.Abs(f)) < math.Pow10(int(p))
}

// inDomain reports whether flag byte b is one of domain's values. An empty
// domain is unconstrained, and 0x00 is R6's absent value, which binds NULL --
// and a NULL passes a CHECK.
func inDomain(b byte, domain []string) bool {
	if len(domain) == 0 || b == 0 {
		return true
	}
	for _, v := range domain {
		if len(v) == 1 && v[0] == b {
			return true
		}
	}
	return false
}

// textLen is the byte length the writer binds for text field v with declared
// valid length n: the first min(n, len(v)) bytes, right-trimmed of blanks
// (Vol 3 section 5.2's KindText row).
func textLen(v []byte, n int) int {
	n = min(max(n, 0), len(v))
	for n > 0 && v[n-1] == ' ' {
		n--
	}
	return n
}

// allZero reports whether v is all 0x00 -- R6's never-set text.
func allZero(v []byte) bool {
	for _, b := range v {
		if b != 0 {
			return false
		}
	}
	return true
}

// lengthValue reads a length field -- a short or a long, which destinations
// validates -- as an unsigned count, so a negative one clamps to the text's
// full width in textLen rather than to nothing.
func lengthValue(v []byte) int {
	if len(v) == 2 {
		return int(binary.BigEndian.Uint16(v))
	}
	return int(binary.BigEndian.Uint32(v))
}

// halfOf is field b as the writer would bind it after this contribution: the
// incoming value in c's packed run when c carries b, else the image's.
func halfOf(dst *destinations.Destination, e *model.Entry, c *model.Contribution, u *model.UpdateUnit, b uint8) []byte {
	col := &dst.Columns[b]
	if !c.Mask.Has(b) {
		return e.Image[col.Offset:col.End()]
	}
	off := int(c.BlobOff) + maskedWidth(dst, c.Mask&(model.Bit(b)-1))
	return u.Blob[off : off+int(col.Width)]
}

// predictable reports whether an ORA error code is one pre-validation exists
// to predict: ORA-01438 (precision), ORA-12899 (length) or ORA-02290 (a CHECK,
// i.e. a domain). The completion path's Rejected arm counts a row refused with
// one of these as rows_rejected_by_oracle_unpredicted -- a refusal the mapping's
// facts should have seen coming (Vol 0 section 5A.5).
func predictable(errCode uint16) bool {
	switch errCode {
	case 1438, 12899, 2290:
		return true
	}
	return false
}
