package config

import (
	"fmt"
	"net/netip"
)

// The source allow-list, parsed once (Vol 1 section 3.5).
//
// udp.sourceAllowList holds IPs or CIDR blocks and is empty by default, meaning
// allow all. The receiver compares every datagram's source against it, so the
// text form is turned into netip.Prefix values ONCE -- parsing per datagram
// would allocate on the goroutine ADR-006 keeps allocation-free, 2,400 times a
// second, to learn the same answer.
//
// netip and not net: netip.Prefix and netip.Addr are pointer-free value types,
// so a receiver-held slice of them is one allocation at startup and none
// afterwards. net.IPNet carries a []byte per entry and net.Addr would put one
// on the data path.

// SourcePrefixes parses udp.sourceAllowList into the form the receiver matches
// against.
//
// An empty list returns a nil slice, which the receiver treats as "skip the
// check entirely" rather than "matched against nothing" -- the distinction
// matters because the second reading would reject every datagram.
//
// It cannot fail on a validated Config: V-SRC1 (validate.go) rejects an
// unparseable entry at load, using the same parseSourceEntry below so that
// "valid" has exactly one definition. The error return is kept for the caller
// that parses a Config it did not validate, and because silently dropping an
// entry here would blind the feed in the one mode that discards data.
func (u UDPConfig) SourcePrefixes() ([]netip.Prefix, error) {
	if len(u.SourceAllowList) == 0 {
		return nil, nil
	}

	out := make([]netip.Prefix, 0, len(u.SourceAllowList))
	for i, e := range u.SourceAllowList {
		p, err := parseSourceEntry(e)
		if err != nil {
			return nil, fmt.Errorf("udp.sourceAllowList[%d]: %w", i, err)
		}
		out = append(out, p)
	}
	return out, nil
}

// parseSourceEntry accepts either a bare IP address or a CIDR block.
//
// A bare IP becomes a single-host prefix -- /32 for IPv4, /128 for IPv6 --
// which is what makes one matching rule serve both spellings (Vol 1
// section 3.5).
//
// The result is Masked(), so an entry written with host bits set ("10.1.2.3/8",
// which an operator means as "the 10/8 network") matches the network rather
// than depending on how Contains treats unmasked bits. Normalising here means
// the receiver's hot path does no masking at all.
func parseSourceEntry(e string) (netip.Prefix, error) {
	if addr, err := netip.ParseAddr(e); err == nil {
		return netip.PrefixFrom(addr.Unmap(), addr.Unmap().BitLen()), nil
	}
	p, err := netip.ParsePrefix(e)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("%q is neither an IP address nor a CIDR block", e)
	}
	return p.Masked(), nil
}
