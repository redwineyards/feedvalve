package model

// Datagram is one received datagram, carried through the QD ring by value.
// Fixed size and pointer-free: the whole ring is one allocation the collector
// never traces.
type Datagram struct {
	RecvMono int64 // receipt monotonic ns, taken ONCE immediately after ReadFrom.
	// This is the source of the ordering guard's receipt clock and of
	// the fan-out budget's start
	N   int32      // bytes valid in Buf
	_   [4]byte    // explicit pad
	Buf [1024]byte // the datagram as read, 8-byte prefix included.
	//
	// 1024 and not 520 deliberately. The envelope cap is 516 bytes
	// (bible section 3.2), so this is roughly a 2x margin, which means an
	// oversized datagram is still carried INTACT and can be reported by
	// framing as the envelope-assumption break it is -- rather than being
	// silently truncated at the ring and misframed downstream. The extra
	// 4 MB across the ring buys the ability to diagnose that case instead
	// of corrupting it.
}

// Bytes returns the valid portion of the datagram.
func (d *Datagram) Bytes() []byte { return d.Buf[:d.N] }
