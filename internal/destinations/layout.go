package destinations

import "feedvalve/internal/model"

// Gate G-LAY-3 (Vol 4 section 7.1) -- ASM-16b at build time.
//
// The field map itself lives in mapping.json, which is data and cannot be
// checked by the compiler. What CAN be checked at build time is the contract
// the data has to satisfy: these are the image lengths the three shipped
// destinations are declared to have, and the build fails if any of them stops
// fitting Entry.Image.
//
// The one-sided form is correct here. Vol 0 section 2 rule 7's two-sided
// uint(x) + uint(-x) pins an exact value; ASM-16b is "at most", so a single
// conversion of the slack to uint is the right shape -- negative slack
// overflows and the build fails, while spare room is fine.
//
// Together with the loader's check that each destination's widths sum to its
// declared imageLen, this gives both halves: the compiler proves the declared
// lengths fit, and the load proves the data matches the declared lengths. A
// change to mapping.json that overruns the image fails at load; a change to
// ImageSize that breaks a shipped destination fails at build.
const (
	quoteImageLen  = 181 // MD_TOKEN_QUOTE, Vol 0 section 4.3
	masterImageLen = 37  // MD_SECURITY_MASTER, section 4.4
	statusImageLen = 142 // MD_MARKET_STATUS, section 4.5
)

// The worst case (MD_TOKEN_QUOTE, 181 of 256) carries 75 bytes of headroom
// today. If a destination ever approaches ImageSize, that is the conversation
// about raising it to 288 (Vol 0 section 4.6).
const (
	_ = uint(model.ImageSize - quoteImageLen)
	_ = uint(model.ImageSize - masterImageLen)
	_ = uint(model.ImageSize - statusImageLen)
)
