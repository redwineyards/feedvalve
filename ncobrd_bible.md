# NNF Broadcast Bible — Chapters 1, 2 & 10

**Source:** `TP_COM_Trimmed_NNF_Protocol_3_5_20251229105314.pdf` — *Trimmed Protocol for Non-NEAT
Front End (NNF), NSEIL Commodity Derivatives Trading System, Version 3.5, Dec-2025*.
Chapter 1 (Introduction, p.14), Chapter 2 (General Guidelines, pp.15–22), Chapter 10 (Broadcast,
pp.141–176).

**What this document is.** A distilled, unambiguous restatement of those three chapters for system
architects and developers building a broadcast feed handler. Every structure below has had its
field sizes summed and checked against the spec's own declared `Packet Length`; where the two
disagree the discrepancy is resolved here and marked ⚠. The spec's per-field *Offset* columns are
reproduced only after that check — **several of them are wrong**, and the errata are listed in
Appendix B.

**What this document is not.** It is not the whole protocol. Interactive/order-entry messages
(Ch.3–7, 11–16), Market By Aggregate (Ch.8, transaction codes 7215/7216) and Bhavcopy (Ch.9) are out
of scope, and are referenced only where a broadcast structure embeds one of theirs.

**Conventions used below**

| Marker | Meaning |
|---|---|
| ⚠ | The spec is wrong, self-contradictory, or silent here. Read the note before writing code. |
| **Offset** | Byte position **relative to the start of the message**, i.e. offset 0 = first byte of `BCAST_HEADER`. On the wire, add 8 for the broadcast-data prefix (see §3.4). |
| *(pad)* | A byte present on the wire that carries no field. It must be read/skipped, not ignored. |

---

## Part I — Chapter 1: Introduction

NSEIL operates NEAT, a fully automated, screen-based, **order-driven market on price-time
priority**. A trading member reaches the Trading System (TS) either through NSEIL's own NEAT front
end or through a **Non-NEAT Front End (NNF)** — a front end built and maintained by a third-party
vendor.

The only architecturally relevant consequences:

1. **NSEIL publishes the design documents; NSEIL does not support or warrant the NNF.** Correctness
   of framing, decoding and display is entirely the implementer's responsibility. There is no
   host-side validation that will catch a mis-parsed broadcast.
2. **No host-end inquiries are permitted for NNF users** (Ch.2, Designer guideline 4). You cannot
   ask the host to re-send or clarify. The broadcast stream is the only source of truth, so a
   dropped or mis-framed packet is permanently lost data — design for detect-and-resync, not
   retransmission.

### 1.1 Communication network connections

Two distinct virtual circuits exist. Confusing them is a design-level error, not a coding one.

| Circuit | Direction | Carries | Header used |
|---|---|---|---|
| **Interactive VCID** | Bidirectional NNF ⇄ host | All requests and their responses, plus unsolicited messages to the trader (e.g. trade confirmations) | `MESSAGE_HEADER` |
| **Broadcast BCID** | **Unidirectional**, host → NNF | All broadcast market data — everything in this document | `BCAST_HEADER` (mostly) |

Notes that matter for deployment:

- The broadcast circuit is one-way. There is no ACK, no flow control, and nothing you send on it.
- Standard TCP/IP is used on exchange infrastructure with default features enabled: **IP
  fragmentation is on, QoS is not**. Fragmentation operates at message level and one member's
  traffic does not block another's.
- Some broadcasts are stated to fall back to the interactive circuit if broadcast is unavailable
  (explicitly for 7211 Spread MBP, §4.13). A handler that only listens on BCID will silently miss
  those in a degraded state.

---

## Part II — Chapter 2: The rules that apply to every message

These eight rules govern **every** structure in the protocol. Most decoding bugs in practice are a
violation of rule 2.2 or 2.3.

### 2.1 Byte order — big-endian, always

The trading system host is **big-endian**, as is every protocol layer in the TCP/IP suite. Any
multi-byte type (`SHORT`, `LONG`, `LONG LONG`, `DOUBLE`, and the spec's `UINT`) arrives most
significant byte first.

The spec phrases this as "twiddling" — if your machine is little-endian (x86 and clones), reverse
the bytes of every multi-byte field after receiving and before sending. Example from the spec: a
received `02BC` is actually the value `BC02`.

> **Implementation guidance.** Do not `memcpy` a wire buffer onto a native struct on a little-endian
> host — nothing will be right. Read field by field through a big-endian cursor. `CHAR` arrays are
> byte sequences and are never twiddled.

### 2.2 Alignment — `#pragma pack(2)`, and "word" means 2 bytes

The spec (Designer guideline 3) states three rules:

1. `char` / `unsigned char`, **and arrays of them**, are **byte aligned**.
2. **All other structure members are word aligned.**
3. **Structures are word aligned; a structure of odd size is padded to an even number of bytes.**

⚠ **"Word" here means 2 bytes, not 4.** This is not stated, but is proven by the spec's own offset
tables: `MS_ASSET_UPDT_INT_RATE_INFO` (6503) places a `LONG` at offset 46, and `MS_BCAST_VCT_MSGS`
places a `SHORT` at 78 — both legal at 2-byte alignment, illegal at 4. Equivalently: this is C
`#pragma pack(2)`.

The practical consequences, in the order they bite:

- A `CHAR` at an **even** offset followed by any non-`CHAR` member forces **one pad byte** so the
  next member starts even. This is the single most common padding in the protocol (see 7200/7208).
- A struct whose fields sum to an odd number gets **one trailing pad byte**. That padded size —
  not the field sum — is the **stride of an array of that struct**.
- Padding is *interior* or *trailing* depending on where the odd member sits. Interior padding
  shifts every subsequent field; trailing padding does not shift anything within the struct but does
  shift the next element of an array and everything after the struct.

### 2.3 ⚠ Do not trust the spec's Offset columns — reconcile against Packet Length

Every structure table in the spec declares a `Packet Length` **and** a per-field `Offset` column.
Several offset columns are written as if the struct were fully packed, and come out 1–2 bytes short
of the same table's declared length. **The declared `Packet Length` is authoritative**; the gap is
real alignment padding, and putting it in the wrong place shifts fields silently.

**Mandatory procedure before writing or editing any parser:**

1. Sum the field sizes as listed.
2. Apply the pack(2) rules from §2.2 to find where padding must fall.
3. Check the padded total equals the declared `Packet Length`.
4. If it already equals the declared length with no padding, **add none** — do not "align blindly".
   `MS_SECURITY_UPDATE_INFO` (7305) sums to exactly 294 and contains an unpadded **3-byte**
   `ST_SEC_ELIGIBILITY_PER_MARKET`; adding alignment there would break it.

Part V of this document is that reconciliation, done for all 15 broadcast structures.

### 2.4 Prices, values and precision

- Every price field is an **integer scaled by the contract's precision**: multiply by
  10^precision before sending, divide by 10^precision on receipt.
  *Example:* precision 2, price 10.12 → send `1012`; receive `202300` → display `20.23`.
- Price fields may be **positive, negative or zero**. Use signed types; a price is not a magnitude.
- Digits after the decimal must equal the contract's precision, both directions.
- **Trade values** (price × quantity) received from the host are divided by 10^precision to get
  rupees and then **rounded to 2 decimals regardless of the contract's precision**. Trade values are
  **always positive**, whatever the trade price.
- ⚠ **Version 3.5 changed price scale/precision from (6,4) to (7,2)** across a long list of pages.
  Any storage column, fixed-point type or test fixture carried over from ≤3.4 must be re-checked.
- Wherever a field description says "refer point no. 6 in Guidelines for Designers", it means this
  section — the field is a scaled price.

### 2.5 Time

- **All time fields are seconds since midnight, 1 January 1980** — not the Unix epoch. Offset from
  Unix time is 315 532 800 seconds.
- `Timestamp` in `MESSAGE_HEADER` is **nanoseconds** since 01-Jan-1980 00:00:00 for the transaction
  codes listed in the spec's appendix, stamped at the matching engine.
- `TimeStamp1` carries host time in **jiffies**, where **1 second = 65 536 jiffies**. It is 8 bytes
  at the host; the front end conventionally splits it into two 4-byte halves stored as doubles, used
  when requesting message-area download.
- `TimeStamp2` carries the **machine number**, 8 bytes. ⚠ Its position inside the field differs by
  circuit: **interactive → byte 7; broadcast → byte 0.**

### 2.6 Strings, numerics and reserved fields

- All alphabetic data is **upper case** before sending.
- **No NULL-terminated strings.** Pad with **blanks**. Strings from the host are blank-padded and
  are **not** NULL-terminated — a trailing-space trim is required, and a `strlen`-style read is a
  bug.
- All numeric fields must be **zeroed** before sending unless assigned.
- Fields named **`Reserved`** map to a `CHAR` buffer and are initialised to NULL when sending. On
  receipt they carry no meaning — but they still occupy bytes and **must be read**.
- Transaction codes suffixed `IN` flow front end → host; `OUT` flow host → front end. Broadcast is
  `OUT` by definition.

### 2.7 Data types

| Type | Bytes | Signedness |
|---|---|---|
| `CHAR` | 1 | Signed |
| `SHORT` | 2 | Signed |
| `LONG` | **4** | Signed |
| `LONG LONG` | 8 | Signed |
| `DOUBLE` | 8 | Signed, floating point |
| `BIT` | — | Bit field within a byte; see §2.9 |

⚠ `LONG` is **4 bytes**, not the C `long` of the build host. Bind it to an explicit `int32`.

### 2.8 The three headers

Every message is prefaced by a 40-byte header. **There are three of them, all 40 bytes, and two have
different layouts.** Picking the wrong one still "parses" and yields plausible garbage.

#### `MESSAGE_HEADER` — 40 bytes (interactive messages; also broadcast 6503 and 6541)

| Field | Type | Size | Offset |
|---|---|---|---|
| TransactionCode | SHORT | 2 | 0 |
| LogTime | LONG | 4 | 2 |
| AlphaChar | CHAR[2] | 2 | 6 |
| TraderId | LONG | 4 | 8 |
| ErrorCode | SHORT | 2 | 12 |
| Timestamp | LONG LONG | 8 | 14 |
| TimeStamp1 | CHAR[8] | 8 | 22 |
| TimeStamp2 | CHAR[8] | 8 | 30 |
| MessageLength | SHORT | 2 | 38 |
| | | **40** | |

- `LogTime` — zero when sending; host message-generation time when receiving.
- `AlphaChar` — first two characters of `Symbol` when the structure carries Symbol/Series, else
  blank. **Special case:** in `SYSTEM_INFORMATION_OUT` during logon it carries the **number of
  modules**, which determines how many `DOWNLOAD_REQUEST` packets the front end must send.
- `ErrorCode` — zero when sending. **Non-zero on receipt means the payload is `MS_ERROR_RESPONSE`,
  not the structure you expected** (§2.10).
- `MessageLength` — length of the **entire message including the 40-byte header**.

#### `INNER_MESSAGE_HEADER` — 40 bytes (⚠ *different layout*)

Prefaces each structure inside Update-Local-Database / Message-Download response data.

| Field | Type | Size | Offset |
|---|---|---|---|
| TraderId | LONG | 4 | 0 |
| LogTime | LONG | 4 | 4 |
| AlphaChar | CHAR[2] | 2 | 8 |
| TransactionCode | SHORT | 2 | 10 |
| ErrorCode | SHORT | 2 | 12 |
| Timestamp | LONG LONG | 8 | 14 |
| TimeStamp1 | CHAR[8] | 8 | 22 |
| TimeStamp2 | CHAR[8] | 8 | 30 |
| MessageLength | SHORT | 2 | 38 |
| | | **40** | |

Field meanings are identical to `MESSAGE_HEADER`. ⚠ Only the first five fields are rearranged —
`TransactionCode` moves from offset **0** to offset **10**.

#### `BCAST_HEADER` — 40 bytes (**the header for nearly every broadcast message**)

| Field | Type | Size | Offset |
|---|---|---|---|
| Reserved | BYTE[2] | 2 | 0 |
| Reserved | BYTE[2] | 2 | 2 |
| LogTime | LONG | 4 | 4 |
| AlphaChar | CHAR[2] | 2 | 8 |
| **TransactionCode** | SHORT | 2 | **10** |
| ErrorCode | SHORT | 2 | 12 |
| BCSeqNo | LONG | 4 | 14 |
| Reserved | CHAR[1] | 1 | 18 |
| Reserved | CHAR[3] | 3 | 19 |
| TimeStamp2 | CHAR[8] | 8 | 22 |
| Filler2 | BYTE[8] | 8 | 30 |
| MessageLength | SHORT | 2 | 38 |
| | | **40** | |

- **`TransactionCode` is at offset 10** — i.e. at **byte 18 of the broadcast data** once the 8-byte
  prefix (§3.4) is counted. This is the dispatch key for the whole feed.
- `BCSeqNo` — BCAST sequence number (for the Ericsson switch). The only per-stream sequence number
  available; the natural basis for gap detection.
- `TimeStamp2` — machine number, **stored in byte 0** on the broadcast circuit.
- `MessageLength` — entire message including this 40-byte header. **It does not include the 8-byte
  prefix.**
- `LogTime`, `AlphaChar`, `ErrorCode` as in `MESSAGE_HEADER`.

### 2.9 Bit-field structures

Several structures are bit fields, and **the spec gives two different bit orders — one for
little-endian machines and one for big-endian machines.** The wire is big-endian, so **the
big-endian listing is the one that describes the bytes you receive**; the little-endian listing
describes how a native C struct would have to be declared on an x86 host to overlay the same bits.

Rule of thumb for the big-endian listing: **the first field listed occupies the most significant
bit of the first byte**, and reading continues toward the least significant bit.

All bit-field structs used by broadcast messages are given in §4.14.

### 2.10 Error message

When `ErrorCode` in the `MESSAGE_HEADER` is non-zero, the structure actually sent is
`MS_ERROR_RESPONSE` — **182 bytes**, regardless of the transaction code:

| Field | Type | Size | Offset |
|---|---|---|---|
| MESSAGE_HEADER | STRUCT | 40 | 0 |
| Key | CHAR[14] | 14 | 40 |
| ErrorMessage | CHAR[128] | 128 | 54 |
| | | **182** | |

`Key` holds the **token number of the contract**. Error codes are listed in the spec's appendix.

> Check `ErrorCode` **before** dispatching on transaction code, or an error response will be parsed
> as whatever structure the transaction code implies.

---

## Part III — Chapter 10: Broadcast transport

This is the layer beneath the message catalog: how bytes arrive, and the four transformations
between a UDP datagram and a parseable message.

### 3.1 Why compression exists

Broadcast traffic — the online quote stream — grows sharply at market open and close. To contain it,
the exchange compresses a specific subset of transaction codes with **LZO** (Lempel-Ziv-Oberhumer,
`http://www.oberhumer.com/opensource/lzo`, LZO version 1.07 in the spec's sample), and packs several
messages into one buffer.

**The eight compressed transaction codes — exhaustive:**

| Code | Represents | Where defined |
|---|---|---|
| 7200 | Market By Order / MBP | Ch.10 §4.7 |
| 7201 | Market Watch | Ch.10 §4.9 |
| 7202 | Ticker & Market Index | Ch.10 §4.6 |
| 7208 | Only MBP | Ch.10 §4.8 |
| 7213 | Asset Price Details | Ch.10 §4.10 |
| 7215 | MBA Quantity Change | Ch.8 (out of scope) |
| 7216 | MBA Price Change | Ch.8 (out of scope) |
| 7220 | Limit Price Protection Ranges | Ch.10 §4.16 |

**Everything not in this table is uncompressed** — notably **7305 / 7340** (security master), 7324,
7306, 7320/7210, 7211, 7214, 6501, 6503, 6511/6521/6522/6531/6571, 6541, 9010/9011.

⚠ The compression flag on the wire is per sub-packet (§3.3) and is **authoritative**. Use this table
to sanity-check, not to decide.

### 3.2 The envelope: sequential packing

The host packs *n* messages into one buffer in **FIFO order** — 1st message first — and the front
end must unpack and process them **in that same order**. A buffer may hold **a mixture of compressed
and uncompressed** messages.

```c
struct {
    CHAR  cNetId[2];        /* byte 0 = segment netId */
    SHORT iNoPackets;       /* number of sub-packets sequentially packed */
    CHAR  cPackData[512];   /* the sub-packets, back to back */
} BcastPackData;            /* 516 bytes maximum */
```

**Segment netId values:**

| Segment | netId |
|---|---|
| Equity | 4 |
| Equity Derivatives | 2 |
| Currency Derivatives | 6 |
| **Commodity Derivatives (this document)** | **8** |

`cPackData` is capped at **512 bytes**, so the whole envelope is at most **516 bytes**. A datagram
larger than that is malformed.

### 3.3 The sub-packet: compressed-length prefix

Each sub-packet inside `cPackData` is:

```c
struct {
    SHORT iCompLen;                      /* length of the data AFTER compression */
    CHAR  cCompData[MAX_MESSAGE_SIZE];   /* the broadcast data */
} BcastCmpPacket;
```

equivalently stated by the spec as:

```c
COMPRESSION_BROADCAST_DATA {
    SHORT CompressionLen;
    CHAR  BroadcastData[];
}
```

- **`iCompLen == 0` → the data is NOT compressed.** Use it as-is.
- **`iCompLen > 0` → the data IS compressed**, and `iCompLen` is the length of the compressed bytes.
  Pass exactly that many bytes to the decompressor.

⚠ **The spec never says how long an uncompressed sub-packet is.** `iCompLen` is zero, so it carries
no length. The only length available is **`MessageLength` in the broadcast header**, which counts
from the header onward — so the sub-packet consumes `2 + 8 + MessageLength` bytes (length field +
prefix + message). Two defensive consequences:

1. `MessageLength < 40` (the header size) must be **rejected**. Accepting it advances the cursor too
   little and misframes every later sub-packet in the same buffer.
2. Advance the cursor by the computed size and re-check `iNoPackets`; a mismatch means you have lost
   framing, not that the exchange sent a short buffer.

### 3.4 ⚠ The 8-byte prefix — the single most common decoding error

> **Inside the broadcast data — compressed or uncompressed — the first 8 bytes come *before* the
> message header and must be skipped. The message header / broadcast header starts at the 9th
> byte.**

- **Byte 0 repeats the segment netId** (8 for Commodity Derivatives).
- **Bytes 1–7 are ignored.**
- For compressed sub-packets this applies **after decompression**, not before.

Getting this wrong shifts **every** field by 8 bytes. Getting it right gives you a free
frame-alignment check: **byte 0 of every payload must equal the envelope's netId.** A mismatch on
sub-packet 2 or later almost always means the *previous* sub-packet's length was computed wrongly.

> **Design note.** Handle the prefix in exactly one place. Either strip it once before dispatch, or
> have every parser take the full payload and skip it internally — but never both, and never
> per-message ad hoc.

### 3.5 Decompression

The algorithm is **LZO1Z**. The spec's reference implementation calls the LZO library function
`lzo1z_decompress` and wraps it in an API of the implementer's own making:

```c
lzo_decomp(char *inp_buff, unsigned int *inp_len,
           char *buffer_decomp, unsigned int *output_len,
           unsigned short *errorCode);
```

| Parameter | Meaning |
|---|---|
| `inp_buff` | Input (compressed) buffer |
| `inp_len` | Length of the compressed input |
| `buffer_decomp` | Output (decompressed) buffer |
| `output_len` | Length after decompression |
| `errorCode` | Error code out |

⚠ The build instructions in the spec (Visual C++ 6.0, `C:\lzo1.07`, link `lzo.lib`) are a **1990s
sample, not a requirement**. Any correct LZO1Z decompressor satisfies the protocol; the wire format
is what is normative.

⚠ **Compressed sub-packets are untrusted network input.** `lzo1z_decompress` and naive ports of it
are unchecked and will read or write out of bounds on malformed input. Use a bounds-checked
decompressor that returns an error, size the output buffer for the largest legal message, and
contain failures per packet — one corrupt sub-packet must not take down the relay.

### 3.6 The receive pipeline, end to end

```
UDP datagram (≤ 516 bytes)
 └─ BcastPackData: cNetId[2], iNoPackets, cPackData[512]
     └─ for i in 1..iNoPackets, in FIFO order:
         ├─ SHORT iCompLen
         ├─ if iCompLen > 0 → LZO1Z-decompress the next iCompLen bytes
         │  else            → take the next (8 + MessageLength) bytes as-is
         └─ broadcast data:
             ├─ byte 0      = netId  (verify == envelope netId)
             ├─ bytes 1..7  = ignored
             └─ byte 8..    = BCAST_HEADER (40 bytes) + message body
                 ├─ header.ErrorCode      → non-zero ⇒ MS_ERROR_RESPONSE
                 ├─ header.TransactionCode (payload offset 18) → dispatch
                 └─ header.MessageLength  → total from header onward
```

**Read-buffer sizing.** Size the UDP read buffer *larger* than 516. A `recvfrom`-style read silently
discards what does not fit and reports no error, so an exactly-sized buffer makes an oversized,
malformed datagram indistinguishable from a legitimately full one.

---

## Part IV — Chapter 10: The broadcast message catalog

Every layout below is **verified** (Part V). Offsets are relative to the start of the message
(offset 0 = first byte of the 40-byte header); **add 8 for the position within the broadcast data**.

### 4.0 Catalog index

| Txn code | Constant | Structure | Length | Compressed | § |
|---|---|---|---|---|---|
| 6501 | `BCAST_JRNL_VCT_MSG` | `MS_BCAST_MESSAGE` (Ch.7) | — | no | 4.1 |
| 6503 | `BCAST_ASSET_INT_RATE_CHG` | `MS_ASSET_UPDT_INT_RATE_INFO` | 52 | no | 4.15 |
| 6511 / 6521 / 6522 / 6531 / 6571 | market status | `MS_BCAST_VCT_MSGS` | 320 | no | 4.5 |
| 6541 | `BCAST_CIRCUIT_MSG` | `MESSAGE_HEADER` only | 40 | no | 4.12 |
| 7200 | `BCAST_MBO_MBP_UPDATE` | `MS_BCAST_MBO_MBP` | 410 | **yes** | 4.7 |
| 7201 | `BCAST_MW_ROUND_ROBIN` | `MS_BCAST_INQ_RESP_2` | 472 | **yes** | 4.9 |
| 7202 | `BCAST_TICKER_AND_MKT_INDEX` | `MS_TICKER_TRADE_DATA` | 484 | **yes** | 4.6 |
| 7206 | `BCAST_SYSTEM_INFORMATION_OUT` | `MS_SYSTEM_INFO_DATA` (Ch.3) | — | no | 4.2 |
| 7208 | `BCAST_ONLY_MBP` | `MS_BCAST_ONLY_MBP` | 470 | **yes** | 4.8 |
| 7210 | `BCAST_STOCK_STATUS_CHG_PREOPEN` | `MS_SECURITY_STATUS_UPDATE_INFO` | 462 | no | 4.4b |
| 7211 | `BCAST_SPD_MBP_DELTA` | `MS_SPD_MKT_INFO` | 204 | no | 4.13 |
| 7213 | `BCAST_ASSETS` | `MS_ASSET_DATA` | 76 | **yes** | 4.10 |
| 7214 | `BCAST_INTEREST_ASSETS` | `MBP_INFO` | 262 | no | 4.11 |
| 7220 | `BCAST_LIMIT_PRICE_PROTECTION_RANGE` | `MS_BCAST_LIMIT_PRICE_PROTECTION_RANGE` | 344 | **yes** | 4.16 |
| 7305 | `BCAST_SECURITY_MSTR_CHG` | `MS_SECURITY_UPDATE_INFO` | 294 | no | 4.3 |
| 7306 | `BCAST_PART_MSTR_CHG` | `PARTICIPANT_UPDATE_INFO` | 84 | no | 4.4a |
| 7320 | `BCAST_STOCK_STATUS_CHG` | `MS_SECURITY_STATUS_UPDATE_INFO` | 462 | no | 4.4b |
| 7324 | `BCAST_INST_MSTR_CHG` | `MS_INSTRUMENT_UPDATE_INFO` | 80 | no | 4.4c |
| 7340 | `BCAST_SEC_MSTR_CHNG_PERIODIC` | `MS_SECURITY_UPDATE_INFO` | 294 | no | 4.3 |
| 9010 | `BCAST_TURNOVER_EXCEEDED` | `MS_BROADCAST_LIMIT_EXCEEDED` | 98 | no | 4.4d |
| 9011 | `BROADCAST_BROKER_REACTIVATED` | `MS_BROADCAST_LIMIT_EXCEEDED` | 98 | no | 4.4d |

---

### 4.1 General message broadcast — 6501

Structure: `MS_BCAST_MESSAGE`, defined in **Chapter 7** (out of scope here).

| Field | Meaning |
|---|---|
| TransactionCode | `BCAST_JRNL_VCT_MSG` (6501) |
| BranchNumber | Branch number of the trader's branch |
| BrokerNumber | Trading Member ID of the broker |
| ActionCode | Action taken — e.g. `SYS` (system), `LIS` (listing) |
| BroadcastDestination | Destination: Trader Workstation or Control Workstation |
| BroadcastMessageLength | Length of the message text |
| BroadcastMessage | The message text |

### 4.2 Change in system status / parameters — 7206

Sent when global operating parameters change or market status changes. Structure:
`MS_SYSTEM_INFO_DATA` — see *System Information Response*, **Chapter 3**. Transaction code
`BCAST_SYSTEM_INFORMATION_OUT` (7206).

---

### 4.3 Security master change — 7305 / 7340

`MS_SECURITY_UPDATE_INFO`, **294 bytes**. Sent whenever any parameter of a security changes (7305);
**7340** (`BCAST_SEC_MSTR_CHNG_PERIODIC`) is the identical structure re-sent periodically during the
day for every security that changed. **Not compressed.**

✅ **Verified: fields sum to exactly 294. There is no padding anywhere in this structure** — the
spec's offset column is correct throughout. Do **not** align the 3-byte
`ST_SEC_ELIGIBILITY_PER_MARKET`; its array stride is 3, not 4.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| Token | LONG | 4 | 40 |
| SEC_INFO (Ch.5) | STRUCT | 30 | 44 |
| PermittedToTrade | SHORT | 2 | 74 |
| IssuedCapital | DOUBLE | 8 | 76 |
| WarningQuantity | LONG | 4 | 84 |
| FreezeQuantity | LONG | 4 | 88 |
| CreditRating | CHAR[12] | 12 | 92 |
| ST_SEC_ELIGIBILITY_PER_MARKET[4] | STRUCT | 3×4 = 12 | 104 |
| IssueRate | SHORT | 2 | 116 |
| IssueStartDate | LONG | 4 | 118 |
| InterestPaymentDate | LONG | 4 | 122 |
| IssueMaturityDate | LONG | 4 | 126 |
| MarginPercentage | LONG | 4 | 130 |
| MinimumLotQuantity | LONG | 4 | 134 |
| BoardLotQuantity | LONG | 4 | 138 |
| TickSize | LONG | 4 | 142 |
| Name | CHAR[25] | 25 | 146 |
| Reserved | CHAR[1] | 1 | 171 |
| ListingDate | LONG | 4 | 172 |
| ExpulsionDate | LONG | 4 | 176 |
| ReAdmissionDate | LONG | 4 | 180 |
| RecordDate | LONG | 4 | 184 |
| LowPriceRange | LONG | 4 | 188 |
| HighPriceRange | LONG | 4 | 192 |
| ExpiryDate | LONG | 4 | 196 |
| NoDeliveryStartDate | LONG | 4 | 200 |
| NoDeliveryEndDate | LONG | 4 | 204 |
| ST_ELIGIBILITY_INDICATORS | STRUCT | 2 | 208 |
| BookClosureStartDate | LONG | 4 | 210 |
| BookClosureEndDate | LONG | 4 | 214 |
| ExerciseStartDate | LONG | 4 | 218 |
| ExerciseEndDate | LONG | 4 | 222 |
| Multiplier | LONG | 4 | 226 |
| AssetInstrument | CHAR[6] | 6 | 230 |
| AssetName | CHAR[10] | 10 | 236 |
| UnderlyingToken | LONG | 4 | 246 |
| IntrinsicValue | LONG | 4 | 250 |
| ExtrinsicValue | LONG | 4 | 254 |
| ST_PURPOSE | STRUCT | 2 | 258 |
| LocalUpdateDateTime | LONG | 4 | 260 |
| DeleteFlag | CHAR | 1 | 264 |
| Remark | CHAR[25] | 25 | 265 |
| BasePrice | LONG | 4 | 290 |
| | | **294** | |

⚠ Note `Name` (25) at 146 is followed by a `Reserved` CHAR[1] at 171 — that reserved byte is exactly
what re-aligns the following `LONG` to an even offset. It is a real field in the table, not implicit
padding, but its function is alignment. Similarly `DeleteFlag`(1) + `Remark`(25) = 26 keeps
`BasePrice` even.

**Field semantics**

| Field | Meaning |
|---|---|
| Token | Token number of the security — unique per symbol-series combination |
| SEC_INFO | Symbol and Series (EQ / IL / TT) of the security |
| PermittedToTrade | `0` listed but not permitted to trade; `1` permitted to trade |
| IssuedCapital | Issue size |
| WarningQuantity / FreezeQuantity | Warning and freeze quantities |
| CreditRating | Credit rating of the security |
| Eligibility (in ST_SEC_ELIGIBILITY_PER_MARKET) | `1` if the security may trade in that market |
| Status (in ST_SEC_ELIGIBILITY_PER_MARKET) | `1` Pre-open (Normal market only) · `2` Open · `3` Suspended · `4` Pre-open extended · `5` Stock Open With Market |
| IssueRate | Price of the issue |
| IssueStartDate / InterestPaymentDate / IssueMaturityDate | Issue, interest-payment and maturity dates |
| MarginPercentage | Initial margin percent collected on the contract |
| MinimumLotQuantity | Minimum lot that may be ordered |
| BoardLotQuantity | Regular lot size |
| TickSize | Tick size / minimum spread |
| Name | Security name |
| ListingDate / ExpulsionDate / ReAdmissionDate / RecordDate | Corresponding dates |
| LowPriceRange / HighPriceRange | Lower/upper operating-range price limits **(scaled price)** |
| ExpiryDate | Last trading date before any corporate action |
| NoDeliveryStartDate / NoDeliveryEndDate | Window in which physical delivery is stopped for book closure |
| MinimumFill / AON (in ST_ELIGIBILITY_INDICATORS) | Whether MF / All-or-None order attributes are allowed for this security |
| ParticipateInMarketIndex | Set if the security participates in the market index |
| BookClosureStartDate / BookClosureEndDate | Shareholder record-book window |
| ExerciseStartDate / ExerciseEndDate | Exercise window |
| Multiplier | Multiply packet volume by this to get actual ordered quantity. **Never send it to the host.** |
| AssetInstrument | Underlying asset type, e.g. `INDEX` |
| AssetName | Underlying asset name, e.g. `NIFTY` |
| UnderlyingToken | Token of the underlying asset / **futures contract** (for options on futures) |
| IntrinsicValue / ExtrinsicValue | Contract intrinsic and extrinsic value |
| ST_PURPOSE | Corporate-action flags — exercise style, EGM, AGM, interest, bonus, rights, dividend |
| LocalUpdateDateTime | Local database update date-time |
| DeleteFlag | `N` active · `Y` deleted |
| Remark | Free-text remarks |
| BasePrice | Base price of the stock **(scaled price)** |

---

### 4.4 Master and status change messages

#### 4.4a Participant status change — 7306

`PARTICIPANT_UPDATE_INFO`, **84 bytes**. ⚠ Fields sum to **83**; one **trailing pad byte** at offset
83.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| ParticipantId | CHAR[12] | 12 | 40 |
| ParticipantName | CHAR[25] | 25 | 52 |
| ParticipantStatus | CHAR | 1 | 77 |
| ParticipantUpdateDateTime | LONG | 4 | 78 |
| DeleteFlag | CHAR | 1 | 82 |
| *(pad)* | — | 1 | 83 |
| | | **84** | |

`ParticipantStatus`: `S` suspended · `A` active. `DeleteFlag`: `Y` / `N`.
`ParticipantUpdateDateTime` is seconds since 1 Jan 1980.

#### 4.4b Security status change — 7320 / 7210

`MS_SECURITY_STATUS_UPDATE_INFO`, **462 bytes**. 7320 = `BCAST_STOCK_STATUS_CHG`,
7210 = `BCAST_STOCK_STATUS_CHG_PREOPEN`. ✅ Exact, no padding.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| NumberOfRecords | SHORT | 2 | 40 |
| TOKEN_AND_ELIGIBILITY[35] | STRUCT | 12×35 = 420 | 42 |
| | | **462** | |

`TOKEN_AND_ELIGIBILITY` — **12 bytes**:

| Field | Type | Size | Offset |
|---|---|---|---|
| Token | LONG | 4 | 0 |
| ST_SEC_STATUS_PER_MARKET[4] | STRUCT | 2×4 = 8 | 4 |

`ST_SEC_STATUS_PER_MARKET` — **2 bytes**: a single `Status` SHORT at offset 0.

`Status`: `1` Pre-open · `2` Open · `3` Suspended · `4` Pre-open extended.

⚠ **`NumberOfRecords` is a count, not the array size.** The message is fixed at 462 bytes and always
carries room for 35 records; only the first `NumberOfRecords` are real. Parsing all 35
unconditionally invents securities out of trailing zero padding. Clamp the count to `[0, 35]` and
warn if it had to be clamped.

#### 4.4c Instrument master change — 7324

`MS_INSTRUMENT_UPDATE_INFO`, **80 bytes**. ⚠ Fields sum to **79**; one **trailing pad byte** at 79.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| InstrumentId | SHORT | 2 | 40 |
| InstrumentName | CHAR[6] | 6 | 42 |
| InstrumentDescription | CHAR[26] | 26 | 48 |
| InstrumentUpdateDateTime | LONG | 4 | 74 |
| DeleteFlag | CHAR | 1 | 78 |
| *(pad)* | — | 1 | 79 |
| | | **80** | |

`InstrumentName` is the type, e.g. `FUTCOM`; `InstrumentDescription` the full name, e.g.
`FUTURES ON COMMODITY`. `DeleteFlag`: `Y` deleted · `N` active.

#### 4.4d Turnover limit exceeded / broker reactivated — 9010 / 9011

`MS_BROADCAST_LIMIT_EXCEEDED`, **98 bytes**. ✅ Exact, no padding.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| BrokerCode | CHAR[5] | 5 | 40 |
| CounterBrokerCode | CHAR[5] | 5 | 45 |
| WarningType | SHORT | 2 | 50 |
| Token | LONG | 4 | 52 |
| InstrumentName | CHAR[6] | 6 | 56 |
| Symbol | CHAR[10] | 10 | 62 |
| ExpiryDate | LONG | 4 | 72 |
| StrikePrice | LONG | 4 | 76 |
| OptionType | CHAR[2] | 2 | 80 |
| CALevel | SHORT | 2 | 82 |
| TradeNumber | LONG | 4 | 84 |
| TradePrice | LONG | 4 | 88 |
| TradeVolume | LONG | 4 | 92 |
| Final | CHAR | 1 | 96 |
| cFiller | CHAR | 1 | 97 |
| | | **98** | |

- **9010** — broker's turnover is about to exceed or has exceeded; **9011** — broker reactivated.
- `WarningType`: `1` about to exceed · `2` exceeded (broker deactivated). **9010 only.**
- `CounterBrokerCode` is **not in use**.
- `Symbol`, `TradeNumber`, `TradePrice`, `TradeVolume`, `Final` are meaningful **only for 9010**;
  they describe the broker's last trade. `TradePrice` is a **scaled price**.
- `cFiller` is reserved for future use.

---

### 4.5 Market status change — 6511 / 6521 / 6522 / 6531 / 6571

`MS_BCAST_VCT_MSGS`, **320 bytes**. ⚠ Fields sum to **319**; one **trailing pad byte** at 319.

⚠ **Spec erratum:** the structure table's "Transaction Code" cell reads
`BCAST_TURNOVER_EXCEEDED (9010) / BROADCAST_BROKER_REACTIVATED (9011)` — copy-pasted from §4.4d. The
correct codes are the market-status ones in the field-description table below.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| Token | LONG | 4 | 40 |
| SEC_INFO (Ch.5) | STRUCT | 30 | 44 |
| MarketType | SHORT | 2 | 74 |
| ST_BCAST_DESTINATION | STRUCT | 2 | 76 |
| BroadcastMessageLength | SHORT | 2 | 78 |
| BroadcastMessage | CHAR[239] | 239 | 80 |
| *(pad)* | — | 1 | 319 |
| | | **320** | |

**Transaction codes**

| Code | Constant | Sent when |
|---|---|---|
| 6511 | `BC_OPEN_MSG` | Market opened |
| 6521 | `BC_CLOSE_MSG` | Market closed |
| 6522 | `BC_POSTCLOSE_MSG` | Market in post-close session |
| 6531 | `BC_PRE_OR_POST_DAY_MSG` | Market pre-opened |
| 6571 | `BC_PRE_OPEN_ENDED` | Pre-open period ended |

**⚠ Market-open sequence — it differs between the first and subsequent openings:**

- **Regular (first) opening of the day:** `BC_OPEN_MSG` (6511) **only**.
- **Re-opening later the same day** (e.g. after a sun outage or circuit hit): `BC_PRE_OR_POST_DAY_MSG`
  (6531) → `BC_PRE_OPEN_ENDED` (6571) → `BC_OPEN_MSG` (6511).

A state machine that requires a pre-open before every open will stall at the regular market open.

`MarketType`: `1` Normal · `2` Odd Lot · `3` Spot · `4` Auction.
`ST_BCAST_DESTINATION` set to `1` means the message is for the TWS.
`BroadcastMessageLength` gives the valid length of `BroadcastMessage` — **use it; the remainder of
the 239 bytes is not meaningful.**

---

### 4.6 Ticker and market index — 7202 *(compressed)*

`MS_TICKER_TRADE_DATA`, **484 bytes**. ✅ Exact, no padding.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| NumberOfRecords | SHORT | 2 | 40 |
| ST_TICKER_INDEX_INFO[17] | STRUCT | 26×17 = 442 | 42 |
| | | **484** | |

`ST_TICKER_INDEX_INFO` — **26 bytes**:

| Field | Type | Size | Offset |
|---|---|---|---|
| Token | LONG | 4 | 0 |
| MarketType | SHORT | 2 | 4 |
| FillPrice | LONG | 4 | 6 |
| FillVolume | LONG | 4 | 10 |
| OpenInterest | LONG | 4 | 14 |
| DayHiOI | LONG | 4 | 18 |
| DayLoOI | LONG | 4 | 22 |
| | | **26** | |

`NumberOfRecords` — number of records actually populated, **maximum 17**. Same clamp-and-warn rule as
§4.4b. `FillPrice` is the traded price (**scaled**), `FillVolume` the traded quantity.

---

### 4.7 Market by Order / Market by Price — 7200 *(compressed)*

`MS_BCAST_MBO_MBP`, **410 bytes**. Best buy and best sell orders **with order-level detail**.

⚠ **Interior padding.** The nested `ST_INTERACTIVE_MBO_DATA` declares 235 bytes but its fields sum
to 235 only if `NetChangeIndicator` (CHAR at offset 16) is left unpadded — which would put a `LONG`
at odd offset 17. Under pack(2) **one pad byte sits at offset 17** and the struct's true size is
**236**, which is exactly what makes the outer message reach its declared 410. Every offset in the
spec's outer table from `Record Buffer` onward is therefore **1 too low**.

**Corrected outer layout:**

| Field | Type | Size | Offset (corrected) | Offset (spec, wrong) |
|---|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 | 0 |
| ST_INTERACTIVE_MBO_DATA | STRUCT | **236** | 40 | 40 |
| RecordBuffer = ST_MBP_INFO[10] | CHAR | 10×10 = 100 | **276** | 275 |
| TotalBuyQuantity | DOUBLE | 8 | **376** | 375 |
| TotalSellQuantity | DOUBLE | 8 | **384** | 383 |
| ST_INDICATOR | STRUCT | 2 | **392** | 391 |
| ClosingPrice | LONG | 4 | **394** | 393 |
| OpenPrice | LONG | 4 | **398** | 397 |
| HighPrice | LONG | 4 | **402** | 401 |
| LowPrice | LONG | 4 | **406** | 405 |
| | | **410** | | |

`ST_INTERACTIVE_MBO_DATA` — declared 235, **true size 236**:

| Field | Type | Size | Offset (corrected) |
|---|---|---|---|
| Token | LONG | 4 | 0 |
| BookType | SHORT | 2 | 4 |
| TradingStatus | SHORT | 2 | 6 |
| VolumeTradedToday | LONG | 4 | 8 |
| LastTradedPrice | LONG | 4 | 12 |
| NetChangeIndicator | CHAR | 1 | 16 |
| ***(pad)*** | — | **1** | **17** |
| NetPriceChangeFromClosingPrice | LONG | 4 | 18 |
| LastTradeQuantity | LONG | 4 | 22 |
| LastTradeTime | LONG | 4 | 26 |
| AverageTradePrice | LONG | 4 | 30 |
| AuctionNumber | SHORT | 2 | 34 |
| AuctionStatus | SHORT | 2 | 36 |
| InitiatorType | SHORT | 2 | 38 |
| InitiatorPrice | LONG | 4 | 40 |
| InitiatorQuantity | LONG | 4 | 44 |
| AuctionPrice | LONG | 4 | 48 |
| AuctionQuantity | LONG | 4 | 52 |
| RecordBuffer = ST_MBO_INFO[10] | CHAR | 18×10 = 180 | 56 |
| | | **236** | |

`ST_MBO_INFO` — **18 bytes** (order-level; 5 best buys then 5 best sells):

| Field | Type | Size | Offset |
|---|---|---|---|
| TraderId | LONG | 4 | 0 |
| Qty | LONG | 4 | 4 |
| Price | LONG | 4 | 8 |
| ST_MBO_MBP_TERMS | STRUCT | 2 | 12 |
| MinFillQty | LONG | 4 | 14 |
| | | **18** | |

`ST_MBP_INFO` — **10 bytes** (price-level; 5 best buys then 5 best sells):

| Field | Type | Size | Offset |
|---|---|---|---|
| Qty | LONG | 4 | 0 |
| Price | LONG | 4 | 4 |
| NoOfOrders | SHORT | 2 | 8 |
| | | **10** | |

**⚠ `BookType` gating — a processing rule, not a parsing rule:**

| BookType | Market |
|---|---|
| `1` | RL (Regular Lot) |
| `2` | ST (Stop Loss) |
| `5` | Odd Lot |
| `6` | SP (Spread) |
| `7` | AU (Auction) |

> **Process the message only if BookType is `1` or `2`. Skip it in all other cases.**

**Field semantics**

| Field | Meaning |
|---|---|
| Token | Unique per symbol-series combination |
| TradingStatus | `1` Preopen · `2` Open · `3` Suspended · `4` Preopen Extended |
| VolumeTradedToday | Total quantity of the security traded today |
| LastTradedPrice | Latest trade price **(scaled)** |
| NetChangeIndicator | `+` increase · `-` decrease, of order price vs. LTP |
| NetPriceChangeFromClosingPrice | ⚠ Presently carries the **closing price**, not a change. Both LTP and closing price are in the packet; the front end computes `((closing − LTP) / closing) × 100` itself. |
| LastTradeQuantity / LastTradeTime | Quantity and time of the last trade |
| AverageTradePrice | Average of all trades in the security **(scaled)** |
| AuctionNumber | Max 9999; zero outside auctions |
| AuctionStatus | See market status in the spec's appendix |
| InitiatorType | `control` or `trader`. Presently always `control` (only the Exchange initiates auctions); otherwise blank |
| InitiatorPrice / InitiatorQuantity | Initiator's auction order; zero otherwise |
| AuctionPrice / AuctionQuantity | Auction price and quantity; zero otherwise |
| RecordBuffer (MBO) | **Five best buy orders and five best sell orders** from the order book |
| RecordBuffer (MBP) | **Five best buy prices and five best sell prices** from the order book |
| TotalBuyQuantity / TotalSellQuantity | Total buy / sell order quantity in the security (`DOUBLE`) |
| Indicator (ST_INDICATOR) | Buy / Sell flags and whether the latest trade was less or more than the immediately previous LTP |
| ClosingPrice / OpenPrice / HighPrice / LowPrice | **(scaled prices)** |

---

### 4.8 Only Market by Price — 7208 *(compressed)*

`MS_BCAST_ONLY_MBP`, **470 bytes**. Price-level depth for **up to two securities per message**.

⚠ **This is the worked example of §2.3 and the highest-value correction in this document.**
`INTERACTIVE_ONLY_MBP_DATA` declares 213 bytes and its fields sum to 213 — but `NetChangeIndicator`
(CHAR at offset 16) is the only odd-sized member, so under pack(2) **one pad byte sits at offset 17**
and the **record stride is 214**. Only then does the message total work out:
`40 + 2 + 2 × 214 = 470` = the declared packet length. With a 213-byte stride you get 468 and the
spec's own declared length is unreachable.

**Why this is subtly wrong rather than obviously wrong:** `Token`, `BookType`, `TradingStatus`,
`VolumeTradedToday` and `LastTradedPrice` all sit before offset 17 and decode **correctly**.
Everything after — `OpenPrice`, `HighPrice`, `LowPrice`, the whole MBP ladder — becomes noise, and
`records[1]` starts one byte early. It looks like a data-quality problem, not a framing bug.

**Outer layout — ✅ correct as printed:**

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| NoOfRecords | SHORT | 2 | 40 |
| INTERACTIVE_ONLY_MBP_DATA[2] | STRUCT | **214**×2 = 428 | 42 |
| | | **470** | |

`INTERACTIVE_ONLY_MBP_DATA` — declared 213, **true stride 214**:

| Field | Type | Size | Offset (corrected) | Offset (spec, wrong) |
|---|---|---|---|---|
| Token | LONG | 4 | 0 | 0 |
| BookType | SHORT | 2 | 4 | 4 |
| TradingStatus | SHORT | 2 | 6 | 6 |
| VolumeTradedToday | LONG | 4 | 8 | 8 |
| LastTradedPrice | LONG | 4 | 12 | 12 |
| NetChangeIndicator | CHAR | 1 | 16 | 16 |
| ***(pad)*** | — | **1** | **17** | — |
| NetPriceChangeFromClosingPrice | LONG | 4 | **18** | 17 |
| LastTradeQuantity | LONG | 4 | **22** | 21 |
| LastTradeTime | LONG | 4 | **26** | 25 |
| AverageTradePrice | LONG | 4 | **30** | 29 |
| AuctionNumber | SHORT | 2 | **34** | 33 |
| AuctionStatus | SHORT | 2 | **36** | 35 |
| InitiatorType | SHORT | 2 | **38** | 37 |
| InitiatorPrice | LONG | 4 | **40** | 39 |
| InitiatorQuantity | LONG | 4 | **44** | 43 |
| AuctionPrice | LONG | 4 | **48** | 47 |
| AuctionQuantity | LONG | 4 | **52** | 51 |
| RecordBuffer = MBP_INFORMATION[10] | CHAR | 12×10 = 120 | **56** | 55 |
| BbTotalBuyFlag | SHORT | 2 | **176** | 175 |
| BbTotalSellFlag | SHORT | 2 | **178** | 177 |
| TotalBuyQuantity | DOUBLE | 8 | **180** | 179 |
| TotalSellQuantity | DOUBLE | 8 | **188** | 187 |
| ST_INDICATOR | STRUCT | 2 | **196** | 195 |
| ClosingPrice | LONG | 4 | **198** | 197 |
| OpenPrice | LONG | 4 | **202** | 201 |
| HighPrice | LONG | 4 | **206** | 205 |
| LowPrice | LONG | 4 | **210** | 209 |
| | | **214** | | |

`MBP_INFORMATION` — **12 bytes** (5 best buy entries followed by 5 best sell entries):

| Field | Type | Size | Offset |
|---|---|---|---|
| Quantity | LONG | 4 | 0 |
| Price | LONG | 4 | 4 |
| NumberOfOrders | SHORT | 2 | 8 |
| BbBuySellFlag | SHORT | 2 | 10 |
| | | **12** | |

⚠ **`NoOfRecords` is a count, not the array size.** The message is always 470 bytes with room for
**2** records; `NoOfRecords` says how many carry data. Clamp to `[0, 2]`, warn if clamping was
needed, and parse only that many. Parsing both slots unconditionally decodes trailing padding into a
**second, entirely fabricated security** that looks completely real downstream.

⚠ `BbTotalBuyFlag` and `BbTotalSellFlag` **currently contain zero** — the buy-back concept is not
implemented. Do not build logic on them.

Other field semantics are as §4.7 (`BookType` here is documented as RL/ST/SL/NT/OL/SP with the full
list in the spec's appendix; `AuctionNumber` is *currently not in use* for 7208).

### ⚠ 4.8a 7200 and 7208 are mutually exclusive

> *"These packets of broadcast above mentioned, at any time either 7200 or 7208 only will come but
> not both. This decision of sending which packet is taken by NSEIL control."*

Both parsers must exist, and which one runs is an operational property of the segment, not a
compile-time choice. **Do not assume order-level (MBO) data will be available.**

---

### 4.9 Market watch update — 7201 *(compressed)*

`MS_BCAST_INQ_RESP_2`, **472 bytes**. Best buy/sell and last trade price, round-robin across
securities. ✅ Exact, no padding.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| NoOfRecords | SHORT | 2 | 40 |
| ST_MARKET_WATCH_BCAST[5] | STRUCT | 86×5 = 430 | 42 |
| | | **472** | |

`ST_MARKET_WATCH_BCAST` — **86 bytes**:

| Field | Type | Size | Offset |
|---|---|---|---|
| Token | LONG | 4 | 0 |
| ST_MKT_WISE_INFO[3] | STRUCT | 26×3 = 78 | 4 |
| OpenInterest | LONG | 4 | 82 |
| | | **86** | |

`ST_MKT_WISE_INFO` — **26 bytes**, one per market:

| Field | Type | Size | Offset |
|---|---|---|---|
| ST_INDICATOR | STRUCT | 2 | 0 |
| BuyVolume | LONG | 4 | 2 |
| BuyPrice | LONG | 4 | 6 |
| SellVolume | LONG | 4 | 10 |
| SellPrice | LONG | 4 | 14 |
| LastTradePrice | LONG | 4 | 18 |
| LastTradeTime | LONG | 4 | 22 |
| | | **26** | |

Buy/Sell volume and price are the **best** order's quantity and price. `BuyPrice`, `SellPrice`,
`LastTradePrice` are **scaled prices**. `NoOfRecords` clamps to `[0, 5]` as in §4.4b.

---

### 4.10 Asset price details — 7213 *(compressed)*

`MS_ASSET_DATA`, **76 bytes**. ✅ Exact, no padding.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| Token | LONG | 4 | 40 |
| BidPrice | LONG | 4 | 44 |
| AskPrice | LONG | 4 | 48 |
| DealPrice | LONG | 4 | 52 |
| Symbol | CHAR[10] | 10 | 56 |
| Instrument | CHAR[6] | 6 | 66 |
| ClosingPrice | LONG | 4 | 72 |
| | | **76** | |

`Symbol` / `Instrument` describe the **underlying asset**. `ClosingPrice` is the last trade day's
closing (last deal) price of the asset. All four prices are **scaled**.

---

### 4.11 Interest rate assets feed — 7214

`MBP_INFO`, **262 bytes**. **Not compressed.** ✅ Exact, no padding.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| Token | LONG | 4 | 40 |
| Symbol | CHAR[10] | 10 | 44 |
| MbpBuy[5] | STRUCT | 20×5 = 100 | 54 |
| MbpSell[5] | STRUCT | 20×5 = 100 | 154 |
| BookType | SHORT | 2 | 254 |
| InstrumentType | CHAR[6] | 6 | 256 |
| | | **262** | |

`MbpBuy` and `MbpSell` — **20 bytes** each, identical layout:

| Field | Type | Size | Offset |
|---|---|---|---|
| OrderCount | LONG | 4 | 0 |
| OrderPrice | LONG | 4 | 4 |
| YieldRate | LONG | 4 | 8 |
| OrderQuantity | DOUBLE | 8 | 12 |
| | | **20** | |

⚠ The spec's field-description table describes **MBPSells before MBPbuys**, while the offset table
puts **`MbpBuy` first at offset 54**. The offset table is authoritative: **buys at 54, sells at 154.**

`OrderCount` = number of orders at that price · `OrderPrice` = price of the orders **(scaled)** ·
`YieldRate` = yield for that price point · `OrderQuantity` = total volume at that price. `Token` and
`Symbol` refer to the **underlying asset**.

---

### 4.12 Broadcast circuit check (heartbeat) — 6541

If no data has flowed on the broadcast circuit for a stipulated period, the host sends a pulse.
**The period is currently 9 seconds and is changeable by NSEIL control.**

Structure: **`MESSAGE_HEADER` only** (⚠ *not* `BCAST_HEADER`), 40 bytes, transaction code
`BCAST_CIRCUIT_MSG` (6541).

> **Design note.** This is your only liveness signal. Silence longer than the pulse interval means
> the circuit is down, not that the market is quiet. Because the interval is exchange-configurable,
> make the timeout a configuration value, not a constant — and note that `MESSAGE_HEADER` puts
> `TransactionCode` at offset **0**, not 10.

---

### 4.13 Spread market by price — 7211

`MS_SPD_MKT_INFO`, **204 bytes**. Broadcast **for every activity occurring** on a spread
combination. ✅ Exact, no padding.

⚠ **Arrives on the broadcast circuit, and on the interactive circuit if broadcast is unavailable.**

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| Token1 | LONG | 4 | 40 |
| Token2 | LONG | 4 | 44 |
| MbpBuy | SHORT | 2 | 48 |
| MbpSell | SHORT | 2 | 50 |
| LastActiveTime | LONG | 4 | 52 |
| TradedVolume | LONG | 4 | 56 |
| TotalTradedValue | DOUBLE | 8 | 60 |
| MbpBuys[5] | STRUCT | 10×5 = 50 | 68 |
| MbpSells[5] | STRUCT | 10×5 = 50 | 118 |
| TotalOrderVolume | STRUCT | 16 | 168 |
| OpenPriceDifference | LONG | 4 | 184 |
| DayHighPriceDifference | LONG | 4 | 188 |
| DayLowPriceDifference | LONG | 4 | 192 |
| LastTradedPriceDifference | LONG | 4 | 196 |
| LastUpdateTime | LONG | 4 | 200 |
| | | **204** | |

`MbpBuys` / `MbpSells` — **10 bytes** each, identical layout:

| Field | Type | Size | Offset |
|---|---|---|---|
| NoOrders | SHORT | 2 | 0 |
| Volume | LONG | 4 | 2 |
| Price | LONG | 4 | 6 |
| | | **10** | |

`TotalOrderVolume` — **16 bytes**: `Buy` DOUBLE @0, `Sell` DOUBLE @8.

| Field | Meaning |
|---|---|
| Token1 / Token2 | Security with the **earlier** / **later** expiry date |
| MbpBuy / MbpSell | Total number of buys / sells for the combination |
| LastActiveTime | Timestamp of the last activity |
| TradedVolume | Total number of trades today |
| TotalTradedValue | Total traded value. **Scaled**; for lakhs of rupees divide by `10^(precision + 2)` |
| Open/DayHigh/DayLow/LastTraded **PriceDifference** | First / max / min / latest spread-spread trade price difference of the day |
| LastUpdateTime | Same value as `LastActiveTime` |

---

### 4.14 Bit-field structures used by broadcast messages

Reminder from §2.9: **the big-endian listing describes the wire.** First field listed = most
significant bit of the first byte.

#### `ST_INDICATOR` — 2 bytes *(used by 7200, 7208, 7201)*

| Big-endian (wire) | Bits | Byte |
|---|---|---|
| LastTradeMore | 1 | 0 (MSB) |
| LastTradeLess | 1 | 0 |
| Buy | 1 | 0 |
| Sell | 1 | 0 |
| Reserved | 4 | 0 (LSBs) |
| Reserved | CHAR 1 | 1 |

Flags indicating Buy, Sell, and whether the latest trade was **less** or **more** than the
immediately previous LTP.
*(Little-endian declaration order, for reference: Reserved 4, Sell 1, Buy 1, LastTradeLess 1,
LastTradeMore 1, Reserved CHAR.)*

#### `ST_MBO_MBP_TERMS` — 2 bytes *(inside `ST_MBO_INFO`, 7200)*

| Big-endian (wire) | Bits | Byte |
|---|---|---|
| MF (Minimum Fill) | 1 | 0 (MSB) |
| AON (All Or None) | 1 | 0 |
| Reserved | 6 | 0 |
| Reserved | CHAR 1 | 1 |

#### `ST_SEC_ELIGIBILITY_PER_MARKET` — **3 bytes** *(7305)*

| Big-endian (wire) | Size | Byte |
|---|---|---|
| Eligibility | BIT 1 | 0 (MSB) |
| Reserved | BIT 7 | 0 |
| Status | SHORT 2 | 1–2 |

⚠ **3 bytes, and the array of 4 has stride 3** (total 12). This is the one odd-sized struct in the
protocol that is **not** padded to even — the spec's own offsets require it, and 7305's total of 294
confirms it.

#### `ST_ELIGIBILITY_INDICATORS` — 2 bytes *(7305)*

| Big-endian (wire) | Bits | Byte |
|---|---|---|
| ParticipateInMarketIndex | 1 | 0 (MSB) |
| AON | 1 | 0 |
| MinimumFill | 1 | 0 |
| Reserved | 5 | 0 |
| Reserved | CHAR 1 | 1 |

#### `ST_PURPOSE` — 2 bytes *(7305)*

| Big-endian (wire) | Bits | Byte |
|---|---|---|
| Dividend | 1 | 0 (MSB) |
| Rights | 1 | 0 |
| Bonus | 1 | 0 |
| Interest | 1 | 0 |
| AGM | 1 | 0 |
| EGM | 1 | 0 |
| Reserved | 1 | 0 |
| ExerciseStyle | 1 | 0 (LSB) |
| ExAllowed | 1 | 1 (MSB) |
| ExRejectionAllowed | 1 | 1 |
| PlAllowed | 1 | 1 |
| IsThisAsset | 1 | 1 |
| IsCorporateAdjusted | 1 | 1 |
| Reserved | 3 | 1 (LSBs) |

---

### 4.15 Asset interest rate update — 6503

`MS_ASSET_UPDT_INT_RATE_INFO`, **52 bytes**. Sent when interest-rate or volatility information for
an asset changes. ✅ Exact, no padding.

⚠ **Uses `MESSAGE_HEADER`, not `BCAST_HEADER`** — so `TransactionCode` is at offset **0**.

| Field | Type | Size | Offset |
|---|---|---|---|
| MESSAGE_HEADER | STRUCT | 40 | 0 |
| Token | LONG | 4 | 40 |
| ForeignInterestRate | SHORT | 2 | 44 |
| Volatility | LONG | 4 | 46 |
| DomesticInterestRate | SHORT | 2 | 50 |
| | | **52** | |

⚠ `Volatility` is a `LONG` at offset **46** — even but not 4-aligned. This is the clearest proof that
"word aligned" in this protocol means **2 bytes** (§2.2). A parser assuming 4-byte alignment inserts
2 phantom bytes and overruns the message.

`Token` is the token number of the **asset**.

---

### 4.16 Limit price protection ranges — 7220 *(compressed)*

`MS_BCAST_LIMIT_PRICE_PROTECTION_RANGE` (formerly `MS_BCAST_TRADE_EXECUTION_RANGE`), **344 bytes**.
Transaction code `BCAST_LIMIT_PRICE_PROTECTION_RANGE` (7220), introduced with Limit Price Protection
in the Commodity Derivatives segment (spec v3.4). ✅ Exact, no padding.

| Field | Type | Size | Offset |
|---|---|---|---|
| BCAST_HEADER | STRUCT | 40 | 0 |
| LIMIT_PRICE_PROTECTION_RANGE_DATA | STRUCT | 304 | 40 |
| | | **344** | |

`LIMIT_PRICE_PROTECTION_RANGE_DATA` — **304 bytes**:

| Field | Type | Size | Offset |
|---|---|---|---|
| MsgCount | LONG | 4 | 0 |
| LIMIT_PRICE_PROTECTION_RANGE_DETAILS[25] | STRUCT | 12×25 = 300 | 4 |
| | | **304** | |

`LIMIT_PRICE_PROTECTION_RANGE_DETAILS` — **12 bytes**:

| Field | Type | Size | Offset |
|---|---|---|---|
| TokenNumber | LONG | 4 | 0 |
| HighExecBand | LONG | 4 | 4 |
| LowExecBand | LONG | 4 | 8 |
| | | **12** | |

`MsgCount` = number of tokens present in this packet — **clamp to `[0, 25]`** as in §4.4b.
`HighExecBand` / `LowExecBand` are the high and low LPP trade-execution bands **(scaled prices)**.
⚠ `MsgCount` is a `LONG` here, unlike the `SHORT` counts elsewhere.

---

## Part V — Layout verification table

Every broadcast structure in Chapter 10, with its declared `Packet Length`, the sum of its fields as
printed, and the resolution. **This is the table to re-derive whenever a structure is added or
changed** (§2.3).

| Txn code | Structure | Declared | Field sum | Δ | Resolution |
|---|---|---|---|---|---|
| 7305 / 7340 | `MS_SECURITY_UPDATE_INFO` | 294 | 294 | 0 | ✅ No padding. Do not align `ST_SEC_ELIGIBILITY_PER_MARKET` (stride **3**). |
| 7324 | `MS_INSTRUMENT_UPDATE_INFO` | 80 | 79 | +1 | **Trailing** pad at 79 (odd struct → even). |
| 7306 | `PARTICIPANT_UPDATE_INFO` | 84 | 83 | +1 | **Trailing** pad at 83. |
| 7320 / 7210 | `MS_SECURITY_STATUS_UPDATE_INFO` | 462 | 462 | 0 | ✅ 40 + 2 + 35×12. |
| 9010 / 9011 | `MS_BROADCAST_LIMIT_EXCEEDED` | 98 | 98 | 0 | ✅ |
| 6511/6521/6522/6531/6571 | `MS_BCAST_VCT_MSGS` | 320 | 319 | +1 | **Trailing** pad at 319 (after `BroadcastMessage[239]`). |
| 7202 | `MS_TICKER_TRADE_DATA` | 484 | 484 | 0 | ✅ 40 + 2 + 17×26. |
| **7200** | `MS_BCAST_MBO_MBP` | **410** | **409** | **+1** | ⚠ **Interior** pad at offset **17 inside `ST_INTERACTIVE_MBO_DATA`** (after `NetChangeIndicator`) → struct is **236**, not 235. All outer offsets from `RecordBuffer` on shift +1. |
| **7208** | `MS_BCAST_ONLY_MBP` | **470** | **468** | **+2** | ⚠ **Interior** pad at offset **17 in each record** → `INTERACTIVE_ONLY_MBP_DATA` stride is **214**, not 213. `40 + 2 + 2×214 = 470`. |
| 7201 | `MS_BCAST_INQ_RESP_2` | 472 | 472 | 0 | ✅ 40 + 2 + 5×86. |
| 7213 | `MS_ASSET_DATA` | 76 | 76 | 0 | ✅ |
| 7214 | `MBP_INFO` | 262 | 262 | 0 | ✅ |
| 7211 | `MS_SPD_MKT_INFO` | 204 | 204 | 0 | ✅ |
| 6503 | `MS_ASSET_UPDT_INT_RATE_INFO` | 52 | 52 | 0 | ✅ `LONG` at odd-for-4 offset 46 — pack(2) confirmed. |
| 7220 | `MS_BCAST_LIMIT_PRICE_PROTECTION_RANGE` | 344 | 344 | 0 | ✅ 40 + 4 + 25×12. |

**Summary:** only **7200 and 7208** carry interior padding — and both for the same reason, a `CHAR`
`NetChangeIndicator` at offset 16 of a record. Three structures carry a single trailing pad byte.
The other ten are exact as printed.

---

## Part VI — Implementation checklist

**Framing**

- [ ] UDP read buffer is **larger than 516 bytes**, and an oversized datagram is logged, not silently
      truncated (§3.6).
- [ ] `iNoPackets` sub-packets are processed **in FIFO order**, mixing compressed and uncompressed
      freely (§3.2).
- [ ] `iCompLen == 0` → uncompressed; `> 0` → LZO1Z, decompressing exactly `iCompLen` bytes (§3.3).
- [ ] Uncompressed sub-packet length is derived from `BCAST_HEADER.MessageLength`, and
      **`MessageLength < 40` is rejected** (§3.3).
- [ ] The **8-byte prefix** is skipped in exactly one place, **after** decompression for compressed
      packets (§3.4).
- [ ] Payload **byte 0 is checked against the segment netId** (8 for commodity) as a frame-alignment
      oracle — logged, never used to drop data (§3.4).
- [ ] Decompression is **bounds-checked** and a malformed packet is contained, not fatal (§3.5).

**Decoding**

- [ ] All multi-byte fields read **big-endian** (§2.1).
- [ ] `LONG` is bound to a **4-byte signed** type, not the host `long` (§2.7).
- [ ] Padding is read explicitly (an `r.pad("name", n)`-style call), not skipped silently — so the
      `-debug`/trace offsets keep lining up 1:1 with a hex dump (§2.2).
- [ ] The **correct header** is used per message: `BCAST_HEADER` for most, **`MESSAGE_HEADER` for
      6503 and 6541** (§2.8).
- [ ] `ErrorCode` is checked before dispatch; non-zero ⇒ `MS_ERROR_RESPONSE` (§2.10).
- [ ] Dispatch is on `TransactionCode` at header offset **10** (payload offset **18**) (§2.8).
- [ ] Every `NoOfRecords` / `NumberOfRecords` / `MsgCount` is **clamped to the array bound and used
      to limit parsing** — never the array length (§4.4b, §4.8, §4.16).
- [ ] Bit fields are decoded per the **big-endian** listing (§2.9, §4.14).
- [ ] Blank-padded strings are right-trimmed; no NULL terminator is assumed (§2.6).

**Semantics**

- [ ] Prices are divided by `10^precision`; trade values are rounded to **2 dp** and treated as
      always positive (§2.4).
- [ ] Times are converted from **seconds since 1 Jan 1980** (§2.5).
- [ ] 7200 messages with `BookType` other than `1` or `2` are **skipped** (§4.7).
- [ ] `NetPriceChangeFromClosingPrice` is treated as the **closing price**, and net change is
      computed as `((closing − LTP) / closing) × 100` (§4.7).
- [ ] `BbTotalBuyFlag` / `BbTotalSellFlag` are ignored — always zero (§4.8).
- [ ] The handler supports **7200 or 7208, whichever the exchange sends** — not both, not a fixed
      choice (§4.8a).
- [ ] Loss of the **6541 heartbeat** (default 9 s, exchange-configurable) is alarmed as circuit-down
      (§4.12).
- [ ] There is **no retransmission** — a gap in `BCSeqNo` means permanently lost data and must be
      surfaced (§1, §2.8).

---

## Appendix A — Constants quick reference

**Segment netId:** Equity 4 · Equity Derivatives 2 · Currency Derivatives 6 · **Commodity
Derivatives 8**

**Compressed transaction codes (exhaustive):** 7200, 7201, 7202, 7208, 7213, 7215, 7216, 7220

**Trading status:** `1` Pre-open · `2` Open · `3` Suspended · `4` Pre-open extended ·
`5` Stock Open With Market *(7305 only)*

**Market type:** `1` Normal · `2` Odd Lot · `3` Spot · `4` Auction

**Book type:** `1` RL · `2` ST · `5` Odd Lot · `6` SP · `7` AU — *7200: process only `1` and `2`*

**Flags:** `DeleteFlag` `Y`/`N` · `ParticipantStatus` `S`/`A` · `PermittedToTrade` `0`/`1` ·
`NetChangeIndicator` `+`/`-`

**Sizes:** `BCAST_HEADER` / `MESSAGE_HEADER` / `INNER_MESSAGE_HEADER` = 40 · broadcast data prefix =
8 · `BcastPackData` ≤ 516 (`cPackData` ≤ 512) · `MS_ERROR_RESPONSE` = 182

**Time:** epoch = 1 Jan 1980 00:00:00 (Unix offset 315 532 800 s) · 1 s = 65 536 jiffies

---

## Appendix B — Errata found in the source specification

Each of these is a place where the PDF is wrong, self-contradictory, or silent. They are resolved in
the body of this document; they are collected here so a reader comparing against the PDF is not
confused by the difference.

1. **§4.8, `INTERACTIVE_ONLY_MBP_DATA` (7208)** — the offset column is unpadded and the declared
   213-byte size ignores pack(2). True stride **214**; offsets from `NetPriceChangeFromClosingPrice`
   onward are each **1 too low**. Only 214 reconciles with the declared 470-byte message.
2. **§4.7, `ST_INTERACTIVE_MBO_DATA` (7200)** — same defect. True size **236**, not 235; the outer
   table's offsets from `Record Buffer` (275) onward are each 1 too low. Only 236 reconciles with the
   declared 410.
3. **§4.5, `MS_BCAST_VCT_MSGS`** — the structure table's Transaction Code cell wrongly reads
   `BCAST_TURNOVER_EXCEEDED (9010)` / `BROADCAST_BROKER_REACTIVATED (9011)`, copy-pasted from
   `MS_BROADCAST_LIMIT_EXCEEDED`. The correct codes are 6511 / 6521 / 6522 / 6531 / 6571.
4. **§4.11, `MBP_INFO` (7214)** — the field-description prose lists **MBPSells before MBPbuys**,
   contradicting the offset table (`MbpBuy` @54, `MbpSell` @154). The offset table is correct.
5. **§2.2 "word aligned"** — never defined. It means **2 bytes**, proven by the `LONG` at offset 46 in
   6503 and the `SHORT` at 78 in `MS_BCAST_VCT_MSGS`.
6. **§3.3 uncompressed sub-packet length** — never stated. It must be derived from
   `BCAST_HEADER.MessageLength`, which is why `MessageLength < 40` has to be rejected.
7. **§4.4c / §4.4a / §4.5** — `MS_INSTRUMENT_UPDATE_INFO` (79), `PARTICIPANT_UPDATE_INFO` (83) and
   `MS_BCAST_VCT_MSGS` (319) each sum to one byte short of their declared length. The gap is a
   **trailing** pad byte, per the odd-struct rule.
8. **§4.14, `ST_SEC_ELIGIBILITY_PER_MARKET`** — a **3-byte** (odd) structure whose 4-element array
   nevertheless occupies 12 bytes. It is the documented exception to "odd structs are padded to
   even"; 7305's exact 294-byte total depends on it.
9. **§3.5 build instructions** — the Visual C++ 6.0 / `C:\lzo1.07` / `lzo.lib` procedure is a dated
   sample, not a normative requirement. Any correct LZO1Z decompressor conforms.
10. **§4.7 `NetPriceChangeFromClosingPrice`** — the field name contradicts its own description: it
    presently carries the **closing price**, and the net change must be computed by the front end.
