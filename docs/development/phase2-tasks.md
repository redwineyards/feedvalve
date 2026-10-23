# Phase 2 — The Receive and Decode Path

**Status:** ready to start. No open question blocks the critical path; three (OQ-W1, OQ-W2, OQ-L1)
affect stored *values* and are flagged per task.
**Source of truth:** `docs/lld/01-receive-decode.md` (Volume 1), bound to `00-foundations.md` (Vol 0).
**Gate authority:** `docs/lld/04-lifecycle-ops.md` §7.1–§7.3.
**Builds on:** Phase 1, delivered — `develop` at the P1-11 merge.

---

## 1. What Phase 2 is

> Everything between the kernel handing us a datagram and a fully-stamped `model.UpdateUnit`
> landing in `shardCh[k]`. The feed is read, framed, decompressed, resolved, gap-detected,
> decoded and fanned out — **and nothing consumes the units yet.**
> No `Entry` is merged. No row is written. No Oracle session is opened on the packet path.

Phase 1 built the contract and a lifecycle that binds a socket, drains QD and discards. Phase 2
fills the seam Phase 1 left at `Deps.Decode` — `internal/app/graph.go:101` names it. Where the tree
stands today:

| Package | Today | Phase 2 adds |
|---|---|---|
| `internal/lzo` | `doc.go` only | all of it (P2-01) |
| `internal/decode` | `doc.go` only | all of it (P2-05, P2-06) |
| `internal/wire` | `const.go` — the protocol constants, gated by **G-BLD-1** | the prefix, the netId oracle, header resolution (P2-02) |
| `internal/framing` | QD: `ring.go`, `consume.go`, `budget.go` — the ring, built and property-tested in P1-08 | the envelope split that *fills* it (P2-03) |
| `internal/policy` | `doc.go` only; the class table has been config-loaded since P1-05 | first consumption, at fan-out (P2-08) |

This is the phase where the project first touches untrusted input. Every rejection path in Volume 1
exists because a malformed datagram must cost one sub-packet, or one datagram, and never the
process.

### What Phase 2 is *not*

| Deferred to | Work |
|---|---|
| **Phase 3** | shards, arenas, the accumulating/cohort rings, the merge, the epoch state machine, digest suppression, the ordering guard, quarantine |
| **Phase 4** | the writer pool, `MERGE` assembly, array binds, the two-phase execute, the Oracle error taxonomy, DDL |
| **Phase 5** | SIGHUP reload, the liveness and market-session state machines, the runbook, the remaining gates |

`shardCh[k]` is Phase 2's output boundary and Phase 3's input. Until Phase 3, the units are drained
by a no-op consumer — which is also exactly what **G-ALLOC-1** measures against, so the throwaway
consumer is test infrastructure, not scaffolding to delete.

> **If a `*sql.DB`, an `Entry` or a `Batch` appears in a Phase 2 branch, stop.** Vol 0 §1.3's import
> rule makes most of it a compile error, and **G-DEP-1** (green since P1-02) makes the rest one.

---

## 2. What the contract says to build

Counts taken from the volumes by command, not from memory:

| Quantity | Count | Where |
|---|---|---|
| Packages completed by this phase | **4** — `wire`, `lzo`, `framing`, `decode` | Vol 1 §1.1 |
| Packages extended | **2** — the read-loop half of `app`, the table-provider half of `destinations` | Vol 1 §1.1, §10.2 |
| Transcodes with a decoder | **9** | Vol 1 §9.1 |
| Transcodes recognised but not decoded | **2** (6541, 6503) | Vol 1 §9.6 |
| Hand-off boundaries out of the volume | **3** — Q1, liveness gauge, the fatal error | Vol 1 §1.2 |
| Gates owned by Volume 1 | **21** (Vol 4 §7 declared 24 until the Phase 3 errata; resolved), of which **16 are new here** | Vol 4 §7 |
| LLD ADRs raised by Volume 1 | **7** — ADR-L06 … ADR-L12 | Vol 1 §13 |

### The decoder inventory (Vol 1 §9.1)

| Code | Structure | Declared | Compressed | Dest | `MaxUnits` | Unit scope |
|---|---|---|---|---|---|---|
| **7208** | `MS_BCAST_ONLY_MBP` | 470 | **yes** | 0 `MD_TOKEN_QUOTE` | 2 | token |
| **7200** | `MS_BCAST_MBO_MBP` | 410 | **yes** | 0 `MD_TOKEN_QUOTE` | 1 | token |
| **7305** | `MS_SECURITY_UPDATE_INFO` | 294 | no | 1 `MD_SECURITY_MASTER` | 1 | token |
| **7340** | `MS_SECURITY_UPDATE_INFO` | 294 | no | 1 `MD_SECURITY_MASTER` | 1 | token |
| **6511 6521 6522 6531 6571** | `MS_BCAST_VCT_MSGS` | 320 | no | 2 `MD_MARKET_STATUS` | 1 | **general** |
| **6541** | `MESSAGE_HEADER` only | 40 | no | — | 0 | — |
| **6503** | `MS_ASSET_UPDT_INT_RATE_INFO` | 52 | no | — | 0 | — |

Everything else the bible catalogues — 7201, 7202, 7206, 7210, 7211, 7213, 7214, 7220, 7306, 7320,
7324, 9010, 9011, and Chapter 8's 7215/7216 — is **out of scope and must stay out**: resolved as
`BCAST_HEADER`, gap-detected, counted as `unknown_transcode{code}`, framing unaffected (ADR-011).
That counter is the mechanism for closing the inventory later; a decoder added speculatively now is
a decoder for a schema OQ-13 has not chosen.

### Gates new in Phase 2

| Gate | Layer | What it holds |
|---|---|---|
| **G-LAY-1** | build | every decoder's pack(2) arithmetic reconciles with its declared packet length — *the ninth build-time gate, deliberately left unconstrained by Phase 1* |
| **G-SC-2** | config-load | every configured transcode resolves to a registered decoder (V-T1); a config declaring **both** 7200 and 7208 is **accepted** |
| **G-HDR-1/2** | test | header-kind resolution, and `MsgLen` against payload length |
| **G-FRM-1…8** | test | mixed sub-packet FIFO, `MessageLength < 40`, oversize datagram, truncated LZO stream, netId mismatch, `NoOfRecords` over bound, non-zero `ErrorCode`, decoder panic |
| **G-ORD-4** | test | `BCSeqNo` comparison survives the `int32` wrap |
| **G-ALLOC-1** | test | **zero allocations** across the whole receive iteration |
| **G-FIX-1** | test | 7208 decoded against an *independently written* absolute-offset table |
| **G-DIFF-1** | test | field-by-field diff against `poc`'s `-debug` trace — **the exit criterion** |

Already green from Phase 1 and must stay green: G-BLD-1, G-DEP-1, G-DEP-3, G-PTR-1, G-AIX-1.

> **A count to reconcile before anyone audits against it.** Vol 4 §7 says Volume 1 owns **24** gates,
> but the list it gives on the same line enumerates **21** distinct ids: G-LAY-1, G-BLD-1, G-DEP-1/3,
> G-SC-2, G-ALLOC-1, G-PTR-1, G-HDR-1/2, G-FRM-1…8, G-ORD-4, G-DIFF-1, G-FIX-1, G-AIX-1. The
> shortfall is probably G-AIX-1 being one suite of five assertions, but the volume does not say so.
> **The 16 above is derived from the enumeration, not from the headline 24** — the enumeration is
> the auditable artefact. Resolve the three before Phase 2 sign-off, the way P1-07 resolved §8.2's
> "129" into 131 distinct instruments across 129 rows.

---

## 3. Dependency graph

```
P2-01 lzo (standalone)  ─┐
P2-02 wire (standalone) ─┤
                         ├─→ P2-03 framing: the envelope split
P2-04 read loop ─────────┘         │
      (closes seam 2)              ↓
                         P2-05 decode: registry, dispatch, FieldReader
                                   ├─→ P2-06 the nine decoders ──┐
                                   └─→ P2-07 gap detection       │
                                                 │               ↓
                                                 └──→ P2-08 fan-out + stall budget
                                                             └─→ P2-09 allocation discipline
                                                                   └─→ P2-10 differential validation
```

**Critical path:** 01/02 → 03 → 05 → 06 → 08 → 09 → 10.
**Parallelisable from day one:** P2-01 (`lzo`) and P2-02 (`wire`) share nothing but `model`, and
P2-04 touches only `app`. P2-07 needs the resolved header from P2-05 but none of the decoders.

---

## 4. The tasks

### P2-01 · `internal/lzo` — LZO1Z decompression

**Scope.** Vol 1 §5. A pure-Go, bounds-checked LZO1Z decompressor, ported from `poc/decompress.go`.

- **`DecompressSafe` only on the packet path.** Compressed sub-packets are untrusted network input;
  the unchecked variant exists for tests over known-good fixtures and nowhere else.
- **Buffer ownership** (§5.2): one `decompBuf`, allocated before the read loop, owned by the
  receiver, reused per sub-packet. Never per-packet allocation.
- **Sizing `decompBuf` at 2048** (§5.3) — and what that number is *not*: it is ASM-14's assumed
  bound on the decompressed size of the compressed transcodes, not a protocol maximum.
- **The four distinguishable failure reasons** (§5.4), each its own
  `decompress_failed{reason}` slot — a single lumped counter cannot tell a truncated stream from a
  corrupt opcode, which are different feed problems.
- **The panic boundary** (§5.5) and what must sit *outside* it (ADR-L10).
- **The decompressed-length cross-check** (§5.7).

**Done when.** The frozen fixtures in `poc/testdata/lzo_fixtures.json` round-trip; a truncated stream
is **rejected, not silently short-decoded** (G-FRM-4); a malformed opcode produces an error rather
than a panic escaping the package; a benchmark shows zero allocations per decompression.

**Gates.** G-FRM-4; G-DEP-3 must stay vacuous. **Depends on.** — **Rough:** 2 days.

> **The fixtures are the oracle and they stay frozen.** `github.com/rhnvrm/lzo1z` was removed from
> the module and its output frozen, keyed by SHA-256 of the input. A decompressor checked only
> against a compressor from the same codebase agrees with itself even when both are wrong. Do not
> reintroduce the dependency to "regenerate" anything that is not a genuinely new input.

---

### P2-02 · `internal/wire` — the 8-byte prefix and header resolution

**Scope.** Vol 1 §6, the single most error-prone 48 bytes in the system.

- **The prefix handled in exactly one place** (§6.1). Byte 0 repeats the netId, bytes 1–7 are
  ignored, the header starts at the 9th byte — *after* decompression for compressed packets.
- **The netId oracle** (§6.2): a mismatch is logged, never dropped, and on sub-packet 2+ it is the
  cheapest available signal that the previous length was computed wrongly.
- **The two header layouts side by side** (§6.3) and **the resolution rule** (§6.4).
- **Parsing the resolved header** (§6.6), the `MsgLen` checks after the parse (§6.7), and the
  `ErrorCode != 0` path (§6.8).

**Done when.** A 6541 payload (8 + 40) resolves as `MESSAGE_HEADER` and a 7208 payload (8 + 470) as
`BCAST_HEADER` (G-HDR-1); payloads at 60 / 48 / 478 bytes each pass their `MsgLen` check and a
mismatch is rejected (G-HDR-2); a payload whose byte 0 is 4 against an envelope netId of 8 **warns
and continues** (G-FRM-5); a non-zero `ErrorCode` takes its specified path (G-FRM-7).

**Gates.** G-HDR-1, G-HDR-2, G-FRM-5, G-FRM-7. **Depends on.** — **Rough:** 2–3 days.

> **`MsgHeader.LogTime` is at a different offset in each layout: 4 in `BCAST_HEADER`, 2 in
> `MESSAGE_HEADER`** (bible §2.8; Vol 0 said "both layouts" and Vol 1's V-V1 corrected it). Read at
> one fixed offset, the other layout yields `AlphaChar` — two characters of `Symbol` construed as a
> timestamp, and that value feeds the ordering guard in Phase 3. This is the reconciliation most
> likely to be implemented from the stale text.

> **Resolution must precede both the gap detector and the registry** (§6.5). Not a preference: the
> detector needs `BCSeqNo`, which only one layout has.

---

### P2-03 · `internal/framing` — the envelope split

**Scope.** Vol 1 §4. QD already exists (P1-08); this is the half that fills it from a datagram.

- **The nesting with byte offsets** (§4.1) and **the loop as cursor arithmetic** (§4.2):
  `cNetId`, `iNoPackets`, then length-prefixed sub-packets in FIFO order.
- **`MessageLength < 40` costs the rest of the datagram, and nothing less** (§4.3) — accepting one
  advances the cursor too little and misframes every later sub-packet in the same buffer.
- **The zero-length sub-packet and the zero-advance guard** (§4.4) — the infinite-loop case.
- **Residual bytes after the last sub-packet** (§4.5).
- `packetLen == 0` means uncompressed, length from the header's `MessageLength`; anything else is
  LZO1Z. **7305 is never compressed.**
- **ADR-L06:** read `MessageLength` directly at payload offset 46 during framing rather than parsing
  a header first — so framing does not depend on `wire`.

**Done when.** The §4.6 worked datagram — three sub-packets, compressed/uncompressed/compressed,
both header kinds — frames exactly, in order (G-FRM-1); an uncompressed sub-packet declaring
`MessageLength = 39` is rejected and the rest of the datagram abandoned (G-FRM-2); a zero-length
sub-packet terminates rather than spinning.

**Gates.** G-FRM-1, G-FRM-2. **Depends on.** P2-01. **Rough:** 3 days.

---

### P2-04 · The read loop, the source check, and the oversize decision

**Scope.** Vol 1 §3. Extends what P1-09 built rather than replacing it.

- **`net.ListenUDP`, not `net.ListenPacket`** (§3.1), and the 64 KiB read buffer (§3.2) — sized so an
  oversized datagram is *distinguishable* from a full one, which an exactly-sized buffer cannot be.
- **Socket options and reading back what the kernel actually gave us** (§3.3) — delivered in P1-09
  via `readBufferSize`; this task consumes the effective value rather than the requested one.
- **The source address check** (§3.5), against `udp.sourceAllowList` / `sourceEnforce`.
- **Read errors that are not shutdown** (§3.7), and **the oversized datagram** (§3.8).

**Done when.** A 600-byte datagram is handled per §3.8 and counted, **loudly** (G-FRM-3); a datagram
from a non-allowed source is handled per `sourceEnforce`; a read error with `ctx.Err() != nil` is
shutdown and not a failure.

**Gates.** G-FRM-3. **Depends on.** — **Rough:** 2 days.

> **This card closes open seam 2, carried from Phase 1.** Vol 4 F6 wants a 64 KB read buffer so
> oversize is distinguishable; Vol 0 §9.5.2 sizes `Datagram.Buf` at 1024 "so an oversize datagram is
> carried intact and diagnosable". A datagram between 1024 B and 64 KB is therefore truncated on the
> copy into QD. **Decide it here and write the decision down.**

> **Two defects in the Phase 1 code are this card's to close**, both found by the `/simplify` review
> and deliberately left for the phase that owns the policy:
> - `internal/app/lifecycle.go` truncates an oversize datagram **silently** at the only point where
>   the true length still exists. `obs.IDDatagramsOversize` and `obs.ClassOversizeDatagram` are
>   declared and unused — they are waiting for this task.
> - The read loop's `n > len(d.Buf)` guard is correct and must stay; what is missing is the counter
>   and the WARN beside it.

---

### P2-05 · `internal/decode` — registry, dispatch and the `FieldReader`

**Scope.** Vol 1 §8.

- **Two levels: "we can decode this" and "we are configured to"** (§8.1). The distinction is what
  makes `unknown_transcode{code}` meaningful rather than a configuration echo.
- **Registration and construction** (§8.2) — data-driven, one file per code, so adding a transcode is
  a new file plus a config entry and never an edit to a decoder that already works.
- **Dispatch as a linear scan** (§8.3), not a map and not a 65,536-entry array, with the reasoning
  recorded so a future reader does not "optimise" it.
- **The `FieldReader` cursor** (§8.4): sequential, big-endian, debug-traceable, with `pad` an
  explicit logged read rather than a silent cursor bump.
- **ADR-L07:** field identifiers are `uint16` constants, never formatted strings — a formatted
  identifier allocates on the hot path.
- **Layout verification as a build-time gate** (§8.5).
- **V-T1 and V-T6 join the `-selfcheck` rule set**, which P1-06 left declared and unproduced
  (`internal/config/schema.go` marks both "needs the decoder registry (Phase 2)").

**Done when.** One constant expression per decoder in the `mbpLayoutCheck` style compiles only when
the stride arithmetic is exact (G-LAY-1); `-selfcheck` reports every configured transcode resolving
to a registered decoder, and **accepts** a config declaring both 7200 and 7208 (G-SC-2); a decoder
rigged to panic is contained and counted as `panics_recovered{decode}` (G-FRM-8).

**Gates.** G-LAY-1, G-SC-2, G-FRM-8. **Depends on.** P2-02, P2-03. **Rough:** 3–4 days.

> **The 7200/7208 exclusivity is not ours to resolve.** They are mutually exclusive by NSEIL control
> (bible §4.8a), so which one arrives is the exchange's choice. A build that can only decode one is a
> build that goes blind the day they switch — which is why G-SC-2 asserts a config declaring **both**
> is *accepted*, not rejected.

---

### P2-06 · The nine decoders

**Scope.** Vol 1 §9.2–§9.6, one file per transcode.

| Section | Codes | The thing that will bite |
|---|---|---|
| §9.2 | **7208** | the **214-byte stride**, not 213: `NetChangeIndicator` is the only odd-sized member, so one pad byte sits at offset 17 and 40 + 2 + 2×214 = 470 |
| §9.3 | **7200** | **ADR-L08** — its 10-byte ladder element re-packed into destination 0's 12-byte layout |
| §9.4 | **7305, 7340** | one structure, two codes; never compressed; `ST_SEC_ELIGIBILITY_PER_MARKET` is an **unpadded** 3 bytes and 294 is exact, so adding alignment here is the error |
| §9.5 | **6511 6521 6522 6531 6571** | five codes, one structure, destination 2, **general shard** — and five contributors, which is why `maxContributors` is 8 and not 4 |
| §9.6 | **6541, 6503** | recognised, **not** decoded: header-only and `MaxUnits = 0` |
| §9.7 | — | what the decoders deliberately do **not** do |

**Done when.** Each decoder's layout constant compiles (G-LAY-1); a 7208 message with
`NoOfRecords = 5` against `mbpMaxRecords = 2` is **clamped and warned**, never parsed into a
fabricated second security (G-FRM-6); 7208 decodes field-for-field against an independently written
absolute-offset table (G-FIX-1); a negative price decodes as negative (prices may be negative,
bible §2.4).

**Gates.** G-LAY-1 (per decoder), G-FRM-6, G-FIX-1. **Depends on.** P2-05. **Rough:** 5–6 days.

> **G-FIX-1 exists because of a real bug and the gate must be built the hard way.** The offset table
> must **not** be produced by any builder that walks the parser's own field order — a fixture builder
> that shares the parser's assumptions agrees with it even when both are wrong, which is exactly how
> `poc`'s 213-byte stride survived. Write the offsets out by hand from Part V.

> **Three open questions change stored values, not structure.** **OQ-W1** — per-contract price
> precision has no broadcast field, and getting it wrong is a factor of 10 or 100. **OQ-W2** — is the
> 1980 epoch UTC- or IST-anchored? **OQ-L1** — is `closing_price` at record offset 198 or 18? Decode
> what the bible says, record the assumption at the call site, and make each one fail loudly rather
> than plausibly.

---

### P2-07 · Gap detection

**Scope.** Vol 1 §7.

- **What it is for and what it cannot do** (§7.1) — it detects, it does not recover.
- **Which messages the detector sees, and which it must not** (§7.2).
- **The comparison rule** (§7.3): `int32(a-b) > 0`, **never** `a > b`.
- **First packet, restart, duplicate, regression, and the resync rule** (§7.5), with **ADR-L12**: an
  implausibly large `BCSeqNo` jump is a resynchronisation, not a gap of that magnitude.
- **Unknown BCAST transcodes are gap-detected** (§7.6) — deliberate, not an oversight: they share
  the sequence space.

**Done when.** `LastSeq` at `int32 0x7FFFFFFF` followed by `BCSeqNo = 0x80000000` is treated as
**newer** (G-ORD-4); first packet, duplicate, regression and resync each take their specified path;
a gap emits what §7.4 specifies and nothing more.

**Gates.** G-ORD-4. **Depends on.** P2-05. **Rough:** 2 days.

> **A 4-byte signed counter at 10,000 messages/s wraps in ~59 hours**, so the wrap is reachable
> inside a long session. Written as `a > b`, every affected row freezes permanently after it.

---

### P2-08 · Fan-out, shard selection and the stall budget

**Scope.** Vol 1 §10 — the output boundary.

- **One decoded message becomes *n* `UpdateUnit`s** (§10.1), through `fanoutBuf`, which stays at
  **40 units** even though the Phase 2 maximum is 2 (Vol 0 §9.1).
- **Stamping the unit** (§10.2) and where the tables come from — the table-provider half of
  `destinations`, plus `internal/policy`'s class table, which has been loaded since P1-05 and is
  consumed here for the first time.
- **Both clocks on every unit** (§10.3): `LogTime` and `RecvMono`.
- **Shard selection** (§10.4), and the **general shard** for destination 2's market-wide codes.
- **The send discipline, the stall budget, and the fatal path** (§10.5).
- **Expiry is fatal. There is no drop path.** (§10.6.)

**Done when.** A message fanning out to *n* units delivers all *n* in order; a full `shardCh[k]`
spins then blocks against the budget and goes **fatal** on expiry, with the reason, the shard id and
the counter snapshot; `qd_send_blocked` counts publishes that had to **wait** and nothing else.

**Depends on.** P2-06, P2-07. **Rough:** 3 days.

> **This card closes the most serious defect the `/simplify` review found.** The Phase 1 receive loop
> currently does `if !g.qdProducer.TryPublish(&d) { qdSendBlocked.Add(1) }` — it **silently discards
> the datagram**, contradicting req §10.2 ("FeedValve discards no unit anywhere"), which
> `internal/framing/budget_test.go` quotes verbatim as "There is no drop path". It also misreports
> the one counter an operator reads for decoder stall. `Producer.PublishWithin`,
> `Config.StallBudget()` and `Graph.Fatal` all exist, were built in P1-08/P1-09, and have **no
> production caller** — the full-ring policy was reimplemented, worse, at the call site. Replace the
> call site with the primitives; do not extend the call site.

---

### P2-09 · Allocation discipline and the zero-allocation gate

**Scope.** Vol 1 §11.

- **Why the rule is absolute rather than a preference** (§11.1).
- **The inventory of what is pre-allocated and who owns it** (§11.2).
- **`active_tokens_1s` as a generation-stamped open-addressed set, not a cleared map** (§11.3,
  **ADR-L09**).
- **Logging without allocating on the suppressed path** (§11.5) — the common case is suppression, so
  the arguments must not be formatted to be thrown away.

**Done when.** `testing.AllocsPerRun(1000, …)` over **the whole receive iteration** — read, envelope
split, FIFO loop, decompress, prefix skip, header resolve, dispatch, decode, fan out, gap-detect,
active-token update — reports **zero** (G-ALLOC-1).

**Gates.** G-ALLOC-1. **Depends on.** P2-08. **Rough:** 2–3 days.

> **Run this gate early and often, not at the end.** It is far cheaper to keep at zero than to get
> back to zero: a single `fmt.Sprintf` on a WARN path, or one `[]byte`→`string` conversion, is enough
> to fail it, and finding which of ten tasks introduced it is work that need not exist.

---

### P2-10 · Differential validation against `poc` — *the exit criterion*

**Scope.** Vol 1 §8.6 and **G-DIFF-1**. Run `poc` as a **subprocess** with `-debug`, run the new
decoder's equivalent trace, and diff the two field streams.

**Done when.** For every supported transcode — with `NoOfRecords` at 0, 1 and above the array bound,
a compressed and an uncompressed sub-packet in one datagram, and a record with a **negative** price
— the two traces agree on **field name, absolute offset range, hex and decoded value for every
field.**

**Depends on.** All. **Rough:** 3 days.

> **This is the gate against the worst failure in the system** (HLD R1): plausible values in the
> wrong fields, silent in Oracle until somebody reconciles a position months later. It works because
> `poc` is an *independent* implementation of the same spec — so the oracle is its **output**.
> req §7 both mandates validating against `poc` and forbids reusing its code: **no `poc` code is
> linked, imported or copied** (ADR-012). Run it as a process and diff text.

> **`poc` must stay buildable for this.** It is in the module and its tests pass today; nothing in
> Phase 2 may break it, and `internal/` must not import it.

---

## 5. Effort and sequencing

| | |
|---|---|
| **Single developer** | ~27–33 working days |
| **Two developers** | ~16–20 working days — one takes 02 → 03 → 05 → 06, the other takes 01, 04, 07, then 09 |

The spine is `wire` → `framing` → `decode` → the decoders, and it is longer than Phase 1's. P2-06 is
the single biggest card and the one that most rewards undivided attention; P2-01 and P2-04 are the
natural parallel work for a second developer in week one.

---

## 6. Open seams carried into Phase 2

| # | Seam | Resolution |
|---|---|---|
| **1** | **Read buffer vs `Datagram.Buf`** (Phase 1 seam 2). A datagram between 1024 B and 64 KB is truncated on the copy into QD | **P2-04 decides it.** The 1040 B `Datagram` size is not in question — only what the receiver does with an oversize frame before enqueueing |
| **2** | **G-SC-2's decoder half** (Phase 1 seam 3) | **P2-05.** The config-side half landed in P1-06; V-T1 and V-T6 are declared and unproduced until the registry exists |
| **3** | **The full-ring drop path** in `internal/app/lifecycle.go` | **P2-08**, which must replace the call-site policy with `PublishWithin` / `StallBudget` / `Fatal` |
| **4** | **Silent oversize truncation** in the read loop | **P2-04**, with `datagrams_oversize` and `ClassOversizeDatagram` |
| **5** | **OQ-V5 — unicast or multicast?** Multicast needs a group join, and `golang.org/x/sys` is excluded on AIX grounds | **Blocks P2-04's socket setup if the answer is multicast.** Ask now; everything else in P2-04 is unaffected |
| **6** | **OQ-W1 / OQ-W2 / OQ-L1** — price precision, the 1980 epoch anchor, `closing_price`'s offset | **P2-06 proceeds** on the bible as written, with each assumption recorded at the call site and made to fail loudly |

Two defects found during the `/simplify` pass are **not** Phase 2's: `clock_step_detected` comparing
two monotonic readings, and `obs.Logger.SetLevel` writing a plain field read by every logging
goroutine. Both belong to Vol 4 and land with Phase 5's reload work. They are recorded here only so
nobody adopts them as Phase 2 scope.

---

## 7. Exit criteria, restated as a checklist

- [ ] `go build ./...`, `go vet ./...`, `go test ./...` all green on the dev host
- [ ] `go test -race ./internal/...` green (**`CC` pointed at clang** on the Windows dev host — the
      installed mingw64 gcc has a broken preprocessor driver; diagnosed in P1-08)
- [ ] `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./...` green; still zero cgo
- [ ] **All 9** of §7.1's build-time gates now constrain something — **G-LAY-1 included**, which
      Phase 1 could not exercise because it had no decoders
- [ ] **G-SC-2** green: every configured transcode resolves, and a config with both 7200 and 7208 is
      accepted
- [ ] **G-FRM-1…8**, **G-HDR-1/2**, **G-ORD-4**, **G-FIX-1** green
- [ ] **G-ALLOC-1** reports **zero** allocations across the whole receive iteration
- [ ] **G-DIFF-1** green against `poc` for every supported transcode, negative prices included
- [ ] One malformed datagram costs one sub-packet or one datagram — **never the process**: the panic
      boundary is exercised by a rigged decoder (G-FRM-8)
- [ ] A full `shardCh[k]` goes **fatal** on budget expiry; `grep -r TryPublish internal/app` shows no
      bare drop path
- [ ] No `Entry`, `Batch`, `database/sql` or `oradb` import anywhere in `wire`, `lzo`, `framing`,
      `decode` or `policy` (G-DEP-1)
- [ ] `poc` still builds and its tests still pass; nothing under `internal/` imports it
- [ ] The nine decoders are the **only** nine; `unknown_transcode{code}` is the only thing a tenth
      produces

---

## 8. Provenance

Every count in §2 was taken from the volumes by command at the time of writing. The decoder
inventory is Vol 1 §9.1 verbatim. The gate attribution is Vol 4 §7's "By owning volume" paragraph,
not an inference from gate names; **Volume 1 itself names no gate ids at all**, which is why that
paragraph is the authority rather than a convenience.

The gate arithmetic, spelled out because it does not close: that paragraph says **24** and
enumerates **21** distinct ids. Subtracting the five already green from Phase 1 (G-BLD-1, G-DEP-1,
G-DEP-3, G-PTR-1, G-AIX-1) leaves the **16 new** counted in §2 — derived from the enumeration. Had
it been derived from the headline figure it would read 19, and a reviewer auditing Phase 2 against
"19 new gates" would conclude three were missing.

**Resolved** (Phase 3 errata doc commit): Vol 4 §7 now declares Vol 1 **21**, Vol 2 26, Vol 3 7 and
Vol 4 38 — each equal to its enumeration — summing to 92 against eighty-nine distinct gates, the
difference being G-PTR-1 and G-ROW-1/2, each credited to two volumes.

Seams 3 and 4 of §6 are not from the LLD: they are defects in the merged Phase 1 code, found by the
`/simplify` review of `internal/`, reported rather than fixed because both require a behaviour
decision that belongs to the phase that owns the policy.
