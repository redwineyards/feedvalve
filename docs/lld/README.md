# FeedValve — Phase 1 Low-Level Design

Five volumes, 13,700 lines, detailing `../architecture/phase1_hld_design1.md` to the point where it
can be implemented without re-deciding anything.

The HLD is authoritative on *what* the system does. These volumes are authoritative on *how*, and
where they found the HLD wrong they say so in a **"Deltas against the HLD"** section rather than
diverging silently. Twenty-nine such deltas exist; the ones that are genuine defects rather than
refinements are listed below.

## The volumes

| # | File | Lines | Owns |
|---|---|---|---|
| 0 | [`00-foundations.md`](00-foundations.md) | 2,487 | **The frozen contract.** Package tree and import direction, every cross-boundary type with byte-exact layouts, the `Entry.Image` field maps, the digest, `ColMask` semantics, the complete config schema, the instrument ledger, the concurrency contract |
| 1 | [`01-receive-decode.md`](01-receive-decode.md) | 3,097 | Socket, envelope framing, LZO1Z, the 8-byte prefix, header resolution, gap detection, the decoder registry, per-transcode decoders, fan-out, the stall budget |
| 2 | [`02-shard-mirror.md`](02-shard-mirror.md) | 3,222 | The shard main loop, the flush wakeup, the merge, the ordering guard, the epoch/flush state machine, the pump, the completion path, quarantine |
| 3 | [`03-writer-persistence.md`](03-writer-persistence.md) | 2,430 | Writer pool, `MERGE` text and binds, array binds and per-row attribution, transactions, the Oracle error taxonomy, retry and pool-stop, DDL |
| 4 | [`04-lifecycle-ops.md`](04-lifecycle-ops.md) | 2,492 | Startup and shutdown ordering, SIGHUP reload, circuit liveness, market session, the failure-mode table, observability, 89 gates, build and deploy |

**Read Volume 0 first.** Volumes 1–4 bind to it and cite it as `Vol 0 §x`. It exists so that four
volumes written in parallel could not each re-derive the `Entry` layout, the config keys and the
counter names slightly differently.

## The six defects that blocked LLD

The HLD's second review found six items that could not be implemented as written. All six are closed.

| | Defect | Closed by |
|---|---|---|
| **L1** | The flush flag has no wakeup, and the failure coincides exactly with the case the non-rate-limited class exists for | Vol 2 §3 — a level (`atomic.Bool`, `Swap(true)`) plus an edge (cap-1 non-blocking `flushWake`), with the loop's stage B reading the level after *every* case so a busy shard never needs the edge |
| **L2** | The shard's main loop is never specified | Vol 2 §2 — five cases plus `ctx.Done()`, two-stage, deliberately no explicit priority (Go's uniform-random choice gives P(starved for *n* passes) = 2⁻ⁿ; expected wait ~0.7 µs against a 5–20 ms commit) |
| **L3** | The per-row outcome has three cases; the completion path has two | Vol 2 §8 — three arms, one per row of Vol 0 §5.4's obligation table, traversed **by destination segment** because `BatchRow` carries `Slot` but not `DestID` |
| **L4** | "Skip the row, continue the batch" has no fallback branch | Vol 2 §9 — quarantine on first rejection, zero retries, release by config reload or by a disjoint-mask merge |
| **L5** | `Entry.Image` has no field map | Vol 0 §4 — per-destination offset/width/type/SQL column/mask bit/NULL encoding, image lengths 181 / 37 / 142 |
| **L6** | No digest algorithm, and no collision argument | Vol 0 §5 — `maphash.Bytes` over `Image[:ImageLen]`, mask as a separate predicate clause, residual collision risk 1.4 × 10⁻⁸ per year |

## Findings that change the design

Ordered by consequence.

1. **A database outage becomes a process crash, through the retry path** (Vol 2, D2-2). HLD §7.3
   types the dirty structure `[]int32` and calls it a ring, but a slice has no O(1) head insertion —
   and the failure path head-inserts, deliberately, so retries stay ahead of newer rows. At 50,000
   rows accumulated and 2,000 failing per completion round, that is ~400 MB of `memmove` ≈ 200 ms of
   shard time per 200 ms epoch. The shard saturates, Q1 backs up, and the stall budget goes fatal.
   Fixed with a real circular buffer: the same trace costs ~8 µs.

2. **`maxContributors = 4` is contradicted by the HLD's own schema.** `MD_MARKET_STATUS` takes five
   contributors (6511/6521/6522/6531/6571, bible §4.5), so §8.2's instantiation fails §10.3's
   `-selfcheck` rule against the constant §7.3 declares. Resolved at 8, with `LastTime` narrowed to
   the 4-byte wire width; `Entry` becomes **424 B**, not 368. Corrected in the HLD.

3. **OQ-4 resolved, and better than feared** (Vol 3, ADR-W01). `go-ora` array-binds `MERGE`
   correctly, but per-row attribution is unreachable: Oracle sends per-element affected counts and
   bind errors, and the driver parses both and discards them (`command.go:891-896`,
   `network/summary_object.go:259-262`), with the fields unexported and no accessor. The HLD assumed
   this collapses the three-outcome path to two. It does not — array-bind in steady state, and on a
   row-data error *only*, re-run that one batch row by row through the same statements. Closes
   Appendix A gap S4 with no driver fork.

4. **`ConnMaxLifetime` does not protect a pinned `*sql.Conn`** (Vol 3, W1). `database/sql` checks
   expiry only on put/get and the cleaner walks only the free list. CLAUDE.md invariant 4 exists
   precisely to stop dead sessions surfacing as `ORA-03113`, and it does not cover the writer
   sessions. Writers release on a hold deadline and on idle instead.

5. **`Tx.StmtContext` silently re-prepares** when `stmt.cg != nil` (Vol 3, W2), so the obvious
   prepared-statement pattern soft-parses on every execution, invisibly.

6. **7200 must ship in Phase 1** (Vol 1, V-D1). 7200 and 7208 are mutually exclusive by NSEIL
   control (bible §4.8a), so which one arrives is not our choice. The HLD treats 7200 only as a
   future revisit trigger.

7. **§10.2's queue thresholds are unimplementable** (Vol 4, D4-3). Q2/Q2c cannot fill by
   construction and Q1 runs ~0.08% occupied, so "warn at 50%" is a death rattle, not an early
   warning, and a depth gauge sampled every 60 s reads zero forever.

8. **A third instrument shape was already in use.** §10.1 declared two shapes and then listed nine
   instruments that are neither. The naming gap concealed a concurrency defect: `dirty_depth` and
   `cohort_depth` are `len()` of a shard-owned slice, so sampling them from the stats goroutine is a
   data race on a target with no race detector. Corrected in the HLD.

## Cross-volume reconciliation

Resolved after the volumes were written. These are binding.

| Collision | Resolution |
|---|---|
| Three panic counters proposed independently — `decode_panic{code}` (Vol 1), `writer_panic` (Vol 3), `panics_recovered{stage}` (Vol 4) | **`panics_recovered{stage}` is canonical**, with `{stage}` ∈ {decode, shard, writer, stats}. The other two are withdrawn: they count the same events under narrower names. The offending transcode is carried on the ERROR line, which Vol 4 already specifies, so the `{code}` label bought nothing a log line did not |
| Destination 0's column count — Vol 0 said 55, Vol 3's A3 said 45 | **45.** Vol 0's own enumeration of the MBP group is `bid_qty/px/ord_1..5` + `ask_qty/px/ord_1..5` = 6 × 5 = 30, not 40; 40 would require keeping `BbBuySellFlag`, which the same section discards. No image byte, mask bit or memory figure moves — only the bind count and the DDL |
| `MsgHeader.LogTime` annotated "both layouts" in Vol 0, corrected by Vol 1's V-V1 | **Different offsets: 4 in `BCAST_HEADER`, 2 in `MESSAGE_HEADER`** (bible §2.8). Read at one fixed offset, the other layout yields `AlphaChar` — two characters of `Symbol` construed as a timestamp, feeding the ordering guard |
| `SEC_INFO.OptionType` at @24 in Vol 0, corrected by Vol 1's V-V2 | **@26.** @24 overlaps `StrikePrice` (22–26) and makes the struct 28 bytes against a declared 30. The four *absolute* offsets Vol 0 derives are unaffected |
| `maxBatchSize` reload direction | **Reloadable in both directions** within a startup-only ceiling (Vol 0 §7.3 C2). Buffers are pre-allocated at the ceiling, so any value within it is safe. Vol 4 correctly bound to Vol 0 over a looser instruction |
| Memory envelope — Vol 0 said 189 MB resident, Vol 2 said 212.4 MB | **212.4 MB.** Vol 2's figure includes quantisation Vol 0's occupied-bytes arithmetic omits: arena chunking, Go map bucket rounding, and pre-sized rings. 0.53% of the 40 GB partition; every HLD §7.8 and §11.3 conclusion survives |

## Open questions, by what they block

**Could change structure**

- **OQ-L2 / OQ-13** — `MS_BCAST_VCT_MSGS` carries a `LONG Token` at offset 40 (bible §4.5) that the
  `market_type`-keyed destination discards. If market status is per-security rather than
  market-wide, the destination *count* changes and destination 2 goes from 6.9 MB to 90 MB. The
  bible does not say which.
- **OQ-V5** — is the broadcast circuit unicast or multicast? Nothing in the bible says, and
  multicast changes the socket setup itself: a group join needs `golang.org/x/sys`, which Vol 0
  excludes on AIX grounds.
- **OQ-L7** — does the supervisor restart on non-zero exit and deliver SIGHUP? Blocks the entire
  exit-code taxonomy *and* the whole reload story: without SIGHUP every "reloadable" key is
  effectively startup-only.

**Block correctness of stored values**

- **OQ-W1** — where does per-contract price precision come from? Bible §2.4 says "the contract's
  precision" but no broadcast field carries it. Fails loudly, by a factor of 10 or 100.
- **OQ-W2** — is the 1980 epoch UTC- or IST-anchored? §2.5's Unix offset reads as UTC; NSE runs IST.
- **OQ-L1** — is `closing_price` at record offset 198 or 18?

**Block only a number**

OQ-W3 (the Oracle spike — four assumptions and `array_bind_rows_max`), OQ-22 (serial attribution
RTT → `flush.maxBatchSize`), OQ-L3 (`maphash` on ppc64; FNV-1a is the fallback), OQ-23 (Go map
bucket layout), OQ-24 (stall warning threshold), OQ-L8 to OQ-L11 (stderr destination, `netstat`
attribution, log collector, AIX version).

## Conventions

- **Signatures and doc comments only** — these are design documents. No function bodies anywhere.
- Every protocol claim cites a section of `../../ncobrd_bible.md`. A protocol assertion without a
  citation is a defect.
- Every assumed number is labelled assumed and carries a way to measure it.
- Every dependency is checked against `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64`.
