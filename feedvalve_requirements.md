# FeedValve — System Requirements Document

**Status:** Issued for design
**Version:** 2.0 (supersedes v1 draft of 2026-09-08)
**Audience:** System architect / designer
**Author:** Product/engineering input (compiled from stakeholder description)
**Date:** 2026-09-09

**Changes in v2:** volume figures corrected and re-derived from a measured bandwidth ceiling;
ordering, coalescing and rate-limiting semantics disambiguated; target platform specified;
observability deferred; horizontal scaling removed from scope; design work split into two phases
(Section 11).

---

## 1. Purpose

FeedValve is a Go service that ingests a stock exchange's broadcast market data feed over UDP,
decodes it, and persists the relevant data to an Oracle database. This document defines the
functional and technical requirements the system must satisfy. It states *what* the system must do
and the constraints it must do it under; the pipeline architecture, concurrency model and module
decomposition are the designer's to propose (Section 11).

**The design effort is split into two phases.** Phase 1 produces high-level designs, from which one
is chosen; Phase 2 produces low-level designs for that chosen option. Deliverables for each are in
Section 11.

## 2. Definitions

These terms are used precisely throughout this document; earlier informal usage caused ambiguity and
is now fixed.

| Term | Meaning |
|---|---|
| **Datagram** | One UDP datagram as received from the socket. Carries a `BcastPackData` envelope: a 2-byte netId, a packet count, then length-prefixed sub-packets. Maximum wire size **516 bytes**. |
| **Packet** | One sub-packet extracted from a datagram, after decompression where applicable. One packet = one decoded message = the unit that produces DB updates. A datagram carries one or more packets. |
| **Transcode** | The transaction code in the broadcast header identifying the message type (e.g. 7208, 7305). |
| **Token** | Numeric identifier of a single derivative contract (future/option). |
| **Token-scoped transcode** | A message carrying data for one or more specific tokens. |
| **General transcode** | A message not associated with any token (market open/close, session-wide announcements). |
| **Coalescing** | Retaining only the newest value for a (token, transcode) and writing that, discarding superseded intermediate values. **Lossless with respect to final state**; lossy with respect to history. Applies to high-frequency snapshot transcodes. |
| **Rate limiting** | Discarding packets outright to cap a rate, without regard to preserving final state. Used in this document only where explicitly stated; it is **not** a synonym for coalescing. |
| **Active token** | A token that has received at least one packet in the current measurement window. Distinct from the *tracked* token universe, most of which is idle at any instant. |

Note that a single packet may update **more than one token** — the 7208 MBP message carries up to
two `INTERACTIVE_ONLY_MBP_DATA` records, each with its own token. Rate figures below are stated in
packets, not in per-token record updates.

## 3. Background / Domain Knowledge

### 3.1 Wire format

- The feed arrives as **unicast UDP** addressed to the host. It is not multicast and not broadcast
  at the IP level, despite the exchange calling it a "broadcast" feed.
- Each datagram bundles one or more length-prefixed sub-packets. A sub-packet with a declared
  packet length of zero is uncompressed and its length comes from the broadcast header's
  `messageLength`; anything else is **LZO1Z**-compressed.
- Only these transcodes are compressed: **7200, 7201, 7202, 7208, 7213, 7215, 7216, 7220**. Notably
  **7305 is never compressed**.
- Every broadcast data payload (after decompression, for compressed packets) begins with an 8-byte
  prefix; byte 0 repeats the segment netId and the broadcast header starts at the 9th byte.
- All multi-byte fields are big-endian, and the message structs are `#pragma pack(2)` — several
  offset tables in the exchange spec are written as if fully packed and are 1–2 bytes short of their
  own declared packet length. That gap is real interior padding.

**Normative protocol references:** `brd_data_compression_context.md` (repo root, condensed spec) and
the exchange NNF protocol PDF. The existing `poc/` decoder is a **working reference for protocol
behavior only** — see Section 7 for its scope.

### 3.2 Message classes

1. **Token-scoped transcodes** — price/depth broadcasts (MBP), master and contract table updates. A
   given token receives many such packets over the day.
2. **General transcodes** — market open/close and other session-wide announcements. Sparse across
   the trading day, but operationally critical: every occurrence must reach the database.

Different transcodes map to different DB columns and, in some cases, different tables. There is no
single universal row shape.

### 3.3 Volume and sizing envelope

The governing figure is **aggregate ingress bandwidth, which will not exceed 10 Mbit/s**. All other
volume figures below are derived from it and are consistent with it; where a figure is derived rather
than stakeholder-supplied it is marked *(derived)*. The architect should size against these.

| Quantity | Figure |
|---|---|
| Peak aggregate ingress | **≤ 10 Mbit/s** ≈ 1.25 MB/s |
| Peak datagram rate *(derived)* | ~2,400 datagrams/sec at maximum datagram size; higher if datagrams are smaller |
| Peak aggregate packet rate *(derived)* | **~5,000–10,000 packets/sec**, assuming 2–4 packets per datagram |
| Peak per-token packet rate | **1,000–5,000 packets/sec**, as a *burst for an individual hot token* |
| Concurrently hot tokens *(derived)* | **Small — order of tens.** A single token sustaining 5,000 packets/sec consumes most of the 10 Mbit/s budget on its own, so the set of tokens at peak rate simultaneously is necessarily tiny. |
| Tracked token universe | **150,000–200,000** tokens, state-resident, the large majority idle at any given instant |
| Target DB update rate, high-frequency transcodes | **1–10 updates/sec per active token** |
| Peak aggregate DB write rate *(derived)* | Bounded above by the packet rate; **plan for ≤ ~5,000 row-updates/sec worst case**, with the expected steady-state figure substantially lower after coalescing |

**The v1 draft stated the per-token peak (1,000–5,000/sec) and the per-token DB target (1–10/sec) as
if they applied to all 150K–200K tokens simultaneously.** They do not. Those are per-token burst
figures for a small hot set. The aggregate ceiling in row 1 governs.

## 4. Target Platform and Environment

The system is deployed on a single AIX LPAR with the following characteristics. Designs must fit
within them.

| Property | Value |
|---|---|
| OS / architecture | **AIX, ppc64** (`CGO_ENABLED=0 GOOS=aix GOARCH=ppc64`) |
| Online virtual CPUs | 4 |
| Maximum virtual CPUs | 12 (headroom for DLPAR growth; not available by default) |
| Processor type | Dedicated, SMT-8 |
| Processor mode | **Capped** |
| Entitled capacity | **4** |
| Memory budget | **40 GB** |
| Socket receive buffer | `udp_recvspace = 655360` bytes (640 KB) |

Implications the design must account for:

- **Capped entitled capacity of 4 is the real parallelism budget, not the logical thread count.**
  With dedicated SMT-8, the runtime may observe considerably more logical CPUs than the LPAR is
  entitled to consume. Worker-pool sizing and `GOMAXPROCS` must be driven by entitled capacity;
  sizing CPU-bound pools off the detected logical CPU count will oversubscribe a capped partition
  and add latency rather than throughput. This must be configurable, not auto-detected.
- **The socket buffer gives roughly half a second of slack** *(derived)*: 640 KB ÷ 516 bytes ≈ 1,270
  maximum-size datagrams, which at ~2,400 datagrams/sec is ~0.5 s of buffering before the kernel
  discards. This is the origin of the read-loop stall budget in NFR4. A per-socket `SO_RCVBUF`
  request cannot exceed `udp_recvspace` without a tunable change.
- **Memory is not a binding constraint for token state** *(derived)*: 200,000 tokens at even 4 KB of
  resident state each is ~800 MB against a 40 GB budget. A design that holds a full in-memory mirror
  of the target tables is affordable; the architect should not reject that class of design on memory
  grounds.
- Pure Go only — no cgo, no Oracle Instant Client, no `liblzo2`. LZO1Z decompression must be
  implemented in Go.

## 5. Functional Requirements

| # | Requirement |
|---|---|
| FR1 | Receive market data on a unicast UDP socket. |
| FR2 | Split each received datagram into its individual sub-packets and LZO1Z-decompress those that are compressed. Malformed or corrupt input must be rejected without terminating the process. |
| FR3 | Parse each decompressed packet and extract its transcode, its header sequence number and timestamp, and its token(s) where applicable. A single packet may carry multiple tokens. |
| FR4 | Route each parsed message to the correct DB update logic based on its transcode — different transcodes update different columns and, in some cases, different tables. |
| FR5 | For high-frequency, token-scoped transcodes, reduce the write rate from packet arrival rate down to a configurable target DB update rate (~1–10/sec per active token) by **coalescing**, such that the most recent state for each (token, transcode) is always the state eventually written. |
| FR6 | For transcodes configured as **critical** (including general/non-token transcodes such as market open/close), persist every received occurrence. Critical transcodes must not be coalesced or rate-limited. See Section 10.1 for the delivery guarantee this can and cannot provide. |
| FR7 | Treat general (non-token) messages as a distinct message class from token-scoped messages throughout routing, coalescing and work distribution. |
| FR8 | Enforce write ordering per Section 9 ("Ordering"): a superseded update must never overwrite a newer one for the same (token, transcode). |
| FR9 | Emit basic operational counters and logging sufficient to confirm the system is running correctly: packets received, packets by transcode, DB writes attempted/succeeded/failed, coalesce/discard counts, and detected feed sequence gaps. Rich observability is deferred — see Section 8. |

## 6. Non-Functional / Technical Requirements

| # | Requirement |
|---|---|
| NFR1 | **Coalescing is configured per transcode.** Each transcode is configured as one of: coalesced (latest-wins), intermediate-preserving, or critical/unthrottled. There is no single global policy, and no transcode is subject to coalescing unless configured for it. |
| NFR2 | **Rate limiting (lossy discard) is configured per transcode and is off by default.** It applies only to transcodes explicitly configured for it. It is a distinct mechanism from coalescing (Section 2) and must not be conflated with it in the design. |
| NFR3 | **Concurrent DB writes** — the write path must be parallelizable across workers/connections rather than serialized on a single path, subject to the ordering constraint in Section 9. |
| NFR4 | **Decoupling of the socket read path from the DB write path.** The read loop must never block on database work. Derived budget: given the ~0.5 s of socket buffering available (Section 4), **the read loop must not stall for more than ~100 ms** under any normal condition, giving a 5× margin before kernel-level loss begins. |
| NFR5 | **Fast, bounded downstream processing** immediately after decode/parse, so queues do not grow without limit under peak load. Every internal queue must have a defined capacity and a defined, documented behavior on overflow. |
| NFR6 | **DB updates in bounded batches.** Batch size must be configurable, with a configurable maximum flush interval so that a partially filled batch cannot be held indefinitely (which would violate the latency goal in Section 9). |
| NFR7 | **Concurrency sized to entitled capacity.** Concurrency should be applied where it demonstrably improves throughput or latency, sized per Section 4 — not applied uniformly. Each design must state where it parallelizes and why. |
| NFR8 | **Everything operationally tunable must be configurable**, at minimum: DB batch/chunk size and maximum flush interval; per-transcode policy (coalesced / intermediate-preserving / critical) and per-transcode coalescing interval; per-transcode rate limits where enabled; the list of critical transcodes; number of DB connections; number of worker goroutines per pool; all queue and buffer sizes; socket receive buffer size; `GOMAXPROCS`. |
| NFR9 | **Scale**: support 150,000–200,000 tracked tokens resident, within the memory budget of Section 4, at the ingress rates of Section 3.3. |
| NFR10 | **Work distribution and policy decisions must be addressable at (token, transcode) granularity**, not only globally — consistent with the ordering rule in Section 9, which is likewise defined per (token, transcode). |
| NFR11 | **Modular architecture** — clear separation of concerns, with modules that are independently testable and, where reasonable, independently replaceable. The specific module decomposition is the architect's to propose and justify; no particular set of modules is mandated. |
| NFR12 | **Robustness** — a malformed, corrupt or unparseable packet must be logged and dropped without affecting subsequent packets or terminating the process. |

## 7. Constraints (fixed; not open to the architect)

- **Database: Oracle**, accessed via the **`go-ora`** pure-Go driver (no cgo, no Instant Client).
  Not open for reconsideration. Oracle specifics that follow from this: bind placeholders are `:1`
  / `:name`, never `?`; there is no `ON CONFLICT` (upserts are `MERGE INTO … USING dual`); and
  connection-pool sizing interacts with the instance's `sessions`/`processes` limits and with
  `IDLE_TIME` profile timeouts.
- **Target platform** as specified in Section 4.
- **Language: Go**, pure Go only, cross-compiled for AIX/ppc64.
- **No code is reused from `poc/`.** The system is built from scratch. `poc/` remains a normative
  reference for *protocol behavior* — framing, the 8-byte prefix, LZO1Z decompression, `pack(2)`
  layout arithmetic, the compressed-transcode set — and its decoder is known to produce correct
  field values. Designs should treat it as an executable specification of the wire format, not as a
  starting codebase.
- Choice of concurrency primitives, queueing approach and internal data structures is **not** fixed
  and is expected to be weighed and justified.

## 8. Non-Goals / Out of Scope

- **Rich metrics and observability — deferred.** Per-transcode/per-token metrics, latency
  histograms, queue-depth gauges and a scrape-ready endpoint are deferred to a later phase. FR9's
  basic counters and logging are the scope for now. Designs should not be shaped around a metrics
  system, but should note where instrumentation would attach.
- **UI/reporting layer** for metrics.
- **Horizontal scaling / multiple instances.** The system is a single process on a single host. The
  token universe is not expected to change materially, the aggregate feed fits comfortably within one
  host's capacity, and the feed is unicast to this host — so a second instance would not receive it
  without additional infrastructure. Designs should not assume, and need not provide for, sharding
  across instances.
- **Multi-exchange / multi-feed support.** Single feed source.
- **Historical backfill, replay, or exchange retransmission recovery.**
- **Database high-availability and recovery.** Per Section 10.2, a database outage is an accepted
  application outage.
- **DB schema design and the transcode→column/table mapping.** Deferred to implementation. See
  Section 12 for the consequence this has for Phase 2.

## 9. Resolved Product Decisions

These have been answered by the stakeholder and are constraints on the design, not open questions.

**Coalescing and staleness.** Configurable per transcode. High-frequency transcodes use latest-value-
wins. Transcodes requiring intermediate values to be preserved will be configured **only among the
sparse transcodes**. This yields an invariant the architect may rely on: **no transcode is both
high-frequency and intermediate-preserving.** Every transcode is either coalescible or low-volume
enough to persist in full.

**Message semantics.** All messages may be treated as **snapshots** — each carries complete current
state for its fields, not a delta or increment. This is what makes latest-wins coalescing safe.

**Ordering.** Updates for a given (token, transcode) must be applied to the DB in feed order; a
superseded update must never overwrite a newer one. Ordering is established from the **broadcast
header's sequence number, with the header timestamp as a secondary discriminator** — not from
goroutine or socket arrival order, which UDP does not guarantee. Where the sequence number and
timestamp are zero or a default/unset value, **ordering enforcement is skipped for that message and
last-writer-wins applies.**

Two consequences worth stating explicitly:

- Ordering is defined **per (token, transcode)**, not per token across all transcodes. Because
  different transcodes write disjoint columns or tables, a high-frequency transcode's sequence
  number must not suppress an older-but-unapplied write from a sparse transcode. Defining it this
  way also makes the ordering rule consistent with (token, transcode)-granular work distribution
  (NFR10) rather than in conflict with it.
- The mechanism this implies is **stale-write rejection** — retain the last-applied sequence number
  per (token, transcode) and discard anything not newer — which composes naturally with coalescing
  rather than fighting it, and does not require serializing writes.

**Latency.** No numeric SLA is available. The target is the **lowest end-to-end latency achievable**
from packet arrival to DB visibility, for both high-frequency and critical transcodes, subject to the
other constraints. Because no SLA exists to rank designs against, **each candidate design must state
its own expected end-to-end latency** — see Section 11.

**Coalescing/rate-limiter state persistence.** In-memory only. It need not survive a process
restart; loss of in-flight coalesced state on restart is acceptable.

**Deployment topology.** Single instance, single host. See Section 8.

## 10. Stated Assumptions and Residual Risks

These are accepted positions, recorded so the design rests on them explicitly rather than implicitly.

### 10.1 Delivery is best-effort, not guaranteed

FR6 requires that every occurrence of a critical transcode be persisted. **This cannot be an absolute
guarantee**, and the design must not claim one: the transport is UDP with no retransmission, the
coalescing/queue state is in-memory only, and replay/recovery tooling is out of scope. A message lost
on the wire, or in a queue at the moment of a crash, is lost permanently.

FR6 is therefore to be read as: *the system must not itself drop, coalesce or rate-limit critical
transcodes*, and must apply no lossy policy to them at any stage. Additionally, the system should
**detect and log gaps in the header sequence number** so that loss is visible rather than silent,
even though it cannot be recovered.

*Accepted risk:* silent loss of a critical message is possible and would not be recoverable.

### 10.2 The database is assumed available

A database outage is treated as an application outage: if the DB is down, FeedValve may stop, and no
recovery, spooling or catch-up behavior is required. This resolves what would otherwise be an
unsatisfiable three-way conflict between "never block the socket", "never drop critical messages" and
"never grow queues without bound".

*Residual requirement:* this covers a hard outage, not transient slowness. A DB that is merely slow
(a lock, a checkpoint, a network hiccup) will still back pressure up the pipeline while the process
keeps running, so **NFR5's bounded-queue-with-defined-overflow-behavior still applies** and each
design must state what happens when a queue fills — which is a design question, not an operational
one.

### 10.3 Socket-level loss is out of scope

Kernel/NIC-level datagram drops are not tracked or compensated for. NFR4's stall budget exists to
make them unlikely, not to detect them.

### 10.4 Ordering fields need field-level confirmation

The ordering scheme in Section 9 depends on the header's sequence number and timestamp being
populated and meaningful in the commodity-derivatives segment. The header carries a 4-byte sequence
number, a 4-byte log time and an 8-byte secondary timestamp. **Which of these are reliably populated
should be confirmed against a live capture before the low-level design commits to a comparison
rule** — including whether the sequence number is per-transcode, per-segment or feed-global, since
that determines whether it is directly comparable across the messages being ordered. The
"zero/default ⇒ skip ordering" fallback in Section 9 exists precisely because some of these fields
may not be populated.

## 11. Design Deliverables Requested

The design effort is split into two phases. **Phase 2 does not begin until a Phase 1 design has been
chosen by the stakeholder.**

### Phase 1 — High-Level Design

**Deliverable: the top 3 high-level designs**, presented for comparison and selection.

Candidate approaches should be explored more widely than the three that are ultimately recommended.
Two illustrative starting points — examples to riff on, not a prescribed shortlist:

- **Approach A:** `datagram → split & decompress → distribute → per-(token,transcode) policy
  (coalesce / pass) → DB update`
- **Approach B:** `datagram → split & decompress → distribute → update in-memory map mirroring the
  table structure → periodic flusher writes changed entries to the DB on a configurable interval`

Each of the three recommended high-level designs must cover:

1. **End-to-end data flow and concurrency model**, including where work is distributed and on what
   key, and how that key relates to the (token, transcode) ordering rule in Section 9.
2. **How high-frequency token data is coalesced** down to the target update rate, and how the
   stale-write-rejection rule is enforced.
3. **How critical and intermediate-preserving transcodes bypass all lossy handling**, and how they
   are kept from being starved by high-frequency traffic.
4. **Module breakdown and responsibilities**, with the boundaries justified (NFR11).
5. **Expected end-to-end latency** from packet arrival to DB visibility — a stated estimate with the
   reasoning behind it, for both the high-frequency and the critical/sparse path. Since no latency
   SLA exists (Section 9), these estimates are the primary basis on which the designs will be
   compared, and should be given as an estimate with its dominant contributing term identified
   (e.g. flush interval, batch fill time, queue wait).
6. **Behavior under peak burst and under DB slowness**, including what happens when each queue
   fills (per Section 10.2).
7. **Fit to the platform in Section 4** — how the design behaves within an entitled capacity of 4,
   and its expected memory footprint at 200,000 tracked tokens.
8. **How each requirement in Sections 5 and 6 is satisfied.**
9. **Trade-offs, risks and open questions.**

Consideration of relevant architectural patterns is expected — producer-consumer, fan-out/fan-in,
actor model, sharding strategies, write-behind caching, debounce/coalesce patterns, backpressure
strategies — with reasoning about which fit the constraints in Sections 3, 4, 6 and 7, rather than a
catalogue of them.

### Phase 2 — Low-Level Design

**Deliverable: the top 3 low-level designs for the high-level design chosen at the end of Phase 1.**

Each should cover, at implementation detail:

1. Concrete data structures for token/transcode state, with memory sizing at 200,000 tokens.
2. Goroutine/worker topology, channel or queue types, and their capacities and overflow behavior.
3. The DB write path in detail: statement shapes (`MERGE` vs. `UPDATE`, array binds), batch
   assembly, transaction boundaries, connection-pool sizing against the Oracle session limits, and
   deadlock/lock-contention analysis given concurrent writers.
4. The configuration surface (NFR8) — every tunable, its type, default and valid range.
5. Failure handling per stage: malformed packets, parse failures, SQL errors, sequence gaps.
6. Test strategy — including how coalescing ratio (FR5), critical-path losslessness (FR6) and the
   ordering rule (FR8) are actually verified, and how load is generated for the rates in
   Section 3.3.
7. Refined latency estimate for the specific low-level design.

## 12. Open Items to Confirm

Not blocking Phase 1, but each must be closed before or during Phase 2.

1. **The transcode inventory.** Which transcodes the system handles, and for each: token-scoped or
   general, compressed or not, and its policy class (coalesced / intermediate-preserving /
   critical). `poc/` currently decodes only 7208 and 7305; the protocol defines many more. Phase 1
   designs should be parameterized over this list rather than assuming a fixed set.
2. **DB schema and the transcode→table/column mapping** (Section 8). Deferred, but Phase 2's write
   path cannot be fully specified without it — in particular whether writes are `UPDATE`-only against
   pre-seeded rows or upserts that may insert previously unseen tokens, which changes statement
   shape, batching and locking. **Phase 1 designs must state which of these they assume.**
3. **Ordering field semantics** (Section 10.4).
4. **The count of concurrently active tokens** in a typical peak second — as opposed to the 150K–200K
   tracked universe. This is the figure that determines the real aggregate DB write rate, and is
   currently derived (Section 3.3) rather than measured.
5. Trading-session boundaries and expected start-of-day/end-of-day behavior, if any is required
   beyond processing the market open/close transcodes as ordinary critical messages.
