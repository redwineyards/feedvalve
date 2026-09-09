# FeedValve — System Requirements Document

**Status:** Issued for design — Phase 1
**Version:** 3.0 (supersedes v2 of 2026-09-09 and the v1 draft of 2026-09-08)
**Audience:** System architect / designer
**Author:** Product/engineering input (compiled from stakeholder description)
**Date:** 2026-09-09

**Changes in v3:** Phase 1 given an explicit selection rubric and a required comparison matrix;
designs must differ at a broad architectural level and state their sensitivity to the open items;
degradation preference order under overload made explicit; per-token fan-out made a functional
requirement; DB write batching fixed as time-window driven; ordering restated as a property rather
than a mechanism.

**Changes in v2:** volume figures corrected and re-derived from a measured bandwidth ceiling;
ordering, coalescing and rate-limiting semantics disambiguated; target platform specified;
observability deferred; horizontal scaling removed from scope; design work split into two phases.

---

## 1. Purpose

FeedValve is a Go service that ingests a stock exchange's broadcast market data feed over UDP,
decodes it, and persists the relevant data to an Oracle database. This document defines the
functional and technical requirements the system must satisfy. It states *what* the system must do
and the constraints it must do it under; the pipeline architecture, concurrency model and module
decomposition are the designer's to propose (Section 11).

**The design effort is split into two phases.** Phase 1 produces high-level designs, from which one
is chosen; Phase 2 produces low-level designs for that chosen option. Deliverables, the selection
rubric and the gate between the phases are in Section 11.

## 2. Definitions

These terms are used precisely throughout this document; earlier informal usage caused ambiguity and
is now fixed.

| Term | Meaning |
|---|---|
| **Datagram** | One UDP datagram as received from the socket. Carries a `BcastPackData` envelope: a 2-byte netId, a packet count, then length-prefixed sub-packets. Maximum wire size **516 bytes**. |
| **Packet** | One sub-packet extracted from a datagram, after decompression where applicable. One packet = one decoded message. A datagram carries one or more packets. |
| **Update unit** | One (token, transcode) update derived from a packet — the unit that is distributed, coalesced, ordered and written. A token-scoped packet yields one update unit **per token it carries** (see FR4); a general packet yields a single non-token unit. |
| **Transcode** | The transaction code in the broadcast header identifying the message type (e.g. 7208, 7305). |
| **Token** | Numeric identifier of a single derivative contract (future/option). |
| **Token-scoped transcode** | A message carrying data for one or more specific tokens. |
| **General transcode** | A message not associated with any token (market open/close, session-wide announcements). |
| **Coalescing** | Applying to the database only the newest update unit for a (token, transcode), superseded ones having been discarded. **Lossless with respect to final state**; lossy with respect to history. Applies to high-frequency snapshot transcodes. |
| **Rate limiting** | Discarding update units outright to cap a rate, without regard to preserving final state. Used in this document only where explicitly stated; it is **not** a synonym for coalescing. |
| **Active token** | A token that has received at least one packet in the preceding **one second**. Distinct from the *tracked* token universe, most of which is idle at any instant. |

Note that a single packet may update **more than one token** — the 7208 MBP message carries up to
two `INTERACTIVE_ONLY_MBP_DATA` records, each with its own token. Rate figures below are stated in
packets, not in update units.

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
| Concurrently hot tokens *(derived)* | **Small — order of tens.** Assuming ~150 bytes per compressed packet, a single token sustaining 5,000 packets/sec consumes roughly 6 Mbit/s — most of the budget on its own — so the set of tokens at peak rate simultaneously is necessarily tiny. |
| Tracked token universe | **150,000–200,000** tokens, state-resident, the large majority idle at any given instant |
| Target DB update rate, high-frequency transcodes | **1–10 updates/sec per active token** |
| Aggregate DB write rate — **expected** *(derived)* | **Order of hundreds of row-updates/sec.** With a hot set in the tens and 1–10 updates/sec each, coalesced steady-state traffic is small. |
| Aggregate DB write rate — **ceiling** *(derived)* | **≤ ~5,000 row-updates/sec**, the uncoalesced worst case if most traffic turns out to be critical or coalescing is bypassed. Size for survivability at this rate; do not size the steady-state design around it. |

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
- **CPU is expected to be comfortably sufficient, and the database write path is the expected
  bottleneck** *(derived)*: 1.25 MB/s of LZO1Z decompression and ~10,000 packets/sec of parsing are
  small workloads for four cores. Designs should concentrate their optimization effort and their
  headroom analysis on the write path rather than on decode.
- **Go's AIX/ppc64 port is second-tier.** No race detector, reduced profiling support, cgo-free
  only, and a thinner `golang.org/x/sys` surface than Linux. Correctness testing of concurrent state
  access must therefore happen on a Linux host with cross-compilation for deployment. Platform-
  specific socket options (e.g. `SO_REUSEPORT`, batched receive) must be confirmed available on AIX
  before a design depends on them.
- Pure Go only — no cgo, no Oracle Instant Client, no `liblzo2`. LZO1Z decompression must be
  implemented in Go.

## 5. Functional Requirements

| # | Requirement |
|---|---|
| FR1 | Receive market data on a unicast UDP socket. |
| FR2 | Split each received datagram into its individual sub-packets and LZO1Z-decompress those that are compressed. Malformed or corrupt input must be rejected without terminating the process. |
| FR3 | Parse each decompressed packet and extract its transcode, its header sequence number and timestamp, and its token(s) where applicable. |
| FR4 | **Fan out each parsed packet into per-token update units.** A packet carrying multiple tokens (e.g. 7208, which carries up to two) must be decomposed into one update unit per token before distribution, so that coalescing, ordering and work distribution operate at (token, transcode) granularity. Update units derived from the same packet are applied **independently**; per-packet atomicity across its tokens is not required. |
| FR5 | Route each update unit to the correct DB update logic based on its transcode — different transcodes update different columns and, in some cases, different tables. |
| FR6 | For high-frequency, token-scoped transcodes, reduce the write rate from packet arrival rate down to a configurable target DB update rate (~1–10/sec per active token) by **coalescing**, such that the most recent state for each (token, transcode) is always the state eventually written. |
| FR7 | For transcodes configured as **critical** (including general/non-token transcodes such as market open/close), persist every received occurrence. Critical transcodes must not be coalesced or rate-limited. See Section 10.1 for the delivery guarantee this can and cannot provide. |
| FR8 | Treat general (non-token) messages as a distinct message class from token-scoped messages throughout routing, coalescing and work distribution. |
| FR9 | Enforce write ordering per Section 9 ("Ordering"): a superseded update must never overwrite a newer one for the same (token, transcode). |
| FR10 | Emit basic operational counters and logging sufficient to confirm the system is running correctly: packets received, packets by transcode, DB writes attempted/succeeded/failed, coalesce/discard counts, and detected feed sequence gaps (conditional on Section 12.3 resolving). Rich observability is deferred — see Section 8. |

## 6. Non-Functional / Technical Requirements

| # | Requirement |
|---|---|
| NFR1 | **Coalescing is configured per transcode.** Each transcode is configured as one of: coalesced (latest-wins), intermediate-preserving, or critical/unthrottled. There is no single global policy, and no transcode is subject to coalescing unless configured for it. |
| NFR2 | **Rate limiting (lossy discard) is configured per transcode and is off by default.** It applies only to transcodes explicitly configured for it. It is a distinct mechanism from coalescing (Section 2) and must not be conflated with it in the design. |
| NFR3 | **Concurrent DB writes** — the write path must be parallelizable across workers/connections rather than serialized on a single path, subject to the ordering constraint in Section 9. |
| NFR4 | **Decoupling of the socket read path from the DB write path.** The read loop must never block on database work. Derived budget: given the ~0.5 s of socket buffering available (Section 4), **the read loop must not stall for more than ~100 ms** under any normal condition, giving a 5× margin before kernel-level loss begins. |
| NFR5 | **Fast, bounded downstream processing** immediately after decode/parse, so queues do not grow without limit under peak load. Every internal queue must have a defined capacity and a defined, documented behavior on overflow, consistent with the degradation order in Section 9. |
| NFR6 | **DB writes are batched on a time window.** Batches are assembled and flushed on a configurable interval — not accumulated per token or per transcode — with a configurable maximum batch size as a secondary cap so a burst cannot produce an unbounded batch. The flush interval is the primary control relating write rate to latency. |
| NFR7 | **Concurrency sized to entitled capacity.** Concurrency should be applied where it demonstrably improves throughput or latency, sized per Section 4 — not applied uniformly. Each design must state where it parallelizes and why. |
| NFR8 | **Everything operationally tunable must be configurable**, at minimum: DB batch flush interval and maximum batch size; per-transcode policy (coalesced / intermediate-preserving / critical) and per-transcode coalescing interval; per-transcode rate limits where enabled; the list of critical transcodes; number of DB connections; number of worker goroutines per pool; all queue and buffer sizes; socket receive buffer size; `GOMAXPROCS`. |
| NFR9 | **Scale**: support 150,000–200,000 tracked tokens resident, within the memory budget of Section 4, at the ingress rates of Section 3.3. |
| NFR10 | **Work distribution and policy decisions must be addressable at (token, transcode) granularity**, not only globally — consistent with the ordering rule in Section 9, which is likewise defined per (token, transcode). |
| NFR11 | **Modular architecture** — clear separation of concerns, with modules that are independently testable and, where reasonable, independently replaceable. The specific module decomposition is the architect's to propose and justify; no particular set of modules is mandated. |
| NFR12 | **Robustness** — a malformed, corrupt or unparseable packet must be logged and dropped without affecting subsequent packets or terminating the process. |
| NFR13 | **Change-driven writes.** Database writes must be driven by a change set: token state that has not changed since its last successful write is never rewritten. A periodic flush must not sweep the full tracked-token universe. |

## 7. Constraints (fixed; not open to the architect)

- **Database: Oracle**, accessed via the **`go-ora`** pure-Go driver (no cgo, no Instant Client).
  Not open for reconsideration. Oracle specifics that follow from this: bind placeholders are `:1`
  / `:name`, never `?`; there is no `ON CONFLICT` (upserts are `MERGE INTO … USING dual`); and
  connection-pool sizing interacts with the instance's `sessions`/`processes` limits and with
  `IDLE_TIME` profile timeouts.
- **Target platform** as specified in Section 4.
- **Language: Go**, pure Go only, cross-compiled for AIX/ppc64.
- **No code is reused from `poc/`.** The system is built from scratch. `poc/` remains a normative
  reference for *protocol behavior only* — framing, the 8-byte prefix, LZO1Z decompression,
  `pack(2)` layout arithmetic, the compressed-transcode set — and its decoder is known to produce
  correct field values. The new decoder must be **validated against `poc/`'s output on the same
  captured input**, since re-deriving the LZO1Z port and the `pack(2)` parsers is exactly where
  subtly-wrong-but-plausible field decoding gets reintroduced.
- **Credentials and log hygiene:** the Oracle password is sourced from the environment only — never
  a command-line flag, never the config file — and the DSN must be redacted anywhere it could reach
  a log.
- Choice of concurrency primitives, queueing approach and internal data structures is **not** fixed
  and is expected to be weighed and justified.

## 8. Non-Goals / Out of Scope

- **Rich metrics and observability — deferred.** Per-transcode/per-token metrics, latency
  histograms, queue-depth gauges and a scrape-ready endpoint are deferred to a later phase. FR10's
  basic counters and logging are the scope for now. Designs should not be shaped around a metrics
  system, but should note where instrumentation would attach.
- **UI/reporting layer** for metrics.
- **Horizontal scaling / multiple instances.** The system is a single process on a single host. The
  token universe is not expected to change materially, the aggregate feed fits comfortably within one
  host's capacity, and the feed is unicast to this host — so a second instance would not receive it
  without additional infrastructure.
- **Multi-exchange / multi-feed support.** Single feed source.
- **Production replay, backfill, or exchange retransmission recovery.** Note this does *not* exclude
  a capture-replay test harness, which is expected (Section 11, Phase 2).
- **Database high-availability and recovery.** Per Section 10.2, a database outage is an accepted
  application outage.
- **DB schema design and the transcode→column/table mapping.** Deferred to implementation, subject
  to the semantic question in Section 12.2 which must still be answered.

## 9. Resolved Product Decisions

These have been answered by the stakeholder and are constraints on the design, not open questions.

**Coalescing and staleness.** Configurable per transcode. High-frequency transcodes use latest-value-
wins. Transcodes requiring intermediate values to be preserved will be configured **only among the
sparse transcodes**. This yields an invariant the architect may rely on: **no transcode is both
high-frequency and intermediate-preserving.** Every transcode is either coalescible or low-volume
enough to persist in full.

**Message semantics.** All messages may be treated as **snapshots** — each carries complete current
state for its fields, not a delta or increment. This is what makes latest-wins coalescing safe.

**Ordering.** The required property is that **an update for a given (token, transcode) never
overwrites a newer one**. Order is established from the **broadcast header's sequence number, with
the header timestamp as a secondary discriminator** — not from goroutine or socket arrival order,
which UDP does not guarantee. Where the sequence number and timestamp are zero or a default/unset
value, ordering enforcement is skipped for that unit and last-writer-wins applies.

Two notes on this:

- Ordering is defined **per (token, transcode)**, not per token across all transcodes. Because
  different transcodes write disjoint columns or tables, a high-frequency transcode's sequence
  number must not suppress an older-but-unapplied write from a sparse transcode. Defining it this
  way also makes the ordering rule consistent with (token, transcode)-granular work distribution
  (NFR10) rather than in conflict with it.
- **Retaining the last-applied sequence number per (token, transcode) and discarding anything not
  newer is one way to satisfy this — a natural fit with coalescing, and one that does not require
  serializing writes. It is offered as a candidate mechanism, not a mandate.** The requirement is
  the property above; the architect may satisfy it differently.

**Degradation preference order.** Under sustained overload, what the system gives up, in order:

1. **Freshness of coalesced state first** — coalescing windows may widen, reducing update frequency.
   Final state per token is still eventually written.
2. **End-to-end latency next** — queue wait and flush intervals may grow.
3. **Intermediate values last** — intermediate-preserving transcodes may degrade to latest-wins.

**Never given up:** read-loop responsiveness (NFR4), delivery of transcodes configured as critical,
and eventual correctness of the final state for every token. Each Phase 1 design must show where in
this order it degrades and by what mechanism.

**Batching.** DB writes are batched on a **time window** (NFR6), not accumulated per token or per
transcode.

**Latency.** No numeric SLA is available. The target is the **lowest end-to-end latency achievable**
from packet arrival to DB visibility, subject to the other constraints. For estimates to be
comparable across designs, latency is measured **from the datagram's arrival at the socket to the
commit of the transaction containing the update** — *assumed definition, to be confirmed; see
Section 12.6.* Each candidate design must state its own expected latency (Section 11).

**Restart expectations.** A mid-session restart is **exceptional, not routine**. The system is
expected to run unattended for the trading day. Designs may weigh write-behind buffering
accordingly, but see the staleness consequence in Section 10.3.

**Coalescing/rate-limiter state persistence.** In-memory only. It need not survive a process
restart.

**Deployment topology.** Single instance, single host. See Section 8.

## 10. Stated Assumptions and Residual Risks

These are accepted positions, recorded so the design rests on them explicitly rather than implicitly.

### 10.1 Delivery is best-effort, not guaranteed

FR7 requires that every occurrence of a critical transcode be persisted. **This cannot be an absolute
guarantee**, and the design must not claim one: the transport is UDP with no retransmission, the
coalescing/queue state is in-memory only, and replay/recovery tooling is out of scope. A message lost
on the wire, or in a queue at the moment of a crash, is lost permanently.

FR7 is therefore to be read as: *the system must not itself drop, coalesce or rate-limit critical
transcodes*, and must apply no lossy policy to them at any stage. Additionally, the system should
**detect and log gaps in the header sequence number** so that loss is visible rather than silent.

*Accepted risk:* silent loss of a critical message is possible and would not be recoverable.

### 10.2 The database is assumed available

A database outage is treated as an application outage: if the DB is down, FeedValve may stop, and no
recovery, spooling or catch-up behavior is required. This resolves what would otherwise be an
unsatisfiable three-way conflict between "never block the socket", "never drop critical messages" and
"never grow queues without bound".

This covers a hard outage, not transient slowness. A DB that is merely slow will still back-pressure
up the pipeline while the process keeps running, so NFR5's bounded queues still apply, degrading in
the order given in Section 9. Because critical transcodes are never dropped, **overflow of a
critical-path queue is a fatal condition: the process must fail loudly and stop rather than discard
silently** — which is consistent with treating DB unavailability as application downtime.

### 10.3 Restart leaves the database stale

After a mid-session restart, in-memory state is empty, so every token that does not tick again before
end of day **retains its pre-restart value in the database**, with no resync path (replay is out of
scope). For slow-moving contracts this could persist for hours. Accepted, on the basis that a
mid-session restart is exceptional (Section 9). Designs holding larger write-behind buffers carry
proportionally more of this risk and should say so.

### 10.4 Socket-level loss is out of scope

Kernel/NIC-level datagram drops are not tracked or compensated for. NFR4's stall budget exists to
make them unlikely, not to detect them.

### 10.5 Ordering fields need field-level confirmation

The ordering scheme in Section 9 depends on the header's sequence number and timestamp being
populated and meaningful in the commodity-derivatives segment. The header carries a 4-byte sequence
number, a 4-byte log time and an 8-byte secondary timestamp. **Which of these are reliably populated
should be confirmed against a live capture before the low-level design commits to a comparison
rule** — including whether the sequence number is per-transcode, per-segment or feed-global, since
that determines both whether it is directly comparable across the messages being ordered and whether
FR10's sequence-gap detection is implementable. The "zero/default ⇒ skip ordering" fallback exists
precisely because some of these fields may not be populated.

## 11. Design Deliverables Requested

The design effort is split into two phases. **Phase 2 does not begin until a Phase 1 design has been
chosen by the stakeholder.**

### 11.1 Selection rubric

Phase 1 designs are ranked against these criteria, **in priority order**:

1. **Robustness under burst** — behavior when ingress spikes toward the Section 3.3 ceiling and when
   the DB slows. Degrades gracefully, in the order given in Section 9, without loss of critical
   transcodes or final-state correctness.
2. **Latency** — expected end-to-end latency as defined in Section 9.
3. **Operational simplicity** — how easily the running system can be understood, tuned and
   diagnosed; how many tunables interact; how failure manifests.
4. **Implementation effort** — build cost and the amount of subtle concurrency the design requires
   to be got right.

Where criteria conflict, the higher-priority one wins. Each design should be argued against this
rubric explicitly rather than in the abstract.

### 11.2 Phase 1 — High-Level Design

**Deliverable: the top 3 high-level designs**, presented for comparison and selection.

**The three must differ at a broad architectural level** — not three variations on one idea. Each
must name the axis on which it differs from the others (for example: write-behind buffer vs. inline
write; shared map vs. shard-owned state; coalesce-then-distribute vs. distribute-then-coalesce). A
set of three that collapses to one shape under scrutiny does not satisfy this deliverable.

Candidate approaches should be explored more widely than the three ultimately recommended. Two
illustrative starting points — examples to riff on, deliberately not a shortlist, and not the only
shapes available (designs that coalesce before distributing, or that place the socket boundary
differently, are equally in scope):

- **Approach A:** `datagram → split & decompress → distribute → per-(token,transcode) policy
  (coalesce / pass) → DB update`
- **Approach B:** `datagram → split & decompress → distribute → update in-memory map mirroring the
  table structure → periodic flusher writes changed entries to the DB on a configurable interval`

Each of the three recommended designs must cover:

1. **End-to-end data flow and concurrency model**, including where work is distributed and on what
   key, and how that key relates to the (token, transcode) ordering rule in Section 9.
2. **Where the socket/decode boundary sits** — whether decode runs on the receive goroutine or
   behind a handoff, and where the first queue is. This is the architect's decision, but it must be
   stated and justified against NFR4's ~100 ms stall budget, since it is the design choice that most
   directly governs it.
3. **How high-frequency token data is coalesced** down to the target update rate, and how the
   ordering property is enforced.
4. **How critical and intermediate-preserving transcodes bypass all lossy handling**, and how they
   are kept from being starved by high-frequency traffic.
5. **Where the design degrades under overload**, mapped onto the preference order in Section 9, and
   what happens when each queue fills (including the fatal case in Section 10.2).
6. **Expected end-to-end latency** per Section 9's definition, for both the high-frequency and the
   critical/sparse path, with the dominant contributing term identified (flush interval, batch fill,
   queue wait). Order-of-magnitude estimates with stated assumptions are sufficient at this stage.
7. **Saturation point and headroom** — at what multiple of the Section 3.3 rates the design breaks,
   and which stage gives out first.
8. **Fit to the platform in Section 4** — behavior within an entitled capacity of 4, and expected
   memory footprint at 200,000 tracked tokens.
9. **Sensitivity to the open items in Section 12** — which of this design's decisions would change
   if the active-token count, the transcode inventory, the ordering-field semantics or the schema
   shape resolve differently than assumed. This is what makes the selection defensible while those
   items are still open.
10. **Module breakdown and responsibilities**, with the boundaries justified (NFR11).
11. **Requirements traceability — only where designs differ**, or where satisfaction is non-obvious.
    A full 21-row matrix per design is not wanted; requirements that every candidate satisfies
    identically need not be restated.
12. **Trade-offs, risks and open questions.**

### 11.3 Required comparison matrix

Phase 1 must include a single side-by-side matrix — this is the artifact the selection is made
from, and it is a deliverable in its own right, not a summary of the prose:

| | Design 1 | Design 2 | Design 3 |
|---|---|---|---|
| Distinguishing architectural axis | | | |
| **Robustness under burst** (rubric 1) | | | |
| **Expected latency + dominant term** (rubric 2) | | | |
| **Operational simplicity** (rubric 3) | | | |
| **Implementation effort** (rubric 4) | | | |
| Saturation point (× Section 3.3 rates) | | | |
| First stage to give out | | | |
| Behavior on DB slowness | | | |
| Where it degrades first (vs. Section 9 order) | | | |
| Memory footprint at 200K tokens | | | |
| Sensitivity to open items | | | |
| Principal risk | | | |

### 11.4 Architectural patterns

Phase 1 owes **a written section on architectural patterns** — producer-consumer, fan-out/fan-in,
actor model, sharding strategies, write-behind caching, debounce/coalesce patterns, backpressure
strategies — reasoning about which fit the constraints in Sections 3, 4, 6, 7 and 9, and which are
poor fits and why. It should connect to the three candidate designs rather than stand alone as a
catalogue.

### 11.5 The Phase 1 → Phase 2 gate

The stakeholder selects one design against the rubric in 11.1, using the matrix in 11.3. Some
overlap between the phases is acceptable and expected — Phase 1 estimates will necessarily involve
provisional structures that Phase 2 then specifies properly.

The selected design is the basis for Phase 2, but is not frozen: **if Phase 2 invalidates an
assumption the selection rested on, that returns to the stakeholder as a Phase 1 decision rather
than being absorbed silently into the low-level design.** *(Gate mechanics — who decides, on what
timeline, and what happens if no candidate is acceptable — are assumed to follow the normal review
process; confirm if not. See Section 12.6.)*

### 11.6 Phase 2 — Low-Level Design

**Deliverable: the top 3 low-level designs for the high-level design chosen at the end of Phase 1.**

Each should cover, at implementation detail:

1. Concrete data structures for token/transcode state, with memory sizing at 200,000 tokens.
2. Goroutine/worker topology, channel or queue types, and their capacities and overflow behavior.
3. The DB write path in detail: statement shapes (`MERGE` vs. `UPDATE`, array binds), batch
   assembly against the time window of NFR6, transaction boundaries, connection-pool sizing against
   the Oracle session limits, and deadlock/lock-contention analysis given concurrent writers.
4. The configuration surface (NFR8) — every tunable, its type, default and valid range, and whether
   it is startup-only or reloadable.
5. Failure handling per stage: malformed packets, parse failures, SQL errors, sequence gaps,
   unknown/unconfigured transcodes.
6. Test strategy — including how coalescing ratio (FR6), critical-path losslessness (FR7) and the
   ordering property (FR9) are verified; how the decoder is validated against `poc/` (Section 7);
   and how load is generated at the Section 3.3 rates. Note that concurrency testing must run on
   Linux, per Section 4.
7. Refined latency estimate for the specific low-level design.

## 12. Open Items to Confirm

Not blocking Phase 1, but each must be closed before or during Phase 2. Phase 1 designs must state
their sensitivity to these (Section 11.2, item 9).

1. **The transcode inventory.** Which transcodes the system handles, and for each: token-scoped or
   general, compressed or not, and its policy class (coalesced / intermediate-preserving /
   critical). `poc/` currently decodes only 7208 and 7305; the protocol defines many more. Phase 1
   designs should be parameterized over this list rather than assuming a fixed set. Behavior for
   transcodes *not* in the configured inventory must also be defined — they will occur.
2. **What "persist every occurrence" means at the database.** The schema is deferred (Section 8),
   but one semantic question cannot be: if successive occurrences of a critical or
   intermediate-preserving transcode update the same row in place, they overwrite each other and
   "every occurrence persisted" is indistinguishable from "the latest occurrence persisted".
   Preserving occurrences requires either an append/history destination or a consumer that observes
   each update. **Phase 1 designs must state which they assume.** Relatedly: whether writes are
   `UPDATE`-only against pre-seeded rows or upserts that may insert previously unseen tokens.
3. **Ordering field semantics** (Section 10.5), which also gates FR10's sequence-gap detection.
4. **The count of concurrently active tokens** in a typical peak second — as opposed to the
   150K–200K tracked universe. This is the figure that determines the real aggregate DB write rate,
   and is currently derived (Section 3.3) rather than measured. Along with item 3, it is measurable
   from a short live capture, and doing so before the selection gate would materially de-risk it.
5. **Raw capture capability and retention.** Item 3 requires a live capture, and Phase 2 needs test
   fixtures, so the ability to record raw payloads is required in practice but is not yet a stated
   requirement; nor is any size cap or retention policy for it.
6. **Two definitions assumed rather than confirmed** in this revision: the latency measurement
   endpoints (Section 9) and the gate mechanics (Section 11.5).
7. Trading-session boundaries and expected start-of-day/end-of-day behavior, if any is required
   beyond processing the market open/close transcodes as ordinary critical messages.
