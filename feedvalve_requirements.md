# FeedValve — System Requirements Document

**Status:** Draft for system design
**Audience:** System architect / designer
**Author:** Product/engineering input (compiled from stakeholder description)
**Date:** 2026-09-08

---

## 1. Purpose

FeedValve is a Go service that ingests a stock exchange's broadcast market data feed over UDP,
decodes it, and persists the relevant data to a database. This document defines the functional and
technical requirements the system must satisfy. It does **not** prescribe an implementation or
architecture — that is the system designer's task (see Section 6, "Design Deliverables Requested").

## 2. Background / Domain Knowledge

- The exchange broadcasts data over UDP. Each UDP datagram may bundle multiple sub-packets, which
  are individually length-prefixed and individually LZO-compressed (some transaction codes are
  compressed, others arrive raw — see the existing `poc/` decoder for reference behavior).
- After decompression, each packet is parsed into a message identified by a **transaction code
  ("transcode")**.
- Transcodes fall into two broad categories:
  1. **Token-scoped transcodes** — carry data for a specific derivative contract (future/option),
     identified by a unique **token**. Examples: market price broadcasts (MBP), master/contract
     table updates. A given token receives many transcodes over the day.
  2. **General/global transcodes** — not associated with any token. Examples: market open/close,
     other session-wide announcements. These are typically sparse but can be operationally critical.
- Different transcodes map to different DB columns and, in some cases, different DB tables
  entirely. There is no single universal row shape.
- Feed rate is highly non-uniform:
  - Some token-scoped transcodes (e.g., price updates) can arrive at **1,000–5,000 packets/sec per
    token** during peak activity.
  - The required DB update rate for those same transcodes is far lower: **1–10 updates/sec per
    token** — i.e., the system is expected to coalesce/throttle, not write every packet.
  - Other transcodes arrive **sparsely** across the trading day (possibly a handful of times) but
    are **critical** — every one of them must be reflected in the DB. These must not be
    rate-limited away.
- Expected active token universe: **150,000–200,000 tokens**.

## 3. Functional Requirements

| # | Requirement |
|---|-------------|
| FR1 | Receive broadcast data on a UDP socket. |
| FR2 | Split received data into individual sub-packets and LZO-decompress compressed ones. |
| FR3 | Parse each decompressed packet and extract its transaction code (transcode), and token where applicable. |
| FR4 | Route each parsed message to the correct DB update logic based on its transcode — different transcodes update different columns and, in some cases, different tables. |
| FR5 | For high-frequency, token-scoped transcodes, reduce write rate from packet arrival rate (~1,000–5,000/sec/token) down to a configurable target DB update rate (~1–10/sec/token) without losing the *latest* state. |
| FR6 | For sparse/critical transcodes (including non-token-scoped ones such as market open/close), guarantee every occurrence is persisted — no rate limiting or dropping. |
| FR7 | Support general (non-token) messages as a distinct message class from token-scoped messages, including in any load-balancing/rate-limiting logic. |
| FR8 | Emit metrics/analytics: at minimum, feed counts per transcode, per token, and overall throughput, plus DB write counts/latency and drop/throttle counts. |

## 4. Non-Functional / Technical Requirements

| # | Requirement |
|---|-------------|
| NFR1 | **Rate limiting** of inbound packets must be applied per token and per transcode (see FR5/FR6 — critical transcodes are exempt from lossy limiting). |
| NFR2 | **Concurrent DB updates** — DB writes must be parallelized across workers/connections, not serialized on a single path. |
| NFR3 | **Fast, lightweight downstream processing** immediately after decode/parse, to avoid queue buildup ("flooding") under peak load. |
| NFR4 | **Decoupling of socket read path from DB write path** — a slow or blocked DB must never block or slow UDP receive. Packet loss at the socket due to a stalled DB is unacceptable. |
| NFR5 | **DB updates in small, bounded batches/chunks** rather than one-row-at-a-time or unbounded batches. |
| NFR6 | **Concurrency/multi-threading** should be used wherever it meaningfully improves throughput or reduces latency, across all stages of the pipeline. |
| NFR7 | **Everything operationally tunable must be configurable**, at minimum: DB update chunk size, rate limit thresholds (per transcode/per token), number of DB connections, number of worker goroutines/pools, queue/buffer sizes. |
| NFR8 | **Scale**: design must support on the order of 150,000–200,000 concurrently tracked tokens. |
| NFR9 | **Per-token, per-transcode load balancing and rate limiting** — work distribution and throttling decisions must be addressable at the (token, transcode) granularity, not just globally. |
| NFR10 | **Metrics and observability** — expose counters/gauges for feed volume per transcode/token, processing latency, queue depths, drop/coalesce counts, and DB write success/failure rates, in a form suitable for scraping/alerting. |
| NFR11 | **Modular architecture** — clear separation of concerns, at minimum: socket/ingestion module, decode/parse module, load balancer module, rate limiter module, DB update/handler module. Modules should be independently testable and, where reasonable, independently replaceable. |

## 5. Explicit Non-Goals / Out of Scope (for this document)

- Choice of specific concurrency primitives, queueing technology, or DB technology beyond what is
  already fixed by the existing `poc/` codebase (Oracle via `go-ora`, UDP ingestion, LZO1Z
  decompression) unless the architect determines a change is warranted and calls it out explicitly.
- UI/reporting layer for the metrics (only emission/exposure is in scope here).
- Multi-exchange/multi-feed support (single feed source assumed unless stated otherwise by the
  architect's design).
- Historical backfill or replay tooling.

## 6. Design Deliverables Requested

The system architect is asked to **produce a design document** (separate from this one) that
includes:

1. **At least 3 candidate packet-flow / processing pipeline approaches.** Two illustrative starting
   points are provided below — these are examples to riff on, not a prescribed shortlist:

   - **Approach A:** `network packet → split & decompress → load balancer → rate limiter (pass/fail) → DB update`
   - **Approach B:** `network packet → split & decompress → load balancer → update in-memory map (mirrors table structure) → periodic DB updater flushes map to tables on fixed intervals`

   Each candidate approach should address: how token-scoped high-frequency data is coalesced down
   to the target update rate, how sparse/critical transcodes are guaranteed not to be dropped, and
   how per-token/per-transcode load balancing is achieved at the 150K–200K token scale.

2. **Consideration of relevant architectural/design patterns** (e.g., producer-consumer, fan-out/
   fan-in, actor model, sharding strategies, write-behind caching, debounce/coalesce patterns,
   backpressure strategies) and which best fit the constraints in Sections 3 and 4.

3. **A recommendation of the top 3 system designs**, each covering:
   - Module breakdown and responsibilities (mapping to NFR11's modular boundaries at minimum).
   - Data flow and concurrency model end-to-end.
   - How each functional and non-functional requirement above is satisfied.
   - Failure modes and behavior under DB slowness/unavailability, and under packet bursts.
   - Configuration surface (what is tunable, per NFR7).
   - Trade-offs, risks, and open questions for each design.

No system design, module breakdown, or technology selection should be performed as part of this
requirements document — that work is explicitly deferred to the architect's design document.

## 7. Open Questions for the Architect to Resolve

- What staleness is acceptable for coalesced high-frequency data (i.e., is "latest value wins"
  sufficient, or must intermediate values be preserved for any transcode)?
- Are there ordering guarantees required between packets for the same token (e.g., must updates be
  applied in arrival order, or is last-write-wins acceptable per field/table)?
- What is the acceptable end-to-end latency budget from packet arrival to DB visibility, separately
  for high-frequency vs. critical/sparse transcodes?
- Should rate-limiter/coalescing state be in-memory only (lost on restart) or does it need to
  survive a restart without data loss for in-flight coalesced updates?
- Deployment topology: single instance, or does the design need to account for horizontal scaling /
  multiple instances splitting the token space?
