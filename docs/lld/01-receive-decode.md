# FeedValve Phase 1 LLD — Volume 1: The Receive and Decode Path

**Status:** draft for review — SECTIONS BELOW MARKED `[STUB]` ARE NOT YET WRITTEN
**Binds to:** `docs/lld/00-foundations.md` (Volume 0, the frozen contract). Every type, config key,
counter and channel used here is Vol 0's unless listed in §14, "Additions to the Vol 0 ledger".
**Detailed from:** `docs/architecture/phase1_hld_design1.md` (the HLD, authoritative).
**Protocol authority:** `ncobrd_bible.md` (the bible). Every protocol claim cites a section.
**Hands off to:** Volume 2 (shard and mirror) at `model.UpdateUnit` on `shardCh[k]`; Volume 4
(lifecycle, observability, failure, test) at socket close, counter emission and the test gates.

---

## Table of contents

- [1. Scope, and what this volume does not solve](#1-scope-and-what-this-volume-does-not-solve)
- [2. The path in one picture](#2-the-path-in-one-picture)
- [3. Socket setup and the read loop](#3-socket-setup-and-the-read-loop)
- [4. Envelope framing](#4-envelope-framing)
- [5. Decompression](#5-decompression)
- [6. The 8-byte prefix and header resolution](#6-the-8-byte-prefix-and-header-resolution)
- [7. Gap detection](#7-gap-detection)
- [8. The decoder registry](#8-the-decoder-registry)
- [9. Per-transcode decoders](#9-per-transcode-decoders)
- [10. Fan-out to the shards](#10-fan-out-to-the-shards)
- [11. Allocation discipline](#11-allocation-discipline)
- [12. Failure-mode table for the receive path](#12-failure-mode-table-for-the-receive-path)
- [13. LLD ADRs raised by this volume](#13-lld-adrs-raised-by-this-volume)
- [14. Additions to the Vol 0 ledger](#14-additions-to-the-vol-0-ledger)
- [15. Deltas against the HLD](#15-deltas-against-the-hld)
- [16. Assumptions](#16-assumptions)
- [17. Open questions](#17-open-questions)

---

## 1. Scope, and what this volume does not solve

[STUB]

## 2. The path in one picture

[STUB]

## 3. Socket setup and the read loop

[STUB]

## 4. Envelope framing

[STUB]

## 5. Decompression

[STUB]

## 6. The 8-byte prefix and header resolution

[STUB]

## 7. Gap detection

[STUB]

## 8. The decoder registry

[STUB]

## 9. Per-transcode decoders

[STUB]

## 10. Fan-out to the shards

[STUB]

## 11. Allocation discipline

[STUB]

## 12. Failure-mode table for the receive path

[STUB]

## 13. LLD ADRs raised by this volume

[STUB]

## 14. Additions to the Vol 0 ledger

[STUB]

## 15. Deltas against the HLD

[STUB]

## 16. Assumptions

[STUB]

## 17. Open questions

[STUB]
