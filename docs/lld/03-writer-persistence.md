# FeedValve Phase 1 LLD — Volume 3: The Writer Pool and Persistence

**Status:** draft for review
**Binds to:** `docs/lld/00-foundations.md` (Volume 0, the frozen contract) and
`docs/architecture/phase1_hld_design1.md` (the HLD, authoritative).
**Protocol authority:** `ncobrd_bible.md` (the bible).
**Siblings:** Volume 1 (receive/decode), Volume 2 (shard and mirror — **owns the completion path**),
Volume 4 (lifecycle, observability, failure, test).

> **What this volume owns.** Everything downstream of `batchQ`: the writer pool, the Oracle session
> per writer, statement identity and caching, bind construction from `Entry.Image`, array binds,
> transaction boundaries, the Oracle error taxonomy, retry and the pool-stop rule, the DDL, and the
> secret-handling rules on this path.
>
> **What this volume does not own.** The shard main loop, the epoch/flush state machine, and the
> **application** of the per-row outcome vector (Volume 2, defects L1–L4). This volume *produces* the
> vector; it never touches an `Entry`. That is enforced by the package graph: `writer` may not import
> `mirror` (Vol 0 §1.2, §1.3).

---

## Table of contents

- [1. Scope, boundaries and the numbers this volume is sized against](#1-scope-boundaries-and-the-numbers-this-volume-is-sized-against)
- [2. Pool construction and the writer goroutine](#2-pool-construction-and-the-writer-goroutine)
- [3. Statement construction — the actual SQL](#3-statement-construction--the-actual-sql)
- [4. The column mask and statement grouping](#4-the-column-mask-and-statement-grouping)
- [5. Bind construction from the image](#5-bind-construction-from-the-image)
- [6. Array binds and OQ-4: per-row attribution](#6-array-binds-and-oq-4-per-row-attribution)
- [7. Transaction boundaries](#7-transaction-boundaries)
- [8. The Oracle error taxonomy](#8-the-oracle-error-taxonomy)
- [9. Retry, fail-fast and the pool-stop rule](#9-retry-fail-fast-and-the-pool-stop-rule)
- [10. DDL](#10-ddl)
- [11. Security on this path](#11-security-on-this-path)
- [12. Failure and degradation](#12-failure-and-degradation)
- [13. Interface summary](#13-interface-summary)
- [14. Additions to the Vol 0 ledger](#14-additions-to-the-vol-0-ledger)
- [15. Deltas against the HLD](#15-deltas-against-the-hld)
- [16. ADRs](#16-adrs)
- [17. Assumptions](#17-assumptions)
- [18. Open questions](#18-open-questions)

---

*(sections below are filled in order; see §18 for anything still stubbed)*

## 1. Scope, boundaries and the numbers this volume is sized against

## 2. Pool construction and the writer goroutine

## 3. Statement construction — the actual SQL

## 4. The column mask and statement grouping

## 5. Bind construction from the image

## 6. Array binds and OQ-4: per-row attribution

## 7. Transaction boundaries

## 8. The Oracle error taxonomy

## 9. Retry, fail-fast and the pool-stop rule

## 10. DDL

## 11. Security on this path

## 12. Failure and degradation

## 13. Interface summary

## 14. Additions to the Vol 0 ledger

## 15. Deltas against the HLD

## 16. ADRs

## 17. Assumptions

## 18. Open questions
