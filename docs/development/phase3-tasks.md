# Phase 3 — The Shard and the Coalescing Mirror

**Status:** ready to start. Rulings D1–D12 (§6) are made; none of Vol 2's open questions (V2 §16,
`docs/lld/02-shard-mirror.md:3192-3207`) blocks the critical path — each affects tuning or memory
figures only.
**Source of truth:** `docs/lld/02-shard-mirror.md` (Volume 2), bound to `00-foundations.md` (Vol 0).
Vol 0 §5A (pre-validation) is in scope because the shard writes all three of its instruments.
**Gate authority:** `docs/lld/04-lifecycle-ops.md` §7.3.3–§7.3.5 (`:1614-1650`) and §7.4
(`:1705-1707`).
**Builds on:** Phase 2, delivered — `develop` at 8699fdf (P2-10, G-DIFF-1).

Citation shorthand: **V0/V2/V3/V4** = `docs/lld/00-foundations.md`, `02-shard-mirror.md`,
`03-writer-persistence.md`, `04-lifecycle-ops.md`. Code paths are relative to the repo root.

---

## 1. What Phase 3 is

> Everything between a `model.UpdateUnit` leaving `shardCh[k]` and a filled `*model.Batch` landing
> on `batchQ`, and everything that happens when that batch comes back on `completionCh[k]`. The
> shard merges, pre-validates, coalesces, guards ordering, runs the epoch state machine, pumps
> batches and applies their completions. **Nothing writes to Oracle yet**; the writer is a test
> double at the consumer.

Phase 2 left one seam open: the discard consumer at `internal/app/shard.go:95-109`, whose comment
says "Phase 3 replaces the loop body" (`:80-83`). Where the tree stands today:

| Package | Today | Phase 3 adds |
|---|---|---|
| `internal/mirror` | `doc.go` only ("Built in Phase 3", `internal/mirror/doc.go:25`) | all of it (P3-02 … P3-12) |
| `internal/model` | `Entry` with `Quarantined`/`SlotSeen` (`entry.go`), `Batch`/`BatchRow.ErrCode` (`batch.go:39-58`), pinned by G-LAY-2 | **nothing.** A1, A2 and A7 have landed. `ring`, `FlushBus`, `Shard` belong to `mirror` (V2 §13.1). No ID field on `Batch` (P3-04) |
| `internal/config` | every Vol 2 key parsed, defaulted, range-checked, including A6 `Config.Gen` (`config/store.go:17-20`, generations start at 1) | nothing. `mirror` **cannot import it** (D1) |
| `internal/obs` | every Vol 2 instrument declared (`ledger.go:97-142, 169, 203-223`); log classes are a closed enum of V4 §6.2's rows (`internal/obs/class.go:21-44`) | **four log classes** for events V4 §6.2 never listed (P3-04) |
| `internal/app` | discard consumer; a placeholder for teardown steps 7–10 (`lifecycle.go:630-632`); one stats ticker (`lifecycle.go:431-463`); `shardSendBlocked` the only per-shard handle (`instruments.go:272-277`) | shard goroutines, `batchQ`/`completionCh`, the flush ticker, the shared drain deadline, the guard-evaluator ticker, the fatal mapping (P3-01, P3-13) |

### What Phase 3 is *not*

| Deferred to | Work |
|---|---|
| **Phase 4** | The writer pool, `MERGE` assembly, array binds, the two-phase execute. **Closing `batchQ`** and the writers' last batch (teardown steps **9–10**, V4:262-263; step 8 is the shard's, V4:261, and lands here). The statement halves of G-MASK-1/2. G-ROW-1/2 from Vol 3's side, including `row_rejected` (D11). G-DRAIN-1/2 and G-LIFE-4 end to end (they need a "healthy fake database", V4:1663). G-UNT-1 (needs a completing writer). |
| **Phase 5** | SIGHUP reload (no-op today, `lifecycle.go:486`), so `Config.Gen` never advances in production and `releaseQuarantine` on reload is exercised only by tests. G-CFG-4 (the `GuardOff` pointer surviving a registry rebuild, V4:1668). Liveness (V2:2802). |
| **Never in Vol 2** | Sorting rows (the writer sorts, V2:1835-1841). Attribution machinery. A `writers.rowAttribution` key. |

> **If a `*sql.DB`, a `go-ora` import, or a `net` import appears in a `mirror` branch, stop.**
> G-DEP-1 rejects it transitively (`internal/app/deps_test.go:131-138`), and **so does
> `internal/config`**, which imports `oradb` (`internal/config/config.go:6, 28`). D1 resolves that
> without relaxing the rule.

---

## 2. What the contract says to build

Counts taken from the volumes by command:

| Quantity | Count | How it was taken |
|---|---|---|
| Packages completed | **1**: `mirror` | `internal/mirror/doc.go:25` |
| Packages extended | **2**: `app` (wiring, ticker, evaluator, drain); `obs` (four log classes) | V2:3218, V4:219; §6 D9/P3-04 |
| Components | **12**: ring, FlushBus, Shard construction, digest, guard, merge, pre-validation, epoch, pump/fillRow, completion, quarantine, Run | V2 §1–§9, V0 §5A |
| LLD ADRs raised by Vol 2 | **5**: ADR-L20 … ADR-L24 | `grep -n '^### ADR-L' 02-shard-mirror.md` |
| New config keys | **1**: `flush.epochStallWarnMs` (A10), already parsed | V2 §13.2; `config.go:76` |
| New instruments | **15**: A11–A25, all in the ledger | V2:2904-2918; `ledger.go:207-223` holds **16** rows — line 223, `shard_depth_max`, is a Vol 4 addition (V0:2243) |
| Instruments with no writer today | `shard_depth{k}`, `shard_depth_max{k}` (shard), `batchq_depth` (stats) | `ledger.go:131, 223, 134`; no use site |
| Fatal conditions in the shard | **5**: S1, S2, S3, `cohort_not_empty`, step 0a. §2.7's heading says "three". **4 return** from `Shard.Run`; S3 is reported and the shard keeps running (D9) | V2:566, 571-587, 2007-2010 |
| Fatal reasons in code today | **6**: F9, F13, F20, F21, F26, V7 | `internal/app/graph.go:29-55` |
| Gates owned by Vol 2 | **25** declared, **26** enumerated, **23 new** here | V4:1516-1517 |

### Gates new in Phase 3

Status: **full** — the gate's whole assertion holds when the card merges; **shard-half** — the
shard's side holds, the rest needs Phase 4's writer; **pending erratum** — holds against the gate
text as corrected in §6.

| Gate | What it holds | Card | Status |
|---|---|---|---|
| **G-ALLOC-2** | `maphash.Bytes(seed, img[:181])` allocates nothing | P3-05 | full |
| **G-ORD-3** | `guard_offered == 0` when no key repeats | P3-06 | full — at the guard, which the merge calls once per contribution; V4:1625's "Merge all N" stimulus is re-run through `merge` in P3-07 |
| **G-ORD-1** | 6531→6571→6511 inside one `LogTime` second, applied in arrival order | P3-07 | full — over two setups; V4:1623's "either one alone" note is erratum'd (§6) |
| **G-DIG-3** | the 128-byte text region is fully rewritten and zero-filled | P3-07 | full |
| **G-INFL-1** | one outstanding batch per row; a later update sets `ReDirty`, not a second ring entry | P3-08 | full |
| **G-MASK-2** | `BatchRow.Mask` is the **union** of disjoint contributions | P3-08 | shard-half (V4:1619 asserts on the statement) |
| **G-MASK-1** | a column re-dirtied mid-flight is in the next batch, with the newer value | P3-09 | shard-half (V4:1618 asserts on the statement) |
| **G-DIG-1** | `Entry.Digest == BatchRow.Digest`, **not** `digest(entry.Image)` | P3-09 | full |
| **G-DIG-2** | a first-ever zero value is not suppressed; `Written` gains the bit on commit | P3-09 | full |
| **G-INFL-2** | a row merged into while in flight is dirty again with the newer image | P3-09 | full |
| **G-EPOCH-1/2** | one tick writes every dirty row in one epoch; a mid-drain merge waits | P3-09 | full |
| **G-ROW-1** | the three outcomes and their obligations | P3-09 | shard-half (shared with Vol 3, V4:1518) **and** pending erratum (`row_quarantined`, "nothing changed") |
| **G-ROW-2** | `Err != nil` ⇒ every Pending row Failed, requeued at the head | P3-09 | shard-half (shared with Vol 3) |
| **G-ROW-3** | `Err == nil` with a Pending row ⇒ `batch_row_unreported`++, shard stops loudly | P3-09 (+ exit in P3-13) | full |
| **G-FLUSH-1** | an epoch opens immediately, not at the next tick | P3-11 | full |
| **G-FLUSH-2** | one follow-up flush; `flush_coalesced == N−1` | P3-11 | pending erratum (D7) |
| **G-FLUSH-3** | wake via channel W, not at the next tick | P3-11 | pending erratum — **the cause** is asserted; "within microseconds" (V4:1634) is logged, not asserted |
| **G-EPOCH-3/4** | a permanently failing row cannot hold an epoch open; Σ`epoch_rows` ≥ dirtied | P3-11 | full |
| **G-INFL-3** | `m = 1` is strictly serial and oldest-first | P3-11 | full |
| **G-INFL-4** | `m` batches outstanding; drain in ~`1/m` the time | P3-11 | pending erratum — concurrency asserted; the "`1/m` of the wall time" clause (V4:1642) is informational |
| **G-ORD-2** | per-transcode auto-disable | P3-12 | full |

**Already green, must stay green:** G-LAY-2 (`internal/model/layout.go`, gated at
`internal/app/gates_test.go:82`), G-LAY-3 (`internal/app/gates_test.go:189`, mutating
`internal/destinations/layout.go`), G-PTR-1 (`internal/model/layout_test.go`). Also every Phase 1/2
gate, **G-ALLOC-1 above all** (`internal/app/alloc_gate_test.go:184`).

**Owned elsewhere; Phase 3 makes its part runnable:**

| Gate | Phase 3's part |
|---|---|
| G-RACE-2 (V4:1706) | `-race -count=20` over G-FLUSH, G-EPOCH, G-INFL, G-ROW and the G-LIFE-4 shard half (P3-14); G-CFG-3 is Phase 5's |
| G-RACE-3 (V4:1707) | the whole-pipeline stress harness as V4 specifies it (P3-14) |
| G-ASSERT-1 (V4:1763) | **the shard half**: the per-shard `TryLock` hook (P3-11), run in G-RACE-3's harness and in the on-target suite (P3-14, V4:1767-1768). Phase 3 is the only phase that writes `Shard.Run`, and `-race` does not run on AIX (V4:1712-1715) |
| G-AIX-1 assertion 2 (V4:1738, 1749) | maphash at 0 allocs/op **and ≥ 200 MB/s** on the LPAR (P3-14) |
| G-LIFE-4 (V4:1661) | **the shard half**: after the shards return, m batches go on `completionCh[k]` without blocking (P3-13) |
| G-DRAIN-1/2 (V4:1663-1664) | the shard halves, against a fake writer (P3-13) |
| G-LIFE-5 (V4:1662) | picks up F27 and `FatalShardInvariant` automatically — the test loops over `numFatalReasons` (`lifecycle_test.go:337`) |
| G-ORD-4 | **the guard half** (V4:1626 tests the guard's serial compare). Credited to Vol 1 and green through gap detection; tested in P3-06, **not counted** |

> **The count to reconcile.** V4:1516 says Volume 2 owns **25** gates; the list on the same line
> expands to **26** ids: G-LAY-2, G-LAY-3, G-ALLOC-2, G-PTR-1, G-MASK-1, G-MASK-2, G-DIG-1…3,
> G-ORD-1…3, G-FLUSH-1…3, G-EPOCH-1…4, G-INFL-1…4, G-ROW-1…3. G-PTR-1 is also credited to Vol 1,
> G-ROW-1/2 also to Vol 3 (V4:1518), and the headlines sum to 91 against "Eighty-nine" (V4:1501).
> **The 23 is derived from the enumeration** (26 minus the three already green), as Phase 2 derived
> its 16. "23 green" means 23 at the status above — not 23 full.

---

## 3. Dependency graph

```
P3-01 Emit/lastParks race (app) ─────────────────────────────────────────────┐
                                                                             │
P3-02 ring ─────────┐                                                        │
P3-03 FlushBus ─────┴─→ P3-04 Sizing/Params seam, Shard construction,        │
                              ShardInstruments, obs log classes              │
P3-05 digest ────────┐               │                                       │
  (G-ALLOC-2)        │               ↓                                       │
                     │      P3-06 guard + GuardTable (G-ORD-3)               │
                     │               │                                       │
                     └─────────────→ ↓                                       │
                            P3-07 merge (G-ORD-1, G-DIG-3) ──────────┐       │
                                     │                               ↓       │
                                     ↓                     P3-12 EvaluateGuards
                            P3-07b §5A pre-validation        (G-ORD-2, pure) │
                                     │                               │       │
                                     ↓                               │       │
                            P3-08 epoch open + pump + fake writer    │       │
                                  (G-INFL-1, G-MASK-2 shard half)    │       │
                                     │                               │       │
                                     ↓                               │       │
                            P3-09 completion: three outcomes         │       │
                                  (G-MASK-1, G-DIG-1/2, G-INFL-2,    │       │
                                   G-EPOCH-1/2, G-ROW-1/2/3)         │       │
                                     ├──────────────────┐            │       │
                                     ↓                  ↓            │       │
                            P3-10 quarantine     P3-11 Shard.Run     │       │
                                  release        (G-FLUSH-1/2/3,     │       │
                                     │            G-EPOCH-3/4,       │       │
                                     │            G-INFL-3/4)        │       │
                                     └─────────┬────────┘            │       │
                                               ↓                     │       │
                            P3-13 app wiring: shards, both tickers, ←┴───────┘
                                  batchQ/completionCh, drain, fatal
                                               │
                                               ↓
                            P3-14 race / stress / alloc / AIX
```

**Critical path:** 02 → 04 → 06 → 07 → 07b → 08 → 09 → 11 → 13 → 14.

**No cycle between P3-12 and P3-13.** P3-12 is mirror-only (the pure evaluator and a race test that
drives `s.merge` directly); the second ticker and the lifecycle test live in P3-13, which is the card
that builds the `GuardTable` in `app`.

**Parallelisable:** P3-01, P3-02, P3-03, P3-05 from day one; P3-12 as soon as P3-07 merges; P3-10
and P3-11 after P3-09.

---

## 4. The tasks

Each card must be mergeable on `develop` on its own: `go build ./...`, `go vet ./...`,
`go test ./...` green, and **G-ALLOC-1 still zero**. Gate tests follow the house style:
`TestG_<GATE>_<N>_<Claim>` with a SETUP / STIMULUS / ASSERTION / WOULD HAVE CAUGHT block and a
non-vacuity check (`internal/app/alloc_gate_test.go:164-208`).

**Zero-allocation claims are `testing.AllocsPerRun` tests, not benchmarks.** `go test ./...` never
runs a benchmark, and after P3-13 G-ALLOC-1 measures the discard consumer, so a benchmark is the only
thing that would notice `merge`, `pump` or `applyCompletion` allocating — and nothing in CI runs it.
V2:2674-2675 says the zero-allocation gate SHOULD cover them. Benchmarks exist only for numbers
(`b.N` loops; `go.mod` pins Go 1.23.3).

### The writer test double (defined at the consumer, used from P3-08)

`internal/mirror/fakewriter_test.go`, because `mirror`'s tests are its consumer. A smaller one lives
in `internal/app`'s tests from P3-13. **No shared `testutil` package:** it would be a fifteenth
internal package and break `TestEveryPackageHasARule`'s frozen list (`deps_test.go:29-32`).

It obeys the shard side of the boundary exactly (V2:1851-1854, 2822-2823; V3:52-62):
1. Receive `*model.Batch` from `batchQ`.
2. Write **only** `Rows[i].Outcome`, `Rows[i].ErrCode` and `b.Err`.
3. Send on `completionCh[b.Shard]` and **never touch the batch again** (V2:2223-2224).
4. **B-V3**: every row adjudicated, never `Err == nil` with a Pending row (V2:82) — except the
   scripted G-ROW-3 mode.
5. **B-V3b**: return before any backoff (V2:83).

Modes: `allCommitted`; `allFailedErr` (`Err != nil`, rows left Pending); `perRow([]Outcome,
[]ErrCode)` (e.g. Rejected with 1438); `unreportedRow(i)`; `holdUntil(chan)`; `latency(d)`. It also
records the maximum number of batches it held at once, which G-INFL-3/4 assert on. Its self-test
asserts it writes nothing outside (2); under `-race` a sloppy double becomes a detected race.

---

### P3-01 · Close the `obs.Emit` and `lastParks` race

**Scope.** Pre-existing (memory note `obs-emit-concurrent-race.md`), not Vol 2 work — but Phase 3
makes it blocking: P3-13 adds guard evaluation and `batchq_depth` sampling to the stats goroutine,
P3-13/14 run full lifecycles under `-race`, and §7's race lines cannot be ticked while it is open.

The two callers of `sampleGauges()` + `Reg.Emit(...)`:
- the stats tick, `internal/app/lifecycle.go:456-457`;
- `logShutdownInitiated`, `:573, :575`, on the Run goroutine.

They overlap because teardown stops stats last (`:634-638`). Unsynchronised: `Emit`'s accumulator
state (`internal/obs/emit.go`), the `Max.LoadAndReset` split, and the plain field `g.lastParks`
(`lifecycle.go:474-476`, declared `graph.go:222`).

**Fix (KISS):** one `emitMu sync.Mutex` on `Graph`, held by **both** callers around
`sampleGauges()` + `Reg.Emit(...)`. **Every later stats-goroutine activity goes under the same
lock** — the guard-evaluator tick and `batchq_depth` sampling (P3-13). Not inside `obs`, whose
contract is "obs owns no goroutine" (`lifecycle.go:427-430`).

**Files.** `internal/app/lifecycle.go`, `graph.go` (the field), `lifecycle_test.go`.

**Tests.**
- Regression: stats driven at a small interval concurrently with `logShutdownInitiated` in a loop,
  **with traffic flowing** so per-shard counters and gauges are written concurrently; `-race
  -count=50` with `CC` at the full clang path (memory note `feedvalve-toolchain-gotchas.md`).
- Adversarial: summed `qd_wake_parked` across both emitters equals `Parks()` — a double-count the
  race detector would not see.

**Defects closed.** The `Emit` race and the `lastParks` race.

**Not in this card.** `clock_step_detected`'s comparison and `Logger.SetLevel`'s plain field —
Phase 5 (`phase2-tasks.md:438-441`).

**Gates.** None owned; unblocks G-RACE-1/2. **Depends on.** — **Rough:** 0.5–1 day.

---

### P3-02 · `mirror.ring`: a genuine circular `int32` buffer

**Scope.** V2 §1.3 (V2:245-284), §8.6 (V2:2233-2255), §11.4, A9, ADR-L21.
- `buf []int32, head, n`; `cap(buf)` **always a power of two** (V2:251), so indexing is a mask.
- `PushBack`, `PopFront`, `Len`, `Reset`, `Peek`; `Swap(&other)` O(1).
- `PushFrontBlock(ss []int32)` O(k), **preserving relative order** (V2:2246-2248).
- Growth doubles and re-linearises, and **reports** that it grew; the caller counts
  `ring_grown{k,d}`. The ring has no instruments.

**Files.** `internal/mirror/ring.go`, `ring_test.go`.

**Tests.**
- Property (seeded `math/rand`): 10⁵ operations against a plain-slice model, including growth while
  wrapped.
- Adversarial: `PushFrontBlock` with `head == 0` on a full buffer forces growth mid-block;
  one-at-a-time front pushes reverse order, the block form does not.
- `AllocsPerRun` = 0 for push/pop/swap below capacity.
- Capacity stays a power of two after every growth; a pre-size of 50,000 gives 65,536 (§6, V2 §10.3
  erratum).

**Defects closed.** The O(n) head insertion of a slice "queue" (ADR-L21); the §11.4 outage-crash path.

**Not in this card.** Any `Shard` field or instrument.

**Gates.** None alone. **Depends on.** — **Rough:** 1–1.5 days.

---

### P3-03 · `mirror.FlushBus`: the level plus the cap-1 edge

**Scope.** V2 §3 (V2:624-835), A8, ADR-L20.
- `FlushBus{req []atomic.Bool; wake []chan struct{}}`; each `wake[k]` capacity 1, **never closed**
  (V2:832-835).
- `Trigger` over every j, **including the caller**: `prev := req[j].Swap(true)`; if `prev`, count
  `flush_coalesced`; non-blocking send on `wake[j]`, else `flush_wake_dropped{j}`.
- `Taken(k)`, `Pending(k)` (V2:656-681). Pre-resolved handles; the bus does no lookups.

**Files.** `internal/mirror/flushbus.go`, `flushbus_test.go`.

**Tests.**
- A self-trigger from a goroutine not in `select` does not block (V2:745-786).
- One test per row of V2:738-743's edge/level table; `Taken` clears only `req[k]`.
- Race: S goroutines calling `Trigger` against per-shard `Pending`/`Taken`, `-race -count=20`.
- `AllocsPerRun` = 0 for `Trigger`.

**Defects closed.** The mechanism half of **L1** (V0 §0.2); stage B lands in P3-11.

**Not in this card.** Calling `Trigger` from merge (P3-07). **The router never calls `Trigger`**
(V2:827-831).

**Gates.** None alone. **Depends on.** — **Rough:** 1 day.

---

### P3-04 · The `Sizing`/`Params` seam, `Shard` construction, `ShardInstruments`, log classes

**Scope.** V2 §1.1–§1.4 (V2:98-346), A4/A5.
- The `Shard` struct (V2:113-170) **plus** `tickPending` and `maxBatchSize`, which §1.1 omits but
  V2:471, 1586, 2839 use.
- `arenaChunk{e [arenaChunkRows]model.Entry}`; `allocEntry(d) int32`, a bump allocator with an
  explicit zero (V2:223-231).
- `Index[d]` pre-sized; arena chunk 0; `Accum[d]`/`Cohort[d]` rings at `expectedRows/shards`.
- `Free` of `maxInFlightPerShard` batches (`Rows` at `maxBatchSizeCeiling`), plus **`all
  [m]*model.Batch` and `sentAt [m]int64`** on `Shard`. A batch's index is found by a linear scan of
  `all` (m = 2 by default). `Free` is a stack and batches return out of order, so a free-list
  position is not an identity; **no field is added to `model.Batch`**.
- Preallocated `failedRows[d]` scratch at the ceiling (V2:2235).

**The config seam (D1).** `mirror` declares `Sizing` (startup: shards, `expectedRows`,
`arenaChunkRows`, `maxInFlightPerShard`, `maxBatchSizeCeiling`, the `maphash.Seed`) and `Params`
(reloadable: `Gen`, `Window`, `MaxBatchSize`, `EpochStallWarn`, `DrainTimeout`, `ordering.*`) —
only fields `mirror` reads. `NewShard` takes a `func() *mirror.Params`; `cfgGen` is initialised from
the first `Params` it loads, so the first epoch does not "release" an empty quarantine (generations
start at 1, `config/store.go:17-20`). The production loader is wired in P3-13 (D1).

**Instruments.** `mirror.ShardInstruments` holds every handle Vol 2 writes: A11–A25 (V2:2904-2918),
the Vol 0 set at V2:2920-2927, `shard_loop_max_ns{k}`, `shard_depth{k}`, `shard_depth_max{k}`, and
the three §5A instruments. `app` resolves them inside `resolveInstruments`
(`internal/app/instruments.go:173`), **before `reg.Freeze()`** (`internal/app/run.go:179, 184`) —
the defect recorded at `instruments.go:158-168`. Per-transcode handles (`guard_offered`,
`guard_superseded`, `ordering_guard_disabled`, `blob_len_mismatch`,
`prevalidate_rejected_transcode`) are resolved in `resolveTranscodeInstruments` (`:295`), indexed by
configured position (D2).

**Log classes (`internal/obs/class.go`).** V4 §6.2 (`04:1356-1357`) lists no class for four Vol 2
events. Add:
- WARN `epoch_stalled` (V2:466);
- ERROR `blob_len_mismatch`, rate-limited (V2:933);
- ERROR `guard_disabled` (V2:1361);
- ERROR `drain_deadline` (V2:549).

Inserting a WARN moves `firstErrorClass` and relabels the `log_events_suppressed{class}` slots, so
update `TestClassSeverityBoundary` (`internal/obs/allow_test.go:22`) and
`TestClassTableMatchesTheEnum` (`internal/obs/dim_test.go:135`; `class.go:61` calls it
`TestClassTableMatchesEnum`) **deliberately**, in this card. V4 §6.2
erratum (§6).

**Files.** `internal/mirror/shard.go`, `params.go`, `instruments.go`, tests;
`internal/obs/class.go` + tests; `internal/app/instruments.go` (resolution only);
`internal/model/layout_test.go` (`TestMemoryFigures`).

**Tests.**
- INV-ZERO: a fresh `allocEntry` is zeroed even after a prior entry in the chunk was dirtied
  (V2:238-243).
- INV-SLOT: slot indices stable across chunk growth; `arena_chunks{k,d}` (a gauge, `ledger.go:212`)
  **stores** the new chunk count.
- Index pre-size: **no bucket evacuation** below `expectedRows/shards` — not zero allocations across
  50,000 inserts, since Go 1.23 maps pre-allocate only a few overflow buckets (V2:2625).
- Resolving a `ShardInstruments` after `Freeze` panics, pinned against the recorded defect.
- §10 memory bound: extend `TestMemoryFigures` (`internal/model/layout_test.go:118`) with the ring
  (power-of-two), index and chunk-quantisation figures at the **default** sizing (its constant uses
  400,000 rows, `:120`), and measure what `NewShard` allocates at defaults.
- The `batchSentAt` scan: m batches returned in reverse order each resolve to their own `sentAt`.
- G-DEP-1 stays green with `mirror` importing only `model`, `obs`, `destinations`.

**Defects closed.** The `config` → `oradb` transitive import, without relaxing the rule.

**Not in this card.** Merging, the loop, goroutines. Build stays allocation-only.

**Gates.** None owned; G-LAY-2/G-PTR-1 stay green. **Depends on.** P3-02, P3-03.
**Rough:** 2–3 days.

> **Name things the code's way.** The LLD's `Fail` is `Entry.Quarantined`; its
> `destinations.Table` is `*destinations.Set` (`Deps.Dests`).

---

### P3-05 · The digest helper and G-ALLOC-2

**Scope.** V0 §5.2; V2 §4 step 7 (V2:997), `fillRow` step 4. `digest(seed, img) =
nonZero(maphash.Bytes(seed, img))`, with `nonZero(h)` mapping 0 to 1. One process-wide seed, created
by `app`, passed as an argument (so no dependency on P3-04).

**Files.** `internal/mirror/digest.go`, `digest_test.go`.

**Tests.**
- **G-ALLOC-2:** `AllocsPerRun` over `digest(seed, img[:181])` = 0 (V4:1596).
- `nonZero(0) == 1`, `nonZero(h) == h` otherwise — tested **directly**; a seed search for a zero
  hash is a 2⁶⁴ search.
- The digest depends only on `img[:ImageLen]` (G-DIG-3's precondition).

**Defects closed.** OQ-L3's dev-host half.

**Not in this card.** The FNV-1a fallback — built only if the LPAR measurement fails (YAGNI).

**Gates.** **G-ALLOC-2.** **Depends on.** — **Rough:** 0.5 day.

---

### P3-06 · The ordering guard and the `GuardTable`

**Scope.** V2 §5 (V2:1155-1472), FR9's defensive half.
`guard(e, c, u, d) verdict` → `Apply` / `Supersede` / `Bypass` (V2:1236), first match wins
(V2:1240-1267):
1. **Bypass** if the guard is off, `GuardOff[code]`, `FlagOrderingUnusable`, or `!SlotSeen`.
2. Count `guard_offered{transcode}`.
3. Token-scoped: `BCSeqNo` by **serial** `int32(a-b)`; general: `LogTime` **plain** signed.
4. `LogTime` secondary; 5. `RecvMono >` as the final tiebreak.

**`GuardOff` storage (D2).** `decode.RegistryEntry` has no `GuardOff` (`internal/decode/decoder.go`,
unlike V0:838), and the registry is immutable and swapped on reload (`registry.go:44-47`).
`mirror.GuardTable`: one entry per configured transcode, `{code; off atomic.Bool; offered,
superseded *obs.Counter}`, a linear scan over ≤ 11 codes. Its counters arrive pre-resolved (P3-04);
P3-13 builds it by zipping them with `reg.Codes()`. Outside the registry, a swap cannot reset it —
G-CFG-4's intent; Phase 5 points `RegistryEntry.GuardOff` into it.

**Files.** `internal/mirror/guard.go`, `guardtable.go`, tests.

**Tests.**
- **G-ORD-3:** N distinct rows, one contribution each: every call Bypasses on `!SlotSeen`;
  `guard_offered == 0`. Held at the guard; the gate's "Merge all N" stimulus (V4:1625, the
  "entry was created by THIS contribution" path of V2 §5.3 step 4) is re-run through `merge` in P3-07.
- **G-ORD-4 guard half** (not counted): `LastSeq = 0x7FFFFFFF`, `BCSeqNo = 0x80000000` → `Apply`;
  a `LogTime` pair straddling the sign bit compares **plainly** (V4:1626).
- Property: serial compare antisymmetric for `|a-b| < 2³¹`; a serial/plain swap mutant fails a row.
- Each Bypass condition alone and in pairs, pinning first-match order.

**Defects closed.** FR9's defensive half; HLD §6.5's global ratio avoided by construction.

**Not in this card.** The evaluator; any write to `off`.

**Gates.** **G-ORD-3.** **Depends on.** P3-04. **Rough:** 2 days.

---

### P3-07 · `merge(u model.UpdateUnit)`

**Scope.** V2 §4 (V2:837-1153), ten steps (V2:864-1065):
1. Resolve or create the entry (`rows_created{d}`).
2. Guard; on Supersede, touch nothing and count `units_superseded_out_of_order` — **here, not in
   the guard** (V2:893-895; V2:1267's footer is erratum'd, §6).
3. Write `LastSeq`/`LastTime`/`LastRecv` **together**; set `SlotSeen`.
4. Blob width check — mismatch counts `blob_len_mismatch{code}` and merges **nothing**. Otherwise
   copy masked columns low bit to high, `Column.Width` each, to `Column.Offset`
   (`internal/model/unit.go:3-12`). P3-07b inserts its per-field check here.
5. `ColMask |= c.Mask`.
6. Quarantined → `merge_into_quarantined{d}`, continue.
7. NFR13 suppression, **rate-limited units only**, consulting `Written` (ADR-L04). For a
   non-rate-limited unit the predicate is skipped entirely (V2:995-1000).
8. Place: InFlight → `ReDirty`; Dirty → no-op; else `Dirty` + `Accum[d].PushBack` (`ring_grown`).
9. `Trigger` iff non-rate-limited **and** something was dirtied (V2:1053-1056).
10. `units_applied`/`units_merged`.

INV-MASK (V2:971-973) and INV-STATE (V2:1081-1085) hold after every step.

**Files.** `internal/mirror/merge.go`, `merge_test.go`.

**Tests.**
- **G-ORD-1:** a destination-2 row (`market_type = 1`), 6531, 6571, 6511 at `LogTime == T`; each
  setup ends at 6511 with `guard_superseded == 0`. **Two setups, because each mechanism is
  load-bearing in a different one** — no single input makes both fail:
  - **R (the `RecvMono` tiebreak):** **every contributor slot pre-seeded with `LastTime == T`**,
    `LastRecv = r0` (an earlier re-open in the same second, V2:1446-1450); arrivals at `r0+1, r0+2,
    r0+3`.
    Without the pre-seed a fresh row lands Trace B with no tiebreak at all (V2:1439-1441).
    Non-vacuity **by input**: every `RecvMono = r0` (the tiebreak can no longer discriminate) → must
    fail.
  - **P (the per-slot array):** a fresh row, all three with the **same** `RecvMono` — Trace B, every
    contribution applies through its own slot (V2:1433-1441). Non-vacuity **by input**: collapse the
    three onto one slot via `Contribution.Slot` → the second and third supersede (Trace A,
    V2:1419-1423) → must fail.

  Collapsing slots in setup R would **not** fail: with a strictly rising `RecvMono` the tiebreak
  alone applies every contribution in arrival order (V2:1450). V4:1623's "either one alone fails this
  test" is therefore true of the pair of setups, not of one (§6 erratum).
- **G-DIG-3:** `message_text` 200 → 20 bytes: the 128-byte region fully rewritten with a zero tail;
  the short value's digest is independent of the long value's tail (V4:1622).
- **G-ORD-3 through `merge`** (V4:1625's stimulus; the gate is P3-06's): N distinct rows from one
  transcode, each entry created by its own contribution at step 1 → `guard_offered == 0`,
  `rows_created == N`.
- `units_superseded_out_of_order` +1 on **each** Supersede, and exactly once (the guard does not
  count it; §6 erratum to V2:1267).
- **§12.3 P1+P2 together** (V2:2856): one test that a mid-flight merge survives completion (INV-MASK)
  **and** that the committed digest is the batch's, not a recomputation — a fix to one that leaves
  the other must fail it. (Driven through P3-09's completion once it exists; until then the merge
  half, with the test extended in P3-09.)
- Property: INV-STATE over random merge sequences with `InFlight` toggled.
- Adversarial:
  - `BlobLen` one byte short: image untouched, `blob_len_mismatch`++.
  - Quarantined row: `ColMask` updated, nothing enqueued.
  - A non-rate-limited unit whose contributions were **all superseded**, or **all landed on
    quarantined rows**, does not call `Trigger`.
  - A non-rate-limited unit whose image digest matches is **still dirtied** (no suppression).
- `AllocsPerRun` = 0 for `merge` on an existing row (V2:1138-1141).

**Defects closed.** R14 at the merge; ADR-L04's permanent-NULL defect at the predicate.

**Not in this card.** Pre-validation (P3-07b); epoch logic.

**Gates.** **G-ORD-1, G-DIG-3.** **Depends on.** P3-03, P3-05, P3-06. **Rough:** 3 days.

---

### P3-07b · Vol 0 §5A pre-validation at merge, per contribution

**Scope.** V0 §5A (V0:1646-1710), ruling D12. "Validate at merge, per contribution — not at pump,
per row" (V0:1677). The field is refused **before it is written into the image, so the rest of the
row stays writable** (V0:1679-1682). The DDL facts are already on the column:
`SQLPrecision`, `SQLMaxBytes`, `Domain` (`internal/destinations/column.go:93-99`).
- In step 4, per masked column, check the incoming wire-form bytes against the column's facts:
  `precision` (scaled numerics vs `NUMBER(p,s)`), `length` (text valid length vs `SQLMaxBytes`),
  `domain` (`KindChar1` vs `Domain`), `time` (`KindTime1980` out of range), `nonfinite`
  (`KindDouble` NaN/Inf). A column with no fact is unchecked.
- A refused field: its bytes are not copied and its bit is **cleared from that contribution's
  effective mask** before step 5, so `ColMask` never claims it. Count
  `prevalidate_rejected{d,reason}` and `prevalidate_rejected_transcode{code}`. The rest of the
  contribution merges normally.
- `rows_rejected_by_oracle_unpredicted{d}`: provide `predictable(errCode) bool` (ORA-01438,
  ORA-12899, a `CHECK` violation); P3-09's Rejected arm counts a Rejected row whose code is
  predictable — a row Oracle refused that pre-validation passed.
- `NotNull` is **advisory** (V0 §5A.2): not checked here; see §6.
- Pre-validation is defence in depth (V0 §5A.4): P3-09's quarantine stays in full.

**Files.** `internal/mirror/prevalidate.go`, `prevalidate_test.go`; `merge.go` (the step-4 call).

**Tests.**
- One table row per reason, each against a column carrying that fact; a column without the fact
  passes the same value.
- A contribution with one bad and one good column: the good column lands, the bad one's image bytes
  and `ColMask` bit are untouched, the row is dirtied for the good one.
- A contribution whose **only** column is refused dirties nothing and does not `Trigger`.
- `AllocsPerRun` = 0 for `merge` with pre-validation on.
- (In P3-09) the fake writer's `perRow(Rejected, 1438)` on a row that passed pre-validation →
  `rows_rejected_by_oracle_unpredicted`++; with ORA-00001 → not counted.

**Defects closed.** A mapping-predictable refusal costing a serial row-by-row re-run and a quarantine.

**Not in this card.** The catalogue check (V0 §7.6, Phase 4) that keeps the facts honest.

**Gates.** None (instruments only). **Depends on.** P3-07. **Rough:** 1.5–2 days.

---

### P3-08 · Epoch open, `pump` and `fillRow`

**Scope.** V2 §6.3 and §7 (V2:1474-1957).

`maybeOpenEpoch() error`, **in this order** (V2:1551-1596):
1. Clear `tickPending`.
2. Every ring empty → `epoch_empty`, return, **leaving `req` set**.
3. `Taken()`.
4. Any cohort non-empty → `cohort_not_empty`, return an error.
5. Swap `Accum[d]`/`Cohort[d]`.
6. Set the epoch fields.
7. **Load `Params` once** (D3); `Gen` changed → `releaseQuarantine` (stub here; P3-10 fills it).
   Refresh `s.guard` from the same load: P3-07's merge reads `ordering.guard` from that field, which
   `NewShard` sets and nothing else does yet.
8. `epoch_opened`; publish the depth gauges.
9. `pump()`.

`pump()` (V2:1726-1763): precondition `EpochOpen`; postcondition `len(Free) == 0` or every cohort
empty. Destinations 0, 1, 2 against **one** `maxBatchSize` budget. Non-blocking `batchQ` send;
`CohortOut++` **only after** it succeeds; `default` counts `flush_deferred` and returns an error
(S1). Record `sentAt[i]`, `i` found by scanning `all`.

`fillRow` (V2:1769-1781): copy `Mask` **before** `ColMask = 0`; digest here; copy the 256-byte image;
the four rules of V2:1809-1833. **No sorting.**

**Files.** `internal/mirror/epoch.go`, `pump.go`, tests, `fakewriter_test.go`.

**Tests.**
- **G-INFL-1:** fake in `holdUntil`. Merge one row repeatedly across an open epoch: exactly one
  outstanding batch, later updates set `ReDirty`, ring length unchanged.
- **G-MASK-2, shard half:** two transcodes, disjoint columns, one row: `BatchRow.Mask` is the union;
  unmentioned image bytes keep their prior values.
- Step 2/3 order: empty rings + `req` set → `epoch_empty`++, `req` still set.
- S1: a full `batchQ` → error and `flush_deferred`++.
- `cohort_not_empty`: push into `Cohort[d]` behind the API → error.
- `AllocsPerRun` = 0 for `pump` (V2:2674-2675).

**Defects closed.** HLD §7.3's ordering defect; **R14 at fill** (`ColMask` cleared at pump).

**Not in this card.** `closeEpoch`, `applyCompletion`, the loop.

**Gates.** **G-INFL-1, G-MASK-2** (shard half). **Depends on.** P3-07b. **Rough:** 3 days.

---

### P3-09 · `applyCompletion` and `closeEpoch`: the three outcomes (closes L3)

**Scope.** V2 §8 (V2:1959-2297), §6.4, ADR-L22.

Step 0 (V2:2005-2027): **0a** wrong shard or future epoch → error; **0b** `Err != nil` → every
Pending row Failed; **0c** `Err == nil` with a Pending row → `batch_row_unreported{k}`++, Pending →
Failed, rows requeued at the head (V2:2022-2026), and a **distinguishable** error (S3) — `Run`
reports it rather than returning (P3-11, D9).

Per-row walk: assert `InFlight`, else `inflight_desync{k}` and error (S2) (V2:2054-2064). Arms
(V2:2087-2192):
- **Committed:** `Digest = r.Digest`, `Written |= Mask`, `ColMask` untouched.
- **Failed:** `ColMask |= Mask`, into `failedRows[d]`.
- **Rejected:** quarantine, `row_quarantined{d}`++, `rows_quarantined_current{d}`, V0-Δ1's
  disjoint-mask release (V2:2141-2187); `rows_rejected_by_oracle_unpredicted` via P3-07b's
  `predictable`. **Not `row_rejected`** — that is the writer's (D11; V2:2148 is erratum'd).

Batch return (V2:2211-2220): reset `Rows[:N]` **before** `N = 0`; push to `Free`; `CohortOut--`;
`completion_lag_max_ns` from `sentAt[i]`. **Then the block head-push (§8.6 step 14), and only then
step 13** (`closeEpoch` or `pump`) — D8. `closeEpoch` re-opens immediately without recursion
(V2:521-523).

**Files.** `internal/mirror/completion.go`, `quarantine.go` (the reject arm, `row_quarantined`,
`rows_quarantined_current`), tests.

**Tests.**
- **G-MASK-1:** in flight → merge the same column → Committed: next batch carries it, newer value.
- **G-DIG-1:** batched → re-merged in flight → Committed: `Entry.Digest == BatchRow.Digest ≠
  digest(entry.Image)`.
- **G-DIG-2:** the row **starts with `Digest != 0`** (an earlier commit), and the new column's
  first-ever value equals the zero fill with its bit not in `Written`. Not suppressed; the batch
  carries the bit; on commit `Written` gains it. From `Digest == 0` the short-circuit at V2:996
  decides and the `Written` clause is never exercised.
- **G-INFL-2:** merged in flight → complete → dirty again with the newer image.
- **G-EPOCH-1:** N rows over several `maxBatchSize` batches, fake `allCommitted`: all written before
  close, `ceil(N/maxBatchSize)` batches, one `Epoch`.
- **G-EPOCH-2:** a mid-drain merge lands on `Accum`, absent now, present next epoch.
- **G-ROW-1:** Committed / Rejected 1438 / Failed. Rejected leaves `Digest`/`Written`/`ColMask`
  unchanged, is not requeued, sets `Quarantined`, `row_quarantined`++. Failed: `ColMask |= Mask`,
  head requeue, `retry_requeued`++.
- **G-ROW-2:** `Err != nil`, all Pending → all requeued at the head **in original order**.
- **G-ROW-3:** `Err == nil`, one Pending → `batch_row_unreported{k}`++, rows requeued first,
  non-nil error.
- §12.3 P1+P2 combined test (from P3-07) completed through the real completion path.
- Adversarial: D8 (a completion that empties the cohort with Failed rows — they head the **next**
  epoch's first batch); 0a both ways; S2 by clearing `InFlight` behind the API; V0-Δ1 both ways.
- `AllocsPerRun` = 0 for `applyCompletion` (V2:2718-2719) over the Committed and Failed arms. The
  Rejected arm's `Quarantine[d] = append(…)` (V2:2147, A4: "expected empty") may grow a slice — the
  one allocation the completion path is allowed, on a path that already costs a quarantine.

**Defects closed.** **L3**; **L4's** fallback branch; HLD §7.12 risk 9 via the failure path.

**Not in this card.** Release on `Gen` (P3-10).

**Gates.** **G-MASK-1, G-DIG-1, G-DIG-2, G-INFL-2, G-EPOCH-1, G-EPOCH-2, G-ROW-1, G-ROW-2,
G-ROW-3.** **Depends on.** P3-08. **Rough:** 4 days.

---

### P3-10 · `releaseQuarantine` on a configuration generation change

**Scope.** V2 §9.5 (V2:2418-2460), ADR-L23. Fill P3-08's step-7 stub: when `Params.Gen` has
advanced, release every quarantined row — re-dirty, push to `Accum`. **This card owns only
`quarantine_released{d}`**; `row_quarantined` and `rows_quarantined_current` are P3-09's.

**Files.** `internal/mirror/quarantine.go`, tests.

**Tests.**
- Quarantine a row; the test's `func() *mirror.Params` returns `Gen + 1`; open an epoch: released,
  re-dirtied, in the cohort; `rows_quarantined_current` back to 0.
- Unchanged `Gen` releases nothing; a fresh shard's first epoch releases nothing (`cfgGen` seeded).
- Two `Gen` bumps between epochs release once; a released row rejected again re-quarantines.

**Defects closed.** L4's release half.

**Not in this card.** SIGHUP (Phase 5); automatic release (OQ-21).

**Gates.** None owned. **Depends on.** P3-09. **Rough:** 1–1.5 days.

---

### P3-11 · `Shard.Run`: the loop, the tick arm, the drain (closes L2)

**Scope.** V2 §2 (V2:348-622), ADR-L24. `Run() error` — **no context argument** (D5).

Stage A `select`: unit; completion; tick; wake (counts `flush_wake_received`); drain expiry. Stage B
after every case: `!EpochOpen && (draining || tickPending || req[k])` → `maybeOpenEpoch`, then the
quiescence check (V2:481 is right; V2:409 omits `tickPending`).

`onTick` (V2:461-471): epoch open → `TickMissed`, `tick_skipped_epoch_open`, and the stall check
against `Params.EpochStallWarn` loaded at the tick (D3), which also refreshes `s.guard` (P3-07) — WARN once per epoch (P3-04's class),
`epoch_stalled`++, **never** clear `req`. Otherwise set `tickPending`. `flush_on_tick{k}` is counted
where `maybeOpenEpoch` opens with `tickPending` set. The stall check reads an **injectable monotonic
clock** on `Shard`, because config forbids `epochStallWarnMs` below 1 s (`validate.go:69`).

Each pass: `shard_depth{k}` stores `len(s.in)`; `shard_depth_max{k}` and `shard_loop_max_ns{k}`
observe.

Drain (V2:525-564; D4): `in` closes → nil the arm, set `draining`, **and only now** copy the shared
`drainExpired` into the local select arm (nil until then). With `drainTimeoutMs = 0` (legal,
`validate.go:68`) an expired deadline therefore cannot pre-empt units still buffered in Q1.
`quiescent()` = `!EpochOpen ∧ CohortOut == 0 ∧ every Accum empty ∧ len(Free) == m` → return nil. On
expiry: `drain_abandoned_rows{d}` = `Accum + Cohort` lengths (teardown step 8, V4:261), ERROR only if
non-zero, return nil.

**S3 is reported, not returned (D9).** `mirror` cannot call `app`'s `Fatal`, and `Run` must keep
going, so `NewShard` takes one `report func(error)`: on S3's error `Run` calls it once and continues
the loop; every other error (S1, S2, `cohort_not_empty`, 0a) returns. `app` supplies the function
that maps it to F27 (P3-13); tests supply a recorder. A function value, not an interface — one real
caller and one test double.

**G-ASSERT-1, shard half:** under `-tags feedvalve_assert`, a per-shard `sync.Mutex` whose `TryLock`
wraps the loop body; a failed `TryLock` panics (V4:1763-1767). The production build compiles it out.

**Files.** `internal/mirror/run.go`, `assert_on.go`/`assert_off.go`, `run_test.go`. Tests drive the
tick channel directly; the real ticker is P3-13.

**Tests.**
- **G-FLUSH-1:** empty rings, `req[k]` raised, no tick: one unit → an epoch opens without a tick;
  `epoch_empty` did not consume the request.
- **G-FLUSH-2:** one batch held; N requests during it → exactly one follow-up epoch;
  `flush_coalesced == N−1` on a one-shard bus, `(N−1)×S` on an S-shard bus whose other shards are
  idle (D7).
- **G-FLUSH-3:** shard parked, tick channel **never fed**; `Trigger` from another goroutine → the
  wake arm is taken, `flush_wake_received{k}`++, an epoch opens. Asserts the cause; elapsed time is
  logged, not asserted (flakes under `-race -count=20`).
- **G-EPOCH-3:** `perRow(Failed)` for one row, ticks every 10 ms: each epoch closes, the row heads
  the next cohort, `epoch_opened` keeps rising.
- **G-EPOCH-4:** mixed workload, M intervals: `Σ epoch_rows{k}` ≥ `rows_dirtied`; an
  `Accum`/`Cohort` swap mutant fails.
- **G-INFL-3:** `m = 1`: the fake's max-held is 1; dirty order monotone across batches.
- **G-INFL-4:** `m = 2`, `latency(20ms)`: the fake's max-held is 2 and never exceeds `m`; wall time
  vs `m = 1` is logged, with only a loose bound (< 1.0×).
- `epoch_stalled`: the injected clock advanced past `EpochStallWarn` with an epoch open → one WARN,
  one increment per epoch.
- Drain: quiescent return with full `Free`; `drainExpired` closed with dirty rows and an outstanding
  batch → per-destination abandoned counts, nil; `drainTimeoutMs = 0` with units still in `in` →
  every unit merged before the deadline arm is reachable.
- Adversarial: a tick during an open epoch leaves `req[k]` set; a stale wake token after `Taken`
  produces one pass and nothing else.
- S3 inside `Run`: the fake's `unreportedRow(i)` → `report` called exactly once, `Run` does **not**
  return, the requeued rows are in the next epoch's first batch, and a later `in` close still
  reaches a quiescent return. S1/S2 by contrast return their error.
- `-tags feedvalve_assert`: two goroutines entering one shard's body panic.

**Defects closed.** **L2**; **L1's** level reading; HLD §7.12 risks 9 and 10.

**Not in this card.** Goroutine start, recovery, fatal mapping, tickers (P3-13).

**Gates.** **G-FLUSH-1/2/3, G-EPOCH-3/4, G-INFL-3/4.** **Depends on.** P3-09.
**Rough:** 3.5–4 days.

---

### P3-12 · `EvaluateGuards`: the ordering-guard evaluator (pure)

**Scope.** V2 §5.5 (V2:1345-1397). `EvaluateGuards(table, params, *snapshot)` in `mirror`: per
`GuardTable` entry, deltas of `offered`/`superseded` since the last *actual* evaluation (the
snapshot advances only when one happens). If `ordering.guard = auto`, Δoffered ≥ `guardMinSamples`
and Δsuperseded/Δoffered **`>`** `guardSuppressionRatio` (V2:1358; a ratio of exactly 1.0 is legal,
`validate.go:256`, and therefore never disables): `off.Store(true)`, gauge
`ordering_guard_disabled{transcode}` = 1, ERROR (P3-04's class). One-way; the evaluator is the
**only** writer of `off`. **No ticker here** — P3-13 owns it.

**Files.** `internal/mirror/guardeval.go`, tests.

**Tests.**
- **G-ORD-2:** two transcodes on one destination, one with constant `BCSeqNo`, one ascending, mixed
  so the **global** ratio stays below 0.5; past 100 samples, one evaluation: only the constant one
  disables, its rows resume updating, the gauge is 1 for it only.
- Adversarial: below `guardMinSamples` never disables; ratio exactly equal to the threshold does not
  disable; a disabled guard stays disabled; `ordering.guard: on` never disables; a skipped interval
  does not advance the snapshot.
- Race: a goroutine calling `EvaluateGuards` while another drives `s.merge` directly, under `-race`.

**Defects closed.** HLD §6.5's global-ratio defect; FR9's auto-disable.

**Not in this card.** The ticker and the 50 ms lifecycle test (P3-13); G-CFG-4; re-enabling.

**Gates.** **G-ORD-2.** **Depends on.** P3-07. **Rough:** 1–1.5 days.

---

### P3-13 · `app` wiring: shards, tickers, `batchQ`/`completionCh`, the drain, the fatal path

**Build** (step 12, allocation only; `internal/app/graph.go`):
- `mirror.Params` derived from `config.Config` in one function and stored **inside `app.Runtime`**
  (`internal/app/runtime.go`) — one published pointer (ADR-L11), not a second `atomic.Pointer`.
  Shards get `func() *mirror.Params { return g.loadRuntime().Mirror }`.
- `NewShard` per k; the `FlushBus`; the `GuardTable` zipped from the pre-resolved per-transcode
  handles (P3-04) and `reg.Codes()`, right where Build already asserts `reg.Len()` against the
  configured count (`graph.go:310-319`); the seed.
- `batchQ` cap `shards × maxInFlightPerShard`; `completionCh[k]` cap `maxInFlightPerShard` — **load
  bearing** (V2:554-560; V4:1661).
- One `drainExpired chan struct{}`.

**Run:**
- Shards replace `startShardConsumers`, keeping the stage name `"shard-consumers"` so
  `lifecycle_test.go:127-128` keeps meaning what it says.
- A `"flush-ticker"` stage: one `time.Ticker` at `Params.Window`, a non-blocking send per
  `tickCh[k]` (cap 1), `tick_send_dropped{k}`.
- In `startStats`, a **second ticker** at `ordering.guardEvalIntervalSec` calling `EvaluateGuards`,
  and `batchq_depth` = `len(batchQ)` in `sampleGauges` — both under P3-01's `emitMu`.
- Each shard goroutine recovers a panic once, logs the stack, `g.Fatal(FatalShardPanic)` (F26). Errors
  map per D9: S1 → `FatalBatchQSendFail`; S2, `cohort_not_empty`, 0a → **`FatalShardInvariant`**
  (new) with a `detail` naming the counter, and return. **S3 → `Fatal(FatalBatchRowUnreported)`
  (F27, new), delivered through the shard's `report` function (P3-11), and the shard keeps running**
  into the normal drain — its state is consistent after
  the requeue, so its rows can still be written and its Q1 still drains.

**Teardown:**
- The deadline is armed at **`t_shutdown`** (`g.drainStarted`, set in `logShutdownInitiated`,
  `lifecycle.go:564`): `time.AfterFunc(drainTimeout, close(drainExpired))` (V4:260). Stopped if every
  shard is quiescent first.
- Step 6 is already in place: `stopShardConsumers` (`shard.go:120-133`, called at
  `lifecycle.go:621-628`) closes every `shardCh[k]` (V4:259) and then waits on `shardDrainDone`.
  With live shards that wait **is step 7** — the shards draining under the shared deadline (V4:260)
  — so the stage keeps its place and gains no new call. Step 8 is inside each shard (P3-11). The
  flush ticker stops once the shards have returned (its sends are non-blocking, so order is not
  load-bearing). `batchQ` close stays Phase 4.
- `shard_loop_max_ns{k}` into `stallFatal`'s line (`internal/app/fanout.go:183-187`).

**G-ALLOC-1 stays at zero.** The discard consumer moves into `alloc_gate_test.go` as
`startDiscardConsumers`, closing `shardDrainDone` the same way; the fixture uses it
(`alloc_gate_test.go:127-128`). "The throwaway consumer is test infrastructure"
(`phase2-tasks.md:42-44`). `AllocsPerRun` counts every goroutine, so a live shard's map inserts and
chunk allocation on fresh tokens would pollute the receive-path gate.

**`testGraph` sizing.** Each `Build` at defaults now allocates ~41 MB (arena chunk-0s ≈ 20.8 MB,
indexes 11.7 MB, rings 6.3 MB, batches 2.3 MB), and G-LIFE-5 builds one graph per FatalReason
(`lifecycle_test.go:337-339`) under a `-race` memory multiplier. `testGraph`
(`lifecycle_test.go:29`) sets small `ExpectedRows` and `ArenaChunkRows`.

**Files.** `internal/app/graph.go`, `runtime.go`, `shard.go`, `lifecycle.go`, `instruments.go`,
`fanout.go`, `alloc_gate_test.go`, `lifecycle_test.go`, new `shard_wiring_test.go` with an
`allCommitted` fake consuming `batchQ`.

**Tests.**
- G-LIFE-1/2 still green: no traffic, under 500 ms, no ERROR.
- Datagrams in, fake writer, SIGTERM → `drain_abandoned_rows == 0` (G-DRAIN-1 shard half).
- Shared deadline: four shards each holding a batch, `drainTimeoutMs = 200` → ~200 ms, not 800 ms
  (G-DRAIN-2 shard half).
- **G-LIFE-4 shard half:** after the shards return at the deadline with m batches held, the fake
  sends all m on `completionCh[k]` with nobody reading and does not block — pinning
  `cap == maxInFlightPerShard`.
- Rigged shard panic → exit 4, `F26-shard-panic`, stack. A G-ROW-3 batch → F27, the shard drains,
  exit 4. A rigged `cohort_not_empty` → `FatalShardInvariant`, exit 4.
- G-LIFE-5 green over eight reasons (loops over `numFatalReasons`).
- **G-ORD-2 lifecycle:** guard-eval interval 50 ms, one transcode disables in a running graph.
  `ordering.guardEvalIntervalSec` is whole seconds with a floor of 1 (`validate.go:82`), so 50 ms is
  reachable only by the test overwriting the `time.Duration` that `Build` derives onto `Graph`
  before `Run` — no new config key (YAGNI).
- Flush ticker: a `tickCh[k]` left full (shard not reading) → `tick_send_dropped{k}`++ and the
  ticker never blocks (A13 is the ticker's, V2:2906).
- `TestEveryAcquiredResourceIsReleased` green with the new stages; `go test -race ./internal/app/
  -count=20` green.

**Defects closed.** The Phase 2 seams at `shard.go:78-83`, `lifecycle.go:630-632` (steps 7–8) and
`fanout.go:183-187`.

**Not in this card.** Writers; closing `batchQ`; SIGHUP republishing.

**Gates.** None of Vol 2's; shard halves of G-DRAIN-1/2 and G-LIFE-4. Keeps G-ALLOC-1, G-LIFE-1/2/3/5
and G-DEP-1 green. **Depends on.** P3-01, P3-10, P3-11, P3-12. **Rough:** 4–4.5 days.

> **Why no in-binary stub writer (D10).** A stub marking rows Committed records digests for rows
> never written (HLD risk 8), which NFR13 then suppresses against Phase 4's real writer. A stub
> marking them Failed spins with no backoff. With nothing consuming `batchQ`, each shard sends its m
> batches, the epoch never closes, `epoch_stalled{k}` WARNs once, `tick_skipped_epoch_open` rises,
> and merging keeps Q1 draining so F9 never fires. A shutdown with traffic runs to the deadline and
> logs an honest `drain_abandoned_rows`. Phase 4 removes this by wiring a real consumer.

---

### P3-14 · Race, stress, allocation and the target

**Scope.**
- **G-RACE-2:** `-race -count=20` over the G-FLUSH, G-EPOCH, G-INFL, G-ROW tests and P3-13's G-LIFE-4
  shard half (V4:1706 names G-LIFE-4 too), with
  `PATH=/c/msys64/clang64/bin:$PATH CC=/c/msys64/clang64/bin/clang.exe`.
- **G-RACE-3, as V4:1707 specifies it**, in `internal/app`: the whole pipeline — fake socket →
  framing → decode → 4 shards → fake writers — for 60 s under `-race`, `GOMAXPROCS = 2 × shards`,
  a randomised 0–100 µs jitter at every channel boundary, injected through `Deps.Decode` and the fake
  writer. Asserts the G-MASK-1, G-ORD-1, G-INFL-1 and G-ROW-1 properties, plus a post-run INV-STATE
  walk, zero `inflight_desync`, zero `batch_row_unreported`, full `Free` at quiescence. Built with
  `-tags feedvalve_assert` too, so G-ASSERT-1's shard `TryLock` runs inside it. A short default run;
  60 s behind a flag.
- **Steady-state allocation:** an `AllocsPerRun` test over a full merge → pump → complete cycle on
  warm rows = 0, alongside the per-function tests of P3-07/07b/08/09. Benchmarks
  (`BenchmarkShard*`) only for numbers.
- **G-AIX-1 assertion 2:** in `internal/app/aix_test.go` (`:24-30` names it), a `Test` that calls
  `testing.Benchmark` over `digest(seed, img[:181])` and asserts **0 allocs/op and ≥ 200 MB/s**
  (V4:1749) — a bare benchmark cannot assert. Added to the cross-compiled set and
  `scripts/lpar-smoke.sh`.
- **G-ASSERT-1 on the target:** the cross-compiled `mirror` test binary is also built with
  `-tags feedvalve_assert` and run on the LPAR, because G-ASSERT-1 "runs … in G-AIX-1's on-target
  suite, where it is the only concurrency check available at all" (V4:1767-1768).

**Files.** `internal/app/stress_test.go`, `internal/mirror/*_bench_test.go`,
`internal/mirror/cycle_alloc_test.go`, `internal/app/aix_test.go`, `scripts/lpar-smoke.sh`.

**Done when.** Race runs green; the stress harness runs 60 s clean on the dev host; the cycle test
reports 0; the LPAR reports maphash at 0 allocs/op and ≥ 200 MB/s, or OQ-L3's FNV-1a fallback is
triggered and recorded.

**Defects closed.** OQ-L3 (target half).

**Gates.** G-RACE-2/3 runs, G-ASSERT-1 shard half, G-AIX-1 a2 (owned elsewhere).
**Depends on.** P3-13. **Rough:** 3–3.5 days.

---

## 5. Effort and sequencing

| Card | Rough (days) |
|---|---|
| P3-01 | 0.5–1 |
| P3-02 | 1–1.5 |
| P3-03 | 1 |
| P3-04 | 2–3 |
| P3-05 | 0.5 |
| P3-06 | 2 |
| P3-07 | 3 |
| P3-07b | 1.5–2 |
| P3-08 | 3 |
| P3-09 | 4 |
| P3-10 | 1–1.5 |
| P3-11 | 3.5–4 |
| P3-12 | 1–1.5 |
| P3-13 | 4–4.5 |
| P3-14 | 3–3.5 |
| **Single developer** | **~31–36** |
| **Two developers** | **~26–30**, bounded by the critical path (02 → 04 → 06 → 07 → 07b → 08 → 09 → 11 → 13 → 14 sums to 27–30.5 days). One takes that spine; the other 01, 03, 05, then 12, 10 (4–5.5 days, all off it), and shaves a day or so by splitting 13/14. A second developer buys reviews, not calendar |

The spine is P3-07 → P3-07b → P3-08 → P3-09 → P3-11. P3-09 is the largest card and carries nine
gates; D8's ordering and V0-Δ1 are both one-line mistakes no other gate catches.

---

## 6. Open seams, rulings and errata

### Rulings

| # | Seam | Ruling |
|---|---|---|
| **D1** | `mirror` cannot import `config`, which imports `oradb` (`config.go:6`); G-DEP-1 is transitive | `mirror.Sizing` + `mirror.Params`. The `*mirror.Params` lives **inside `app.Runtime`** — one published pointer per ADR-L11 (`internal/app/runtime.go:9-30`), not a second `atomic.Pointer` with a skew window. Shards get a `func() *mirror.Params`. `cfgGen` initialised from the first `Params`. The rule is not relaxed |
| **D2** | `GuardOff` storage: absent from `RegistryEntry` (vs V0:838); registry immutable, swapped on reload (`registry.go:44-47`) | `mirror.GuardTable` outside the registry. Its counters resolved in `resolveTranscodeInstruments` (`instruments.go:295`) **before Freeze**, by configured position; `Build` zips them with `reg.Codes()`. Resolving in `Build` panics (`instruments.go:158-168`) |
| **D3** | When the shard reads config: V2:1634 says "only at epoch open"; V2:465 and V4:540 also read at each tick | One `Params` load at epoch open and one at each tick. Erratum to V2 §6.3 |
| **D4** | Drain deadline: V2 per shard (V2:385, 535); V4 one shared (V4:260, G-DRAIN-2; `config.go:75` agrees) | **Vol 4 wins.** One `drainExpired`, armed from `t_shutdown` (`g.drainStarted`). Each shard keeps a nil local arm until it has seen `in` closed, so `drainTimeoutMs = 0` never skips buffered Q1 units |
| **D5** | V2 §2.2's `ctx.Done()` arm (V2:402-403); nothing in the design cancels it, and on SIGTERM it would skip the drain | **No context arm** (YAGNI). A clean stop goes through the `shardCh` close; an abnormal one through the deadline. Erratum to V2 §2.2 |
| **D6** | *(withdrawn — the draft deferred §5A pre-validation to Phase 4; superseded by D12)* | — |
| **D7** | G-FLUSH-2: `flush_coalesced` has no shard label (`ledger.go:110`) and `Trigger` counts it per already-set bit, so N−1 requests give (N−1)×S | Keep V2's mechanism. Assert N−1 on a one-shard bus and (N−1)×S on an S-shard bus. Erratum to V4:1633 |
| **D8** | Failed-row head push vs epoch close (V2:2219-2220 vs 2239-2241) | The head push runs **before** step 13 (P3-09); otherwise Failed rows miss the next cohort and G-EPOCH-3 fails |
| **D9** | Fatal reasons for S1, S2, S3, `cohort_not_empty`, 0a. Six reasons exist today: F9, F13, F20, F21, F26, V7 (`graph.go:29-55`) | Add **F27 `FatalBatchRowUnreported`** (V4:1064) and **`FatalShardInvariant`**; F26 is **not** overloaded — its runbook is "file a bug with the stack" and its observable is `panics_recovered{stage=shard}` (V4:1063), neither of which holds for a non-panic defect. S1 → `FatalBatchQSendFail`. S2, `cohort_not_empty`, 0a return → `FatalShardInvariant`. **S3 → `Fatal(F27)` and the shard keeps running into the normal drain** (its state is consistent after the requeue), reported through `NewShard`'s `report func(error)` (P3-11). Errata to V2 §2.7, V4:168, the V4 §5 catalogue and V4:1662 |
| **D10** | Nothing consumes `batchQ` until Phase 4 | No in-binary writer stub (P3-13 callout) |
| **D11** | `row_rejected` owner: ledger says writer (`ledger.go:150`), V3:1140 and G-STMT-2 (V4:1652) agree; V2:2148 has the shard count it | **The writer's.** The shard counts only `row_quarantined{d}` (A19). G-ROW-1's "`row_rejected`++" reads `row_quarantined`++ (erratum). Prevents a Phase 4 double count |
| **D12** | V0 §5A pre-validation: "validate at merge, per contribution" (V0:1677); all three instruments are the shard's (`ledger.go:203-205`); the DDL facts are on the column (`column.go:93-99`); the rejection behaviour is specified (V0:1679-1682) | **Phase 3, card P3-07b**, inside merge step 4. Vol 2 never names the step: erratum to V2 §4 |
| **D13** | A row released from quarantine with no merge in between carries `ColMask == 0`; the writer rejects an empty mask (V3 W-4) and the row re-quarantines, so a correct operator fix reads as "did not take" (P3-10 review) | The reject arm's stay-quarantined branch folds the refused mask back, `ColMask \|= r.Mask` (after the V0-Δ1 test), so the release rewrites the refused columns — V2 §9.4's "a release writes the latest state". Errata to V2 §8.4 and G-ROW-1 (V4:1648) |

### Other seams

| # | Seam | Resolution |
|---|---|---|
| 1 | The `obs.Emit` and `lastParks` races (`lifecycle.go:456-457, 573-575, 474-476`) | P3-01, first. Do not blame a Phase 3 card for `-race` failures before it merges |
| 2 | Reload releases quarantine (V2 §9.5), but SIGHUP is a no-op until Phase 5 | P3-10 tests the `Gen` compare through an injected `Params` loader; in production only V0-Δ1 releases until Phase 5 |
| 3 | `batchSentAt[b]` undefined (V2:2218) | `all [m]` + `sentAt [m]` on `Shard`, linear scan (P3-04) |
| 4 | No increment point for `flush_on_tick{k}` (V2:1613) | Where `maybeOpenEpoch` opens with `tickPending` set (P3-11) |
| 5 | Ring pre-size vs memory figure: 50,000 → 65,536, ~262 KB per ring, ~6.3 MB over 24, against V2 §10.3's 4.8 MB (V2:2626) | Erratum to V2 §10.3; `TestMemoryFigures` extended (P3-04) |
| 6 | Four Vol 2 log events have no class in V4 §6.2 (`04:1356-1357`) | Classes added in P3-04; erratum to V4 §6.2 |
| 7 | `NotNull` is advisory (V0 §5A.2): "check it, warn on it" — but at merge the mirror cannot tell an insert from an update | Not checked in Phase 3; revisit with Phase 4's catalogue check (V0 §7.6) |
| 8 | `policy` is still empty | Not Phase 3's; Vol 2 consumes only `FlagNonRateLimited` |

**Not Phase 3's scope:** G-LIFE-4 and G-DRAIN-1/2 end to end, G-UNT-1 (Phase 4);
`clock_step_detected`, `Logger.SetLevel`, G-CFG-4 (Phase 5).

### Errata to file

Applied to the LLD in one doc commit at the end of the phase:

- **V2 §2.2** (V2:402-403) — no `ctx.Done()` arm (D5).
- **V2 §2.6** (V2:385, 535) — one shared deadline, armed from `t_shutdown`, gated on `in` closed (D4).
- **V2 §2.7** — five returns, not "three" (V2:566 vs 571-587, 2007-2010); "deliberately *not* fatal"
  vs "It is fatal too" (V2:582-585); **S3 does not return** — the shard keeps running into the drain
  (V2:577-580, 2022-2026 say "the drain may yet write them", but nobody would once `Run` returned,
  and the decoder keeps fanning into a Q1 nobody reads, `lifecycle.go:612-628`); fatal mapping per D9.
- **V2 §4** — pre-validation runs inside step 4 (D12); V2:2148's `row_rejected` belongs to the writer
  (D11).
- **V0 §5.3 / V2 §4 step 7** — the suppression predicate also requires `!e.InFlight` (P3-07
  review). `Digest` describes the last commit, not the batch in flight, so a value that returns to
  the committed one mid-flight (A committed, B pumped, A merged) matches it and would be dropped,
  leaving Oracle at B and the mirror at A with nothing to requeue the row. Figure 2.4 already has no
  suppression arc out of `InFlight`; the step 7 text is brought into line with it. Accepted.
- **V2 §4 steps 2-4** — step 4's width validation runs **before** step 3 (P3-07 review). A
  contribution with a wrong packed width is rejected whole, advancing no ordering slot, which is
  what step 2's "no partial application" requires; otherwise a corrupt unit could make later valid
  ones look out of order. The copy itself stays at step 4.
- **V2 §5.3** (V2:1267, and the flowchart at V2:1302) — the footer's `units_superseded_out_of_order++`
  is the merge's, at §4 step 2 (V2:893-895), not the guard's: counting it in both would double it.
  The guard counts only `guard_superseded{code}`. P3-07 obligation: its tests assert the counter
  rises by exactly 1 per Supersede.
- **V2 §1.1** — `tickPending`, `maxBatchSize`, `all`/`sentAt` missing from the struct.
- **V2 §2.3** — V2:409 omits `tickPending`.
- **V2 §6.3 / §6.4** — `flush_on_tick{k}` counts only an open with `tickPending` set (seam 4); a
  `closeEpoch` re-open driven by `TickMissed` is not counted, so under sustained load
  `flush_on_tick` runs below the tick-caused share of `epoch_opened` (P3-11 review). Documented,
  not changed.
- **V2 §6.3** — V2:1634, config read at epoch open **and** at each tick (D3); V2:1613's increment point.
- **V2 §6.3 step 8** (V2:1590) — `dirty_depth{k,d}` publishes `Accum[d].Len()`, not 0: the
  accumulating rings are non-empty after a step-7 quarantine release (P3-10).
- **V2 §8.4** — the Rejected arm's stay-quarantined branch folds the refused columns back,
  `ColMask |= r.Mask`, **after** the V0-Delta-1 disjoint test (which needs `ColMask` without them),
  exactly as the Failed arm does (P3-10 review). Otherwise a row not merged into while quarantined is
  released with `ColMask == 0` and batched with `Mask == 0`, which Vol 3 W-4 (A17) refuses as a Vol 2
  defect (`batch_row_empty_mask`, re-quarantined) — runbook step 4 then reads "the fix did not take"
  though it did — and the refused columns are never rewritten unless a later merge touches them,
  contradicting §9.4's "a release writes the **latest** state". Nothing is re-offered early: merge
  step 6 still keeps a quarantined row off the rings.
- **V2 §8.5–8.6** — `batchSentAt` defined; head push before step 13 (D8).
- **V2 §9.4** — duplicated paragraphs (V2:2393-2404 vs 2406-2416); stale `Fail` for `Quarantined`
  (V2:1089, 2643, 2800, 2837).
- **V2 §9.5** — the pseudocode's `publish rows_quarantined_current{d} = 0` is right only with one
  shard: the gauge has no shard label and every shard shares it (`ledger.go`, `dimDest`;
  `app/instruments.go`), so a release subtracts its own count, `rows_quarantined_current{d} −=
  released[d]`, matching V0-Delta-1's `−= 1` (V2:2160). Released rows go to the tail in quarantine
  order (P3-10).
- **V2 §10.3** — rings 6.3 MB, not 4.8 MB.
- **V4:168** — exit-4 list: add V7, F27, `FatalShardInvariant`. The same count recurs at V4:255
  (step 2's `reason=` list) and V4:1358 ("the five terminal conditions").
- **V4 §5 catalogue** (V4:1063-1064) — a row for `FatalShardInvariant`.
- **V4 §6.2** (V4:1356-1357) — the four new classes.
- **V4:1501, :1516** — "Eighty-nine" and "25" against 91 and 26.
- **V4:1623** — G-ORD-1's "either one alone fails this test" holds across two setups (P3-07), not
  one: the `RecvMono` tiebreak alone resolves the canonical sequence (V2:1450), so only the
  pre-seeded setup proves it necessary and only the equal-`RecvMono` setup proves the slots are.
- **V4:1633** — G-FLUSH-2 per D7.
- **V4:1634, :1642** — G-FLUSH-3's "within microseconds" and G-INFL-4's "`1/m` of the wall time" are
  informational; the gates assert the cause (wake arm taken) and the concurrency (≤ m held, m
  reached), which do not flake under G-RACE-2's `-race -count=20`.
- **V4:1648** — G-ROW-1: "nothing changed" reads "`Digest`, `Written` unchanged, `ColMask` = the
  refused `Mask`" (the quarantine flag is the specified change; the mask fold is the V2 §8.4 erratum
  above), and `row_rejected`++ reads `row_quarantined`++ (D11).
- **V4:1662** — G-LIFE-5's "five `FatalReason`s … 4×5" becomes **eight** (six today, plus F27 and
  `FatalShardInvariant`).
- **This plan, P3-10's first test** — the released row is on `Accum` (the next epoch), not "in the
  cohort": V2 §6.3 step 7 runs after step 5's swap, and §9.5 / Figure 2.4 push it to the tail of
  `Accum[d]` ("one epoch after the reload", §9.3).
- **Phase 2's loose end** — Vol 1's gate count, 24 declared vs 21 enumerated (`phase2-tasks.md` §8).

---

## 7. Exit criteria, restated as a checklist

- [ ] `go build ./...`, `go vet ./...`, `go test ./...` all green on the dev host
- [ ] `go test -race ./internal/...` green with `CC` at the full clang path — possible only after P3-01
- [ ] **G-RACE-2:** `-race -count=20` green over G-FLUSH-\*, G-EPOCH-\*, G-INFL-\*, G-ROW-\*
- [ ] **G-RACE-3:** the whole-pipeline harness runs 60 s clean, jittered, `GOMAXPROCS = 2 × shards`,
      with `-tags feedvalve_assert`
- [ ] `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./...` green; still zero cgo
- [ ] **All 23 new Vol 2 gates green at the status in §2** — full, shard-half (G-MASK-1/2, G-ROW-1/2)
      or pending erratum (G-FLUSH-2, G-FLUSH-3, G-INFL-4, G-ROW-1) — each landed by exactly one card
- [ ] G-LAY-2, G-LAY-3, G-PTR-1, G-ALLOC-1, G-DIFF-1 still green; the guard half of G-ORD-4 tested
- [ ] `AllocsPerRun` tests (not benchmarks) report 0 for `merge`, `pump`, `applyCompletion` and the
      full cycle
- [ ] G-AIX-1 assertion 2 shipped: maphash 0 allocs/op and ≥ 200 MB/s on the LPAR, or the FNV-1a
      fallback recorded
- [ ] `mirror` reaches no `oradb`, `writer`, `database/sql`, `go-ora`, `net` **or `config`**:
      `go list -deps -test ./internal/mirror | grep -E 'oradb|config|database/sql|go-ora|/net$'` empty
- [ ] G-LIFE-1/2/3/5 green; a clean stop with no traffic under 500 ms with no ERROR line
- [ ] Four shards each holding a batch shut down in ~`drainTimeoutMs`, not 4×; the shard half of
      G-LIFE-4 holds
- [ ] A shard panic exits 4 with F26 and a stack; a G-ROW-3 batch exits 4 with F27 **after** the drain
- [ ] `grep -rn "time.After(" internal/mirror` finds no drain deadline
- [ ] The §6 errata are filed against the LLD
- [ ] `poc` still builds and its tests pass (`TestThePocOracleIsStillBuildable`); nothing under
      `internal/` imports it

---

## 8. Provenance

This breakdown came from a four-agent, read-only workflow, run **`wf_ea95a938-ef0`**: a Vol 2
extraction, a map of the current code, a drafter (14 cards, 23 new gates), and an adversarial critic
(33 corrections). Every correction is applied here, together with rulings D1–D12 of the approved
plan, which override the draft wherever they differ — most visibly P3-07b (pre-validation pulled into
Phase 3), F27 plus `FatalShardInvariant` in place of an overloaded F26, S3 running into the drain,
the `row_rejected` owner, and the P3-12/P3-13 split that removes a dependency cycle. The draft and
critique are in that run's `journal.jsonl`.

One correction is refined rather than applied verbatim. The critique's G-ORD-1 fix (its item 22)
pre-seeds `LastTime == T` and *also* collapses slots in that same setup. On a pre-seeded row with a
rising `RecvMono` the collapse still applies all three (V2:1450), so that half of the check could
never fail. P3-07 therefore splits it into two setups. G-LIFE-5's count is **eight**, not the plan's
"seven": six reasons exist today (`graph.go:29-55`), not five.

The gate arithmetic, spelled out because it does not close: V4:1516 says **25** and enumerates
**26**. Subtracting the three already green (G-LAY-2, G-LAY-3, G-PTR-1) leaves the **23 new** counted
in §2. Derived from the headline it would read 22, and an audit against "22" would find one too many.

Citations kept from the draft were spot-checked against the files at `8699fdf`; corrected ones
include `graph.go:29-55` (six FatalReasons, not two), G-LAY-3's location (`gates_test.go:189`),
`ledger.go:207-223` (16 rows), and `arena_chunks` as a gauge.
