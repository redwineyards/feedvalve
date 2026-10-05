# Phase 1 — Skeleton and the Frozen Contract

**Status:** ready to start. No open question blocks it.
**Source of truth:** `docs/lld/00-foundations.md` (Volume 0), frozen at commit `e679d40`.
**Gate authority:** `docs/lld/04-lifecycle-ops.md` §7.1 and §7.2.

---

## 1. What Phase 1 is

> A binary that starts, loads and validates its configuration, proves the contract holds,
> emits one instrument line, and exits cleanly — **cross-compiled and running on the LPAR.**
> No datagram is read. No row is written. No Oracle connection is opened.

Phase 1 exists because the import boundaries, the struct layouts and the build-time gates are
cheap to establish on an empty tree and expensive to retrofit once four packages depend on them.
Every one of these tasks is fully specified in Volume 0 — none of them needs a measurement, a
DBA, or an answer from NSEIL.

### What Phase 1 is *not*

| Deferred to | Work |
|---|---|
| **Phase 2** | socket and read loop, framing, LZO1Z, header resolution, gap detection, the per-transcode decoders |
| **Phase 3** | shards, arenas, the accumulating/cohort rings, the epoch state machine, digest suppression, the ordering guard |
| **Phase 4** | `oradb`, the writer pool, statement assembly, array binds, the two-phase execute, DDL |
| **Phase 5** | SIGHUP reload, the liveness and market-session state machines, the runbook, the remaining 78 gates |

If any of the above starts appearing in a Phase 1 branch, the skeleton has stopped being a skeleton.

---

## 2. What the contract actually says to build

Counts taken from Volume 0 by command, not from memory:

| Quantity | Count | Where |
|---|---|---|
| Packages under `internal/` | **14** (+ `cmd/feedvalve`) | §1.1 |
| Type declarations in the inventory | **24** — of which **16** are `model`'s | §3 |
| Structs with a byte-exact pinned size | **7** | §3.4–3.8, §9.5.2 |
| Configuration keys | **62** — 58 in the config file, 4 in the mapping file | §7.2 |
| Instruments | **129** | §8.2 + §8.2a |
| Build-time gates (unbypassable) | **9** | Vol 4 §7.1 |
| Config-load gates | **2** | Vol 4 §7.2 |

> **Two counts corrected during implementation.** §3's 24 declarations span four packages: 16 in
> `model`, 4 in `decode`, 2 in `config`, 2 in `oradb`. With `Datagram` and `SessionEvent` from §9.5.2
> and Vol 4, `model` holds **18** types. And §7.2's key table has 65 rows but three of them —
> `db.maxOpenConns`, `db.maxIdleConns`, `db.password` — are marked **"not a key"**; of the remaining
> 62, four moved into the mapping file with P1-04.

The seven size-pinned structs and their required totals:

| Type | Bytes | Declared |
|---|---|---|
| `Entry` | **424** | §3.7 |
| `UpdateUnit` | **360** | §3.5 |
| `BatchRow` | **288** | §3.8 |
| `MsgHeader` | **32** | §3.6 |
| `Contribution` | **24** | §3.4 |
| `SessionEvent` | **16** | §3.13 |
| `Datagram` | **1040** | §9.5.2 |

> **Reconciliation note.** Vol 4's **G-LAY-2** lists only the first six — it was written before the
> QD merge added `Datagram`. P1-10 extends the gate to all seven. See §6.

---

## 3. Dependency graph

```
P1-01 scaffolding
  ├─→ P1-02 import-direction gate ────────────────┐
  └─→ P1-03 model (24 types, 7 size pins)         │
        ├─→ P1-04 destinations + mapping file     │
        │     └─→ P1-05 config (65 keys)          │
        │           ├─→ P1-06 -selfcheck          │
        │           └─→ P1-07 obs (129 instr.) ─┐ │
        │                                        │ │
        ├─→ P1-08 QD ring (standalone)           │ │
        │                                        ↓ │
        └────────────────────→ P1-09 app lifecycle │
                                     └─→ P1-10 build gates ←┘
                                           └─→ P1-11 LPAR smoke
```

**Critical path:** 01 → 03 → 04 → 05 → 06/07 → 09 → 10 → 11.
**Parallelisable once 03 lands:** P1-08 (QD ring) and the scan half of P1-10 are independent of the
config/destinations spine; P1-02 needs only P1-01.

---

## 4. The tasks

### P1-01 · Module and package scaffolding

**Scope.** `go.mod` (`module feedvalve`, `go 1.23.3`). The 14 `internal/` packages of Vol 0 §1.1 plus
`cmd/feedvalve`. Each package gets a `doc.go` whose comment states, in prose, **what it owns and what
it must never import** — copied from the §1.2 table so the rule is readable at the point of
temptation, not only in a test.

**Explicitly:** `model`, `wire`, `lzo`, `framing`, `decode`, `destinations`, `policy`, `mirror`,
`writer`, `oradb`, `config`, `obs`, `health`, `app`.

**Done when.** `go build ./...` and `go vet ./...` both pass on the empty tree, and
`CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./...` passes too.

**Depends on.** —  **Rough:** ½ day.

**Note.** `obs` is importable by every package; `model` imports nothing but stdlib. Getting these two
right at creation time is most of what makes P1-02 pass later.

---

### P1-02 · The import-direction gate (G-DEP-1)

**Scope.** A table-driven test that shells `go list -deps <pkg>` for each of `model`, `wire`, `lzo`,
`framing`, `decode`, `destinations`, `policy` and fails on any of `database/sql`,
`github.com/sijms/go-ora/v2`, `feedvalve/internal/oradb`, `feedvalve/internal/writer`.
**`mirror` additionally fails on `net`.**

**Done when.** The test passes on the clean tree, **and** fails when a forbidden import is deliberately
added to each of the eight guarded packages — demonstrated, not assumed.

**Gates.** G-DEP-1.  **Depends on.** P1-01.  **Rough:** ½ day.

**Why this is task 2 and not task 20.** This is the only mechanism that survives a well-intentioned
3 a.m. refactor importing `oradb` into `decode` "just for the error type". The receive path's freedom
from the database is a property the whole mirror design rests on, and prose cannot enforce it.

---

### P1-03 · `model` — the value types

**Scope.** `model`'s share of Vol 0 §3 — **18 types** (16 from §3, plus `Datagram` from §9.5.2 and
`SessionEvent` from Vol 4):

- **Scalars and enumerations** (§3.1): `DestID`, `TransCode`, `ShardID`, `Class`, `HeaderKind`,
  `UnitFlags`, `RowOutcome`, with `String()` methods. *(`ColKind` is **not** one of them — Vol 0 §4.2
  declares it in `destinations`, where P1-04 built it.)*
- **`RowKey`** (§3.2) and **`ColMask`** (§3.3), including the mask helpers.
- **The seven size-pinned structs** listed in §2 above, each with explicit `_ [n]byte` padding
  fields — never implicit compiler padding (Vol 0 §2 rule 6).
- **`Batch`** (§3.8), and the `Reject` type with `Counter()` plus the error sentinels (§3.11, §3.13).
  *(§3.9's decoder registry belongs to `decode` and lands in Phase 2; §3.10's `Config` root belongs
  to `config` and landed with P1-05.)*

**Done when.**
1. Every struct's `unsafe.Sizeof` **and every field's `unsafe.Offsetof`** match Vol 0's offset tables
   exactly, asserted as compile-time constants (not runtime tests).
2. The arena-resident types (`Entry`, `UpdateUnit`, `BatchRow`, `Datagram`) are **pointer-free** —
   no pointer, slice, map, string, chan, func or interface field, at any depth.
3. `model` imports nothing outside stdlib.

**Gates.** G-LAY-2 (extended to 7 types), G-PTR-1.  **Depends on.** P1-01.  **Rough:** 1–2 days.

**Trap.** Three `bool`s before a `[256]byte` is 3 bytes plus 5 of padding, not "flags+pad 32" — this
exact error cost the HLD a 24-byte-per-entry discrepancy across 400,000 rows. Write the padding out
and let the constant expression prove it.

---

### P1-04 · `destinations` — the mapping file and image layout

**Scope.** The single mapping artefact that drives both the image layout and, later, SQL generation.

- **File format and loader** for the destination table: name, target table, row-key rule, ordered
  column list.
- **`Column`** per Vol 0 §4.2 — `Bit`, `Offset`, `Width`, `Kind`, `Scaled`, `Scale`, `LenBit`, `SQL`,
  plus the pre-validation metadata `SQLPrecision`, `SQLMaxBytes`, `NotNull`, `Domain`.
- **`ColMask` bit indices are explicit in the file, never positional.** A reordered column list must
  not silently re-map live entries.
- **Image-layout derivation**: offsets computed from the declared widths, with the total checked
  against `imageSize` (256).
- **Contributor slot assignment**: ascending transcode code among a destination's declared
  contributors, fixed at startup, ≤ `maxContributors` (8).
- **Strict identifier validation** on every table and column name *before* it can ever reach a SQL
  string.

**Done when.** The three destinations of Vol 0 §4.3–4.5 load and produce the declared image lengths
(**181**, **37**, **142**); a duplicate bit index is rejected; an invalid SQL identifier is rejected;
a destination whose columns exceed `imageSize` is rejected; a ninth contributor is rejected.

**Gates.** G-LAY-3 (build-time, one-sided `const _ = uint(imageSize - destImageLen)` per compiled-in
destination).  **Depends on.** P1-03.  **Rough:** 2–3 days.

**Note.** The pre-validation metadata lives **here**, not in `oradb` — `mirror` must be able to read
it, and `mirror` may not import the database packages. That constraint is why these fields are on
`Column` and not somewhere more natural-looking.

---

### P1-05 · `config` — schema, load, validate, atomic swap

**Scope.** All **65** keys of Vol 0 §7.2 across the ten groups (`udp`, `runtime`, `flush`, `writers`,
`db`, `ordering`, `stats`, `liveness`, `debug`, `destinations[]`, `transcodes[]`), each with its
declared type, default, range and **reload class** (`startup-only` /
`reloadable` / `reloadable-downward-only`).

- Pointer-optional fields so "absent" is distinguishable from "zero".
- **Unknown keys are rejected, not ignored** — a typo must fail the load, loudly.
- `atomic.Pointer[Config]` with a `Gen` counter.
- The eight cross-field reconciliations of §7.3 (C1–C8), notably
  `statementFailWriters ≤ writers.count` and `heartbeatDown > heartbeatWarn`, both **rejected
  wholesale** rather than clamped.
- `db.maxOpenConns` / `maxIdleConns` **derived** from `writers.count + 1`, not independent keys.
- The three flags: `-config`, `-dbcheck`, `-selfcheck` — **delivered here**, along with the
  `main` → `run(ctx) error` split and the Vol 4 §1.2 exit codes, so the flags have somewhere to
  live. P1-09 extends that skeleton with the relay's startup ordering, signal handling and drain
  rather than replacing it; P1-06 adds the cross-checks `-selfcheck` runs.
  `-dbcheck` needed a real connection, so `oradb` gained `dsn`/`redactDSN`/`Open`/`Check` early —
  the rest of the connection layer (statement cache, `oracleErrCode`, the writer's pinned session)
  stays in Phase 4.

**Done when.** A full config round-trips; a typo'd key fails the load with the offending key named;
a `startup-only` key changing across a reload is refused **and the previous config keeps running**;
each of C1–C8 has a test that trips it.

**Depends on.** P1-03, P1-04.  **Rough:** 2–3 days.

> **Security, non-negotiable.** `db.password` is read **only** from `FEEDVALVE_DB_PASSWORD`. There is
> no password flag and there will never be one — a flag is visible in `ps`. The DSN contains the
> password and **must never reach any log**: build log lines from `DBConfig.describe()` over the
> individual fields; `redactDSN` is a second line of defence, not the first. (Vol 0 §7.5.)

---

### P1-06 · `-selfcheck` — the config-load rule set

**Scope.** `config.ValidateAll(cfg, nil)` and the CLI path that runs it and exits.

Rules that must be in place for Phase 1:
- **ASM-16b** — every destination's full row image, accumulated across all contributing transcodes,
  fits `imageSize`.
- **ASM-15 / V-D7** — every destination's row key is derivable from the shard key of **every**
  contributing transcode. *(This is the single most valuable rule in the set: a violation means two
  shards write one row, producing intermittent lost updates that are invisible until production.)*
- **ASM-16a** — `Σ width` of a transcode's contributed columns ≤ `unitBlobSize` (256).
- Contributor count per destination ≤ 8.
- `runtime.datagramQueueCapacity` is a power of two.
- `flush.maxBatchSize ≤ runtime.maxBatchSizeCeiling`.

**Done when.** `-selfcheck` exits 0 on the shipped config; there is **one deliberately-broken config
per rule id**, each producing exactly that rule's id and no other; and `-selfcheck` completes with
**no `FEEDVALVE_DB_PASSWORD` set and no network available** — it must be runnable from a build host
that has neither.

**Gates.** G-SC-1. *(G-SC-2 also needs the decoder registry, so its transcode-resolution half lands
in Phase 2; the config-side half lands here.)*

**Depends on.** P1-05.  **Rough:** 1 day.

---

### P1-07 · `obs` — the instrument ledger

**Scope.** The three instrument shapes of Vol 0 §8.1 and all **129** instruments of §8.2 + §8.2a.

| Shape | Semantics |
|---|---|
| **Counter** | monotonic `atomic.Uint64` |
| **Max** | CAS-max high-water mark, **reset at emission** |
| **Gauge** | published by its owning goroutine — never read across a goroutine boundary from a shard-owned slice |

- **Fixed label arrays resolved at startup.** Every instrument must resolve to a struct field, never
  a map lookup on a hot path.
- The grouped 60-second emission line of Vol 4 §6.1.
- The two loggers `appLogger` and `debugLogger`.

**Done when.** All 129 instruments are declared and reachable; one emission produces the specified
grouped line; a benchmark shows **zero allocations** on the counter and max paths.

**Depends on.** P1-03, P1-05.  **Rough:** 2–3 days.

> **Do not port the packet logger.** `poc/logger.go`'s full-payload logger is deleted by HLD §10.2 —
> it writes several times the wire rate to a world-readable, append-only file. The `-maxout` cap goes
> with it. A secret that lands there is permanent.

---

### P1-08 · QD — the lock-free SPSC datagram ring

**Scope.** The single-producer/single-consumer ring of Vol 0 §9.5, as a standalone, independently
tested primitive.

- Capacity **8192**, power of two, masked wrap. Element `model.Datagram` (1040 B) → **8.5 MB**,
  allocated once, pointer-free.
- Head and tail indices on **separate 128-byte cache lines** (ppc64 line size is 128, not 64).
- Wakeup: **bounded spin, then park on a cap-1 edge.** A pure busy-spin would burn ~25% of the
  entitled capacity of 4.
- **Single consumer is non-negotiable** — a consumer pool would demote structural ordering to the
  auto-disabling guard. Enforce it in the constructor.
- Enqueue (receiver side) and fan-out (decoder side) each get their own budget; both are fatal on
  expiry per req §10.2.

**Done when.** A two-goroutine property test under `-race` on the dev host shows **nothing lost,
nothing duplicated, strict FIFO** across millions of items; a full-ring and an empty-ring test each
exercise the park/wake edge; a benchmark shows zero allocations in steady state.

**Depends on.** P1-03.  **Rough:** 2–3 days.

**Why Phase 1 and not Phase 2, where it is first used.** This is the most subtle concurrency code in
the project — memory ordering, false sharing, and a spin-then-park wakeup with a real liveness
hazard on the cap-1 edge. It is fully testable against no feed at all. Proving it standalone under
the race detector is far cheaper than debugging it while also debugging framing, and it is the one
component whose bugs would present as *occasional missing market data* rather than as a crash.
Note also that **the race detector does not exist on the AIX target** — the only opportunity to run
this code under `-race` is on the dev host, which argues for doing it in isolation and early.

---

### P1-09 · `app` — the lifecycle skeleton

**Scope.** `main` → `run(ctx) error`, with `main` holding nothing but flag parsing and the exit code.

- Signal handling: SIGINT/SIGTERM cancel the context; SIGHUP is wired but a no-op until Phase 5.
- **Startup order**, with the socket bound **last** so nothing arrives before its consumer exists.
- **Shutdown order**, including the **QD drain stage** between "stop reading" and "stop decoding".
- The process exit-code taxonomy.
- **No `log.Fatal*` and no `os.Exit` anywhere outside `cmd/feedvalve/main.go`.**

**Done when.** The binary starts, idles, and shuts down cleanly on signal with **every deferred
cleanup observed to run** (assert it, don't eyeball it); a forced error in construction unwinds the
partially-built graph without panicking.

**Gates.** G-BLD-3 (half — the fatal-logger scan).  **Depends on.** P1-05, P1-07.  **Rough:** 1–2 days.

**Why the lifecycle belongs at the start and not the end.** You need a clean, provable shutdown
before you can test anything downstream of it. Retrofitting ordered drain onto a running pipeline is
how drain bugs become permanent. This is also the regression `CLAUDE.md` is written about: `poc` was
restructured precisely because `appLogger.Fatalf` skipped the `defer` that closed the database.

---

### P1-10 · The build-time gates

**Scope.** The nine gates of Vol 4 §7.1, to the extent Phase 1 can land them.

| Gate | Phase 1 scope |
|---|---|
| **G-LAY-2** | **All 7** size-pinned types — extend the gate beyond Vol 4's listed 6 to include `Datagram` 1040 |
| **G-LAY-3** | ASM-16b at build time, one-sided per compiled-in destination |
| **G-BLD-1** | Pin the protocol constants: `broadcastDataPrefixLen == 8`, `bcastHeaderSize == 40`, `messageHeaderSize == 40`, `innerMessageHeaderSize == 40`, `maxPackedDataLen == 512`, `maxDatagramLen == 516`, `msErrorResponseLen == 182` |
| **G-BLD-2** | Pin `maxDestinations == 3`, `maxContributors == 8`, `maxColumns == 64`, `unitBlobSize == 256`, `imageSize == 256` |
| **G-BLD-3** | `go/ast` source scan run as a test: fails on any `log.Fatal*`, `logger.Fatal*`, `os.Exit` or `runtime.Goexit` outside `cmd/feedvalve/main.go`, **and on any reference to `runtime.NumCPU` anywhere** |
| **G-DEP-1** | delivered by P1-02 |
| **G-DEP-2** | `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./...` as a required CI step, plus `go list -deps ./... \| grep -c runtime/cgo` asserting **0** |
| **G-DEP-3** | `go list -deps` on the non-test build asserts `github.com/rhnvrm/lzo1z` is **absent** — it is a test-only fixture compressor |
| **G-LAY-1** | **Phase 2.** Needs the decoders; the Part V stride arithmetic has nothing to constrain yet |

**Done when.** Each landed gate is shown to **fail** when its invariant is deliberately broken, then
restored. A gate that has never been seen to fail has not been tested.

**Depends on.** P1-02, P1-03, P1-09.  **Rough:** 1 day.

---

### P1-11 · Cross-compile and LPAR smoke — *the exit criterion*

**Scope.** `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build`, deploy to the LPAR, run.

**Done when, on the target:**
1. `-selfcheck` exits 0 against the shipped configuration.
2. The binary starts, emits **one** instrument line, and shuts down cleanly on SIGTERM.
3. The size assertions hold **on big-endian ppc64** — this is the only layer that runs there.
4. `time.LoadLocation("Asia/Kolkata")` resolves, or `time/tzdata` is confirmed linked (**OQ-L5**).
5. No cgo in the resulting binary.

**Depends on.** All.  **Rough:** 1 day *plus LPAR access*.

> **Start the access request now.** This is the longest-lead item in Phase 1 and the one most likely
> to slip. Everything else is keyboard work; this one needs another team.

---

## 5. Effort and sequencing

| | |
|---|---|
| **Single developer** | ~14–20 working days |
| **Two developers** | ~9–12 working days — one takes the 03 → 04 → 05 → 06 spine, the other takes 02, 08, 07 and the scan half of 10 |

The spine is `model` → `destinations` → `config`. Nothing in P1-07, P1-08 or P1-10 blocks it, and
P1-08 in particular is the one task that benefits from being someone's undivided attention.

---

## 6. Open seams carried into Phase 1

Recorded here so they are decided deliberately rather than discovered.

| # | Seam | Resolution |
|---|---|---|
| **1** | **G-LAY-2 lists 6 types; there are 7.** Vol 4 §7.1 predates the QD merge that added `Datagram` | P1-10 implements all 7. Vol 4's table should be corrected to match on the next documentation pass |
| **2** | **Read buffer vs. `Datagram.Buf`.** Vol 4 F6 specifies a 64 KB read buffer (so oversize is distinguishable from full) and trims to 512. Vol 0 §9.5.2 sizes `Datagram.Buf` at 1024 "so an oversize datagram is carried intact and diagnosable". A datagram between 1024 B and 64 KB would be truncated on the copy into QD | **Phase 2 decision**, in the receive path. Flagged here because it constrains `Datagram`, which P1-03 freezes. The 1040 B size itself is not in question — only what the receiver does with an oversize frame before enqueueing |
| **3** | **G-SC-2 spans phases.** Its transcode-resolution half needs the decoder registry | Config-side half in P1-06; decoder-side half in Phase 2 |
| **4** | **OQ-L5 — zoneinfo on AIX** | Import `time/tzdata` unconditionally (~450 KB, pure Go). P1-11 assertion 4 confirms. Failing open — treating every hour as in-window — is the safe default |

---

## 7. Exit criteria, restated as a checklist

- [ ] `go build ./...`, `go vet ./...`, `go test ./...` all green on the dev host
- [ ] `go test -race ./internal/...` green (QD property test included)
- [ ] `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./...` green; zero cgo in `go list -deps`
- [ ] All **8** Phase 1 build-time gates demonstrated failing, then restored
- [ ] **G-SC-1** green, with one deliberately-broken config per rule id
- [ ] All **7** size pins hold, verified **on the LPAR**, big-endian
- [ ] `-selfcheck` runs with no password and no network
- [ ] One instrument line emitted on the target; clean shutdown on SIGTERM with every defer observed
- [ ] No `log.Fatal*` / `os.Exit` outside `main.go`; no `runtime.NumCPU` anywhere
- [ ] No DSN, and no password, in any log line

---

## 8. Provenance

Every count in §2 was taken from `docs/lld/00-foundations.md` by command at the time of writing, not
from recollection. Three figures quoted in earlier discussion were wrong and are corrected here:
config keys **65** (not 77), instruments **129** (not 128), and size-pinned structs **7** (not 6).
The type count, **24**, was right.
