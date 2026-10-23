# Phase 4: the writer and persistence (Vol 3)

Status: plan, not started. Branch `phase4`, cut from `phase3` (`9df2ed3`).
Spec: `docs/lld/03-writer-persistence.md`. Gates: `docs/lld/04-lifecycle-ops.md` §7.
Shape follows `phase3-tasks.md`.

## 1. What this phase is, and is not

**Is.** `internal/writer` (pool, loop, grouping, binder, two-phase execute, health, stop ledger),
the Oracle additions to `internal/oradb` (`DB`, `Session`, classification), the pure statement
generator in `internal/destinations`, the schema scripts, a scripted fake driver
(`internal/oradb/oratest`), the catalogue and NOT NULL check, and the `app` wiring: writer start,
stop watcher, `batchQ` close and teardown steps 9-10. It closes every gate Phase 3 deferred.

**Is not.** The SIGHUP reload driver, `internal/health`, hot-reload of the remaining `writers.*`
keys, the liveness and market-session state machines, the runbook (all Phase 5). No tuning from a
real-Oracle run. No CHECK-constraint domain comparison, no auto-DDL, no migrations.

The shard side of the boundary is finished (Phase 3). The writer must write ONLY
`Rows[i].Outcome`, `Rows[i].ErrCode` and `b.Err`, send the batch on `completionCh[b.Shard]`, and
never touch it again. Obligations B-V3 (every row adjudicated) and B-V3b (return before backoff)
are tested on every path.

## 2. Counts and gate table

15 cards, about 33 days for one developer.

| Gate | Status | Card |
|---|---|---|
| G-SEC-2 | full, no DB | P4-01 |
| G-SEC-1 | full, no DB (re-asserted P4-03) | P4-13 |
| G-LIFE-3 | full, counting stub | P4-03, ordering P4-11 |
| G-MASK-1 / G-MASK-2 | statement half P4-04, bind order P4-05 | no DB |
| G-SC-1 | identifier regex | P4-04, P4-13 |
| G-ORD-1 | lock-order writer half | P4-06, race half P4-14 |
| G-STMT-2 | full (fake DB) | P4-07b |
| G-ROW-1 / G-ROW-2 | writer half P4-07a/b, end to end P4-14 | fake DB |
| G-ROW-3 | writer companion (`batch_row_unreported` stays 0) | P4-07a, P4-10 |
| G-STMT-1 | pure parts P4-08, unit P4-09, exit 4 end to end P4-10 | fake DB |
| G-CFG-7 | full via `Resize` (reload driver is Phase 5) | P4-09 |
| G-LIFE-4 | writer half P4-09, end to end P4-11, `-count=20` P4-14 | fake DB |
| G-ASSERT-1 | writer `TryLock` hook and test | P4-09 |
| G-LIFE-1 | writer ordering P4-10, full P4-11 | |
| G-OBS-1 / G-OBS-3 | handles before Freeze, name parity | P4-10 |
| G-DRAIN-1 / G-DRAIN-2 / G-UNT-1 | full (fake DB, with stall) | P4-11 |
| G-DEP-1 / G-DEP-2 / G-BLD-3 | import rules, no cgo, no `os.Exit` | P4-02, P4-09, P4-13 |
| G-AIX-1 | cross-compile checked per card, binaries P4-13 | |
| G-RACE-2 / G-RACE-3 | full with fake DB under `-race` | P4-14 |
| V3 self-gates | ADR-W02 prepare-once P4-06, golden P4-04, phase 2 direct P4-07b | |
| OQ-W3 live spike | skip unless `FEEDVALVE_ORACLE_DSN`, non-gating | P4-13 |

## 3. DAG

```
P4-01 ──────────────┐
P4-02 ──────────────┼─ P4-03 ─┬─ P4-12
P4-04 ─ P4-05 ──────┘         │
   └──────────────── P4-06 ───┴─ P4-07a ─ P4-07b ─┐
P4-01 ─ P4-06 ─ P4-08 ────────────────────────────┴─ P4-09 ─ P4-10 ─ P4-11 ─ P4-14
P4-04, P4-09, P4-10 ─ P4-13 ──────────────────────────────────────── P4-14
```

Edges: 03 needs 01 and 02. 05 needs 04. 06 needs 03, 04, 05. 07a needs 06. 07b needs 07a.
08 needs 01 and 06 (`stmtKey` is defined in P4-05's `stmtkey.go` and used from 06). 09 needs 07b
and 08. 10 needs 09. 11 needs 10. 12 needs 03 and 04. 13 needs 04, 09, 10. 14 needs 11 and 13.
P4-01, P4-02 and P4-04 can run in parallel. Every card is mergeable green on its own, and each of
P4-05, P4-09 and P4-11 ends with `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go vet` and `go test -c` of
the packages it touched.

## 4. Cards

### P4-01 · oradb classification (1.5 d)
- **Scope.** V3 §8.1-8.3, A4. `oracleErrCode(err) (int, bool)` via `errors.As` on go-ora
  `*network.OracleError`. `ErrClass` {RowData, Session, Deadlock, Space, Statement, Unknown}.
  Order: `context.Canceled` gives ok=false; `DeadlineExceeded` and `driver.ErrBadConn` give
  Session; the ORA table; else Unknown. Never match text. 24381 is Unknown, 01722 is RowData
  (D6). `Error{Class, Code, Err}` carries an already-redacted `Err`. `Classify` hangs off a small
  redactor built from DSN and password (`redactDSN(err, dsn, password, s)`; use the real names
  `Describe` and `redactDSN`).
- **Files.** `oradb/classify.go`, `redact.go`, `classify_test.go`, `doc.go` (fix the
  `oracleErrCode` promise).
- **Tests.** Table over every ORA number in §8.2. Canceled bare and wrapped. Text-lookalike
  `errors.New("ORA-00942")` stays Unknown. G-SEC-2: a driver error quoting the full DSN, wrapped,
  leaks no password through `Error()`, `Unwrap`, `%w`, `%v`, `%+v`, **including the `Open` and
  `Check` error paths**.
- **Gates.** G-SEC-2.
- **Not here.** Session, Prepare, the `DB` wrapper (P4-03), counting `oracle_error` (P4-07a).

### P4-02 · oratest, the scripted fake driver (1.5 d)
- **Scope.** Ruling D1. One non-production package, `internal/oradb/oratest`, with a registered
  `database/sql` driver: `ConnBeginTx`, `ExecerContext`, `Prepare`/`StmtExecContext`,
  `NamedValueChecker` (accepts `[]int64`, `[]sql.NullString`, `[]sql.NullFloat64`,
  `[]sql.NullTime`), `Pinger`, `QueryContext` (canned rows). Scripting hook keyed by SQL text,
  call number and per-row index; typed ORA errors as `*network.OracleError`; conn drop
  (`ErrBadConn`, ORA-03113); latency honouring ctx; counters for Open, Close, driver Prepare per
  connection, Begin, Commit, Rollback, Exec per statement.
- **Import rule.** `oratest` imports only `database/sql/driver` and go-ora's `network`. **Never
  `oradb`** (oradb's own tests import oratest; the reverse is a cycle). Hence no edge to P4-01.
- **deps_test edits, here, deliberately.** oradb and oratest may name go-ora; the receive path and
  `mirror` keep forbidding sql, go-ora, oradb, writer and oratest; add a `go list -deps
  ./cmd/feedvalve` check that oratest is absent. `TestEveryPackageHasARule` stays green.
- **Tests.** Array arg arrives as one slice per placeholder; scripted ORA-00001 at exec N; one
  scripted scalar failure; Prepare counter per connection; `ErrBadConn` evicts; latency aborted by
  ctx; Begin/Commit/Rollback counts.
- **Gates.** G-DEP-2, supports every fake-DB gate.
- **Not here.** Oracle semantics a stub cannot prove (RowsAffected on array MERGE, all-NULL NCHAR,
  `t_commit`): the live spike, P4-13.

### P4-03 · oradb.DB, Session, Prepare, ExecTx (2 d)
- **Scope.** V3 §2.3, §2.6, A5. A new `*oradb.DB` wrapper (owns the redactor): `PingContext`,
  `Stats`, `Close`, `Prepare(ctx, text)` on the owning `*sql.DB` (ADR-W02), `Classify`, and the
  **exported** constructor `OpenDriver(ctx, driverName, dsn, DBSettings) (*DB, error)` as the
  documented substitutability seam (the writer and app tests live in other packages and need it).
  `Session`: `Acquire` pins a `*sql.Conn`, `Expired(now)`, `Release`, `ExecTx` (`BeginTx` nil
  opts, rollback on error and on panic, re-panic). Times carry monotonic readings.
- **Compatibility.** Keep `oradb.Open` returning `*sql.DB` until the app seam moves in one atomic
  edit (`Deps.DB`, `run.go` `openDB`, the dbcheck path via `oradb.Check`). `oradb` never imports
  `config` or `destinations` (config imports oradb).
- **Tests.** Against oratest: settings applied, ping under `ConnectTimeout`, redacted ping failure,
  one conn pinned across two transactions, commit / rollback / rollback-on-panic with re-panic,
  `Expired` with injected `now`, ADR-W02 part 1 (one driver Prepare per connection), Release
  returns the conn.
- **Gates.** G-LIFE-3 (counting stub replaces the trivial `lifecycle_test` one), G-SEC-1
  re-asserted.
- **Not here.** Any writer logic; the catalogue query (P4-12).

### P4-04 · destinations.StatementFor, BindPlan, DDL (2 d)
- **Scope.** V3 §3.1-3.5, §10, ADR-W06. `StatementFor(d, mask) (text, BindPlan, error)`, pure, no
  `database/sql`. MERGE form: starts with `MERGE`, no literals or comments, `:1..:n` ascending and
  never repeated, key only in `ON`, scaled columns divided in SQL by `10^Scale`, `updated_at`
  always present, mask 0 an error. `BindPlan` lists, per bind, column, kind, offset, width, `LenBit`,
  scale and ladder expansion index. `keyColumn` per destination added to `mapping.json` **as a
  validated identifier next to the existing `RowKeyRule`**, with the regex check in `validate.go`.
  Ships `db/schema/*.sql` (three CREATE TABLEs, INSERT/UPDATE grants only) as documentation.
- **Tests.** Golden for full and several partial masks per destination; bind counts 47 / 11 / 8;
  properties over every mask of dest 2 and a sample of dest 0; hostile identifier rejected;
  DDL column list equals the mapping's SQL names (45 data columns, 30 ladder); mask-removed columns
  vanish from all four lists.
- **Gates.** G-MASK-1 and G-MASK-2 statement halves, G-SC-1, the golden self-gate.
- **Not here.** Execution, value binding, cache, `-selfcheck` wiring (P4-13).

### P4-05 · writer binder (2 d)
- **Scope.** V3 §5. `NewBinder(dest, mask, ceiling)`, `Bind(rows, updatedAt) []any` of concretely
  typed slices all exactly `len(rows)`, allocated once per writer; `BindRow` for phase 2. NUL char1
  to NULL; NaN/Inf to NULL (`bind_nonfinite`); `s<=0` to NULL (`bind_bad_time`); trimmed text;
  ladder expansion at `57+12*i`; row key zero-extended; one `updated_at` per batch. Length-equality
  assertion before exec (W-3). D12: drop the dead year-2100 check. `Instruments` is a plain struct
  of handles defined in the writer (D5). Also `stmtkey.go`: `stmtKey{Dest, Mask}` with `String()`.
- **Tests.** Hand-written absolute-offset tables for one 7208 row and one 6511 text image (not
  built by the parser's field order); every conversion edge; slices of 1, 250 and ceiling rows;
  `AllocsPerRun` zero on the second `Bind`; short-arena injection fails before any exec; reflect
  walk for no `[]byte` and no untyped nil.
- **Gates.** Writer half of G-MASK-1.
- **Not here.** Executing, grouping, caching.

### P4-06 · grouping and statement cache (2 d)
- **Scope.** V3 §4.1-4.4, ADR-W07. Per destination sort a `[]int32` permutation scratch by
  (Mask, Row) (D7; `Batch.Rows` never reordered), walk runs. Empty mask: `OutcomeRejected`,
  `batch_row_empty_mask{d}`, rate-limited log, batch continues (W-4). Per-writer plain map capped
  at `writers.stmtCacheSize`; when full prepare, execute, close uncached and count
  `stmt_cache_uncached`. Prepare via `DB.Prepare`; execute via `tx.StmtContext`.
- **Tests.** Sort property test against `sort.SliceStable`; `Rows` byte-identical before and after;
  lock order (DestID, Mask, Row) ascending; cache hit, per-connection prepare, cap behaviour,
  `closeAll`; ADR-W02 behavioural test (prepare once, execute twice in two transactions, one driver
  Prepare per connection).
- **Gates.** G-ORD-1 writer half, ADR-W02 self-gate, W-4, W-6.

### P4-07a · runBatch, phase 1, commit, sweep (2 d)
- **Scope.** V3 §6.3, §7.1, §7.4, §7.5. One transaction per batch across dests 0-2, read
  committed, `queryTimeoutSec` per batch (D13). Phase 1 array-binds each run, commits. Own
  `recover` outside `ExecTx`; deferred sweep: panic sets `b.Err = "writer panic"`, counts
  `panics_recovered{stage=writer}`, retires the connection, and every Pending row becomes Failed
  whenever `Err != nil`. **The sweep never overwrites a terminal outcome.** `Committed` is set only
  after `Commit` returns nil. Failed rows carry the batch class code (D9). `Canceled` gives
  `Err "shutdown"`, Failed rows, no counters. `oracle_error` counted through an injected
  `func(code int)` taken from the concrete registry method (no type assertion, no fallback).
  `updatedAt = time.Now().UTC()` once per batch. `runBatch` is a **single attempt**; the retry loop
  lives in P4-09.
- **Tests.** All-success commit with per-dest `rows_written`. **Commit failure gives all Failed and
  none counted as written.** Panic in bind, exec and commit: every row terminal, `Err` set, batch
  returned once. **Panic after Commit leaves Committed rows intact.** Session error gives all
  Failed. Table over all scenarios (success, phase 2, abandon, panic, Canceled, empty mask,
  FailFast): a full-field snapshot of `Batch` and every `BatchRow` shows only `Outcome`, `ErrCode`
  and `Err` changed. B-V3 table: no Pending row survives any path, and `Err != nil` whenever any
  row is Failed; N==0 handled; `writers_busy` bracket balanced on panic.
- **Gates.** G-ROW-1 / G-ROW-2 writer halves, G-ROW-3 companion, W-5.
- **Not here.** Phase 2 (07b), retry, backoff, goroutines.

### P4-07b · phase 2 and abandon (1.5 d)
- **Scope.** V3 §6.4-6.6, §7.3, ADR-W01. On `ErrRowData` only: `ExecTx` rolls back, phase 2 re-runs
  the WHOLE batch once with scalar `BindRow`, one exec per row. A row-data error gives Rejected,
  `ErrCode`, `row_rejected{d}`, continue. Any other class abandons; earlier Rejected rows stay
  Rejected (A15). No phase 3. `RowsAffected != len(run)` is a WARN plus `rows_affected_mismatch`
  only (unit stays `rows`, count `len(run)`, KISS).
- **Tests.** Phase 2 exercised directly with a flag that skips phase 1, not only by fault
  injection. ORA-00001 on array exec then exactly one scalar failure: that row Rejected, rest
  Committed (G-STMT-2). 01438 attribution. Non-row error abandons. Mismatch changes no outcome.
  The P4-07a snapshot table extended with the phase 2 paths.
- **Gates.** G-STMT-2, W-7.

### P4-08 · retry policy, poolHealth, backoff, stop ledger (2 d)
- **Scope.** V3 §9. Pure state machines. One in-batch retry on a fresh session for Session /
  Deadlock / Space / Unknown, none for Statement, RowData goes to phase 2. Backoff 100 ms x2 to
  5 s, +-20% jitter (`math/rand/v2`). `poolHealth` Healthy / Degraded / FailFast, lock-free
  `OnSuccess` when Healthy, `OnFailure` returns Continue / RetireWriter / StopPool; 30 s pool-wide
  clock, 20-failure per-writer count; Space cleared only by a committed batch; Ping restores
  Healthy only after a Session class. Ledger `map[stmtKey]{writers bitmask, first}`; same identity
  on `statementFailWriters` distinct writers within `statementFailWindowSec` stops the pool. A
  retried-then-successful batch counts `write_retry{class}` plus one WARN and touches no health
  state (D6c). Add `space` and `unknown` retry-class dim values, with ledger, golden and G-OBS-3
  parity edits in the same commit.
- **Tests.** Injected clock. All transitions; 3 writers each failing 7 times does not trip; row
  rejection leaves state; Space survives a Ping success; backoff bounds and cap at attempt 8;
  ledger cases (one writer twice no, two writers inside 60 s yes, 61 s apart no, different
  identities never combine); `OnSuccess` / `OnFailure` under `-race`.
- **Gates.** Pure parts of G-STMT-1, B-V3b policy.

### P4-09 · writer loop and Pool (3 d)
- **Scope.** V3 §2.1-2.6, ADR-W04, W05, W08. `NewPool`, `Start(ctx, batchQ <-chan, completion
  []chan<-)`, `Resize`, `Stopped`, `StopReason`, `Close`. Loop: backoff sleep only with no batch
  in hand; lazy `Acquire`; select on batchQ, idle, hold, stop, ctx; config pointer loaded once per
  batch; the **retry loop** (calls `runBatch`); **blocking** send on `completion[b.Shard]`, then no
  further access. Counters are computed from locals captured before the send. Shrink quit checked
  only between batches. Hold and idle release only between batches. ErrStatement retires only that
  writer after returning its batch. FailFast: writers keep receiving and fail every batch at once
  until batchQ closes (D2). Zero running writers stops the pool (D4). Typed `StopError` (D3).
  `assert_on.go` / `assert_off.go` (`feedvalve_assert`) with the per-writer `TryLock` hook, mirroring
  `internal/mirror`. Constructor rejects an unbuffered completion channel. `sleep(d)` and the clock
  are injected.
- **deps_test edits, here.** `writer` may name `database/sql`, not go-ora; the receive path and
  `mirror` still may not reach `writer`.
- **Tests.** G-STMT-1 as V4 words it (ORA-00942 failing one identity on writers 0 then 1). Hold and
  idle release with injected clock, never mid-batch. **B-V3b, deterministic:** the injected sleep
  records `len(completionCh[k])` when called and asserts 1. **No post-send access:** a completion
  consumer writes every batch field on receipt, under `-race`. FailFast drains with Err set. G-CFG-7
  via `Resize` 4 to 8 (8 goroutines, pool max open/idle 9, `writers_configured` 8), shrink after
  in-flight batches, regrow. G-LIFE-4 writer half: `maxInFlightPerShard` outstanding, nobody
  reading, every writer returns. Queued batches all processed before writers exit on batchQ close.
  Close on a never-started pool. Goroutine count back to baseline.
- **Gates.** G-STMT-1, G-CFG-7, G-LIFE-4 writer half, G-ASSERT-1, W-1.

### P4-10 · app wiring: instruments, startWriters, stop watcher (2.5 d)
- **Scope.** V4 §1.4 steps 11 and 13, §1.2. Resolve every writer handle before `reg.Freeze()`.
  `Build` constructs the Pool (allocation only); `Run` starts it BEFORE `startShards`, and only when
  `Deps.DB != nil` (tests with nil DB keep their fake writers; no stub writer in the binary). A
  watcher selects on `Pool.Stopped()` and calls `g.Fatal(FatalDBFailFast)` (F13) or
  `g.Fatal(FatalWriterPoolStop)` (F20). `[]chan` to `[]chan<-`. `writers_configured`,
  `writers_running`, `writer_health_state` gauges. Missing writer log classes added to V4 §6.2. Remove
  the D10 "nothing consumes batchQ" comments.
- **Tests.** G-OBS-1, G-OBS-3, determinism golden. Real pool over oratest inside a Graph: shard to
  writer to completion. Statement-failing stub exits class F20, code 4; dead-DB stub gives F13
  after the injected 30 s. DB nil: no pool.
- **Gates.** G-LIFE-1 (writer ordering), G-STMT-1 end to end, G-ROW-3 with the real pool.
- **Not here.** `batchQ` close, teardown 9-10, writer context (P4-11).

### P4-11 · teardown 9-10, writer context, end-to-end drain (3 d)
- **Scope.** V4 §1.5 steps 9-10, §2.5. After `stopShardConsumers` joins, `close(g.batchQ)`,
  `Pool.Close`, then the deferred `db.Close`, strictly in that order, with guards for a
  never-started pool and shards. **D13:** writers run under a pool-owned context; the app cancels it
  at `drainExpired`, not on SIGTERM, and `Pool.Close` never cancels before shards have joined.
  `context.Canceled` is a shutdown: Failed rows, no counters. **Drain accounting:** the shard keeps
  per-destination `rowsOut[d]`, incremented at the successful `batchQ` send next to `CohortOut++`
  and decremented in `applyCompletion`; abandon adds `rowsOut[d]` to `drain_abandoned_rows`. An
  outstanding batch is never read.
- **Tests.** G-DRAIN-1 (N dirty rows, healthy oratest, all N written, abandoned 0, exit 0).
  G-DRAIN-2 (more rows than 5 s of writes, plus a stall: exit within `drainTimeoutMs` + epsilon,
  abandoned equals remainder per destination, stalled batch included). G-UNT-1. G-LIFE-4 end to
  end. G-LIFE-3 ordering by counter timestamps. A `-race` test with a stalled writer proving the
  accounting does not read the batch. Teardown-once on an early-error path.
- **Gates.** G-DRAIN-1, G-DRAIN-2, G-UNT-1, G-LIFE-4 e2e, G-LIFE-3 ordering, G-LIFE-1 full.
- **Not here.** SIGHUP `Resize`, the health package.

### P4-12 · catalogue and NOT NULL check (2 d)
- **Scope.** V0 §7.6 (authored **before this card merges**, not only listed as an erratum) and
  phase3 seam 7. `oradb.Catalog(ctx, db, tables)` returns neutral `[]CatalogColumn` from
  `ALL_TAB_COLUMNS`, filtered by `OWNER = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')` and `:1` binds.
  The comparison is a pure function in `app`: column exists, type family, precision, char length,
  and every data column nullable (V3 §10.4). `-dbcheck` exits non-zero on any mismatch, including
  NOT NULL. Startup logs one ERROR per mismatch and proceeds (a DBA fix must not need a redeploy).
- **Tests.** oratest canned rows; one distinct finding each for short precision, short varchar,
  NOT NULL data column, missing column, missing table; **owner filter**; `-dbcheck` exit code;
  startup proceeds.
- **Not here.** CHECK domains, grants pre-flight, auto-DDL.

### P4-13 · selfcheck, import rules, scripts, live spike (1.5 d)
- **Scope.** V3 §11, §12, §14. `-selfcheck` generates `StatementFor` for every destination at every
  single-bit mask plus full, with no DB, password or network, and rejects identifiers off
  `[A-Za-z][A-Za-z0-9_]{0,29}`. Re-assert the `deps_test` rules (already edited in P4-02 and P4-09).
  G-BLD-3 scan covers writer and oradb. `build-aix.sh` ships the writer / oradb / oratest test
  binaries; `lpar-smoke.sh` gains the catalogue line. `TestDBIntegrationWriter`, skipped unless
  `FEEDVALVE_ORACLE_DSN` (OQ-W3: `t_commit`, array `RowsAffected`, all-NULL NCHAR, TIMESTAMP round
  trip, ORA-01438 attribution, "transaction usable after a statement error").
- **Tests.** G-SEC-1 scan of writer log output from a full failure run for password, bind values and
  statement text. Selfcheck without any of them. Integration skips cleanly.
- **Gates.** G-SEC-1, G-SC-1, G-DEP-1, G-DEP-2, G-BLD-3, G-AIX-1.

### P4-14 · race, stress, allocation, target (2 d)
- **Scope.** V4 §7 G-RACE-2/3, V3 §15. Whole pipeline for 60 s under `-race` with oratest, injected
  latency and jitter, asserting G-MASK-1, G-ORD-1, G-INFL-1, G-ROW-1 over accumulated state.
  `-count=20` for G-LIFE-4. Allocation budget per batch. Utilisation sanity at 5000 rows/s with a
  5 ms commit stub (about 11% busy). The known `obs.Emit` race is not attributed to this card; the
  card records whether it surfaces.
- **Gates.** G-RACE-2, G-RACE-3, G-ROW-1/2 end to end.
- **Toolchain.** `go test -race` needs `CC` set to the full clang path. Run packages one at a time
  (the full `./internal/...` run was killed for memory in Phase 3).

## 5. Effort

Cards sum to about 33 days: 1.5 + 1.5 + 2 + 2 + 2 + 2 + 2 + 1.5 + 2 + 3 + 2.5 + 3 + 2 + 1.5 + 2.
Critical path: 04 → 05 → 06 → 07a → 07b → 09 → 10 → 11 → 14.

## 6. Rulings, seams and errata

**Rulings.**
- **D1** Fake DB: `internal/oradb/oratest`, scripted `database/sql` driver; `poc`'s stub untouched.
  Constructor seam is exported `oradb.OpenDriver`.
- **D2** FailFast: V4 wins over V3 §9.3. Writers keep receiving and fail every batch immediately.
- **D3** `Pool.StopReason()` is a typed `*StopError{Kind, Identity, Cause}`; FailFast maps to F13,
  pool stop to F20. Both exit 4.
- **D4** A pool whose running count reaches zero stops. `Resize(n)` compares with *configured*
  writers and regrows after self-stops.
- **D5** `writer.Instruments` is a plain struct of handles, defined in the writer. No
  `obs.WriterMetrics`.
- **D6** (a) 24381 is Unknown, one retry, never phase 2. (b) 01722 is a phase 2 trigger. (c) A
  successful retry counts `write_retry{class}` and one WARN, no health change. (d) No retry after a
  phase 2 non-row failure. (e) `updated_at` read once per batch.
- **D7** Grouping sorts a permutation scratch; `Batch.Rows` is never reordered.
- **D8** Statement generation lives in `destinations` (pure). `keyColumn` is a validated identifier
  beside `RowKeyRule`. The app never issues CREATE.
- **D9** `ErrCode` is the bare ORA number on Rejected rows and on Failed rows of a classified
  failure; 0 otherwise. `oradb.ErrClass` never enters `model`.
- **D10** Panic: recover outside `ExecTx`, sweep, return once, count
  `panics_recovered{stage=writer}`, retire that writer. V3's `writer_panic` is withdrawn.
- **D11** `Open` stays; `OpenDriver` and the `DB` wrapper are new. The writer may name
  `database/sql`; go-ora stays in oradb and oratest.
- **D12** Drop the dead year-2100 check in the binder.
- **D13** Writers run under a pool-owned context cancelled at `drainExpired`, not SIGTERM.
  `queryTimeoutSec` is per batch.
- **D14** Catalogue check: neutral rows from oradb, pure comparison in `app`; owner-filtered;
  `-dbcheck` fails hard, startup logs and proceeds.
- **D15** The live spike is non-gating.
- **D16** `oracle_error` stays first-come; `rows_affected_mismatch` unit stays `rows`.
- **D17** Phase 4 loads config once per batch and exposes `Resize`; the SIGHUP driver is Phase 5.

**Errata to apply at the end of the phase (one doc commit).**
- Vol 0: author §7.6 (cited, absent).
- V3 §2.5 L293: ADR-W05 should cite ADR-W08.
- V3 §2.6: signatures (`oradb.DB`, `*destinations.Set`, `writer.Instruments`, `[]chan<-`).
- V3 §8.2 / §6.4 / Vol 0 §3.11 / V4 F14: 24381 Unknown, 01722 RowData.
- V3 §9.3 vs V4 §1.6: FailFast writers keep receiving.
- V3 §7.4 / A18: panic retires the writer.
- **V3 §7.1 final paragraph / §8.3:** the pool context is cancelled by `drainExpired`, not by SIGTERM,
  and `Pool.Close` never cancels before shards join (D13).
- V3 §4.2 sort on a permutation; §5.4 year check dropped; §14.3 `oracle_error` first-come.
- V3 §8.1 `ErrSpace` and retry classes; §7.5 ErrCode filling.
- V4 §6.2 writer log classes; V4 G-ROW-1 `row_rejected` is the writer's; G-DRAIN-2 abandoned rows
  include rows in held batches.
- `oradb/doc.go` and `writer/doc.go` import claims.
- V3 §3.2 / §6.3 ladder bind order: the LLD prints and fixes it attribute-major (all `bid_qty_*`, then
  `bid_px_*`, then `bid_ord_*`, then the ask side). `mapping.json` declares it record by record
  (`bid_qty_1, bid_px_1, bid_ord_1, bid_qty_2, ...`), which is the order P4-04's `BindPlan` follows
  and the binder (P4-05) must follow. Statements name their columns, so DDL column order is
  irrelevant to binds. Correct the LLD text, not the mapping.

## 7. Exit checklist

- `go build ./...`, `go vet ./...`, `go test ./...` green; `-race` green for `internal/writer`
  and `internal/oradb`; `internal/app` race status not worsened.
- Every gate in §2 green with its stated status; every Phase 3 deferral closed.
- No stub writer in the binary; `go list -deps ./cmd/feedvalve` has neither oratest nor a new
  third-party dependency; `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./...` passes.
- Every row of every returned batch is terminal on panic, abandon, FailFast and shutdown;
  `batch_row_unreported` is 0 in all writer tests.
- Writers start before shards and the socket bind; `batchQ` closes only after shards join; writers
  join before `db.Close`.
- `-selfcheck` generates every statement with no DB; the catalogue check runs in `-dbcheck`.
- No password, DSN, bind value or statement text in writer logs.
- `poc` still builds; its `-debug` line format is untouched.
- CLAUDE.md updated (writer, oradb, oratest, `db/schema`); errata applied in one commit; the
  integration test skips cleanly without a DSN.

## 8. Provenance

Produced by workflow `wf_1456df4b-4fb`: a Vol 3 extraction, a code map, a drafter and an adversarial
critic. Every critic correction is applied above: P4-07 split into 07a/07b; `OpenDriver` exported;
oratest never imports oradb and loses its edge to P4-01; `deps_test` edits moved into P4-02 and
P4-09; `stmtKey` placed before P4-08; `rowsOut` replaces reading outstanding batches; the retry loop
sits in P4-09; the B-V3b test is deterministic; a post-send no-touch test and a commit-failure test
were added; the catalogue query is owner-filtered; AIX is checked per card; assert files live in
P4-09; `oracle_error` uses an injected method value; `keyColumn` sits beside `RowKeyRule`; the
D13 versus V3 §7.1 erratum is recorded.
