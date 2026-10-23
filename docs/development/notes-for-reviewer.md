# Notes for reviewer

Things a reviewer should know about Phases 3 and 4 (branches `phase3` and `phase4`, which contains
`phase3`). Written from the working record of those phases. Phase 1 and 2 items are limited to what
carried forward into them. Each item says whether it is open, accepted or fixed.

## Branch and merge state

- `develop` is at `8699fdf`. Nothing is merged. The repo has no usable remote, so nothing is pushed.
- A worktree cannot update a checked-out branch. Run `git merge --ff-only phase4` in the main
  checkout; it brings in `phase3` too. Then remove the `.claude/worktrees/phase3` worktree.
- Phase 4 tip: `a206b26`. Phase 3 tip: `9df2ed3`.

## Not verified

- **Full race gate.** `go test -race -count=20 ./internal/...` was never completed. It was killed for
  memory pressure in Phase 3 and not restarted. What did run under `-race`: `writer` (x3), `oradb`
  (x3), `mirror` (x2), `destinations` (x3), `internal/app` for `TestG_RACE_3` and `EndToEnd` only, and
  the 60 s G-RACE-3 stress run (Phase 3 and Phase 4 versions).
- **LPAR.** `scripts/lpar-smoke.sh` has not been run on the target. It measures the maphash digest
  (OQ-L3: at least 200 MB/s and 0 allocs/op, else record the FNV-1a fallback) and runs the
  `feedvalve_assert` test binaries (G-ASSERT-1 on target).
- **Real Oracle.** `TestDBIntegrationWriter` skips unless `FEEDVALVE_ORACLE_DSN` is set. It is the
  OQ-W3 spike (`t_commit`, array `RowsAffected`, all-NULL NCHAR, TIMESTAMP round trip, ORA-01438
  attribution, transaction usable after a statement error). All writer behaviour was proved only
  against the `oratest` stub driver, which cannot show those things.

## Open risks (not fixed)

- **`ExecTx` rollback deadlock on a driver panic.** If a go-ora call itself panics, `database/sql`
  keeps the transaction's close lock held and `oradb.Session.ExecTx`'s rollback deadlocks. Writer
  code panics are recovered and swept. A driver-internal panic is not. The writer would hang until
  the drain deadline. Documented in a comment on `runBatch`; no test injects it.
- **Retry double-count.** On an in-batch retry, `batch_row_empty_mask` and `bind_bad_time` count the
  same row a second time, because the second attempt re-groups and re-binds. Minor; a fix touches the
  grouper and binder.
- **`obs.Emit` race.** A race between `Emit` and `lastParks` that predates Phase 2 (P3-01 was meant to
  fix it). It did not surface in any Phase 4 race run, and is not attributed to any card. Recorded in
  the `race3_test.go` header and `phase4-tasks.md` section 9.
- **Rate-limited second guard disable.** The ERROR line for a second ordering-guard disable can be
  dropped by the log rate limit. Kept; the gauge is the record.
- **RecvMono ties within one datagram.** Units of one datagram can still tie on `RecvMono` at the same
  row slot. Rare; left as a note. (Ties between distinct datagrams were fixed, see below.)

## Deviations accepted in Phase 4

- `RunInstruments` is held as `Instruments.Run` rather than folded into one flat struct.
- G-CFG-7 asserts `MaxOpenConnections` and the `writers_configured` / `writers_running` gauges, not
  `MaxIdle` (`sql.DBStats` does not expose it) and not an exact goroutine delta.
- The dead-database test (F13) uses `writers.failFastSeconds=1` on the real clock, not the spec's
  30 s, because `Pool.now` is unexported. It still proves the mapping to F13 and exit 4.
- G-LIFE-3 ordering (writers joined before `db.Close`) is checked indirectly. `oratest` has no
  timestamps, and `db.Close` runs in `run`'s deferred close after `Run` returns. The strongest direct
  check is G-LIFE-4 end to end: a writer is mid-statement at SIGTERM and is joined before return.
- G-RACE-3 pipeline stress runs 10 s by default (2 s under `-short`). The 60 s gate is opt-in:
  `-feedvalve.race3 60s`. A 60 s `-race` run passed.
- G-MASK-1 and G-ORD-1 end to end compare `last_traded_price` only, the one column the stress feed
  varies. A lost other column would not show there; the unit-level binder and statement tests cover
  columns individually.
- The card's "5 ms commit stub gives about 11% busy" is wrong. 11% needs a 20 ms commit; 5 ms gives
  about 3.5%. The utilisation test checks both against their own arithmetic.
- The accounting identity in the stress test is "distinct rows committed + quarantined = created, 0
  abandoned", not the card's "written + abandoned + still-dirty = created", because `rows_written`
  counts rewrites.
- A retry that succeeds counts `write_retry{class}` per retry performed (not only on success), plus
  one WARN on success. It does not touch health state (D6c).
- `Binder.BindIdx` (index-based array bind) was added in P4-07a because `Batch.Rows` is never
  reordered (D7) while the grouper returns absolute indices.
- `oradb.Open` now returns `*oradb.DB`; `OpenDriver(ctx, driverName, dsn, DBSettings)` is the exported
  seam for the fake driver. `Deps.DB` is `*oradb.DB`.
- `phase1.go` carries the shared `checkAffected` helper used by phase 2 (outside the card's file list).
- Pool health thresholds (`failFastSeconds`, `failFastWrites`, statement-fail writers and window) are
  read once at `NewPool`; everything else in `Params` is re-read per batch. *(Resolved in Phase 5:
  `Pool.ReloadHealth` re-reads the thresholds on each accepted reload.)*
- A batch whose `Shard` is outside the completion slice is logged and dropped (defect path only).
- The deps rule for `writer` no longer forbids `net`. The writer reaches `net` only through `oradb`
  and go-ora; it never imports `net` itself. Framing, decode, mirror and oratest remain forbidden.
- `oratest` imports only `database/sql/driver` and go-ora's `network`, never `oradb`; production
  `cmd/feedvalve` must not depend on it (checked by `go list -deps`).
- The ladder bind order is record by record (`bid_qty_1, bid_px_1, bid_ord_1, bid_qty_2, ...`), as
  `mapping.json` declares and `BindPlan` follows. The LLD prints it attribute-major; the LLD text is
  what is wrong (erratum recorded).
- Vol 0 section 7.6 (catalogue check) did not exist though cited; P4-12 authored it in
  `docs/lld/00-foundations.md`. `-dbcheck` fails hard on any catalogue or NOT NULL mismatch; normal
  startup logs one ERROR per mismatch and proceeds.
- `writer/doc.go` and `oradb/doc.go` import claims were corrected in P4-13.

## Phase 3 carry-overs

- **No stub writer.** Until Phase 4 nothing consumed `batchQ`, by design (D10): a stub would commit
  digests for rows never written. This is now resolved; the production binary starts the real pool
  only when `Deps.DB != nil`, and tests with a nil DB use their own fake writers.
- **Seam 9, fixed.** On windows/amd64 `RecvMono` tied for distinct datagrams, so the ordering guard
  dropped the second as a re-delivery. `nextRecvMono(now, last) = max(now, last+1)` in
  `internal/app/clock.go`.
- **Pre-validation lives in the shard** (P3-07b, Vol 0 5A); the catalogue check and NOT NULL check are
  the Phase 4 half.
- **`mapping.json` `sqlPrecision`** was corrected in Phase 3 to match the Vol 3 DDL (nine scaled prices
  20, two double quantities 38).
- **D11 counters.** `row_rejected` is the writer's counter; the shard counts only `row_quarantined{d}`
  and `rows_rejected_by_oracle_unpredicted{d}`. Do not double count.
- **Eight fatal reasons** now (F9, F13, F20, F21, F26, V7, F27 `FatalBatchRowUnreported`,
  `FatalShardInvariant`). S1 maps to `FatalBatchQSendFail`.
- **Stale wording left in the LLD** (erratum notes added where practical): the P3-10 card text says
  "in the cohort"; some spec pseudocode still uses old `GuardOff[...]` names; Vol 4 R-GUARD and
  G-CFG-4 keep old wording with notes.
- **Phase 2 gate-count loose end** (24 vs 21 in Vol 4) was reconciled in the Phase 3 errata commit.

## Environment gotchas that affect review

- `go test -race` needs `CC` set to the full clang path (`/c/msys64/clang64/bin/clang.exe`); bare
  `CC=clang` fails and gcc is broken. `internal/app` under `-race` needs an explicit `-timeout`
  (the default 10 m is not enough).
- `gofmt -l` is useless here (CRLF working copies; git warns "LF will be replaced by CRLF"). Use
  `gofmt -d` on CR-stripped copies.
- Editor language-server diagnostics were repeatedly stale throughout both phases (they reported
  errors against code that builds). The real build, vet and test results are the authority.
- The worktree guard refuses compound or loop shell constructs that mention git or run go with
  computed values; run plain single commands.
- `poc` must stay buildable and its `-debug` line format is frozen (G-DIFF-1 oracle).

## Process notes

- Workflow agents never committed; every commit was made after the orchestrator re-ran build, vet,
  tests and the AIX cross-build. A session limit once killed two agents mid-card (P4-11, P4-13); they
  left partial edits, which the relaunched agents reviewed critically before finishing.
- Plan sources: `docs/development/phase3-tasks.md`, `docs/development/phase4-tasks.md` (with the
  "as built" section 9).

## Phase 5 as-built notes

- P5-09: `health.SessionTracker.Apply` treats a repeat of the same code (for example 6511 while Open) as idempotent, never an anomaly. This follows the card and G-SESS-3 and deliberately differs from the LLD 4.3 table row that lists Open to Open on 6511 as an anomaly. Card wins; LLD erratum applied (04-lifecycle-ops.md, section 4.3).
- P5-03/P5-04: V-STRICT1 `KeyPath` is the leaf field name only (`encoding/json` does not report the full path), so a mistyped `runtime.windowMS` is reported as `windowMS`. Matches the Vol 4 table, which asks only for the unknown key.
- P5-03/P5-04: V-DB3 (password key in the file) reports alone; any V-STRICT1 problems in the same file surface only after the credential is removed. Accepted: the credential has to be rotated first anyway.
- P5-03/P5-04: `config.StartupOnlyKeys` has 14 entries, Vol 0 s7.2's 13 plus `runtime.destinationsFile`, an implementation addition not in the card or Vol 4 list (04-lifecycle-ops.md:677). Erratum applied (section 2.5).
- P5-04: `loadForReload` is called by the SIGHUP driver (P5-06).

### Phase 5 resolved

- `obs.AppLogger.SetLevel` race: `level` is atomic (P5-01); debug trace is an immutable snapshot.
- `clock_step_detected` compared non-monotonic readings: it now compares wall movement against
  monotonic movement (P5-13, G-CLOCK-1).
- `runtime.time1980UnixOffsetSec` was loaded and validated but the writer ignored it: it now reaches
  the writer through `writer.Params` and follows a reload (a Phase 4 gap).
- SIGHUP was a no-op, so `Config.Gen` never advanced and quarantine release on reload was test-only:
  the reload driver is live (P5-06..08).
- `writers_configured` was overwritten by the stats sampler with the startup count after a resize:
  the pool owns the gauge.
- Pool health thresholds were read once: `Pool.ReloadHealth` re-reads them.

### Phase 5 open

- **`obs.Emit`/`lastParks` race** is still open; `go test -race ./internal/app/` is not fully green.
  See `docs/development/ci.md`. The `ExecTx` rollback deadlock on a driver panic and the missing CI
  config also remain.
- **Not run:** full `go test -race -count=20 ./internal/...`; the LPAR (including the SIGHUP step for
  OQ-L7/ASM-A9); the live-Oracle spike; OQ-L7..L12.
- **`decode_anomaly` severity.** The class is in the WARN run but three sites use `Errorf`, so they are
  suppressed at `stats.logLevel=error`. Follow-up: split the class by severity.
- **Real-signal SIGHUP test skips on Windows**; only the unix build exercises a delivered signal.
- **Stale comments** in production code still say SIGHUP/health are future work (`graph.go:250`,
  `run.go:270-273`, `writers_wiring.go:39`, `mirror/quarantine.go:85`, `health/doc.go:17`).
- **G-CFG-7** cannot observe `MaxIdleConns` (not exposed by `database/sql`).
- **Rulings awaiting the user:** D3 (V-TZ1 hard rejection versus fail-open), D4 (add `Closed -> Open`
  and `PostClose -> Open` edges), D9 (the four keys stay reloadable; `sourceEnforce` is an incident
  lever). Details in `phase5-tasks.md` sections 6 and 10.
- Branch state: `phase5` is not merged into `develop`; the `phase3`-named worktree still hosts it.
