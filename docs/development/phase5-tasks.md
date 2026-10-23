# Phase 5: reload, health, hardening, runbook (Vol 4)

Status: planned. Branch `phase5`, cut from `phase4` (tip `fce0071`). Worktree path:
`C:/Users/edwin/Documents/code/feedvalve/.claude/worktrees/phase3` (the directory is named `phase3`
but hosts branch `phase5`).
Spec: `docs/lld/04-lifecycle-ops.md` (Vol 4). Gates: its section 7. Shape follows `phase4-tasks.md`.

Line references were re-checked by grep against this tree. Where a reference is approximate it is
given as a function name instead.

## 1. What this phase is, and is not

**Is.**
- The SIGHUP reload path: load, validate, rebuild derived state, one publication, ordered live side
  effects (Vol 4 s2).
- `internal/health`: the circuit liveness machine (ADR-L32) and the market-session machine with its
  feeding channel H (ADR-L33/34).
- Stats-goroutine hardening: F28 restart, and a clock-step detector that measures wall steps (F23).
- The remaining test-time gates (G-CFG-2..6, G-CFG-8, G-SEC-3, G-LIVE-1..3, G-SESS-1..4, G-CLOCK-1,
  G-LOG-1, G-KEY-1).
- The race, on-target and CI residuals, the operator runbook (Q1-Q12), and the doc close-out.

**Is not.**
- No new config keys, flags, third-party modules or HTTP endpoint (Vol 4 s10.5, s6.5).
- No relaxing of any `deps_test.go` rule. `health` stays free of `config`, `oradb`, `writer`,
  `mirror` and `database/sql`, and defines its own `LivenessConfig`; `app` projects into it, as
  `mirrorParams` does. The only permitted `deps_test.go` change is tightening `health`'s row (P5-09).
- No change to `poc`, and no change to the `-debug` line format (G-DIFF-1 oracle).
- No new package.
- No metrics subsystem. The ledger is frozen; every Phase 5 instrument already exists in it
  (`circuit_*`, `session_*`, `session_events_dropped`, `clock_step_detected`, `config_generation`,
  `panics_recovered{stage=stats}` via `obs.StageStats`, `readbuf_effective_bytes`).
- No running of the real-Oracle spike or the LPAR in-session. Those are P5-16 deliverables for the
  operator.

**Already in the tree (so cards say "wire", not "add").**
- `Store.Gen()` (`internal/config/store.go:32`); `sampleGauges` already feeds `config_generation`
  from it (`lifecycle.go:521`). No `Generation()` alias is added.
- `RuleDBNoPassword` V-DB3, `RuleStrictKeys` V-STRICT1, `RuleDestStartupOnly` V-R2 are declared
  (`problem.go:53,58,62`); nothing raises them yet.
- `AppLogger.SetRatePerMin` (`logger.go:197`), `Reg.SetIntervalSec` (`registry.go:153`, called at
  startup `run.go:179`), `readBufferSize(conn)` and the `readbuf_effective_bytes` ledger entry,
  `ClassClockStep`, `model.SessionEvent` (16 bytes, `layout.go:126`), `Pool.Resize`
  (`pool.go:163`), `TestTokenBucketBoundsOneClassAndCountsTheRest` (`obs/logger_test.go:105`).
- NOT yet in the tree: `circuit*`/`session*`/`sessionEventsDropped` handles in the `instruments`
  struct (grep finds only `clockStepDetected`, `panicsRecovered`, `configGeneration`,
  `lastDatagramMonoNs`, `writersConfigured`); any producer of `SessionEvent`; any `internal/health`
  code (only `doc.go`); a `DB.SetConnMaxLifetime`; `Pool.ReloadHealth`.

## 2. Counts and gate table

18 cards (P5-00 is a manual step), about 34 engineering days.

| Gate | Card |
|---|---|
| G-LOG-1 (audit and extend existing test) | P5-01 |
| G-OBS-2, `TestClassSeverityBoundary`, `TestClassTableMatchesTheEnum` | P5-01, P5-02 |
| G-CFG-2, G-SEC-3 | P5-03 |
| G-SC-1 (V-R2 fires exactly once) | P5-04 |
| G-CFG-5, G-CFG-6, G-CFG-8 (SIGHUP path), SIGHUP-while-draining, reload-vs-SIGTERM | P5-06 |
| G-CFG-7 (through SIGHUP) | P5-07 |
| G-CFG-3, G-CFG-4 (G-RACE-2 `-count=20` on G-CFG-3) | P5-08 |
| G-LIVE-1, G-LIVE-3 (health level) | P5-09 |
| G-SESS-1, G-SESS-2, G-SESS-3 plus full edge-set table | P5-10 |
| G-LIVE-2 | P5-11 |
| G-SESS-4 | P5-12 |
| G-CLOCK-1 | P5-13 |
| G-KEY-1 | P5-14 |
| Runbook-to-ledger name check (G-OBS-3 style) | P5-15 |
| G-RACE-1 (process), G-RACE-2 residual, G-AIX-1 and G-ASSERT-1 on target | P5-16 |

G-OBS-1..4 and G-CFG-1 stay green throughout. G-ALLOC-1 stays at 0 allocs on the datagram path and
is re-run after P5-07 and P5-12 together with G-PTR-1 (any `Runtime` change can break it). G-BLD-3's
scan must be shown to reach `internal/app/reload.go` and `internal/health` (P5-06, P5-09).

## 3. Dependencies and merge order

| Card | Needs |
|---|---|
| P5-00 | none (manual, orchestrator) |
| P5-01, P5-02, P5-03, P5-05, P5-09, P5-10, P5-14 | P5-00 only |
| P5-04 | P5-03 |
| P5-06 | P5-01, P5-04, P5-05 |
| P5-07 | P5-06 |
| P5-08 | P5-07 |
| P5-11 | P5-02, P5-09 |
| P5-12 | P5-10, P5-11 |
| P5-13 | P5-11 (P5-12 for merge order) |
| P5-15 | P5-07, P5-11, P5-12, P5-13 |
| P5-16 | P5-08, P5-12, P5-13 |
| P5-17 | all |

Merge order is numeric. P5-11, P5-12 and P5-13 all edit `startStats`/`lifecycle.go` and must merge
in that order; P5-06 and P5-07 both edit `reload.go`/`lifecycle.go` likewise. The reload chain
(01-08) and the health chain (09-13) are independent until P5-15.

## 4. Cards

### P5-00 Precondition: merge phase4 into develop (0.25d, manual, user-confirmed)
- **Scope.** Run by the orchestrator or user in the MAIN checkout (`C:/Users/edwin/Documents/code/feedvalve`,
  which has `develop` checked out; a worktree cannot do this): `git merge --ff-only phase4`.
  `develop` (`8699fdf`) is an ancestor, so this carries phase3 too. Then build, vet and test on
  `develop` as a baseline. `phase5` is cut from `phase4`, so no rebase is needed.
- **Notes.** Do NOT remove the `phase3` worktree directory now: notes-for-reviewer says to, but it
  now hosts branch `phase5`. At phase end, merge `phase5` into `develop` in the main checkout, then
  remove that exact path (one worktree at a time). The `phase3`/`phase4` branches may stay.
- **Done when.** `develop` is at `a206b26`/`fce0071` (the phase4 tip) and baseline is green.

### P5-01 obs: make logger level and debug trace reload-safe (1.5d)
- **Scope.**
  - `AppLogger.level` becomes an atomic; `SetLevel` (`logger.go:200`, currently a plain write) stays
    as the one setter.
  - `DebugLogger.Set(codes, hexDump)` publishes an immutable snapshot via `atomic.Pointer`.
    `Enabled`/`Any`/`HexDumpEnabled` read it once and stay allocation-free. `fieldFormat` untouched.
  - G-LOG-1: no test carries the label today, but `TestTokenBucketBoundsOneClassAndCountsTheRest`
    already covers the mechanism at 10/min. Audit it, extend it to the spec's figures (1,000 events,
    `suppressed=N`, `log_events_suppressed{class}` equal to the remainder, other classes unaffected)
    and add the G-LOG-1 name. It may be a new test; say so in the commit.
- **Files.** `internal/obs/logger.go`, `debug.go`, tests.
- **Tests.** A race test toggling `SetLevel` and `Set` while logging.
- **Gates.** G-LOG-1, G-OBS-2.
- **Done when.** `go test -race ./internal/obs/` is green (the `Emit`/`lastParks` race is in `app`, not here) and `debug_test.go` is unchanged.

### P5-02 obs: log-class split, no behaviour (1d)
- **Scope.** Pure refactor with its own risk (class order feeds `firstErrorClass` and the severity
  boundary).
  - Add `ClassCircuitQuiet` (WARN) and `ClassCircuitDown` (ERROR).
  - Split decode warnings off `ClassSessionAnomaly`, which becomes session-only. Rename call sites:
    `subpacket.go:403-512`, `counters.go:83`.
  - `fanout.go:225` is the Q1 stall-budget fatal, neither decode nor session. Re-home it onto an
    existing fatal/lifecycle class if one exists, else the decode-anomaly class. Decide by reading
    `class.go` first; keep its deliberate no-`Allow` comment.
  - Update `TestClassSeverityBoundary` and `TestClassTableMatchesTheEnum` deliberately.
- **Gates.** G-OBS-1..4 green; no output change on existing paths.
- **Done when.** `go test ./internal/obs/ ./internal/app/` green and no log line changes class label except the intended ones.

### P5-03 config: wire the declared rules and the reload error API (2d)
- **Scope.**
  - Raise V-STRICT1 (`RuleStrictKeys`) and V-DB3 (`RuleDBNoPassword`), both declared but unraised.
  - V-DB3 needs a key-walk over a generic `map[string]any` BEFORE strict decode, at any depth,
    because `DisallowUnknownFields` fires first and hides the key. It reports the key path only,
    never the value, with a rotate-the-credential message.
  - `StartupOnlyKeys []string` exported plus a getter table, replacing the `startupOnly` map func
    (`validate.go:353`). V-R1 stays table-driven and reports key path, running value, attempted value
    (never for `db.*` secrets).
  - `RuleError{Rule, KeyPath, Running, Attempt, Detail}` derived from `Problems[0]`. The spec's
    `Validate`/`ValidateAll` sketch (s2.7) is superseded by the as-built API (erratum).
  - Change the `ValidateAll` signature ONCE here, adding `prevDests *destinations.Set` beside
    `prev` (current: `ValidateAll(c, dests, prev, mode)`, `schema.go:39`). Callers: `run.go:317`,
    `schema_test.go` (many), plus `selfcheck`; list them all in the commit.
  - Startup-only classification: see D9 and open item 2. Do not reclassify any key in this card
    unless the user confirmed.
- **Files.** `internal/config/{validate,problem,load,schema}.go`, `internal/app/run.go`, tests.
- **Gates.** G-CFG-2 (hand-written Vol 0 s7.2 key list compared in both directions), G-SEC-3,
  G-CFG-1, G-SC-1.
- **Tests.** G-SEC-3 sub-test: the error text does not contain the password string used in the
  fixture. A nested `db.password` is caught.
- **Done when.** Startup-only drift fails a test; a `db.password` file is rejected with the rotate message and without the value.

### P5-04 config + app: V-R2 and the reload load path (1.5d, after P5-03)
- **Scope.**
  - V-R2 compares the running `destinations.Set` (kept in `Runtime.Dests`) with the freshly loaded
    one: name, table, rowKey, ordered columns.
  - `loadForReload(path, prev, prevDests)` using `ModeRun` (generalise `loadAll` if smaller).
  - Refresh the V-TZ1 comment (D3).
- **Files.** `internal/config/schema.go`, `internal/app/run.go`, tests.
- **Tests.** An edited mapping file is rejected; an identical file passes.
- **Gates.** G-SC-1 (V-R2 fires exactly once; `-selfcheck` output lists it).
- **Done when.** V-R2 is live.

### P5-05 Downstream setters (2d)
- **Scope.**
  - `oradb.DB` holds `ConnMaxLifetime` and `ConnectTimeout` as atomics (or an `atomic.Pointer` to
    settings). `DB.s` is read concurrently by `Ping`, `Acquire` and the session hold
    (`db.go:24`, `session.go:25,34`), so a plain mutation fails `-race`. Make all readers use the
    atomics. `SetConnMaxLifetime(d)` also calls `sql.DB.SetConnMaxLifetime`; `SetConnectTimeout(d)`
    (D19) beside it.
  - Two sources exist for the lifetime: `Params.ConnHold` (writer, per batch, from live Runtime) and
    `DB.s.ConnMaxLifetimeMinutes` (`session.go:34`, copied into `Session.lifetime` at Acquire).
    State which one the pinned-session hold follows (rule: the atomic in `DB`, so Acquire picks up
    a reload on the next session) and test that a lowered lifetime reaches BOTH the `sql.DB` and a
    newly pinned `Session`.
  - `writer.Pool.ReloadHealth()` re-reads `params()` and updates `poolHealth.cfg` and the statement
    ledger under `h.mu`, so the s5.3 `failFastSeconds` lever works (deferred by notes-for-reviewer).
- **Files.** `internal/oradb/{db,connect,session}.go`, `internal/writer/{pool,health}.go`, tests.
- **Tests.** Stub-driver lifetime test; raise `failFastWindow` mid-run and the budget extends.
- **Gates.** G-STMT-*, G-DEP-1 unchanged (`oradb` does not import `config`).
- **Done when.** `-race` green on `writer` and `oradb`, run one package at a time.

### P5-06 SIGHUP reload driver (3.5d, after P5-01, P5-04, P5-05)
- **Scope.** New `internal/app/reload.go` with `Deps.ConfigPath`. One sequence, serialised by a
  mutex (D12), ordered in this one place:
  1. Load. 2. `ValidateAll(ModeRun)` with `prev` and `prevDests`. On failure: ONE WARN naming first
  rule, key path and both values (key paths only and no values for `db.*`), `config_reload_rejected++`,
  nothing touched.
  3. Build the new registry and a fresh `decodeCounters` (D11).
  4. `Store.Swap` (assigns Gen), then `mirrorParams` with that Gen, then ONE `setRuntime` (D1).
  5. `Pool.ReloadHealth()` (needs the new Runtime in force, and precedes Resize so the new
     `statementFailWriters` is in force when the new count is judged).
  6. `applyLive` (P5-07; stub in this card).
  7. `config_reloads++`, store the `config_generation` gauge immediately, INFO
     `config reloaded generation=n+1 changed=[keys] applied=... failed=...`. Key names only; never
     any value for a `db.*` key.
  - `installReloadHandler`'s body (`lifecycle.go:539`) is replaced; `stopHUP` (`lifecycle.go:93`)
    is called right after `wait()` and before teardown, AND it blocks until an in-flight reload
    finishes (hold the reload mutex while stopping, or return only after the goroutine exits).
  - SIGHUP while Draining, or after stop: logged no-op, no side effects, no panic.
  - No config file: log "nothing to reload", count nothing (D7).
  - All new `Warnf`/`Errorf` sit under `Allow` (`scan_allow_test.go`); `reload.go` has no
    `os.Exit`/`Fatal*`.
- **Files.** `internal/app/{reload,lifecycle,graph,run,counters}.go`, tests.
- **Tests.** Drive the reload function directly; one real-signal smoke test where the platform allows
  it (Windows dev host: skip with reason). Reload concurrent with SIGTERM under `-race`, conn closed
  and pool stopping. A rejected reload logs no password or `db.*` value (G-SEC-1 extension). Extend
  the G-BLD-3 scan-reach test to `reload.go`.
- **Gates.** G-CFG-5, G-CFG-6, G-CFG-8 (lower `writers.count` below `statementFailWriters`: REJECTED,
  not clamped).
- **Done when.** An invalid reload changes nothing; a valid one advances `config_generation`
  immediately (the lpar-smoke script asserts on the INFO line, runbook Q10 on the gauge).

### P5-07 Live side effects and pickup points (3d, after P5-06)
- **Scope.** `applyLive(prev, next)` after `ReloadHealth`, in this order:
  1. GOMAXPROCS only if changed.
  2. `SetReadBuffer`, read back with `readBufferSize`, update `readbuf_effective_bytes`, log the
     effective value. Must tolerate `g.conn` nil or closed (log, never panic).
  3. `SetConnMaxLifetime`, `SetConnectTimeout`.
  4. `SetLevel`, `SetRatePerMin`, `Debug.Set`, `Reg.SetIntervalSec` (these already exist except
     the first and third which P5-01 makes safe).
  5. `Pool.Resize` only if the count changed (D14).
  - A failure is logged and listed under `failed=` in the INFO line; it never rolls back the swap
    (D13). `Resize` errors "pool stopped/not started" appear there too, so the operator is not told
    "reloaded" with the old writer count unnoticed.
  - **Pickup audit (required).** Grep every `g.deps.Cfg` and captured `cfg` read under
    `internal/app` and classify each as startup-only or reloadable; reloadable reads move to
    `loadRuntime().Cfg`, the rest are recorded as documented pickups. Known reads:
    `lifecycle.go:138-182` (listen, read buffer, source prefixes: bind-time), `:211` captured
    `cfg` in the receiver loop (`SourceEnforce` `:277`, `StallBudget` `:328,:345`), `:460`
    (`StatsInterval`, `guardEvalEvery`), `:522` (`writers_configured` from startup
    `Writers.Count`: stop overwriting it; `Resize` owns the gauge, `pool.go:178`), expectedNetId.
    The table is the evidence for "each reloadable key in Fig 4.5 has a test or a documented
    pickup". Renaming `Deps.Cfg` to force the compiler to find stragglers was considered and
    rejected as churn across tests; the table plus grep is the method.
  - `udp.sourceAllowList`/`sourceEnforce`: see D9. If reloadable, hold parsed `srcPrefixes` and the
    enforce flag in `Runtime`, loaded once per datagram, zero allocs.
  - `guardEvalEvery` and the stats interval are re-read from live config (the stats timers re-arm,
    P5-13 owns the loop; this card supplies the accessor).
- **Files.** `internal/app/{reload,lifecycle,writers_wiring,runtime}.go`, tests.
- **Tests.** `Resize(n)` with n unchanged: either re-asserts `SetPoolSize` or is deliberately
  skipped; pick one and pin it (D14).
- **Gates.** G-CFG-7 through SIGHUP (4 to 8: 8 goroutines, `maxOpenConns` and `maxIdleConns` both
  9, `writers_configured`=8); G-ALLOC-1 and G-PTR-1 re-run.
- **Done when.** The pickup table is in the commit message and tested or documented per key.

### P5-08 G-CFG-3 and G-CFG-4 under load (1.5d, after P5-07)
- **Scope.**
  - G-CFG-3: continuous load, SIGHUP-driven reload changing several keys, a probe reading two keys
    from one `loadRuntime()` snapshot that always sees one generation.
  - G-CFG-4: disable the guard bit for transcode X in `mirror.GuardTable`, reload an unrelated key,
    assert the bit is still set (Phase 3 ruling D2).
- **Tests.** `-race -count=20`, one package per invocation, full-path clang `CC`, explicit timeout.
- **Gates.** G-CFG-3, G-CFG-4, G-RACE-2.

### P5-09 health: circuit and trading window, pure (2d)
- **Scope.**
  - `Circuit` states Starting, Live, Quiet, Down; `Observe`, `Tick(nowMono, LivenessConfig)
    Transition`, `State()`. `Transition` carries the log inputs (silent seconds, last transcode,
    outage seconds). Starting->Down on the startup grace.
  - Health-owned `LivenessConfig` and `TradingWindow`; the window gate is fail-open when the
    location is nil. No clock, logger or counter inside the machine (D6, D17).
  - Tighten `deps_test.go`'s `health` row to also forbid `config` and `database/sql` (tightening
    only; confirm the rule syntax supports the stdlib path first, else add a separate test).
    G-BLD-3's scan reaches `internal/health`.
- **Files.** `internal/health/{circuit,window}.go`, tests, `deps_test.go` (tighten only).
- **Gates.** G-LIVE-1 (synthetic monotonic time, thresholds reloaded between runs). G-LIVE-3 is met
  at health level only: a `Window` built from an unresolvable zone warns once, treats every hour as
  in-window, keeps the louder severities. The spec's "Start" leg cannot occur at app level because
  V-TZ1 rejects it (D3, open item 3); cite the existing V-TZ1 test.
- **Done when.** Table-tested; the severity gate never changes the state.

### P5-10 health: market-session machine, pure (1.5d)
- **Scope.**
  - Per-market-type `Session`, `next = stateOf(code)` always; an edge set decides expected versus
    anomaly; a repeat of the same code is applied with no anomaly; gauge values 0-5; market types
    outside 1-4 reported as ignored (D15).
  - Edge set: Fig 4.7 as written. D4's extra edges (Closed->Open, PostClose->Open) are a PROPOSAL
    pending the user (open item 1); the edge set is one table so either choice is a one-line change.
- **Files.** `internal/health/session.go`, tests.
- **Tests.** A table pinning the full edge set; every non-edge pair counts an anomaly AND the
  machine still advances.
- **Gates.** G-SESS-1, G-SESS-2 (same LogTime: arrival order wins), G-SESS-3.

### P5-11 Instruments and circuit wiring in the stats goroutine (2.5d, after P5-02, P5-09)
- **Scope.**
  - Resolve the new handles in `instruments` before `Freeze`: `circuit_state`, `circuit_quiet_events`,
    `circuit_down_events`, `circuit_outage_max_ns`, `session_state`, `session_transitions`,
    `session_anomalies`, `session_events_dropped`, and `panics_recovered{stage=stats}`
    (`reg.CounterAt(obs.IDPanicsRecovered, int(obs.StageStats))`).
  - A 1 s circuit tick in `startStats`, clamped to `heartbeatWarnSec/2` and re-armed from live config
    at each fire. Input is the existing `last_datagram_mono_ns` gauge (D6).
  - App projects `config.LivenessConfig` into the health type; logs and counts each transition under
    the P5-02 classes, all under `Allow`.
- **Files.** `internal/app/{instruments,lifecycle}.go`, tests.
- **Gates.** G-LIVE-2 (app-level stream of 7208 datagrams and no 6541; ten warn-periods; circuit
  stays Live); G-OBS-1 and G-OBS-3 stay green.

### P5-12 Channel H: producer and drain (2.5d, after P5-10, P5-11)
- **Scope.**
  - First verify, per code in the s4.5 table (6511/6521/6522/6531/6571), where `MarketType` lives:
    `decode/vct_msgs_6511.go:144` reads it as the row key from the wire. Confirm the other four
    and which decoder files must expose it (a new field on the unit, not a flag, per SOLID). Only
    then write the producer.
  - `sessionCh chan model.SessionEvent` (capacity 64, fixed) allocated in `Build`. In `subpacket.go`,
    after `decodeSubPacket` and outside the recover boundary, a non-blocking send; full channel
    increments `session_events_dropped` and the `UpdateUnit` is still delivered.
  - The health tick drains completely, FIFO, setting gauges and counters. Session logging uses the
    session classes.
  - A restart of the stats goroutine (P5-13) while the channel fills is covered by capacity 64 and
    the drop counter (ASM-A2 expects zero drops in normal operation). Add a test for a restart
    mid-fill.
- **Files.** `internal/app/{subpacket,graph,lifecycle}.go`, possibly `internal/decode/*`.
- **Gates.** G-SESS-4, G-ALLOC-1 (0 allocs), G-PTR-1.
- **Done when.** The pre-filled channel test passes and `scan_allow_test.go` is satisfied.

### P5-13 Stats goroutine hardening (2.5d, after P5-11)
- **Scope.**
  - Recover and restart (F28): wrap the body, log directly via `appLogger` with stack, increment
    `panics_recovered{stage=stats}`, restart all three timers while `statsStop` is open (D18).
  - Clock-step detector (F23): REPLACE the existing check in `startStats`
    (`now.Sub(last) - interval`, where `now` comes from a ticker value carrying a monotonic reading,
    so it measures ticker lateness, not wall steps). New check compares wall and monotonic deltas
    (`Round(0)` on the wall reading) and fires on `|dwall - dmono| > 1 s` per emission; WARN under
    `ClassClockStep` with magnitude and direction. The first emission and ticker backlog must not
    false-positive.
  - Whole body behind an injectable `now() (wall time.Time, mono time.Duration)`.
  - Audit table of the four panic regions and three policies (s5.5). Lifecycle, ticker and
    receiver stay deliberately unrecovered.
- **Files.** `internal/app/lifecycle.go`, tests; existing skew tests are updated, not hidden.
- **Tests.** Wall +3600 s and -3600 s each increment the counter; a normal jittery tick does not;
  the circuit machine fed by `last_datagram_mono_ns` does not transition across a wall step; ordering
  comparisons, the flush tick and the `ConnMaxLifetime` clock are unaffected (assert each reads
  monotonic time). An injected panic restarts the goroutine and emission continues.
- **Gates.** G-CLOCK-1.
- **Done when.** The pre-existing `Emit`/`lastParks` race is noted if touched, not hidden.

### P5-14 G-KEY-1 (1d)
- **Scope.** An `internal/app` test over the shipped `mapping.json`: for every destination and
  contributing transcode, `ShardOf(unit)` equals the shard owning the contribution's row key. No
  production code expected.
- **Done when.** A deliberately corrupted mapping fails the test.

### P5-15 Runbook (2.5d, after P5-07, P5-11, P5-12, P5-13)
- **Scope.** `docs/runbook.md` with the twelve ordered questions (s6.6) naming exact instruments,
  s6.5 health checks, s6.7 AIX observability, the s8.8 deploy and rollback checklist, the Q10 pickup
  windows (up to `stats.intervalSec`) and the `netstat -s -p udp` dependency.
- **Tests.** An automated test parses `runbook.md` for instrument names and checks them against the
  ledger (G-OBS-3 style); a manual cross-check is not enough.
- **Done when.** Q6a's `traceTranscodes` plus SIGHUP step is demonstrated to work (P5-01, P5-07);
  every open question in s13 is listed with owner: OQ-L7..L12 (raised in Vol 4) and the inherited
  OQ-L3, OQ-L4, OQ-L5, plus OQ-L6 if s13 carries it (check; the table shows L3 and L5 only).

### P5-16 On-target scripts, CI process and race sweep (2d, after P5-08, P5-12, P5-13)
- **Scope.**
  - `scripts/lpar-smoke.sh` SIGHUP step: send HUP, assert the `config reloaded generation=` INFO
    line appeared (not the 60 s-sampled gauge), SKIP versus FAIL as the script does. This is the
    OQ-L7/ASM-A9 experiment.
  - G-RACE-1: write the CI requirement down (no CI config exists). Any `t.Skip` under `-race`
    carries a reason comment.
  - G-RACE-2 sweep, one package at a time, for G-FLUSH, G-EPOCH, G-INFL, G-ROW, G-CFG-3, G-LIFE-4.
  - Hand the LPAR run (G-AIX-1 assertions 1-5, G-ASSERT-1 on target) to the operator.
- **Done when.** Scripts shipped, `aix_test.go` passes; LPAR results recorded in as-built or listed
  outstanding with the reason.

### P5-17 Documentation close-out (1.5d, last)
- **Scope.** CLAUDE.md section (reload classes, health package, channel H, new log classes, reload
  ordering rule, "Things that will bite you"); the errata list below in one doc commit; ADR status
  flips for ONLY the ADRs actually exercised (check L30, L31..L38 individually); "As built" section
  here; exit checklist run.

## 5. Seams and rulings

- **D1** Single publication. Sequence: validate, build registry, `Swap` (assigns Gen),
  `mirrorParams`, one `setRuntime`. A brief window has `Store.Load()` new and `loadRuntime()` old;
  benign because the datapath reads only `loadRuntime()`, and the G-CFG-3 probe reads through it.
- **D2** Reload calls `ValidateAll(ModeRun)`, never bare `Store.Swap`, which skips schema and
  destination rules.
- **D3** V-TZ1 stays a hard rejection, so a bad zone cannot start or reload; the fail-open path in
  `health` is defence in depth and G-LIVE-3 is tested at health level. Flagged to the user (open
  item 3). `main.go` imports `time/tzdata`, so the case is rare.
- **D4** PROPOSED, not settled: Closed->Open and PostClose->Open as expected edges, so an
  overnight process does not raise `session_anomalies` each morning. It shrinks the anomaly signal
  Q-rows correlate with `seq_gap_messages`; the erratum must cite bible s4.5. Default build:
  Fig 4.7 as written until the user confirms.
- **D5** The producer of `SessionEvent` is the decoder side of the receive path;
  `session_events_dropped` is owned by the decoder (Vol 0 s9.2 row H). `MarketType` source verified
  per code in P5-12.
- **D6** Circuit input is the existing `last_datagram_mono_ns` gauge; no second padded atomic.
- **D7** No config file: "nothing to reload", no counter.
- **D8** The destinations file is re-read and compared (spec-literal V-R2); the running set is
  `Runtime.Dests`.
- **D9** Startup-only classification of `udp.sourceAllowList`, `udp.sourceEnforce`,
  `ordering.gapScope`, `ordering.gapAuditPackets`: NOT settled. Vol 0 s7.2
  (`00-foundations.md:1863,1864,1928,1929`) marks all four reloadable, so reclassifying is a
  contract change and `sourceEnforce` is an incident lever. Default build: allow-list and enforce
  stay reloadable, held in `Runtime` (parsed prefixes and flag, no per-datagram allocation). For
  gapScope and gapAuditPackets, P5-07 checks whether `decode.GapDetector` (built in
  `subpacket.go:123`) can take a setter without losing sequence state; if not they are
  startup-only, with the reason in the erratum. Needs the user (open item 2).
- **D10** `Pool.ReloadHealth` on each accepted reload, before `applyLive`.
- **D11** A fresh `decodeCounters` per registry; the old sink is never mutated.
- **D12** Reloads serialised by a mutex; `stopHUP` waits for an in-flight reload; SIGHUP after stop
  or while draining is a logged no-op.
- **D13** A side-effect failure after the swap is logged and listed in the INFO line's `failed=`;
  never rolls back; no new counter.
- **D14** Pool sizing is applied in `Pool.Resize` (`SetPoolSize` first, `pool.go:178`), called only
  when the count changed. Erratum to s2.4 step 3, which puts the `database/sql` limits before the
  resize and re-derives them from the published pointer.
- **D15** A market type outside 1-4 is log-only (rate-limited), no counter.
- **D16** New classes: `ClassCircuitQuiet` (WARN), `ClassCircuitDown` (ERROR), decode-anomaly split
  from `ClassSessionAnomaly`; the Q1 fatal at `fanout.go:225` is re-homed (P5-02). Rate limiting
  stays per class, coarser than s4.3's per-(market_type, from, to). Erratum to s6.2/s4.3.
- **D17** Starting->Down->Live logs "recovered" without the "expect a BCSeqNo gap" text; outage max
  is observed on recovery only.
- **D18** The stats goroutine owns three timers; F28 restarts all of them.
- **D19** `SetConnectTimeout` is added beside `SetConnMaxLifetime` so a reloadable key is not
  silently ignored (a deviation from Vol 4's list of five effects).
- **D20** No new config keys, flags, modules or packages; no `deps_test` relaxations.
- **D21** The as-built `ValidateAll(c, dests, prevDests, prev, mode)` API and `Problems` type
  supersede the s2.7 sketch; `RuleError` is derived from them.
- **D22** No Gen-aliasing: use `Store.Gen()`.

## 6. Open items needing the user

1. **D4** day-boundary session edges (proposal; default is the spec as written).
2. **D9** startup-only reclassification of the four keys (default: allow-list/enforce stay
   reloadable; gap* decided by P5-07's check). Do not touch `sourceEnforce` without a yes.
3. **D3** V-TZ1 hard rejection versus the spec's "WARN at startup, fail open" (G-LIVE-3 "Start"
   leg). Alternative: demote V-TZ1 to a WARN and fail open at runtime.
4. Confirm that the LPAR runs, the live-Oracle spike, OQ-L7 (SIGHUP delivery, restart on non-zero)
   and OQ-L8..L12 are outside this session.
5. P5-00 is a manual step in the main checkout.

## 7. Errata to apply to the LLD

- **s2.1**: the sequence has two publication points (Store and Runtime), not "one store" (D1).
- **s2.4 step 3**: pool limits are applied by `Pool.Resize` only when the count changed (D14).
- **s2.7**: the as-built `ValidateAll`/`Problems`/`RuleError` API supersedes the sketch (D21).
- **s3.5 / G-LIVE-3 / OQ-L5**: V-TZ1 rejects at load; fail-open is health-level only (D3), if the
  user keeps it.
- **Fig 4.7**: Closed->Open and PostClose->Open edges, if confirmed (D4).
- **s4.5 / s10.3**: producer and ownership of `session_events_dropped` is the decoder side (D5).
- **s6.2 / s4.3**: new log classes, per-class rate limiting (D16).
- **Vol 0 s7.2 / Fig 4.5**: reload class of the four keys, only if D9 lands on reclassification.
- **Vol 4 list of five reload effects**: `SetConnectTimeout` added (D19).

## 8. Exit checklist

- [ ] `go build ./...`, `go vet ./...`, `go test ./...` green.
- [ ] `-race` green on each touched package, one package at a time, explicit timeout on `internal/app`.
- [ ] `-race -count=20` green for G-CFG-3 and the G-RACE-2 set.
- [ ] `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build ./cmd/feedvalve` succeeds.
- [ ] `poc` still builds and the `-debug` line is byte-identical (G-DIFF-1).
- [ ] `deps_test.go` changed only by tightening `health`'s row.
- [ ] G-ALLOC-1 still 0 allocs; G-PTR-1 green.
- [ ] G-OBS-1..4 and G-CFG-1 green; G-BLD-3 scan reaches `reload.go` and `internal/health`.
- [ ] Every gate in section 2 landed, or listed as an on-target residual with a named owner.
- [ ] `docs/runbook.md` exists with the automated name check; CLAUDE.md, errata, ADR statuses updated.
- [ ] `phase5` merged into `develop` in the main checkout; the `phase3`-named worktree removed.

## 9. Known open defects not owned by this phase

- The `obs.Emit`/`lastParks` race (why `go test -race ./internal/app/` is not fully green).
- The `ExecTx` rollback deadlock on a driver-internal panic.
- No CI config in the repo.

## 10. As built

To be filled in at close-out (P5-17): deviations from D1..D22, gate results, LPAR and live-DB
outcomes.
