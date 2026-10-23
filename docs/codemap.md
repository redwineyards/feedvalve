# Code map — `cmd/` and `internal/`

What each non-test source file does, limited to its core function. Files ending in `_test.go`
are excluded (`internal/oradb/oratest/driver.go` is a test helper but not a `_test.go` file, so
it is listed). This describes `develop` after Phase 5 was merged in (P5-17).

## The pipeline in one paragraph

`cmd/feedvalve` parses flags and calls `app.Run`. `app` loads `config` and the `destinations` field
map, builds the `obs` instrument registry, opens Oracle through `oradb`, and constructs the graph.
The **receiver** goroutine reads UDP datagrams into the `framing` ring (QD). The **decoder**
goroutine drains QD. It splits each envelope into sub-packets, decompresses them (`lzo`), reads the
header and feeds the gap detector (`decode`). It then runs the per-transaction-code decoder, which
produces `model.UpdateUnit`s, and routes them by token hash to per-shard channels (Q1).
Market-status units also go to the `health` session machine. Each **shard** goroutine (`mirror`)
merges units into its in-memory row mirror, coalesces changes, and on each flush tick or
non-rate-limited event opens an epoch and pumps dirty rows as `model.Batch`es onto batchQ. The
**writer** pool (`writer`) runs each batch as one Oracle transaction of `MERGE` statements and
returns per-row outcomes to the shard. The **stats** goroutine emits counters, evaluates the
ordering guard and runs the circuit-liveness machine. SIGHUP reloads the configuration without a
restart. Shutdown stops the socket first, then drains QD, the shards and the writers, in that order,
under one deadline.

---

## `cmd/feedvalve`

- **`main.go`**: the process entry point. Parses `-config`, `-selfcheck` and `-dbcheck`, wires
  SIGINT/SIGTERM to a cancellable context, and calls `app.Run`. It then maps the returned error to
  an exit code. It holds the only `os.Exit` in the tree, called after every defer has run. It embeds
  `time/tzdata` so the trading-window timezone resolves on AIX. The version is stamped in through
  `-ldflags -X main.version`.

## `internal/app`: wiring, goroutines, lifecycle

- **`doc.go`**: package contract. `app` is the only package that may see both the receive path and
  the database side. It binds the socket last and drains in order.
- **`run.go`**: `Run`, the top-level flow. Loads and validates config plus the destination map, then
  branches:
  - `-selfcheck` prints the config, destinations and decoder catalogue, builds the registry, and
    generates every MERGE statement shape (`destinations.CheckStatements`).
  - `-dbcheck` runs a round trip, then the catalogue check against the field map.
  - The default `relay` path creates and freezes the obs registry, creates the loggers, sets
    GOMAXPROCS, opens the DB, logs catalogue mismatches, calls `Build`, then `Graph.Run`.

  Also defines the exit codes and `ExitError`, and `loadForReload`, which validates a reloaded file
  against the running config.
- **`graph.go`**: `Graph`, the constructed process. `Build` creates the decoder registry, the guard
  table, the runtime snapshot, the QD ring and the Q1 channels. It also builds the shards
  (`buildShards`) and the writer pool (`buildPool`), all before any socket exists. `Fatal` is a
  one-shot channel for terminal conditions (Q1 overflow, DB fail-fast, writer-pool stop, batchQ
  send failure, shard panic or invariant breach, unreported batch row, UDP read failure). It records
  every resource acquired and released.
- **`lifecycle.go`**: `Graph.Run` and the receive side.
  - Startup order: writers, shards, flush ticker, decoder, stats, then bind the socket, then the
    reader and the SIGHUP handler.
  - The receiver loop: read, take a strictly increasing receipt time, check the source allow-list
    from the runtime snapshot, flag oversize datagrams, and publish to QD within the stall budget.
  - `teardown`: close the socket, wait for the reader, close QD and drain the decoder, close the
    shard channels, arm the shared drain deadline, wait for the shards, close batchQ and the writer
    pool, then stop the ticker and stats. `emitStats` and `evaluateGuards` are serialised by a
    mutex.
- **`subpacket.go`**: `receiver`, the decoder goroutine's per-datagram work. It checks the envelope
  netId and calls `framing.Split`. Per sub-packet, inside a panic-recover boundary, it:
  - decompresses with `lzo.DecompressSafe`;
  - checks the netId oracle (log only, never drop);
  - reads the header and handles error responses and short payloads;
  - feeds the gap detector, reconfiguring it when the config generation changes;
  - counts heartbeats and looks up the decoder;
  - checks the decompressed length and `BodyLen`, then runs `Decode` into a 40-unit fan-out buffer.

  It then routes the units and publishes market-status events to the health channel.
- **`fanout.go`**: `route` and `send`. Each unit gets its receipt time and joins the active-token
  sample. It is then sent to its shard channel without blocking. If the channel is full, it waits
  on one reusable timer against the per-datagram stall budget. Expiry logs every shard's depth and
  raises `FatalQ1Overflow`. Nothing is dropped.
- **`shard.go`**:
  - `ShardOf` routes general units to the designated shard, and token units by
    `mix32(token) % shards`.
  - `buildShards` creates the flush bus, batchQ, the per-shard completion and tick channels, and the
    `mirror.Shard`s. `runShard` runs each one, turning a panic or a returned invariant error into a
    fatal condition.
  - `startFlushTicker` sends a tick to every shard each flush window and follows window changes on
    reload. `armDrainDeadline` starts the one shared drain timer.
- **`writers_wiring.go`**: projects config into `writer.Params` (reread per batch, so reloads take
  effect) and builds and starts the `writer.Pool` with batchQ and the completion channels. A watcher
  goroutine turns `Pool.Stopped` into `FatalDBFailFast` or `FatalWriterPoolStop`. `stopWriters`
  closes batchQ and waits for the pool, cancelling it if the drain deadline passes.
- **`stats.go`**: the stats goroutine, run under a supervisor that recovers from panics and restarts
  it with backoff. It has three self-re-arming timers:
  - counter emission, with a check for wall-clock steps against the monotonic clock;
  - the ordering-guard evaluator;
  - the circuit/health tick.
- **`health.go`**: drives the `health` package from the stats goroutine. `circuitTick` feeds the
  last-datagram time to `health.Circuit`, publishes `circuit_state`, and logs transitions. Severity
  is raised or lowered depending on whether it is inside the trading window. `publishSession` (on
  the decoder side) pushes market-status events onto a bounded channel without blocking.
  `drainSessions` (on the stats side) applies them to `health.Sessions` and logs anomalies.
- **`reload.go`**: SIGHUP configuration reload. Reloads are serialised and refused during shutdown
  or when there is no config file. Each reload:
  1. loads and validates the file against the running config and destinations;
  2. builds a new decoder registry and runtime snapshot;
  3. calls `Store.Swap`, then publishes the snapshot;
  4. reloads the writer pool's health thresholds;
  5. applies the live side effects: GOMAXPROCS, receive buffer, DB lifetime and timeout, log level
     and rate, debug trace, stats interval, and writer-pool resize.

  A rejected file changes nothing and is reported with its rule, key and values.
- **`catalogcheck.go`**: compares Oracle's `ALL_TAB_COLUMNS` for each destination table against the
  field map. It checks the key column, every mapped column, and `updated_at` for presence, type
  family, precision or byte capacity, and nullability. Findings fail `-dbcheck` and are logged as
  ERROR at startup.
- **`instruments.go`**: resolves every obs handle before the registry freezes: decode-path,
  per-transcode, per-shard mirror (`mirror.ShardInstruments`), writer, circuit and session. Also
  converts config into `decode.TranscodeSpec`s and `obs.Options`, and maps `decode.BuildError`
  problems onto config's `V-` rule IDs.
- **`counters.go`**: `decodeCounters`, which implements `decode.Counters` (record-count clamp,
  booktype skip, blob overrun, message truncation) and `decode.Reports` (sequence gap, regression,
  resync, gap audit) on top of the obs handles, with rate-limited log lines.
- **`runtime.go`**: `Runtime`, the snapshot of config, decoder registry, destinations, mirror
  `Params` and parsed source prefixes. It is published as one atomic pointer, and the datapath reads
  only this. Also converts config into `mirror.Params` and `mirror.Sizing`.
- **`tokens.go`**: `tokenSet`, a generation-stamped open-addressing set that counts distinct tokens
  per second for `active_tokens_1s_max`.
- **`clock.go`**: the process-start monotonic origin, and `nextRecvMono`, which keeps receipt times
  strictly increasing so the ordering tiebreak never sees a tie.
- **`rcvbuf.go`**, **`rcvbuf_unix.go`**, **`rcvbuf_windows.go`**: read back the socket's effective
  `SO_RCVBUF`, so the log shows when the kernel capped the requested size.

## `internal/config`: configuration

- **`doc.go`**: package contract. Pointer-optional fields, unknown keys rejected, every key classed
  for reload, and no password key.
- **`config.go`**: the resolved `Config` tree (UDP, runtime sizing, flush, writers, DB, ordering,
  stats, liveness, debug, transcodes and their contributions), plus duration helpers.
- **`load.go`**: strict JSON decode with built-in defaults applied by `resolve`. Rejects any
  `password` key anywhere in the file without echoing its value (V-DB3), and reports unknown keys as
  V-STRICT1. Derives the DB pool size from `writers.count + 1`.
- **`validate.go`**: `Validate`, the config-only rule set. It checks per-key ranges and cross-field
  rules: ring capacity is a power of two, general shard is in range, batch size is within the
  ceiling, retry order, heartbeat order, trading window, timezone, DB fields, and trace codes.
  `StartupOnlyKeys` (14 keys) are refused on reload, as is any change to transcode membership. A
  `db.*` value is never echoed in the error.
- **`schema.go`**: `ValidateAll`, which adds rules needing the destination table. Contributions
  must name declared destinations and columns, the blob must fit 256 bytes, and row keys must be
  derivable. Also enforces "a db object is required" per mode, and on reload rejects any change to
  the destination set (V-R2).
- **`problem.go`**: the `V-*` rule identifiers. `ValidationError` collects every problem, each with
  its key path and running/attempted values. `RuleError` summarises the first problem for the
  reload WARN line.
- **`store.go`**: `Store`, an atomic pointer to the current `Config` with a generation number.
  `Swap` validates a candidate before publishing it.
- **`source.go`**: parses `udp.sourceAllowList` entries (bare IP or CIDR) into `netip.Prefix`es.

## `internal/destinations`: the three target tables, the field map, and MERGE generation

- **`doc.go`**: package contract. Owns table names, row-key rules, column/bit/image layout and
  contributor slots, and never imports the database packages.
- **`mapping.go`**: loads the embedded `mapping.json`, or an operator file, with a strict decode.
  Builds each `Destination`: row-key rule, key column, contributor slots sorted by ascending code,
  and column offsets as the running sum of widths. Checks the declared `imageLen` against that sum.
- **`destination.go`**: `Destination` (ID, name, table, key rule, key column, columns, image length,
  contributors) with slot and column lookups and `FullMask`. `Set` is the immutable table, indexed
  by ID and by name.
- **`column.go`**: `Column`, one maskable image field: bit, offset, width, kind, scaling,
  length-bit, SQL name(s), and pre-validation metadata (precision, max bytes, NOT NULL, domain).
  Also `RowKeyRule`.
- **`statement.go`**: `StatementFor` generates the Oracle `MERGE … USING (SELECT :1 … FROM dual)`
  for one destination and column mask. Binds are positional, the key appears only in `ON`, values
  are scaled where needed, and `updated_at` is always set. It also returns the `BindPlan`, which
  says where each placeholder's value lives in the row image. A ladder (group) column expands into
  quantity, price and orders binds.
- **`selfcheck.go`**: `CheckStatements` generates the statement for every destination at every
  single-bit mask plus the full mask, and checks every identifier against
  `[A-Za-z][A-Za-z0-9_]{0,29}`.
- **`validate.go`**: per-set and per-column rules. Exactly 3 destinations, unique names, image
  ≤ 256 B, ≤ 64 columns, bit = index, widths matching kinds, valid scale/lenBit/domain, unique SQL
  names, and a valid key column that is not reused as a data column or as `updated_at`.
- **`ident.go`**: validates table and column names as safe unquoted Oracle identifiers.
- **`layout.go`**: build-time gate G-LAY-3, which fails compilation if a shipped destination image
  exceeds `model.ImageSize`.

## `internal/framing`: datagram ring and envelope split

- **`doc.go`**: package contract. QD (the SPSC ring) and the envelope rules: 8-byte prefix,
  `messageLength` checks, and netId as an alignment oracle.
- **`ring.go`**: QD, a lock-free single-producer/single-consumer ring of `model.Datagram`.
  Power-of-two capacity, head and tail on separate 128-byte cache lines, and separate
  `Producer`/`Consumer` handles. A publish stores tail before signalling a capacity-1 wake channel.
- **`consume.go`**: `Consumer.Drain` copies each slot out before advancing head, spins briefly,
  then parks. Returns only when the ring is closed and empty. Refuses a second consumer.
- **`budget.go`**: `Producer.PublishWithin`. Fast path first; when the ring is full, it yields until
  the caller's budget expires, then returns `model.ErrStallBudgetExpired`.
- **`split.go`**: `Split` walks the envelope (`cNetId`, `iNoPackets`, length-prefixed sub-packets
  in FIFO order) and hands each one to a `SubPacketHandler`. Rules R1–R9 stop framing on bad
  lengths, recording the rule, index and offset. Oversize input is trimmed to 512 bytes.

## `internal/lzo`: decompression

- **`doc.go`**: package contract. Pure-Go LZO1Z decompression only, standard library imports only.
- **`decompress.go`**: port of liblzo2's LZO1Z decompressor. `DecompressSafe` checks every bound
  and is the only variant the packet path may call. `Decompress` is unchecked and used in tests
  only. `MaxDecompressedLen` is 2048.
- **`errors.go`**: four distinct sentinels (input overrun, output overrun, lookbehind overrun,
  input not consumed). Each maps to its own `decompress_failed{reason}`.

## `internal/wire`: protocol surface

- **`doc.go`**: package contract. Protocol constants, the field cursor and the pack(2) rules.
- **`const.go`**: protocol constants (8-byte prefix, 40-byte headers, 512/516-byte envelope,
  182-byte error response), pinned by gate G-BLD-1.
- **`reader.go`**: `FieldReader`, a resettable big-endian cursor (`I16`, `I32`, `I64`, `F64`,
  `Byte`, `Copy`, `Raw`, `Pad`, `Skip`) where call order is wire order. It emits optional per-field
  trace lines at absolute payload offsets, and allocates nothing when tracing is off.
- **`field.go`**: `FieldID`, the uint16 names for every header and body field, with their
  spec-spelled strings.
- **`netid.go`**: `NetIDMatches` and `EnvelopeNetIDMatches`, predicates that accept the netId as a
  binary or an ASCII digit. They never drop anything.

## `internal/decode`: header resolution, registry, per-code decoders

- **`doc.go`**: package contract. One file per transaction code, layout gates, and record counts
  clamped.
- **`header.go`**: `ReadHeader` checks the payload length, rejects a non-zero `ErrorCode` before
  trusting the code, resolves the header kind, and parses BCAST_HEADER or MESSAGE_HEADER field by
  field. Rejects `MessageLength < 40`.
- **`resolve.go`**: `Resolve` classifies the header without consulting the registry. Code
  6541/6503 at offset 8 with a payload length of exactly 48/60 is a MESSAGE_HEADER; anything else
  is a BCAST_HEADER.
- **`decoder.go`**: the `Decoder` interface (`Code`, `BodyLen`, `MaxUnits`, `Decode`) and its
  sentinel errors. `ColSpec` holds the mask, width and per-bit blob offsets. Also defines
  `RegistryEntry`, the `Counters` sink, and `Traceable`.
- **`catalogue.go`**: the compile-time catalogue of decoder constructors, filled by each code file's
  `init()`. `Catalogue` and `CanDecode` separate "unknown" from "unconfigured".
- **`registry.go`**: `Registry` and its implementation, parallel arrays of up to 32 entries sorted
  by code. `Lookup`/`Slot` scan linearly; the slot index is shared with the obs `{code}` labels.
- **`build.go`**: `BuildRegistry` resolves configured transcodes against the catalogue and
  destinations. It assigns contributor slots, builds each `ColSpec`, and constructs decoders,
  reporting every problem at once.
- **`unit.go`**: shared decoder helpers. `stamp` initialises a unit, `contribute` sets its
  row/mask/slot, `put*` write wire-form bytes at registry-computed blob offsets, `sealBlob` enforces
  the blob size, and `clampRecords` applies the record count rule.
- **`gap.go`**: `GapDetector`, BCSeqNo continuity using serial-number arithmetic. Scope is global,
  per-transcode, or off. Regressions are not counted as gaps; jumps over 1,000,000 are reported as
  resyncs. It skips MESSAGE_HEADERs and has an audit mode. `Reconfigure` applies a reloaded scope
  or audit length.
- **`session_event.go`**: `SessionEventOf` extracts a `model.SessionEvent` (market type, code, log
  time) from a decoded market-status unit, for the health session machine.
- **`layout.go`**: documentation of the G-LAY-1 compile-time gate idiom. No code.
- **`mbp_only_7208.go`**: 7208 MS_BCAST_ONLY_MBP feeds `MD_TOKEN_QUOTE`. Reads `NoOfRecords`
  (clamped to 2) and decodes that many 214-byte records (pad byte at offset 17): quote fields and
  the 120-byte MBP ladder, keyed by token.
- **`mbo_mbp_7200.go`**: 7200 MS_BCAST_MBO_MBP (the exchange's alternative to 7208), also feeding
  `MD_TOKEN_QUOTE`. Skipped unless BookType is 1 or 2. Repacks its 10-byte price levels into 7208's
  12-byte ladder layout and reads the outer fields at the spec's corrected offsets.
- **`security_update_7305.go`**: 7305/7340 MS_SECURITY_UPDATE_INFO feeds `MD_SECURITY_MASTER`. A
  294-byte layout with no padding (the eligibility array uses a 3-byte stride). Stores symbol,
  series, expiry, strike, lot, tick, delete flag and base price.
- **`vct_msgs_6511.go`**: the 6511/6521/6522/6531/6571 market-status family feeds
  `MD_MARKET_STATUS`. These are general units keyed by MarketType; the wire token is stored as data,
  not used for routing. Stores the contributing code, the clamped message text, the broadcast
  destination and the timestamp.

## `internal/mirror`: the shard goroutine (coalescing and batching)

- **`doc.go`**: package contract. Must not import the DB or the network. The mirror is the queue:
  overload makes batches bigger, never drops data.
- **`shard.go`**: `Shard`, one partition of all three destinations. Each destination has a
  row-key → slot index, a chunked pointer-free `Entry` arena, accumulating and cohort rings, epoch
  state, the batch free list and the quarantine lists. `NewShard` performs every allocation up
  front.
- **`run.go`**: `Shard.Run`, the goroutine loop. It selects over incoming units (merge), returned
  batches (completion), flush ticks and flush wakeups. After each event it opens an epoch if one is
  due. On channel close it drains until quiescent, or counts abandoned rows when the shared drain
  deadline fires. It warns on epochs that stay open too long.
- **`merge.go`**: `merge`, which applies one `UpdateUnit` to the row mirror. For each present
  contribution it:
  1. finds or creates the entry;
  2. runs the ordering guard;
  3. validates the blob width and records the ordering state;
  4. pre-validates the fields and copies them into the row image;
  5. ORs in the dirty columns;
  6. suppresses unchanged rows by digest, or else marks the row dirty, re-dirty (if in flight) or
     coalesced.

  A non-rate-limited unit triggers a flush on every shard.
- **`guard.go`**: the ordering guard's per-contribution verdict (apply, supersede, or bypass).
  Token units compare BCSeqNo, then LogTime, then receipt time; general units compare LogTime, then
  receipt time. All comparisons are per contributor slot.
- **`guardtable.go`**: `GuardTable`, per-transcode guard state shared by shards: an atomic off-flag
  and the offered/superseded counters.
- **`guardeval.go`**: `EvaluateGuards`, run by the stats goroutine. In auto mode it disables the
  guard for a transcode for the rest of the session when superseded/offered exceeds the configured
  ratio over enough samples.
- **`prevalidate.go`**: refuses individual field values Oracle is certain to reject, before they
  reach the image: NUMBER precision, NaN/Inf, negative time, char domain, text length against
  `SQLMaxBytes`. Each refusal is counted by destination, reason and transcode.
- **`epoch.go`**: `maybeOpenEpoch` swaps the accumulating rings into the cohort (an O(1) snapshot),
  takes a fresh `Params`, releases the quarantine on a config change, then pumps. `closeEpoch`
  records the duration and reopens if another flush is already pending.
- **`pump.go`**: `pump` fills free `Batch`es from the cohort (up to `maxBatchSize`, destinations in
  order). Each row gets its column mask, image copy and digest, and the entry's mask is cleared at
  fill time. Batches are sent on batchQ without blocking; a full batchQ is a fatal defect.
- **`completion.go`**: `applyCompletion` handles a returned batch's per-row outcomes. Committed:
  store the digest and written mask, and requeue if re-dirtied. Failed: requeue at the head for
  retry. Rejected: quarantine. The batch is reset to the free list, then the epoch closes or pumping
  continues. Unadjudicated rows and routing errors are defects.
- **`quarantine.go`**: a row Oracle permanently refused is parked off every ring with no retries.
  It is released on a config reload, or immediately when its mid-flight columns avoid the refused
  ones.
- **`flushbus.go`**: `FlushBus`, the only state shared between shards: a per-shard request flag plus
  a capacity-1 wake channel. `Trigger` broadcasts to all shards.
- **`ring.go`**: a growable power-of-two circular buffer of arena slots, with O(1) `Swap` and a
  `PushFrontBlock` used for retries.
- **`digest.go`**: the change-suppression hash of a row image (`maphash`, never zero).
- **`params.go`**: `Sizing` (startup-only: shard count, rows, chunk size, batches in flight, batch
  ceiling, digest seed), `Params` (reloadable: window, batch size, stall warning, drain timeout,
  guard settings) and `GuardMode`.
- **`instruments.go`**: `ShardInstruments` and `TranscodeInstruments`, the obs handles a shard
  writes.
- **`assert_on.go`** / **`assert_off.go`**: under `-tags feedvalve_assert`, a mutex that panics if
  two goroutines enter one shard's loop (gate G-ASSERT-1). Otherwise compiled out.

## `internal/writer`: Oracle persistence

- **`doc.go`**: package contract. Reaches the driver only through `oradb`. Positional binds,
  `MERGE` only, a two-phase execute, and nothing secret logged.
- **`pool.go`**: `Pool`, `writers.count` goroutines sharing batchQ. `Start` launches them, `Resize`
  grows or shrinks the pool on reload, and `Close` lets them finish queued work. `Stopped` and
  `StopReason` report a self-stop. `ReloadHealth` applies new thresholds.
- **`loop.go`**: one writer's loop:
  - backs off and probes the DB while degraded;
  - takes a batch, releasing the session if it sits idle too long or has been held too long;
  - `serve` runs it with at most one retry, updates the health state, and always returns the batch
    to its shard's completion channel;
  - retires the writer on a statement error or panic, or stops the pool on fail-fast.
- **`writer.go`**: per-writer state and session handling: pin a session, arm the bounded hold, and
  release it with a reason. `Config` and `Params`, the startup and reloadable settings. Helpers that
  fail pending rows and reset a batch for retry.
- **`run_batch.go`**: `runBatch`, one attempt at a batch as one transaction, under the query
  timeout and a panic-recover boundary. Phase 1 runs the array binds. On a row-data error it falls
  back to phase 2, row by row, in a fresh transaction. After commit it marks rows Committed and
  counts rows written per destination.
- **`phase1.go`**: groups each destination's rows by column mask and executes one array-bound
  `MERGE` per group, warning when RowsAffected differs from the number of rows bound.
- **`phase2.go`**: re-executes each row with scalar binds. A row whose error is row-data is marked
  Rejected with its ORA code and the batch continues; any other error aborts the transaction.
- **`group.go`**: `grouper` orders one destination's rows by (mask, row key) so each statement
  identity forms one run. Rows with an empty mask are rejected.
- **`bind.go`**: `Binder` turns row images into positional bind arrays following the
  `destinations.BindPlan`. LONG/SHORT become int64, DOUBLE becomes nullable float (NaN → NULL),
  time1980 becomes a UTC timestamp (≤0 → NULL), and char/text become trimmed nullable strings. Also
  binds the key and `updated_at`, checks that every array has the same length, and has a
  single-row `BindRow` for phase 2.
- **`stmtcache.go`**: a per-writer cache of prepared statements keyed by (destination, mask), up to
  `stmtCacheSize`. Beyond that a statement is used once and closed.
- **`stmtkey.go`**: `stmtKey`, a statement identity (destination + mask).
- **`retry.go`**: the retry decision per error class. Row data goes to phase 2. Session, deadlock,
  space and unknown errors retry once. Anything else gives the batch back.
- **`backoff.go`**: exponential backoff with ±20 % jitter between failed batches.
- **`health.go`**: `poolHealth`, the pool-wide Healthy/Degraded/FailFast machine. It goes to
  FailFast after the fail-fast window or N consecutive failures on one writer, and recovers on a
  commit or a successful probe. Statement errors retire the writer, or stop the pool once enough
  distinct writers have failed on the same statement.
- **`ledger.go`**: `stmtLedger`, the pool-stop rule: the same statement identity failing on N
  distinct writers within a window.
- **`stopreason.go`**: `StopError`/`StopKind` (fail-fast, statement, no writers left), and why the
  pool stopped. Mapped by `app` to a fatal reason.
- **`instruments.go`**: the writer's obs handles: bind NULL-ing counters, statement cache, pool
  gauges, writes and retries, connection recycling, probes and `oracle_error{code}`.
- **`assert_on.go`** / **`assert_off.go`**: the writer half of gate G-ASSERT-1 (one goroutine per
  writer loop), compiled out by default.

## `internal/oradb`: Oracle connection layer

- **`doc.go`**: package contract. The only production package that names go-ora or `database/sql`
  drivers. The password comes only from the environment and is redacted from every error.
- **`settings.go`**: `DBSettings` (no password field), `Describe` (a safe log string),
  `Password`/`HasPassword` from `FEEDVALVE_DB_PASSWORD`, and the pool-stop and drain-timeout
  sentinels.
- **`connect.go`**: `Open`/`OpenDriver` build the DSN, set pool size and connection lifetime, ping
  under the connect timeout, and return a `DB`. `OpenDriver` is the seam that lets the fake driver
  stand in. `Check` (`-dbcheck`) runs `SELECT 1 FROM dual` and reads the server banner.
- **`db.go`**: `DB`, a wrapper around `*sql.DB`. Provides `PingContext`, `Prepare`, `Classify`,
  `SetPoolSize`, and the reloadable connection lifetime and connect timeout. Its string form never
  shows the DSN.
- **`session.go`**: `Session`, one writer's pinned connection (`Acquire`, `Expired`, `Release`).
  `ExecTx` runs a function in one transaction and rolls back on error or panic.
- **`classify.go`**: `Classify` maps a driver error to an `ErrClass` (row data, session, deadlock,
  space, statement, unknown). It reads the ORA number through `errors.As`, never by text. Timeouts
  and bad connections count as session errors. Returns a redacted `*Error`.
- **`redact.go`**: `Redactor`/`redactDSN` scrub the DSN and the password (plain and URL-escaped)
  from driver error text.
- **`catalog.go`**: `Catalog` reads `ALL_TAB_COLUMNS` (type, precision, length, nullability) for
  named tables in the current schema, for the catalogue check.
- **`oratest/doc.go`**: package contract for the fake driver. Test-only; must never be linked into
  the binary or import `oradb`.
- **`oratest/driver.go`**: a scripted fake `database/sql` driver named `oratest`. Per call (keyed
  by SQL text and call number), a script can succeed, return a typed ORA error, drop the connection,
  delay, or override RowsAffected. It accepts the writer's array binds, records every execution, and
  counts opens, pings, prepares, begins, commits and rollbacks.

## `internal/health`: liveness and market-session state machines

- **`doc.go`**: package contract. Pure state machines; must not import `oradb`, `writer` or
  `mirror`, and never gate the data path.
- **`circuit.go`**: `Circuit`, the feed-liveness machine (Starting → Live → Quiet → Down → Live),
  driven by the last datagram's receipt time and the configured warn, down and grace thresholds.
  Each `Tick` returns at most one transition with its log inputs. `SeverityOf` lowers severity
  outside the trading window.
- **`session.go`**: `Sessions`, one market-session machine per market type (1–4). Each market-status
  code sets the state (6531 pre-open, 6571 pre-open-ended, 6511 open, 6521 closed, 6522
  post-close). A transition outside the expected edges is still applied but flagged as an anomaly.
- **`window.go`**: `Window`, the trading-hours check (`HH:MM`–`HH:MM` in an IANA zone, overnight
  supported). It treats every hour as in-window if the zone or times can't be resolved.

## `internal/model`: shared value types (no behaviour)

- **`doc.go`**: package contract. Standard library imports only, pointer-free, sizes pinned.
- **`const.go`**: build-time sizes (3 destinations, 8 contributors, 256-byte image and blob, 64
  columns), pinned by gate G-BLD-2.
- **`unit.go`**: `UpdateUnit`, one decoded token update (360 B, passed by value): token, code,
  flags, seq, log time, receipt time, three per-destination `Contribution`s, and the packed value
  blob.
- **`entry.go`**: `Entry`, one destination row in a shard arena (424 B): column masks, digest,
  per-contributor ordering state, dirty/in-flight/quarantine flags, and the row image.
- **`batch.go`**: `Batch` and `BatchRow`, one write transaction's rows across the three
  destinations, with per-row outcome and ORA code. Passed between shard and writer.
- **`header.go`**: `MsgHeader`, the decoded union of both header layouts.
- **`datagram.go`**: `Datagram`, one received UDP datagram (1024-byte buffer plus receipt time) as
  stored in QD.
- **`key.go`**: `RowKey` (`SingleKey`/`CompositeKey`, which zero-extend) and `ColMask` bitset
  operations.
- **`scalar.go`**: small enums and IDs: `TransCode`, `DestID`, `ShardID`, `Class`, `HeaderKind`,
  `UnitFlags`, `RowOutcome`.
- **`reject.go`**: the `Reject` enum for receive-path rejections, each mapped to its obs counter
  name, plus `ErrStallBudgetExpired`.
- **`session.go`**: `SessionEvent`, a market-session transition passed from decode to health by
  value.
- **`layout.go`**: gate G-LAY-2, which pins the size and field offsets of every fixed-size model
  struct at compile time.

## `internal/obs`: counters, emission, logging

- **`doc.go`**: package contract. No internal imports, three instrument shapes, labels resolved to
  arrays at startup, a frozen registry, and no packet logger.
- **`instrument.go`**: the three shapes. `Counter` is monotonic, `Max` is a CAS high-water mark
  reset at emission, and `Gauge` is a value stored by the owner.
- **`ids.go`**: the `ID` constant for every instrument, in ledger order.
- **`ledger.go`**: the instrument ledger: name, shape, unit, emission group, owning goroutine and
  label dimension for every ID.
- **`dim.go`**: label dimensions and their fixed vocabularies (decompress reasons, retry classes,
  panic stages, and so on), and `Options`. `resolveDims` sizes and pre-renders the labels.
- **`registry.go`**: `Reg` allocates one slab per shape for the whole ledger and resolves handles by
  ID and label index, with shape and arity checks. `Freeze` ends resolution. Also handles the
  dynamic `unknown_transcode` and `oracle_error` slots.
- **`dyntable.go`**: `codeTable`, a fixed-size open-addressing table with CAS insert, for codes not
  known at startup.
- **`emit.go`**: `Emit` writes the health line first, then one line per group that changed (or
  every 10th interval regardless), with counter totals and deltas, from a preallocated buffer.
- **`group.go`**: the emission groups and the health-line projection.
- **`class.go`**: log event classes, WARN then ERROR, each with its own rate-limit bucket. This
  includes the Phase 3–5 classes: epoch stalled, decode anomaly, circuit quiet/down, blob-length
  mismatch, guard disabled, drain deadline, shard defect.
- **`logger.go`**: `AppLogger`, leveled logging with one token bucket per class. Suppressed lines
  are counted and reported on the next line that gets through. The level is atomic, so a reload can
  change it under load.
- **`debug.go`**: `DebugLogger`, the per-transcode field trace (format byte-identical to `poc`'s
  `-debug`) plus a `hexdump -C` style dump. Its selection is an atomic snapshot that `Set` replaces
  on reload.
- **`const.go`**: label-array sizes, pinned by a build-time gate.

## Packages that contain only `doc.go`

- **`internal/policy/doc.go`**: maps a transaction code to rate-limited or non-rate-limited and
  validates the inventory. The rate class currently travels on the decoder registry entry
  (`decode.RegistryEntry.Class`, set from config by `app`), so this package has no code.
