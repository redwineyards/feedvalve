# Code map — `cmd/` and `internal/`

What each non-test source file does, limited to its core function. Files ending in `_test.go`
are excluded. This describes `develop` as of P2-10 (commit `8699fdf`).

## The pipeline in one paragraph

`cmd/feedvalve` parses flags and calls `app.Run`. `app` loads `config` and the `destinations` field
map, builds the `obs` instrument registry, opens Oracle through `oradb`, and starts the goroutines.
The **receiver** goroutine reads UDP datagrams and copies each one into the `framing` ring (QD).
The **decoder** goroutine drains QD. For each datagram it splits the envelope into sub-packets
(`framing.Split`), decompresses compressed ones (`lzo`), and reads the header (`decode.ReadHeader`).
It feeds the sequence-gap detector, then dispatches through the `decode` registry to a
per-transaction-code decoder. That decoder turns the message into `model.UpdateUnit`s, which are
routed to per-shard channels (Q1) by token hash. On this branch the shard consumers only drain and
discard: `mirror` (shard state and coalescing), `writer` (Oracle MERGE), `health` and `policy` exist
only as package docs, scheduled for later phases.

---

## `cmd/feedvalve`

- **`main.go`**: the process entry point. Parses `-config`, `-selfcheck` and `-dbcheck`, wires
  SIGINT/SIGTERM to a cancellable context, and calls `app.Run`. It then maps the returned error to
  an exit code. It holds the only `os.Exit` in the tree, called after every defer has run. It embeds
  `time/tzdata` so the trading-window timezone resolves on hosts with no zoneinfo, such as AIX. The
  build version is stamped in through `-ldflags -X main.version`.

## `internal/app`: wiring, goroutines, lifecycle

- **`doc.go`**: package contract. `app` is the only package that may see both the receive path and
  the database side. It binds the socket last and drains in order on shutdown.
- **`run.go`**: `Run`, the top-level flow. Loads and validates config plus the destination map, then
  branches. `-selfcheck` prints the config, destinations and decoder catalogue, and builds the
  registry to prove every transcode resolves. `-dbcheck` connects and runs `SELECT 1 FROM dual`. The
  default `relay` path creates and freezes the obs registry, creates the loggers, sets GOMAXPROCS,
  opens the DB, calls `Build`, then `Graph.Run`. Also defines the exit codes (0/1/3/4) and
  `ExitError`.
- **`graph.go`**: `Graph`, the constructed process. `Build` builds the decoder registry from config,
  publishes the runtime snapshot, and pre-allocates the QD ring and the per-shard Q1 channels before
  any socket exists. `Fatal` is a one-shot channel for terminal conditions (Q1 overflow, DB
  fail-fast, UDP read failure, and others). The first condition reported wins. It records every
  resource acquired and released so the shutdown order can be asserted.
- **`lifecycle.go`**: `Graph.Run` and the goroutines. Startup order is shard consumers, then the
  decoder, then stats, then binding the UDP socket (setting and reading back the receive buffer),
  then the receiver. The receiver loop: read, timestamp once, count, check the source allow-list,
  flag oversize datagrams, and copy into a `model.Datagram`. It publishes to QD within the stall
  budget, which is fatal on expiry. The stats goroutine emits counters every interval and detects
  clock steps. `teardown` closes the socket, waits for the reader, closes QD and waits for the
  decoder to drain it, closes the shard channels, then stops stats. SIGHUP is logged as a no-op
  until reload lands.
- **`subpacket.go`**: `receiver`, the decoder goroutine's per-datagram work. It checks the envelope
  netId and calls `framing.Split`. Per sub-packet, inside a panic-recover boundary, it decompresses
  with `lzo.DecompressSafe`, checks the netId oracle (log only, never drop), reads the header, and
  handles error responses and short payloads. It feeds the gap detector, counts
  MESSAGE_HEADER/heartbeat codes, and looks up the decoder. It verifies the decompressed length and
  `BodyLen`, then runs `Decode` into a reused 40-unit fan-out buffer. It separates unconfigured
  codes from unknown ones and maps lzo errors to `decompress_failed{reason}`.
- **`fanout.go`**: `route` and `send`. Each decoded unit gets its receipt time and is added to the
  active-token sample. It is then sent to its shard channel without blocking. If the channel is
  full, it waits on one reusable timer against the per-datagram stall budget. Expiry logs full
  shard depths and raises `FatalQ1Overflow`. Nothing is dropped.
- **`shard.go`**: `ShardOf` routes general units to the designated shard and token units by
  `mix32(token) % shards`. Allocates the Q1 channels. Starts placeholder shard consumers that drain
  and discard until `mirror` exists. Stops them by closing channels and waiting.
- **`instruments.go`**: resolves every `obs` counter, max and gauge handle the hot path uses, once,
  before the registry freezes. The handles are static, per-transcode, per-shard and
  per-destination/column. Also converts config into `decode.TranscodeSpec`s and `obs.Options`, and
  maps `decode.BuildError` problems onto config's `V-` rule IDs.
- **`counters.go`**: `decodeCounters`, which implements `decode.Counters` (record-count clamp,
  booktype skip, blob overrun, message truncation) and `decode.Reports` (sequence gap, regression,
  resync, gap audit) on top of the obs handles, with rate-limited log lines.
- **`runtime.go`**: `Runtime`, the config + decoder registry + destinations snapshot, published as
  one atomic pointer so the decoder loads it once per datagram.
- **`tokens.go`**: `tokenSet`, a generation-stamped open-addressing set that counts distinct tokens
  per second for `active_tokens_1s_max` without clearing the table every second.
- **`clock.go`**: the process-start monotonic origin and the startup log class alias.
- **`rcvbuf.go`**, **`rcvbuf_unix.go`**, **`rcvbuf_windows.go`**: read back the socket's effective
  `SO_RCVBUF`, so the startup log shows when the kernel capped the requested size.

## `internal/config`: configuration

- **`doc.go`**: package contract. Pointer-optional fields, unknown keys rejected, every key
  classed for reload, and no password key.
- **`config.go`**: the resolved `Config` tree (UDP, runtime sizing, flush, writers, DB, ordering,
  stats, liveness, debug, transcodes and their contributions), plus duration helpers.
- **`load.go`**: strict JSON decode (`DisallowUnknownFields`) into a pointer-per-field file shape,
  then `resolve` applies built-in defaults. `Load`/`Parse` validate; `LoadUnvalidated` and
  `Defaults` do not. Derives the DB pool size from `writers.count + 1`.
- **`validate.go`**: `Validate`, the config-only rule set. It checks per-key ranges and cross-field
  rules: ring capacity is a power of two, general shard is in range, `maxBatchSize` is within the
  ceiling, retry order, heartbeat order, trading-window times, timezone, DB required fields, and
  trace codes that are actually configured. On reload it rejects changes to startup-only keys and
  to the transcode/contribution membership.
- **`schema.go`**: `ValidateAll`, which adds rules needing the destination table. Contributions
  must name declared destinations and columns, the transcode must be a declared contributor, the
  blob must fit 256 bytes, and row keys must be derivable (a transcode cannot feed differently-keyed
  destinations). Also enforces "a db object is required" per mode.
- **`problem.go`**: the `V-*` rule identifiers, and `ValidationError`, which collects every problem
  rather than stopping at the first.
- **`store.go`**: `Store`, an atomic pointer to the current `Config` with a generation number.
  `Swap` validates a candidate against the running config before publishing it.
- **`source.go`**: parses `udp.sourceAllowList` entries (bare IP or CIDR) into `netip.Prefix`es.

## `internal/destinations`: the three target tables and the field map

- **`doc.go`**: package contract. Owns table names, row-key rules, column/bit/image layout and
  contributor slots, and never imports the database packages.
- **`mapping.go`**: loads the embedded `mapping.json`, or an operator file, with a strict decode.
  Builds each `Destination`: row-key rule, contributor slots sorted by ascending code, and columns
  with offsets as the running sum of widths. Checks the declared `imageLen` against that sum.
- **`destination.go`**: `Destination` (ID, name, table, key rule, columns, image length,
  contributors) with slot and column lookups and `FullMask`. `Set` is the immutable table, indexed
  by ID and by name.
- **`column.go`**: `Column`, one maskable image field: bit, offset, width, kind (long, short, char1,
  double, text, time1980, group), scaling, length-bit, SQL name(s), and pre-validation metadata.
  Also `RowKeyRule` (token or marketType).
- **`validate.go`**: per-set and per-column rules. Exactly 3 destinations, unique names, image
  ≤ 256 B, ≤ 64 columns, bit = index, widths matching kinds, only groups expanding to several SQL
  columns, valid scale/lenBit/domain, and unique SQL names.
- **`ident.go`**: validates table and column names as safe unquoted Oracle identifiers, before they
  can reach a SQL string.
- **`layout.go`**: build-time gate G-LAY-3, which fails compilation if any shipped destination image
  (181 / 37 / 142 bytes) exceeds `model.ImageSize`.

## `internal/framing`: datagram ring and envelope split

- **`doc.go`**: package contract. QD (the SPSC ring) and the `BroadcastPackData` envelope rules:
  8-byte prefix, `messageLength` checks, and netId as an alignment oracle.
- **`ring.go`**: QD, a lock-free single-producer/single-consumer ring of `model.Datagram`.
  Power-of-two capacity, head and tail on separate 128-byte cache lines (gated at build time), and
  separate `Producer`/`Consumer` handles. `TryPublish` copies the datagram in, stores tail, then
  always signals a capacity-1 wake channel. `Close` marks the ring closed and wakes the consumer.
- **`consume.go`**: `Consumer.Drain`, the whole consumer side. Copies each slot out before advancing
  head, spins briefly, then parks on the wake channel. Returns only when the ring is closed and
  empty. Refuses a second concurrent consumer.
- **`budget.go`**: `Producer.PublishWithin`. Fast path first; when the ring is full, it yields until
  the caller's per-datagram budget expires, then returns `model.ErrStallBudgetExpired` (never
  exits).
- **`split.go`**: `Split` walks the envelope (`cNetId`, `iNoPackets`, then length-prefixed
  sub-packets in FIFO order) and hands each one to a `SubPacketHandler`. Compressed sub-packets
  advance by `iCompLen`; uncompressed ones by `8 + MessageLength`. Rules R1–R9 stop framing on bad
  lengths and record the rule, index and offset. Oversize input is trimmed to 512 bytes. It does no
  decompression.

## `internal/lzo`: decompression

- **`doc.go`**: package contract. Pure-Go LZO1Z decompression only (no cgo, no compressor), standard
  library imports only.
- **`decompress.go`**: line-for-line port of liblzo2's LZO1Z decompressor. `DecompressSafe` checks
  every bound and is the only variant the packet path may call. `Decompress` is unchecked and used
  in tests only. `MaxDecompressedLen` (2048) sizes the receiver's buffer.
- **`errors.go`**: four distinct sentinels (input overrun, output overrun, lookbehind overrun,
  input not consumed). Each maps to its own `decompress_failed{reason}`.

## `internal/wire`: protocol surface

- **`doc.go`**: package contract. Protocol constants, the field cursor and the pack(2) rules.
- **`const.go`**: protocol constants (8-byte prefix, 40-byte headers, 512-byte packed data,
  516-byte datagram, 182-byte error response), pinned by gate G-BLD-1.
- **`reader.go`**: `FieldReader`, a resettable big-endian cursor (`I16`, `I32`, `I64`, `F64`,
  `Byte`, `Copy`, `Raw`, `Pad`, `Skip`) where call order is wire order. It emits optional per-field
  trace lines at absolute payload offsets, and allocates nothing when tracing is off.
- **`field.go`**: `FieldID`, the uint16 names for every header and body field, with their
  spec-spelled strings. Trace lines name fields without formatting strings on the hot path.
- **`netid.go`**: `NetIDMatches` and `EnvelopeNetIDMatches`, predicates that accept the netId as
  either a binary or an ASCII digit. They return a bool and never drop anything.

## `internal/decode`: header resolution, registry, per-code decoders

- **`doc.go`**: package contract. One file per transaction code, each with its layout gate, and
  record counts clamped.
- **`header.go`**: `ReadHeader`. Checks the payload is at least 48 bytes, rejects a non-zero
  `ErrorCode` (an error response) before trusting the code, resolves the header kind, and parses
  BCAST_HEADER or MESSAGE_HEADER field by field (`LogTime` sits at different offsets in each).
  Rejects `MessageLength < 40`.
- **`resolve.go`**: `Resolve` classifies the header without consulting the registry. Code
  6541/6503 at offset 8 with a payload length of exactly 48/60 is a MESSAGE_HEADER; anything else
  is a BCAST_HEADER with its code at offset 18.
- **`decoder.go`**: the `Decoder` interface (`Code`, `BodyLen`, `MaxUnits`, `Decode`) and its
  allocation-free sentinel errors. `ColSpec` holds the mask, width and per-bit blob offsets.
  Also defines `RegistryEntry`, the `Counters` sink, the optional `Traceable`, and the gate that
  keeps the fan-out within 40 units.
- **`catalogue.go`**: the compile-time catalogue of decoder constructors, filled by each code file's
  `init()`. Panics on a duplicate code. `Catalogue` and `CanDecode` separate "unknown" from
  "unconfigured".
- **`registry.go`**: `Registry` and its implementation, parallel arrays of up to 32 entries sorted
  by code. `Lookup`/`Slot` scan linearly; the slot index is shared with the obs `{code}` labels.
- **`build.go`**: `BuildRegistry` resolves configured transcodes against the catalogue and
  destinations. It assigns contributor slots, builds each `ColSpec` (mask, packed offsets in
  ascending bit order, width), and constructs decoders. It reports every problem at once.
- **`unit.go`**: shared decoder helpers. `stamp` zeroes and initialises a unit, `contribute` sets
  its row/mask/slot, `put*` write wire-form bytes at registry-computed blob offsets, `sealBlob`
  enforces the 256-byte blob, and `clampRecords` applies the record count rule. Also names the
  destination IDs.
- **`gap.go`**: `GapDetector`, BCSeqNo continuity using serial-number arithmetic (wrap-safe).
  Scope is global, per-transcode, or off. The first message is recorded silently; regressions are
  not counted as gaps; jumps over 1,000,000 are reported as resyncs. It skips MESSAGE_HEADERs and
  can audit both scopes side by side.
- **`layout.go`**: documentation of the G-LAY-1 idiom (two-sided vs one-sided compile-time
  constant gates) and where each gate lives. No code.
- **`mbp_only_7208.go`**: 7208 MS_BCAST_ONLY_MBP feeds `MD_TOKEN_QUOTE`. Reads `NoOfRecords`
  (clamped to 2) and decodes that many 214-byte records (pad byte at offset 17). The quote fields
  and the 120-byte MBP ladder go into the unit blob, keyed by token.
- **`mbo_mbp_7200.go`**: 7200 MS_BCAST_MBO_MBP (the exchange's alternative to 7208), also feeding
  `MD_TOKEN_QUOTE`. One security per message; skipped unless BookType is 1 or 2. Skips the order
  ladder, repacks the 10-byte price levels into 7208's 12-byte ladder layout, and reads the outer
  fields at the spec's corrected offsets.
- **`security_update_7305.go`**: 7305/7340 MS_SECURITY_UPDATE_INFO feeds `MD_SECURITY_MASTER`. A
  294-byte layout with no padding (the eligibility array uses a 3-byte stride). Stores symbol,
  series, expiry, strike, lot, tick, delete flag and base price, keyed by token.
- **`vct_msgs_6511.go`**: the 6511/6521/6522/6531/6571 market-status family feeds
  `MD_MARKET_STATUS`. These are general units keyed by MarketType; the wire token is stored as data,
  not used for routing. Stores the contributing code, the clamped and zero-filled message text (cap
  128), the broadcast destination and the timestamp.

## `internal/model`: shared value types (no behaviour)

- **`doc.go`**: package contract. Standard library imports only, pointer-free, sizes pinned.
- **`const.go`**: build-time sizes (3 destinations, 8 contributors, 256-byte image and blob, 64
  columns), pinned by gate G-BLD-2.
- **`unit.go`**: `UpdateUnit`, one decoded token update (360 B, passed by value): token, code,
  flags, seq, log time, receipt time, three per-destination `Contribution`s (row, mask, blob
  span, slot), and the packed value blob.
- **`entry.go`**: `Entry`, one destination row in a shard arena (424 B): column masks, digest,
  per-contributor ordering state, dirty/in-flight/quarantine flags, and the row image.
- **`batch.go`**: `Batch` and `BatchRow`, one write transaction's rows across the three
  destinations, with per-row outcome and error code. Passed between shard and writer.
- **`header.go`**: `MsgHeader`, the decoded union of both header layouts (kind, code, seq, log
  time, timestamp, lengths).
- **`datagram.go`**: `Datagram`, one received UDP datagram (1024-byte buffer plus receipt time) as
  stored in the QD ring.
- **`key.go`**: `RowKey` (`SingleKey`/`CompositeKey`, which zero-extend) and `ColMask` bitset
  operations.
- **`scalar.go`**: small enums and IDs: `TransCode`, `DestID`, `ShardID`, `Class`, `HeaderKind`,
  `UnitFlags`, `RowOutcome`.
- **`reject.go`**: the `Reject` enum for receive-path rejections, each mapped 1:1 to its obs
  counter name, plus `ErrStallBudgetExpired`.
- **`session.go`**: `SessionEvent`, a market-session transition passed from decode to health.
- **`layout.go`**: gate G-LAY-2, which pins the size and field offsets of every fixed-size model
  struct at compile time.

## `internal/obs`: counters, emission, logging

- **`doc.go`**: package contract. No internal imports, three instrument shapes, labels resolved to
  arrays at startup, a frozen registry, and no packet logger.
- **`instrument.go`**: the three shapes. `Counter` is a monotonic atomic counter, `Max` a CAS
  high-water mark reset at emission, and `Gauge` a value stored by the owner.
- **`ids.go`**: the `ID` constants for all 131 instruments, in ledger order.
- **`ledger.go`**: the instrument ledger: name, shape, unit, emission group, owning goroutine and
  label dimension for every ID.
- **`dim.go`**: label dimensions (`{k}`, `{destination}`, `{code}`, `{reason}`, …), their fixed
  vocabularies, and `Options`. `resolveDims` sizes and pre-renders the labels from config.
- **`registry.go`**: `Reg` allocates one slab per shape for the whole ledger and resolves handles by
  ID and label index, with shape and arity checks. `Freeze` ends resolution, after which any lookup
  panics. Also handles the dynamic `unknown_transcode` and `oracle_error` slots.
- **`dyntable.go`**: `codeTable`, a fixed-size open-addressing table with CAS insert, so codes not
  known at startup get a counter slot without allocating.
- **`emit.go`**: `Emit` writes the health line first, then one line per group that changed (or
  every 10th interval regardless). Counters print total and delta; maxes reset as they are read.
  The output buffer is preallocated.
- **`group.go`**: the 15 emission groups and the health-line projection, the values the runbook
  checks first.
- **`class.go`**: log event classes (WARN and ERROR), each with its own rate-limit bucket.
- **`logger.go`**: `AppLogger`, leveled logging with one token bucket per class (`Allow`, `Warnf`,
  `Errorf`). Suppressed lines are counted and reported on the next line that gets through. INFO and
  terminal lines are never rate-limited.
- **`debug.go`**: `DebugLogger`, the per-transcode field trace (format byte-identical to `poc`'s
  `-debug`, which the differential test depends on), plus a `hexdump -C` style dump.
- **`const.go`**: label-array sizes (unknown-code slots, Oracle error slots, writer count, emit
  cadence), pinned by a build-time gate.

## `internal/oradb`: Oracle connection

- **`doc.go`**: package contract. The only package allowed to import go-ora/`database/sql`. The
  password comes only from the environment, and is redacted from errors.
- **`settings.go`**: `DBSettings` (connection settings, no password field), `Describe` (a safe log
  string), `Password`/`HasPassword` (from `FEEDVALVE_DB_PASSWORD`), and the pool-stop and
  drain-timeout sentinels.
- **`connect.go`**: `Open` builds the DSN, sets pool size and connection lifetime, and pings under
  the connect timeout. `Check` runs `SELECT 1 FROM dual` and reads the server banner. `redactDSN`
  scrubs the DSN and password from driver errors.

## Packages that contain only `doc.go` (scheduled, not yet built)

- **`internal/mirror/doc.go`**: the shard goroutine (row index, entry arenas, dirty and cohort
  rings, epochs, batch free list). Must not import the DB or the network. Phase 3.
- **`internal/writer/doc.go`**: statement assembly from a batch mask, array-bound Oracle `MERGE`,
  transaction boundaries, and per-row outcome attribution (falling back to row-by-row on data
  errors). Phase 4.
- **`internal/health/doc.go`**: circuit-liveness (heartbeat) and market-session state machines.
  Phase 5.
- **`internal/policy/doc.go`**: maps a transaction code to rate-limited or non-rate-limited and
  validates the inventory.
