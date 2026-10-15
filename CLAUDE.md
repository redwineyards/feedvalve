# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Design principles

Three rules govern every change in this repository: **KISS**, **SOLID**, **YAGNI**. They conflict
less often than it looks, and where they do, the order above is the tie-break — a simple design that
is slightly less pure beats a layered one that is harder to read against the protocol spec.

### KISS — keep it simple, stupid

Prefer the boring, obvious construction. Market-data decoding is already unforgiving; the code
around it must not add difficulty of its own.

- **Decode is a flat sequence of reads, not machinery.** Fields are read one at a time at a cursor,
  in wire order, so the code can be checked line-by-line against the spec's offset table. No
  reflection, no `encoding/binary` struct decoding, no code generation, no generic
  schema-interpreter. Padding is written out explicitly (`r.pad(...)`, and the explicit pad fields
  in `internal/model`) rather than left to the compiler to infer.
- **Explicit beats clever.** Sizes and layouts are pinned by compile-time constant expressions, not
  computed at runtime from tags or `unsafe.Sizeof` chains.
- **Errors in non-essential paths are logged and swallowed**, not escalated into retry/circuit-breaker
  layers. A database problem must never stop the relay decoding the feed.
- **Simple-and-documented beats sophisticated-and-implicit.** Where a deliberately plain choice has
  a cost, the cost is written down next to it (the POC's synchronous per-packet write is the
  example). Don't "fix" a documented trade-off without being asked.
- Prefer deleting a special case to adding a flag that selects between two of them.

If explaining a change needs a diagram, look for the simpler version first.

### SOLID — as pressure on shape, not a mandate to add types

The `internal/*` tree is the worked example; keep its properties.

- **S — single responsibility.** One package, one job, stated in its `doc.go`: `framing` splits the
  envelope, `decode` turns payloads into units, `model` holds value types with no behaviour,
  `policy` classifies transaction codes, `oradb` talks to Oracle, `writer` persists, `app` wires the
  graph and owns the lifecycle. New code belongs in the package whose single job it shares — or in a
  new one with a single job of its own, declared the same way. A decoder decodes; it does not log
  packets, touch the database, or decide drop policy.
- **O — open/closed.** Decoder selection is data-driven: a new transaction code means a new decoder
  file plus its registration, not edits threading flags through existing decoders. Preserve that —
  if adding a code forces you to modify a decoder that already works, the seam is in the wrong place.
- **L — substitutability.** Anything accepted behind an interface must work for every implementation
  passed in. The driver seam exists so a stub `database/sql` driver behaves like the real Oracle one
  in tests; don't add behaviour that only holds for one of them.
- **I — interface segregation.** Small interfaces, defined by the consumer, at the point of use. A
  one- or two-method interface is normal and good here; a wide `Service`-style interface is a smell.
  Accept the narrowest thing that does the job (`io.Writer` over a logger struct).
- **D — dependency inversion, enforced by import rules.** The dependency direction is a hard
  constraint, not a style preference, and it is stated per package and checked by a test rather than
  trusted to convention (`internal/app/deps_test.go`): `model` imports nothing outside the standard
  library; the receive-path packages (`framing`, `decode`, `policy`, …) MUST NOT import `oradb`,
  `writer`, `mirror` or `database/sql`; `app` is the only package allowed to see both halves, which
  is precisely why the restrictions are machine-checked there. Packages speak to each other through
  `model`'s value types. **Never relax one of these rules to make a change fit** — a
  `sql.NullString` or a `time.Time` reaching into the receive path drags the database in and breaks
  the pointer-free arena rule in the same stroke.

Do not introduce an interface with exactly one implementation and no test double. That is
speculative indirection, and YAGNI overrules it.

### YAGNI — you aren't gonna need it

Build what the current task needs, and nothing more.

- **No speculative configuration.** No flags, JSON keys, or env vars "for later". Every flag
  documented under "Commands" exists because something needed it; a new one needs the same
  justification. Secrets stay out of flags and config files on purpose (see "Credentials").
- **No abstraction ahead of a second real caller** — no plugin registries, no generic field-mapping
  frameworks, no provider indirection for a single provider.
- **No caching, batching, pooling, or metrics subsystem** until a measurement says the simple version
  is the problem. The small connection pool and the 30-minute connection lifetime have stated
  reasons (see "Things that will bite you"); they are reasoned settings, not a tuning hobby.
- **Respect the phasing.** Packages that currently hold only a `doc.go` are specified by
  `docs/lld/` and scheduled for a named phase. Don't implement one early because it seems adjacent
  to the task; and when you do build one, build what its LLD section specifies, not a superset.
- Deleting speculative code is a legitimate change. Leaving a `TODO` for a feature nobody asked for
  is not.

**What YAGNI does not license.** The protocol and safety checks are not speculation — they exist
because the failure they prevent is silent. Keep every one of them, and add the matching one when you
add a decoder:

- Layout assertions reconciling pack(2) field arithmetic against the spec's declared packet length
  (gate G-LAY-1). A wrong stride decodes the early fields correctly and turns the later ones into
  noise — nothing alarms.
- Record counts clamped to the array size, so trailing padding is never decoded into a fabricated
  security that looks real downstream.
- Bounds-checked decompression and panic recovery on the packet path: sub-packets are untrusted
  network input, and one malformed datagram must log an error, not kill the relay.
- `messageLength` and envelope-cursor validation, which keep a single bad length from misframing
  every later sub-packet in the buffer.
- No `os.Exit`/fatal logger outside the command itself (gate G-BLD-3), so deferred cleanup of Oracle
  sessions and in-flight batches actually runs.
- Tests that are independent of the code under test: frozen LZO fixtures produced by an outside
  implementation, and hand-written absolute-offset tables rather than fixtures built by walking the
  parser's own field order. A test that shares its assumptions with the code agrees with it even when
  both are wrong — that is exactly how the 213-byte stride survived.

## Commands

All of these are plain Go — **no cgo, no C compiler, no `liblzo2`** (LZO1Z decompression is a pure Go
port, see `poc/decompress.go`).

- Build: `go build ./...`
- Vet: `go vet ./...`
- Format check: `gofmt -l .`
- Test: `go test ./...`
- Run the relay: `go run ./poc -udp :9000 -out tcp_reply.log`
  - `-udp`: address to listen for incoming UDP broadcast packets (default `:9000`)
  - `-out`: file that raw/decompressed packet data is appended to (default `tcp_reply.log`)
  - `-debug`: per-field offset/hex/value logging plus a hex dump of each payload (default off) — the
    main tool for diagnosing bad field values, see "Debugging parsed data" below
  - `-netid`: expected segment netId (default 8 = commodity derivatives; `0` disables the check).
    Mismatches are logged, never dropped.
  - `-maxout`: stop appending to `-out` after this many bytes (default 1 GiB; `0` disables the
    limit). The packet log records every payload plus its parsed form, several times the wire rate,
    against an append-only file — the cap keeps a busy feed from filling the disk. It is a floor,
    not log rotation: it keeps the *start* of a session and drops the rest, logging a single
    `WARN packet log reached its ... limit` when it cuts off.
  - `-config`: path to a JSON config file supplying defaults for the flags above, e.g.
    `{"udp": ":9000", "out": "tcp_reply.log", "debug": false, "netid": 8, "maxout": 1073741824}`.
    Command-line flags take precedence over the file, which takes precedence over the built-in
    defaults (see `poc/config.go`). The file also carries the nested `db` object — see "Database"
    below. `config.example.json` is a filled-in template.
  - `-dbcheck`: connect to the database configured under `db`, verify a round trip
    (`SELECT 1 FROM dual`), log the server version, and exit without starting the relay. Read-only;
    the way to prove credentials and network reachability from a deployment host.
- Build for AIX: `CGO_ENABLED=0 GOOS=aix GOARCH=ppc64 go build -o gobrdclnt ./poc`. Since the cgo
  dependency is gone this now **cross-compiles from any host** — it no longer has to be built on an
  AIX machine. `poc/build_aix.sh` predates this and still documents the old cgo/`liblzo2`
  native-build requirements; it is stale and **superseded by `scripts/build-aix.sh`**, which builds
  the real binary (`./cmd/feedvalve`, `-trimpath -ldflags "-X main.version=…"`) plus the artefact set
  Vol 4 §8 names: `config.json`, the `deps.txt` golden list, and the cross-compiled test binaries
  gate G-AIX-1 ships to the LPAR.
- Deploy and smoke-test on the target: `scripts/lpar-smoke.sh`, which runs **on the LPAR** and encodes
  P1-11's five assertions in dependency order — `-selfcheck` (needs no password, no network, no
  database), the test binaries, `-dbcheck`, the full start/SIGTERM cycle, then the host checks. It
  exits non-zero on any failure and reports a check it could not make as `SKIP` rather than `FAIL`,
  because an assertion that could not be made is not one that failed.

## Architecture

This is a proof-of-concept relay/decoder (`poc/`, package `main`) for a stock-exchange broadcast
market data feed (NSE/BSE-style NNF protocol — transaction codes like 7208/7305, field names such as
ISIN/token/MBP). Everything lives in one package.

- `exchange_communication.go` — struct definitions describing the wire layout of exchange messages
  (`BroadcastHeader`, `MSBroadcastOnlyMBP`, `MSSecurityUpdateInfo`, …), plus protocol constants
  (`broadcastDataPrefixLen`, `broadcastHeaderSize`, the per-segment netId values). The structs
  document field names/order; they are **not** used with `encoding/binary` struct decoding or
  reflection.
- `gobrdclnt.go` — the protocol logic:
  1. `main` listens on UDP for `BroadcastPackedData` packets. The read buffer is deliberately much
     larger than the 516-byte envelope: `ReadFrom` silently discards whatever does not fit and
     reports no error, so an exactly-sized buffer would make an oversized datagram
     indistinguishable from a full one. Anything over `maxDatagramLen` is logged as a WARN.
  2. `processBroadcastPackData` unpacks the multi-packet envelope (`cNetId`, packet count, then
     length-prefixed sub-packets in FIFO order). A sub-packet with `packetLen == 0` is uncompressed
     and its length comes from the header's `messageLength`; anything else is LZO1Z-compressed.
     `messageLength` counts from the broadcast header onward, so a value below `broadcastHeaderSize`
     is rejected — accepting one advances the cursor too little and misframes every later
     sub-packet in the same buffer.
  3. Each sub-packet is handled by `handleSubPacket`, which decompresses compressed ones via
     `DecompressSafe` (`decompress.go`) into a reusable buffer allocated once before the UDP loop,
     and recovers from any panic so one malformed/corrupt packet logs an error instead of crashing
     the relay.
  4. Payloads are parsed by `parseBroadcastHeader`, then dispatched on `header.transactionCode`
     (7208 → `parseMSBroadcastOnlyMBP`, 7305 → `parseMSSecurityUpdateInfo`).
- `decompress.go` / `errors.go` — pure Go port of LZO1Z decompression from the reference C
  implementation. `DecompressSafe` is bounds-checked and returns an error on malformed input; this is
  what `main` uses since compressed sub-packets are untrusted network input. `Decompress` is the
  unchecked variant (may panic on malformed input) — used only in tests, where inputs are
  known-good.
- `reader.go` — `fieldReader`, the sequential big-endian cursor all `parse*` functions read through,
  plus `hexDump`.
- `logger.go` — three loggers: `appLogger` (operational/errors → stderr), `packetLogger` (packet and
  parsed-message dumps, `io.Discard` until `initPacketLogger` points it at the `-out` file), and
  `debugLogger` (field-level tracing, off until `initDebugLogger`; gated by `debugEnabled`).
- `config.go` — JSON config file loading. Pointer fields distinguish "absent" from "set to zero".
  The nested `db` object is parsed into a `DBConfig` by `toDBConfig`, which also reads the password
  from the environment.
- `db.go` — the Oracle connection layer (see "Database" below). Not referenced from the packet path.

### Three protocol invariants that are easy to get wrong

1. **Every broadcast data payload starts with an 8-byte prefix.** Byte 0 repeats the segment netId,
   bytes 1–7 are ignored, and the broadcast header starts at the **9th byte** — for compressed
   packets this applies *after* decompression. This is handled centrally: `parseBroadcastHeader` and
   the `parseMS*` functions take the full payload (prefix included) and skip it internally, so
   callers pass the whole thing. Getting this wrong shifts every field by 8 bytes.
2. **All multi-byte fields are big-endian**, and only these transaction codes are compressed: 7200,
   7201, 7202, 7208, 7213, 7215, 7216, 7220. Notably **7305 is not compressed** and always arrives
   with `packetLen == 0`.
3. **The structs are `#pragma pack(2)`, and the spec's per-field offset columns don't always say so.**
   Spec Ch.2 p.16 ("Guidelines for Designers" item 3): `CHAR` and `CHAR` arrays are byte aligned,
   every other member is word aligned, and a struct of odd size is padded to an even number of bytes
   — which also sets the stride of an array of them. Several offset tables are written as if fully
   packed and come out 1–2 bytes short of the same table's declared `Packet Length`. That gap is
   real interior padding, not trailing slack. **Before adding or editing a `parseMS*`, sum the
   fields, apply the pack(2) rules, and check the total equals the declared packet length.** Where
   the two already agree (7305: 294 exactly, with an unpadded 3-byte
   `ST_SEC_ELIGIBILITY_PER_MARKET`), the offset column is correct and no padding belongs — so do not
   align blindly, add padding only where the declared length demands it.

   7208 is the worked example: `INTERACTIVE_ONLY_MBP_DATA`'s fields sum to 213, but
   `NetChangeIndicator` (CHAR @16) is the only odd-sized member, so one pad byte sits at offset 17
   and the record stride is `mbpRecordSize` = **214**, making 40 + 2 + 2×214 = the declared 470.
   `parseInteractiveOnlyMBPData` reads that pad explicitly via `fieldReader.pad`, and
   `mbpLayoutCheck` in `exchange_communication.go` fails the build if the arithmetic stops working.
   Getting this wrong is *subtly* wrong, not obviously wrong: `token` and the other fields before
   offset 17 still decode correctly, while everything after them is off by one (`openPrice`,
   `highPrice`, `lowPrice` become noise) and `records[1]` starts a byte early.

See `brd_data_compression_context.md` (repo root) for the condensed protocol spec these rules come
from, including the envelope layout and per-segment netId values.

### Adding or changing a parsed field

The `parse*` functions read through a `fieldReader` (`reader.go`) rather than hardcoded
`data[a:b]` slices: each `r.i32("name")` / `r.i16` / `r.byte1` / `r.f64` / `r.bytes` call reads at the
cursor and advances it. So **field order in the `parse*` function is the wire order** — to add,
remove or resize a field, edit the struct in `exchange_communication.go` and the matching sequence of
reader calls, and the offsets take care of themselves. There are no offset constants to keep in sync.
Only the total message-size checks at the top of each `parseMS*` function are still written out
explicitly.

Alignment padding is read the same way, with `r.pad("name", n)` — an explicit, debug-logged read
rather than a silent cursor bump, so pad bytes stay visible in the `-debug` trace and the logged
offsets keep lining up 1:1 with the hex dump. See invariant 3 above for when padding is needed.

**Record counts are not the array size.** 7208's message is fixed at 470 bytes and always has room
for `mbpMaxRecords` (2) records, but `NoOfRecords` says how many actually carry data. The parser
clamps the declared count to `[0, mbpMaxRecords]` (warning if it had to) and parses only that many,
leaving the rest zeroed; consumers should read `validRecords()`, never the raw array. Parsing all
the slots unconditionally decodes the message's trailing padding into a second, entirely fabricated
security that looks exactly like a real one downstream.

## Database

`poc/db.go` is an Oracle connection layer built on **`github.com/sijms/go-ora/v2`** — a pure Go
driver, chosen because it needs no cgo and no Oracle Instant Client, which is what keeps
`CGO_ENABLED=0 GOOS=aix GOARCH=ppc64` working. Verified: only go-ora's own packages are compiled
(`go list -deps ./poc`); the `gorm`/`xorm` entries in go-ora's `go.mod` belong to its test
directories and `go mod why gorm.io/gorm` reports the main module does not need them.

`OpenDB` → `Ping` / `Exec` / `ExecTx` / `ServerVersion` / `Close`. `openDB` is the driver-agnostic
half, split out so tests can point it at a stub driver.

### Things that will bite you

1. **Bind placeholders are `:1`, `:2` — never `?`.** Oracle uses positional (`:1`) or named
   (`sql.Named("t", v)` → `:t`) binds. A `?` is not a placeholder; it fails at parse time with
   `ORA-00936: missing expression`, which points nowhere near the real problem. There is also no
   `ON CONFLICT` — upserts are `MERGE INTO … USING dual ON (…) WHEN MATCHED THEN UPDATE … WHEN NOT
   MATCHED THEN INSERT …`.
2. **The DSN contains the password; never log it.** Use `DBConfig.describe()` (built from the
   individual fields, so no URL-escaping edge case can leak) for anything user-visible.
   `redactDSN` is a second line of defence that scrubs driver errors quoting the connection string
   back at us. The packet log is world-readable and append-only — a leak there is permanent.
3. **`sql.Open` does not connect.** `OpenDB` pings under `ConnectTimeout` so a bad host or
   credential fails at startup, not on the first write.
4. **The pool is deliberately small** (4 open / 2 idle). Oracle caps concurrent sessions via the
   instance's `sessions`/`processes` init parameters. `ConnMaxLifetime` (30m) matters separately:
   Oracle profiles (`IDLE_TIME`) and stateful firewalls kill idle sessions silently, and the first
   reuse of a dead one surfaces as `ORA-03113`/`ORA-03114`. Retiring connections first turns that
   error into a reconnect.
5. **`ExecTx` rolls back on panic as well as error**, then re-panics — a panicking callback must not
   leave a transaction open holding row locks.
6. Use `oracleErrCode(err)` rather than string-matching, to tell an expected `ORA-00001` (unique
   constraint) from an operational `ORA-12541` (no listener).

### Credentials

The password comes **only** from `FEEDVALVE_DB_PASSWORD`. There is no flag (flags show up in `ps`)
and no config-file key (config files get committed and copied between environments). Everything
else lives under `db` in the config file.

### Connection lifetime

The connection is opened **once** in `run` when the config has a `db` object, stored in the
package-level `packetDB` (via `setPacketDB`, alongside the `expectedNetID`/logger globals), and
lives for the whole process. It is closed by a `defer` in `run` that fires when `receiveLoop`
returns.

That chain only works because `main` is split into `main` → `run(ctx) error`: **`appLogger.Fatalf`
calls `os.Exit`, which skips every `defer`**, so the old `Fatalf`-on-error style could never have
closed anything. Startup failures now return errors instead.

Shutdown: `signal.NotifyContext` catches SIGINT/SIGTERM → `receiveLoop` closes the UDP listener,
which is the only way to unblock an in-flight `ReadFrom` → the loop returns → `run`'s defers clear
`packetDB` and close the database, then close the output file and the listener. A read error while
`ctx.Err() != nil` means "shutting down", not a failure.

### Per-packet dummy write is synchronous

`processPacket` calls `updateDummyRowForPacket` at the end of **every** switch case — 7208, 7305,
and the unhandled-code default — right after the packet is logged. It runs the standalone
`updateDummyRow(ctx, db *sql.DB, row DummyRow)` function directly on the UDP receive goroutine —
begin transaction → `UPDATE` → commit against the raw `*sql.DB`, no `DB` method or `ExecTx`
involved — updating one fixed row (`packetDummyRowID = 1`) of `feedvalve_dummy`. The DDL is in the
doc comment on `DummyRow`; the row must be seeded or every update affects 0 rows (logged as a WARN,
since a real server reports that as success).

There is no queue and no background writer — this is deliberately simple POC-style code, at the
cost that a slow database directly delays the next `ReadFrom` and the kernel may drop datagrams that
arrive meanwhile. `updateDummyRowForPacket` no-ops when no database is configured (`packetDB ==
nil`) — which is what keeps the decoder tests working — and `updateDummyRow` never returns an error:
a SQL error is logged (`ERROR update dummy row ... err=...`) and swallowed, since a database problem
must not stop the relay decoding the feed. Note it sits *after* the parse, so a payload that fails to
parse writes nothing.

## Debugging parsed data

Run with `-debug` to get, per packet: a `hexdump -C`-style dump of the payload, then one line per
field as it is parsed —

```
field=header.transactionCode         offset=18-20 hex=1c89 value=7305
field=token                          offset=48-52 hex=000181cd value=98765
```

Offsets are **absolute within the broadcast data** (including the 8-byte prefix), so they line up
directly against the hex dump and against a wire capture. If values look wrong, the hex dump tells
you whether the bytes themselves are wrong (a decompression problem) or whether the bytes are right
but landed in the wrong field (an offset/struct problem).

The netId check is a free frame-alignment oracle: a `WARN broadcast data net_id mismatch` on packet 2
or later of a buffer almost always means the previous sub-packet's length was computed wrongly,
leaving the cursor misaligned.

## Tests

`go test ./...` — plain Go, no build tags:

- `decompress_test.go` — decompression correctness only: round-trips fixtures through `Decompress`
  and `DecompressSafe`, sweeps sizes around length-encoding boundaries, and checks truncated input is
  rejected rather than silently mis-decoded.
- `gobrdclnt_test.go` — the envelope → decompress → parse pipeline: prefix skipping, wire-true debug
  offsets, uncompressed sub-packet stride, compressed round-trip, mixed compressed/uncompressed
  buffers in FIFO order, and netId warn-but-don't-drop behavior. Plus the framing/validation
  regressions: `TestMBPRecordWireOffsets` pinning 7208's pack(2) record layout against an
  independent table of absolute byte offsets (deliberately *not* built with
  `buildMSBroadcastOnlyMBPMessage` — a fixture builder that walks the same field order as the parser
  agrees with it even when both are wrong, which is exactly how the 213-byte stride survived),
  7208 `NoOfRecords` bounding the records parsed and logged, `messageLength` below the
  header size being rejected, dispatch stopping when the header fails to parse, oversized packed
  data being trimmed *loudly*, and the `-maxout` cap.

- `db_test.go` — the Oracle connection layer, against a **stub `database/sql` driver** registered in
  the test file, so `go test ./...` needs no database: pool settings applied, ping-on-open,
  `rowsAffected`, `ExecTx` commit/rollback/rollback-on-panic, query timeout vs. caller deadline,
  config parsing and validation, and that neither `describe()` nor a driver error can leak the
  password. Also the dummy-row wiring (that `processPacket` writes for 7208, 7305 *and* the default
  branch, and writes nothing when the parse failed, each write going through the standalone
  `updateDummyRow`'s begin/update/commit and asserted against the stub driver's commit/rollback
  counts), repeated SQL failures being logged and swallowed rather than killing the packet path, and shutdown
  (`receiveLoop` returning on cancellation, `run` returning so its defers fire). `TestDBIntegrationUpdate`
  does a real create/insert/update/rollback round trip but **skips unless `FEEDVALVE_ORACLE_DSN` is
  set**: `FEEDVALVE_ORACLE_DSN='oracle://user:pass@host:1521/SVC' go test -run TestDBIntegration ./poc/`

**There is no compression dependency, and no compressor in this repo.** Decompression is this
repo's own port; the compressed bytes the tests decompress were produced by an independent LZO1Z
implementation (`github.com/rhnvrm/lzo1z`) and are **frozen** in `poc/testdata/lzo_fixtures.json`,
keyed by the SHA-256 of the input. `compressWithReference` looks them up rather than compressing.

That independence is the point: a decompressor checked only against a compressor from the same
codebase agrees with itself even when both are wrong — the same trap that let 7208's 213-byte
stride survive. Freezing costs no coverage, because every test input is deterministic (fixed
strings, `bytes.Repeat`, zeroed buffers, seeded `math/rand`), so the fixtures never varied anyway.

A test case with a **new** input fails loudly with its own length and hash rather than passing
silently; regenerating is documented at the top of `poc/decompress_test.go` and uses the inert
recorder `poc/lzo_record_test.go1`. Nothing but that rename reintroduces the dependency.

Note that real envelopes cap packed data at 512 bytes (`cPackData[512]`), so fixtures with several
full-size messages in one buffer are not representative and will be rejected.

## `poc` is a test fixture now — two things about it are frozen

`poc` is no longer only a proof of concept. It is the **oracle** for gate G-DIFF-1, the Phase 2 exit
criterion: `internal/app`'s differential harness builds `poc`, runs it as a subprocess with
`-debug`, feeds it a fabricated datagram over UDP, and diffs its field trace against the new
decoder's field-for-field. That is how the design satisfies req §7, which both *mandates* validating
the new decoder against `poc` and *forbids* reusing its code — the oracle is its **output**, so no
`poc` code is linked, imported or copied (ADR-012).

Two consequences, which ADR-012 accepted explicitly and HLD risk R11 tracks:

1. **`poc` must stay buildable.** `TestThePocOracleIsStillBuildable` asserts it directly, because
   G-DIFF-1 itself *skips* where there is no toolchain — correct on the AIX target, but it would
   also hide `poc` having been broken on the build host.
2. **The `-debug` line format is a compatibility contract, not a preference.** `poc/reader.go`'s
   `logField` and `internal/obs/debug.go`'s `fieldFormat` are byte-identical by intent:

   ```
   DEBUG field=%-30s offset=%d-%d hex=%x value=%v
   ```

   A changed column, a changed width or a dropped token breaks the one test that can prove the new
   decoder agrees with the one it replaces. `internal/obs/debug_test.go` pins our side against a
   re-declared copy of `poc`'s; nothing can pin `poc`'s side, so **do not reformat that line.** The
   same goes for the `(alignment padding, ignored)` token and for offsets staying absolute within
   the broadcast data, prefix included.

What is *not* frozen is `poc`'s behaviour beyond those two points, and the harness deliberately
models `poc`'s quirks rather than fixing them: it re-parses the broadcast header 2–3 times per
sub-packet, logs the 8-byte prefix as a field of its own, emits `record[0]..recordBuffer[0].x` with a
double dot, and has no `MESSAGE_HEADER` concept at all — so for 6541 and 6503 it reads the BCAST
layout and is simply wrong, which is why those two have no `poc` oracle. Vol 1 §8.6 requires the
harness not to need `poc` modified, and it does not.

## Disabled files

`*.go1` files are excluded from the build by their extension. They are the previous cgo
implementation, kept for reference only: `lzog.go1` (cgo bindings to `liblzo2`),
`lzo_compress_testutil.go1` (cgo LZO1Z compressor for test fixtures), and `gobrdclnt_test.go1` (the
old `testutil`-tagged tests). The cgo toolchain no longer builds in this environment; nothing in the
active build depends on them.

The module root (`go.mod`) has no code of its own — all code is under `poc/`.
