# FeedValve operations runbook

Audience: the operator on the AIX LPAR. Source of truth for the design is
`docs/lld/04-lifecycle-ops.md` (sections 6.5 to 6.7, 8.8, 13); this file is the part that has to be
readable at 3 a.m. Every instrument name below is checked against the instrument ledger by
`internal/obs/runbook_test.go`, so a rename that forgets this file fails the build.

Where the numbers come from: there is no metrics agent, no scrape endpoint and no sidecar. The whole
observability surface is **stderr**, captured by the supervisor. Every `stats.intervalSec` (default
60 s) the process writes grouped counter lines and one `health` line. Counters are published as a
cumulative total and a delta; `_max` and `_max_ns` instruments publish the interval maximum. Read the
**delta** for "is it happening now".

## 1. Health checks (one-shot modes)

| Mode | Command | Needs `FEEDVALVE_DB_PASSWORD` | Exit |
|---|---|---|---|
| `-selfcheck` | `./feedvalve -config config.json -selfcheck` | No. Opens no socket, file or database | 0 or 3 |
| `-dbcheck` | `FEEDVALVE_DB_PASSWORD=... ./feedvalve -config config.json -dbcheck` | Yes. Read-only; proves credentials, listener and service name from this host, logs the server version, checks the catalogue | 0 or 1 |

There is no HTTP health endpoint. Liveness for the supervisor is the process exit code: 1 is a
startup failure, 2 is a panic (read the stack on stderr), 3 is a configuration problem, and 4 is a
terminal runtime condition (read the fatal log line).

## 2. The twelve ordered questions

Work top to bottom and stop at the first abnormal answer. The `Q1` answer can invalidate all the
others, so do it first.

### Q1 Is it running, and has it been restarting?
Read `uptime_seconds` and `config_generation` in the `health` line, and the supervisor's own log. An
`uptime_seconds` that keeps resetting is a restart loop. `config_generation` back at 1 means a restart,
not a reload.

### Q2 Is anything arriving at all?
Read `circuit_state` (0 Starting, 1 Live, 2 Quiet, 3 Down), the `datagrams_received` delta and
`heartbeat_seen`. Silence is a circuit-down signal, not a quiet market: a working circuit sends a pulse
after about 9 s. `circuit_state` 3 with a zero `datagrams_received` delta and the process up means the
feed is gone, not us.

### Q3 Are we losing messages, and is it us or the wire?
Read `seq_gap_messages` and `seq_gap_events`, then run `netstat -s -p udp` and look for socket buffer
overflows. This is the only external tool the runbook depends on. Overflows present: our read loop
stalled, and `shard_send_blocked` will have climbed first (Q4). Overflows absent: the circuit dropped
them and only the exchange can answer. The counters are system-wide, so another UDP receiver on the
host makes attribution ambiguous (OQ-L9).

### Q4 How close are we to the one fatal cliff?
Read `shard_send_blocked`, `shard_depth_max`, `stall_budget_used_max_ns` and `shard_loop_max_ns`. A
shard is designed to be well under 1 % busy, so any non-zero `shard_send_blocked` is a defect, not
load. It is the only warning before the process stops itself.

### Q5 Did the circuit drop and come back?
Read `circuit_down_events` and `circuit_outage_max_ns`, paired with `seq_gap_messages` over the same
interval. A recovery and a gap are one event seen from two instruments.

### Q6 Is the decode correct, or plausible-but-wrong?
Read in this order: `netid_mismatch`, `body_len_mismatch{code}`, `record_count_clamped{code}`,
`decompress_failed`, `framing_abandoned`, then `panics_recovered` with `stage=decode`. A
`netid_mismatch` on sub-packet 2 or later means the previous sub-packet's length was computed wrongly.
If all are zero and values still look wrong, go to Q6a.

### Q6a Are the bytes wrong, or are the right bytes in the wrong field?
Edit the config file to set `debug.traceTranscodes` to the transcode in question, for example `[7208]`,
and `debug.hexDump` to `true`. Then send SIGHUP (`kill -HUP <pid>`). Confirm with Q10 that the reload
landed (the INFO line `config reloaded generation=` appears at once and `config_generation`
advances). Read the trace on stderr. Offsets are absolute within the broadcast data, including the
8-byte prefix, so they line up against the hex dump. Wrong bytes mean decompression; right bytes in the
wrong field mean layout (Q6b). **Turn the trace off again** the same way: it is per-field and verbose.

### Q6b If layout: does the pack(2) arithmetic still reconcile?
Sum the fields, apply pack(2) alignment, and compare with the declared packet length. 7208 is the
worked example: the record stride is 214, not 213, so 40 + 2 + 2 x 214 = 470. Fields before record
offset 17 stay correct and everything after turns to noise.

### Q7 Is the write path keeping up?
Read `writers_busy` and `writers_configured`, always together with `writers_stopped`: without it a pool
that lost half its writers reads as half idle. Then `epoch_duration_max_ns`, `tick_skipped_epoch_open`
and `dirty_depth`. A long epoch with `tick_skipped_epoch_open` rising is intended and self-recovering.

### Q8 Is the database healthy?
Read `writer_health_state` (0 Healthy, 1 Degraded, 2 FailFast), `writes_failed`, `write_retry` and
`row_rejected`. `write_retry` with `class=deadlock` non-zero is a configuration defect (broken disjoint
row ownership), not load. `row_rejected` climbing is usually a scale or width mismatch.

### Q9 Is the ordering guard eating data?
Read `ordering_guard_disabled`, and the ratio `guard_superseded{transcode}` over `guard_offered{transcode}`. Then
the free cross-check: `session_state{market_type}` against `MD_MARKET_STATUS.transcode` in Oracle.
The two must agree. Also watch `session_anomalies` and `session_events_dropped`: the first counts
unexpected session transitions, the second means session events were lost.

### Q10 Did my configuration change take effect?
`config_generation` advanced **and** `config_reload_rejected` unchanged. The absence of an error is not
confirmation. The INFO line `config reloaded generation=N` is the immediate confirmation.

**Pickup windows.** Most keys apply at once. A change to `stats.intervalSec` can take up to one **old**
interval (up to the previous `stats.intervalSec`, 60 s by default) to appear, because the sleeping
goroutine is still on the old period. Sixty seconds of apparent nothing is expected, not a failure.
Startup-only keys cannot be applied by SIGHUP at all; they need a restart.

### Q11 Is anything being silently dropped or truncated at the edges?
Read `unknown_transcode`, `unknown_transcode_other`, `message_truncated`, `datagrams_oversize` and
`datagrams_unexpected_source`. A new `unknown_transcode` code is the inventory discovery mechanism, and
closing it needs a restart, not a reload. Non-zero `datagrams_oversize` means the 516-byte envelope
assumption changed: escalate to the exchange before trusting any decode.

### Q12 What did the last shutdown lose?
Read `drain_abandoned_rows` in the shutdown summary line. Non-zero means the 5 s drain deadline
expired. Expected after a stop caused by the database failing; unexpected after a clean SIGTERM.

## 3. Observability on AIX

- stderr is the entire surface. Rotation is the platform's job; there is no in-process rotation and no
  packet log. Log volume at defaults is about 15 lines a minute plus rate-limited events.
- `netstat -s -p udp` is the only external dependency (Q3).
- `time/tzdata` is compiled in, so the trading-window gate does not need a zoneinfo database.
- Signals come from the standard library; there is no `golang.org/x/sys`.

## 4. Deploy and rollback checklist

Deploy:

1. Build host: `go vet`, `go test ./...`, `go test -race ./...`.
2. Cross-compile with `-trimpath` and the version stamp (`scripts/build-aix.sh`); record the version
   and the artifact SHA-256.
3. Copy to the LPAR as `feedvalve-<version>` **alongside** the running binary. Never overwrite in place.
4. `./feedvalve-<version> -config config.json -selfcheck` must exit 0.
5. `FEEDVALVE_DB_PASSWORD=... ./feedvalve-<version> -config config.json -dbcheck` must exit 0 and log the
   server version. Steps 4 and 5 are the two people skip.
6. Optionally run `scripts/lpar-smoke.sh` and the cross-compiled test binaries.
7. Schedule the switch outside the trading session. There is no zero-downtime path.
8. `kill -TERM` the running process. Wait for exit 0 and a shutdown summary with
   `drain_abandoned_rows` = 0.
9. Repoint the symlink and start the new binary under the supervisor.
10. Within 120 s verify all six: the startup INFO line shows the expected version and
    `config_generation=1`; `circuit_state` reaches 1; `datagrams_received` delta is non-zero;
    `rows_written` delta is non-zero; `writers_stopped` is 0; `seq_gap_messages` is no worse than the
    pre-deploy baseline.

Roll back if, within the first 5 minutes:

- the exit code is 1, 2 or 4;
- `circuit_state` does not reach 1 within `liveness.startupGraceSec` (60 s);
- `writers_stopped` is above 0;
- `panics_recovered` with `stage=decode` is above 0;
- `netid_mismatch` or `framing_abandoned` is non-zero where the previous build had zero;
- `row_rejected` is non-zero where the previous build had zero.

Procedure: SIGTERM, repoint the symlink to the previous binary **and the previous `config.json`**, start.
The mirror is lost either way, exactly as for any restart. Configuration-only rollback is often the real
fix: SIGHUP back to the previous config file at zero restart cost; confirm with Q10. Startup-only keys
cannot be rolled back this way.

## 5. Open questions and owners

Inherited from earlier volumes or raised in Vol 4 section 13. None is closed by this runbook.

| Id | Question | Owner |
|---|---|---|
| OQ-L3 | Is `hash/maphash` allocation-free and fast on `aix/ppc64`? | G-AIX-1 assertion 2, run on the LPAR |
| OQ-L4 | Writer retry backoff numbers (100 ms to 5 s assumed) | Measured against the instance during the OQ-4 spike (DBA / schema owner) |
| OQ-L5 | Does AIX ship a zoneinfo database Go can find? | Resolved by importing `time/tzdata`; confirmed by G-AIX-1 assertion 3 |
| OQ-L7 | Does the supervisor restart on non-zero exit and deliver SIGHUP? | Platform team, or `scripts/lpar-smoke.sh` SIGHUP step |
| OQ-L8 | Where does stderr go, and is it rotated? | Platform team |
| OQ-L9 | Does `netstat -s -p udp` attribute overflows to our socket? | One command on the LPAR under synthetic load (operator) |
| OQ-L10 | Is a log collector reachable (key=value versus JSON)? | Platform team |
| OQ-L11 | LPAR AIX version and Go netpoller behaviour | Run the G-AIX-1 smoke suite on the LPAR (operator) |
| OQ-L12 | Does ops have a runbook-delivery mechanism? | Platform/ops team |

OQ-L6 (is 64 maskable fields enough) is a Vol 0 item and is not carried by the Vol 4 section 13 table;
it is settled with OQ-13 (schema owner) when the real column list lands and is listed here for
completeness.
