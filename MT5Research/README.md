# FXExport

FXExport is a macOS terminal Swift project for exporting MetaTrader 5 historical M1 OHLC data into ClickHouse, verifying it against MT5, repairing canonical ranges only when safe, and providing a read-only Swift history-data API for external CPU and Metal backtest applications on Apple Silicon.

The implementation is intentionally defensive:

- MT5 server timestamps and UTC timestamps are different Swift types.
- Prices are stored as scaled integers, never canonical floating-point values.
- Raw MT5 timestamps are always preserved.
- Canonical ingestion requires verified broker UTC offset segments.
- The current open M1 bar must never be ingested.
- ClickHouse primary keys are not treated as uniqueness constraints.
- FXExport does not run strategies or optimizations internally.
- External Swift backtest applications should read verified canonical data through `FXExportHistoryData`.
- Optional Metal support is limited to read-only OHLC buffer preparation; strategy kernels belong in the external app.

## Architecture

The system has two parts:

1. `EA/HistoryBridgeEA.mq5`
   - Runs inside MetaTrader 5.
   - Connects to the Swift process over localhost TCP.
   - Uses MT5 history APIs such as `CopyRates`.
   - Sends M1 OHLC only.
   - Does not make database, checkpoint, UTC, verification, or repair decisions.

2. `FXExport`
   - Swift terminal executable.
   - Listens for or connects to the MT5 bridge.
   - Validates and converts MT5 data.
   - Writes raw audit rows and canonical OHLC rows to ClickHouse.
   - Runs backfill, live updates, MT5 cross-check verification, canonical-only repair, and history-data readiness checks.
   - Exposes SwiftPM library products:
     - `FXExportHistoryData`: read-only canonical M1 OHLC loading from ClickHouse into columnar arrays.
     - `FXExportMetalData`: optional Metal buffer preparation for those arrays on Apple Silicon.

Important MT5 socket note: standard MQL5 sockets are client-oriented. The sample EA therefore connects to the Swift listener. The Swift transport also supports outbound client mode for future bridge variants.

## Prerequisites

- macOS on Apple Silicon M2/M3.
- Swift 6 toolchain.
- ClickHouse installed with Homebrew and reachable over HTTP, usually `http://localhost:8123`.
- MetaTrader 5 running under Wine on macOS.
- The EA copied/compiled inside MT5.

## Build And Test

From the package directory:

```bash
swift test
swift build -c release
```

The executable is:

```bash
.build/release/FXExport
```

## First Run Quickstart

1. Start the tool and let the startup preflight check ClickHouse. If `Config/clickhouse.json` points to local ClickHouse at `localhost` or `127.0.0.1` and the HTTP endpoint is down, the tool tries safe local start commands automatically:

```bash
brew services start clickhouse
brew services start clickhouse-server
clickhouse start
```

If those attempts fail, the terminal prints the exact next checks to run. Remote ClickHouse URLs are never auto-started.
2. Build the release binary:

```bash
swift build -c release
```

3. Copy and edit the local configs in `Config/`. Keep the ClickHouse password only in `Config/clickhouse.json`; this directory is ignored by Git.
4. Run the database and EA preflight. This applies idempotent migrations, verifies required tables, runs DB-only integrity checks, and compiles `HistoryBridgeEA.mq5` through MetaEditor from the terminal:

```bash
.build/release/FXExport startcheck --config-dir Config --migrations-dir Migrations --skip-bridge
```

5. Attach the compiled `HistoryBridgeEA` in MT5. Set `SwiftHost = 127.0.0.1` and `SwiftPort = 5055`, then allow localhost sockets in MT5/Wine if prompted.
6. Run the full go-live gate. This repeats the DB and EA compile checks, waits for the EA socket, verifies the connected MT5 terminal identity, checks the live server offset through the EA, proves verified offset coverage for the configured MT5 history, and tests `GET_RATES_FROM_POSITION` with `start_pos=1` so the open M1 bar is excluded:

```bash
.build/release/FXExport startcheck --config-dir Config --migrations-dir Migrations
```

If `startcheck` stops at the bridge step, follow the terminal message: start MT5, attach the compiled EA, verify the host/port, then rerun the same command. If it reports missing `broker_time_offsets`, insert active, verified historical offset authority for the exact MT5 company/server/account shown by the EA. Do not calculate the current live offset by hand; `startcheck`, `bridge-check`, `backfill`, `live`, and MT5-backed `verify` check the live offset automatically through the EA.

7. Confirm symbol mappings and broker digits:

```bash
.build/release/FXExport symbol-check --config-dir Config
```

8. Run the initial historical backfill:

```bash
.build/release/FXExport backfill --config-dir Config --symbols all
```

9. Run verification without random MT5 ranges first, then with MT5 random ranges when the bridge is connected:

```bash
.build/release/FXExport verify --config-dir Config --random-ranges 0
.build/release/FXExport verify --config-dir Config --random-ranges 20
```

10. Start live updates only after backfill is complete or intentionally resumed:

```bash
.build/release/FXExport live --config-dir Config
```

For production operation, prefer the supervised runtime instead of running `live` alone:

```bash
.build/release/FXExport supervise --config-dir Config
```

If the first historical import should be owned by the supervisor, start it explicitly:

```bash
.build/release/FXExport supervise --config-dir Config --with-backfill
```

## Configuration

Copy the sample configs:

```bash
mkdir -p Config
cp ConfigSamples/app.sample.json Config/app.json
cp ConfigSamples/clickhouse.sample.json Config/clickhouse.json
cp ConfigSamples/mt5_bridge.sample.json Config/mt5_bridge.json
cp ConfigSamples/broker_time.sample.json Config/broker_time.json
cp ConfigSamples/symbols.sample.json Config/symbols.json
cp ConfigSamples/history_data.sample.json Config/history_data.json
```

Edit every file before production use.

ClickHouse credentials belong only in local ignored files under `Config/`. The Swift HTTP client sends credentials with an HTTP Basic Authorization header and does not put the password into the request URL.

### Persistent Logs And Alerts

`Config/app.json` controls durable terminal logs and alert files:

```json
"logging": {
  "file_logging_enabled": true,
  "log_file_path": "Logs/FXExport.log",
  "alert_file_path": "Logs/alerts.jsonl",
  "max_file_bytes": 10485760,
  "max_rotated_files": 5
}
```

Paths are resolved relative to the package working directory unless absolute. Logs are JSONL and rotate by size. Alert events are also written to the normal log and to `alert_file_path`, so unattended runs can be watched without scraping terminal output. `Logs/` is local runtime state and is ignored by Git.

### Symbols

`Config/symbols.json` explicitly maps logical symbols to MT5 broker symbols:

```json
{ "logical_symbol": "EURUSD", "mt5_symbol": "EURUSD.a", "digits": 5 }
```

The program does not guess suffixes or prefixes. If your broker uses `EURUSDm`, configure it explicitly.

### Broker Time

MT5 historical bar timestamps are broker/server time, not automatically UTC.

`Config/broker_time.json` identifies the configured `broker_source_id` and can optionally pin the expected MT5 terminal identity. It does not provide canonical UTC authority.

Canonical UTC conversion loads active, verified offset segments from ClickHouse table `broker_time_offsets`, filtered by:

- `broker_source_id`
- `mt5_company`
- `mt5_server`
- `mt5_account_login`
- `confidence = 'verified'`
- `is_active = 1`

UTC conversion is:

```text
UTC = MT5_SERVER_TIME - OFFSET_SECONDS
```

Do not mark offset data as verified unless you have actually verified the broker offset for that exact MT5 company/server/account and historical segment. Inferred or unresolved offsets are allowed to exist in audit state, but canonical ingestion will not load them and canonical insert builders reject non-verified rows.

Example verified offset row:

```sql
INSERT INTO fxexport.broker_time_offsets
(
  broker_source_id, mt5_company, mt5_server, mt5_account_login,
  valid_from_mt5_server_ts, valid_to_mt5_server_ts, offset_seconds,
  source, confidence, verification_evidence, is_active, created_at_utc
)
VALUES
(
  'icmarkets-sc-mt5-4', 'REPLACE_WITH_BRIDGE_CHECK_COMPANY', 'ICMarketsSC-MT5-4', 12345678,
  1672531200, 1688169600, 7200,
  'manual', 'verified', 'Verified against broker server/GMT snapshot and known DST schedule', 1, 1700000000
);
```

This strictness is intentional. Brokers can change server timezone policy, daylight-saving behavior, server names, or account routing. A broad inferred offset is useful for planning but is not safe enough for canonical backtesting data.

`expected_terminal_identity` can bind a config to a specific MT5 company/server/account:

```json
"expected_terminal_identity": {
  "company": "Broker Ltd",
  "server": "Broker-Server",
  "account_login": 12345678
}
```

For IC Markets server `ICMarketsSC-MT5-4`, the sample config uses `broker_source_id = "icmarkets-sc-mt5-4"`, pins the expected server name, and accepts only live offsets `7200` and `10800` seconds. IC Markets documents that its MT4/MT5 server time is GMT+2 or GMT+3 when daylight saving is in effect ([trading hours](https://www.icmarkets.com/global/en/trading-pricing/trading-hours/)), and its 2026 notice says the server changed from GMT+2 to GMT+3 on 2026-03-08 ([2026 server time notice](https://www.icmarkets.com.au/blog/us-daylight-savings-server-time-changing-to-gmt3-2026/)).

If `expected_terminal_identity` values are provided, `bridge-check`, `backfill`, `live`, `verify`, and `repair` verify the connected terminal before using DB-backed offset authority. Even when optional fields are omitted, the actual MT5 terminal identity is still used for the DB-backed offset lookup.

## ClickHouse Migrations

Run:

```bash
swift run FXExport migrate
```

This creates:

- `mt5_ohlc_m1_raw`
- `ohlc_m1_canonical`
- `ohlc_m1_conflicts`
- `broker_time_offsets`
- `ingest_state`
- `verification_results`
- `repair_log`
- `runtime_agent_events`
- `runtime_agent_state`

Raw audit data is append-only. Repairs only target canonical data and must preserve conflicts and repair logs.

Canonical ingestion is replace-by-range for the affected broker/source/symbol range. Before replacing a canonical range, Swift reads existing canonical rows for the same UTC identities and writes `ohlc_m1_conflicts` rows for any differing OHLC/hash values. The delete predicate then covers both the raw MT5 server-time range and the converted UTC identity range before reinserting verified rows. After the replacement insert, Swift reads the canonical range back from ClickHouse and verifies row count, unique MT5 and UTC timestamp counts, single MT5 symbol/timeframe/digits identity, zero non-verified UTC offset rows, and the exact MT5 timestamp/UTC/OHLC/digits/hash sequence before advancing the checkpoint. This prevents duplicate canonical bars after a crash between insert and checkpoint update, catches older rows written under a wrong UTC mapping, and preserves conflicting canonical versions for audit. Raw audit rows remain append-only.

## Crash Or Abort Recovery

If the first backfill is interrupted by a crash, reboot, terminal close, MT5 disconnect, or ClickHouse outage, do not drop tables and do not manually edit `ingest_state`.

Safe recovery sequence:

```bash
.build/release/FXExport migrate --config-dir Config --migrations-dir Migrations
.build/release/FXExport verify --config-dir Config --random-ranges 0
.build/release/FXExport backfill --config-dir Config --symbols all
.build/release/FXExport verify --config-dir Config --random-ranges 20
.build/release/FXExport supervise --config-dir Config
```

Backfill is designed to be rerun. A checkpoint is advanced only after raw insert, canonical range replacement, canonical insert, and canonical readback verification all succeed. If the process crashes before that point, rerunning backfill starts again from the last verified checkpoint. Canonical rows for the retried range are deleted and reinserted by both MT5 server-time range and UTC identity range, so duplicate canonical bars should not accumulate.

Do not run `backfill`, `live`, `repair`, or `supervise` in parallel for the same `broker_source_id`. The CLI enforces this with a broker-level runtime lock under `/tmp`; if another writer or supervisor is active, the second command exits before touching MT5, canonical data, or checkpoints.

Before oldest/latest discovery for each symbol, backfill asks the EA for MT5 M1 history status and waits up to 60 seconds for synchronization. If MT5 has not synchronized local history, backfill stops for that symbol instead of snapshotting a partial oldest/latest range.

If MT5 exposes older historical bars after the first partial run, backfill now detects that the newly discovered oldest MT5 bar is earlier than the stored checkpoint oldest and reprocesses from the new oldest bar. This is conservative but prevents silent holes at the beginning of history.

If the checkpoint references a different configured MT5 symbol, or the checkpoint is newer than MT5's latest closed bar, ingestion stops for that symbol. Treat that as a broker/source identity problem and inspect config, MT5 account/server, and `broker_time_offsets` before continuing.

Raw audit rows are append-only. A crash/retry may leave repeated raw audit attempts with the same deterministic `batch_id`, but canonical history data is rewritten and verified before the checkpoint moves.

## MT5 EA Setup

Keep `EA/HistoryBridgeEA.mq5` under the MT5 `MQL5/Experts` tree and let `startcheck` compile it through MetaEditor:

```bash
.build/release/FXExport startcheck --config-dir Config --migrations-dir Migrations --skip-bridge
```

If your package is outside the MT5 Experts tree, copy `EA/HistoryBridgeEA.mq5` into `MQL5/Experts` first or set `MT5RESEARCH_METAEDITOR`, `MT5RESEARCH_WINE`, and `MT5RESEARCH_WINEPREFIX` so the terminal compile check can find the MT5 toolchain.

Attach it to a chart and enable socket/network permissions required by your MT5/Wine setup. Configure:

- `SwiftHost = 127.0.0.1`
- `SwiftPort = 5055`

Then start a Swift command that listens for the EA.

## CLI Commands

At startup, commands that require ClickHouse first run a local readiness check. If local ClickHouse is stopped, the program tries to start it and waits for the HTTP endpoint before continuing. Commands that require MT5 print action-oriented bridge setup guidance when the EA/socket is not ready instead of leaving the user with only a low-level socket error.

```bash
swift run FXExport migrate
swift run FXExport bridge-check
swift run FXExport symbol-check
swift run FXExport backfill --symbols all
swift run FXExport backfill --symbols EURUSD,USDJPY
swift run FXExport live
swift run FXExport supervise
swift run FXExport supervise --with-backfill
swift run FXExport supervise --supervisor-cycles 1
swift run FXExport startcheck
swift run FXExport -startcheck
swift run FXExport failure-guide
swift run FXExport verify
swift run FXExport verify --random-ranges 20
swift run FXExport repair --symbol EURUSD --from 2020-01-01 --to 2020-02-01
swift run FXExport data-check --config Config/history_data.json
```

Global options:

```bash
--config-dir Config
--migrations-dir Migrations
--verbose
--debug
```

`startcheck` options:

```bash
--skip-ea-compile
--skip-bridge
--compile-timeout-seconds 180
```

Use `--skip-bridge` only for the early preflight before the EA is attached. A production go-live run should use full `startcheck` without skips.

`failure-guide` prints the built-in operational failure catalog. It covers ClickHouse outages and exceptions, MT5 bridge disconnects, protocol errors, unsynchronized MT5 history, missing or mismatched verified broker UTC offsets, bad OHLC data, canonical readback failures, duplicate canonical keys, interrupted first-run checkpoints, repair refusal, history-data readiness blocks, disk pressure, and crash/reboot recovery. Each scenario includes what the program does automatically, how data safety is preserved, and the exact human recovery steps when automation cannot safely continue.

`symbol-check` returns a non-zero validation exit code if any configured symbol is missing, not selected, or has different digits than `Config/symbols.json`.

`verify --random-ranges 0` runs DB-only duplicate/OHLC/UTC-confidence checks without opening the MT5 bridge. If random ranges are requested, the command connects to the EA and fails the command if any MT5 cross-check mismatches canonical data.

`repair --from` and `--to` are UTC dates in `YYYY-MM-DD` format. Repair first verifies the requested range against MT5, repairs canonical rows only when the mismatch is unambiguous and UTC mapping is verified, writes `repair_log`, and then verifies the range again. Raw audit rows are never deleted.

`data-check` runs the same safety gate external backtest applications should use, then reads verified canonical M1 bars directly from ClickHouse into a columnar `ColumnarOhlcSeries`. If `"use_metal": true` is set in the config, it also prepares read-only Metal buffers from the same arrays. No local cache is written.

`export-cache`, `backtest`, and `optimize` intentionally fail closed. FXExport is the history-data provider; strategy execution, parameter sweeps, durable optimizer jobs, and result persistence belong in the external Swift backtest application.

## Terminal Output

When stdout is a TTY and `NO_COLOR` is not set, FXExport uses ANSI colors compatible with the macOS Bash 3 terminal. The logger applies a black background to colored terminal lines. Red is reserved for actual error lines and failed agent statuses.

During `supervise`, every production agent prints a timestamped status line with its own non-red color:

```text
2026-05-15 13:34:16 - Agent M1 Updater - Checking for newly closed M1 OHLC bars
2026-05-15 13:34:16 - Agent M1 Updater - OK: live update completed (14 ms)
```

Warnings and skipped work use the agent's assigned color instead of red. Failed agent outcomes use red and are also written to the alert sink when persistent alerts are enabled.

Symbol-level work is printed in plain operator language:

```text
EURUSD - pulling M1 OHLC for March 2012
EURUSD - validating March 2012 for OHLC integrity and verified UTC conversion
EURUSD - March 2012 pulled, verified, UTC correct and canonical data clean (44,640 closed M1 bars)
EURUSD - checking March 2012 against MT5 source of truth
EURUSD - March 2012 verified against MT5; UTC correct and all canonical data clean
```

FXExport only prints "clean" after the relevant safety step has succeeded: validation plus canonical readback for ingestion, or MT5 source-of-truth comparison for verification. MT5 source gaps are reported as source gaps instead of being hidden as calendar-minute gaps.

## History Data Readiness Gate

`data-check` and external backtest applications should run the database safety gate before loading canonical bars. The gate blocks when:

- Any configured symbol has no checkpoint, a non-`live` ingest status, or a checkpoint mapped to a different configured MT5 symbol.
- The requested data end is beyond the target symbol's latest verified checkpoint.
- Canonical rows contain duplicate UTC identities, OHLC invariant failures, or non-verified offset confidence.
- Any latest verification range result is not `clean`, or any latest repair range outcome is `failed`.
- A required safety agent has a current `warning`/`failed` state.
- Required safety agents have never reported OK, or their last OK is stale.

Required fresh OK agent state before history data is considered safe for external backtests:

| Agent | Default max OK age | Purpose |
| --- | ---: | --- |
| `utc_time_authority` | max(180s, 3x configured UTC interval) | Confirms broker server time is still covered by verified offset authority. |
| `symbol_metadata_drift` | max(900s, 3x configured symbol interval) | Confirms MT5 symbols/digits still match config. |
| `live_m1_updater` | max(120s, 6x live scan interval) | Confirms closed-M1 ingestion is currently healthy. |
| `database_verifier_repairer` | max(7200s, 2x verifier interval) | Confirms DB integrity and MT5 random checks are clean according to config. |
| `checkpoint_gap_auditor` | max(900s, 3x checkpoint audit interval) | Confirms every configured symbol has a live checkpoint, checkpoint MT5 symbols still match config, checkpoint/canonical rows are consistent, and live lag is acceptable. |

This means a freshly loaded database is not considered safe for external backtests until the supervisor has run the safety agents successfully. If a first import was interrupted, `ingest_state.status` remains `backfilling`, so history-data reads for backtesting stay blocked until backfill is rerun and all configured symbols reach `live`.

## Swift History Data API

External Swift packages can depend on FXExport and import the history-data module:

```swift
import BacktestCore
import ClickHouse
import Domain

let provider = ClickHouseHistoricalOhlcDataProvider(
    client: clickHouseClient,
    database: "fxexport"
)
let request = try HistoricalOhlcRequest(
    brokerSourceId: try BrokerSourceId("icmarkets-sc-mt5-4"),
    logicalSymbol: try LogicalSymbol("EURUSD"),
    utcStartInclusive: UtcSecond(rawValue: 1_577_836_800),
    utcEndExclusive: UtcSecond(rawValue: 1_735_689_600),
    expectedMT5Symbol: try MT5Symbol("EURUSD"),
    expectedDigits: try Digits(5),
    maximumRows: 5_000_000
)
let series = try await provider.loadM1Ohlc(request)
```

The provider is read-only. It queries `ohlc_m1_canonical`, rejects non-M1 rows, unexpected MT5 symbols when configured, non-verified UTC offsets, non-closed-bar source statuses, duplicate or unsorted UTC timestamps, mixed digits, stored bar-hash mismatches, invalid OHLC invariants, and over-large requests. It does not create local caches because verifier/repair agents may legitimately rewrite canonical ranges after MT5 source-of-truth checks. Let ClickHouse serve the current canonical state; add caches only after a coherent invalidation strategy exists.

For Metal-capable external apps:

```swift
import MetalAccel

let buffers = try MetalBufferManager().makeReadOnlyBuffers(series: series)
```

The Metal helper only uploads verified OHLC columns into read-only shared buffers. It does not run strategy kernels or claim GPU results are correct.

## Production Supervisor Agents

`supervise` runs ten operational agents through one sequential supervisor. The supervisor owns the single MT5 bridge connection, uses the same broker runtime lock as standalone writer commands, records agent events in ClickHouse, and keeps retryable failures from advancing ingestion checkpoints.

The supervisor sorts due agents by explicit priority before every cycle:

| Priority | Agent | Default Timing | Responsibility |
| ---: | --- | --- | --- |
| 10 | `supervisor_coordinator` | 30s | Confirms single bridge ownership and sequential MT5 access. |
| 20 | `health_monitor` | 30s | Checks ClickHouse and current MT5 bridge reachability. |
| 30 | `utc_time_authority` | 60s | Verifies live broker server offset against DB-backed verified offset segments. |
| 40 | `symbol_metadata_drift` | 300s | Checks configured MT5 symbols and digit metadata. |
| 50 | `history_importer` | run once only when enabled | Owns first-run/resume backfill. |
| 60 | `live_m1_updater` | 10s | Ingests newly closed M1 bars. |
| 70 | `database_verifier_repairer` | 3600s | Runs DB checks, MT5 random cross-checks, and safe canonical repair. |
| 80 | `checkpoint_gap_auditor` | 300s | Checks missing checkpoints, non-live ingest states, MT5 symbol mapping drift, checkpoint/canonical consistency, and live lag. |
| 90 | `backup_readiness` | 3600s | Verifies canonical data exists for the configured broker before backup/export workflows. |
| 100 | `alerting` | 30s | Raises persistent alerts for runtime failures, stale safety agents, verifier/repair blockers, MT5 bridge outages, and disk pressure. |

Supersedence rules are conservative. `history_importer` blocks live updates, verifier/repair, checkpoint audit, and backup readiness for that cycle because it owns canonical writes and checkpoints during first-run/resume. A failed or warning UTC authority blocks ingestion and verification because canonical UTC cannot be trusted. Symbol metadata failures block ingestion and verification. Verifier or checkpoint warnings block backup readiness; this includes missing checkpoints, interrupted backfills, checkpoint MT5 symbol drift, canonical checkpoint mismatches, and live lag. Health failures block all MT5-dependent and data-quality agents. Dynamic supersedence persists across cycles until the source agent reports OK, so a failed UTC or symbol check cannot be bypassed merely because its next scheduled check is not due yet. A failed first-run importer is retried on the checkpoint-audit interval instead of being marked completed.

Supervisor intervals are configured under `supervisor` in `Config/app.json`. The default production stance is to leave backfill disabled in the supervisor and run it deliberately, then use `supervise` for ongoing operation.

The alerting agent also uses these thresholds:

- `mt5_bridge_down_alert_seconds`: how long MT5-sensitive failed agent state can persist before it is reported as a bridge outage.
- `minimum_free_disk_bytes`: local filesystem free-space alert threshold.
- `clickhouse_disk_free_alert_bytes`: ClickHouse `system.disks` free-space alert threshold.

## Current Implementation Status

Implemented:

- Swift Package Manager project.
- Strong domain types.
- ANSI terminal logger with black-background color output, `NO_COLOR` support, and per-agent supervisor status lines.
- Persistent JSONL log and alert files with size-based rotation.
- JSON config loading.
- DB-backed verified broker offset authority and explicit UTC conversion.
- M1 OHLC validation.
- Deterministic framed JSON protocol.
- TCP socket transport in Swift with separate connect/accept timeout and request read/write timeout.
- ClickHouse HTTP client and migrations.
- Backfill/live update agents with MT5 history synchronization checks, conflict recording, checkpoint-after-canonical-readback flow, and canonical range replacement.
- Production supervisor with ten operational agents, priority/supersedence rules, broker runtime lock, and runtime event/state tables.
- Operational failure guide command with action-oriented recovery advice for unattended operation.
- Alerting agent checks for failed/stale safety agents, MT5 bridge outage state, ClickHouse/local disk pressure, unresolved verification mismatches, and failed repair outcomes. Heavy canonical duplicate/OHLC/offset scans stay owned by the verifier agent instead of running every alert cycle.
- Resilient live updater that reconnects the MT5 bridge and retries local ClickHouse recovery with backoff without advancing checkpoints on failed batches.
- History-data readiness gate that blocks on incomplete first-run ingest, damaged canonical data, unresolved verification/repair state, or stale safety-agent OK state.
- `startcheck` go-live gate with ClickHouse checks, MetaEditor EA compile, MT5 terminal identity validation, verified broker UTC offset coverage, and `GET_RATES_FROM_POSITION` smoke testing.
- Startup verifier checks plus random historical MT5-vs-ClickHouse range comparison.
- Canonical-only repair command with verify -> repair -> reverify flow.
- Read-only Swift history-data API using validated columnar arrays.
- Optional Metal availability and OHLC buffer-preparation utility.
- MQL5 EA bridge skeleton.
- XCTest coverage for critical domain/protocol/validation/time/checkpoint/history-data logic.

Intentionally not implemented in FXExport:

- Local cache export. Caches can become stale after verifier/repair activity.
- Strategy execution and EA-clone backtesting.
- Long-running optimizer jobs.
- Metal strategy kernels or parameter-sweep execution.

Those responsibilities belong in external Swift research applications that consume FXExport's read-only data API.

## Data Integrity Rules

This project must preserve these invariants:

- Never ingest the open M1 bar.
- Never trust MT5 server time as UTC.
- Never build canonical UTC rows from config-only, inferred, unresolved, or identity-unbound offset segments.
- Never lose raw MT5 timestamps.
- Never use floating-point prices as canonical prices.
- Never advance a checkpoint before successful validated insert.
- Never rely on ClickHouse primary key uniqueness.
- Never silently repair data.
- Never silently ignore ClickHouse response-body errors.
- Never hide protocol errors.
- Never claim a range is verified unless MT5 comparison succeeded.
- Never let a stale cache override repaired canonical ClickHouse data.
