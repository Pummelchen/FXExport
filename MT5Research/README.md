# FXExport

FXExport is a macOS terminal Swift project for exporting MetaTrader 5 historical M1 OHLC data into ClickHouse, verifying it against MT5, repairing canonical ranges only when safe, and serving verified history to FXBacktest through a dedicated HTTP API v1.

The implementation is intentionally defensive:

- MT5 server timestamps and UTC timestamps are different Swift types.
- Prices are stored as scaled integers, never canonical floating-point values.
- Raw MT5 timestamps are always preserved.
- Canonical ingestion requires verified broker UTC offset segments.
- The current open M1 bar must never be ingested.
- ClickHouse primary keys are not treated as uniqueness constraints.
- FXExport does not run strategies or optimizations internally.
- FXBacktest and any other external backtest client must read verified canonical data through the dedicated FXBacktest API v1, never by connecting to ClickHouse directly.
- Optional Metal support inside FXExport is limited to internal read-only OHLC buffer preparation checks; strategy kernels belong in FXBacktest or another external app.

## Architecture

The system has two parts:

1. `EA/FXExport.mq5`
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
   - Serves FXBacktest API v1 on a local HTTP endpoint after the same readiness gates pass.
   - Exposes SwiftPM library products:
     - `FXExportFXBacktestAPI`: shared v1 request/response DTOs and a small HTTP client for FXBacktest.

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

1. Know what the startup preflight does before you run commands. If `Config/clickhouse.json` points to local ClickHouse at `localhost` or `127.0.0.1` and the HTTP endpoint is down, the tool tries safe local start commands automatically:

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
4. Start the resident terminal app:

```bash
.build/release/FXExport
```

5. At the `>` prompt, run the database and EA preflight. This applies idempotent migrations, verifies required tables, runs DB-only integrity checks, and compiles `FXExport.mq5` through MetaEditor from the terminal:

```text
> startcheck --config-dir Config --migrations-dir Migrations --skip-bridge
```

6. Attach the compiled `FXExport` EA in MT5. Set `SwiftHost = 127.0.0.1` and `SwiftPort = 5055`, then allow localhost sockets in MT5/Wine if prompted.
7. Run the full go-live gate. This repeats the DB and EA compile checks, waits for the EA socket, verifies the connected MT5 terminal identity, checks the live server offset through the EA, proves verified offset coverage for the configured MT5 history, and tests `GET_RATES_FROM_POSITION` with `start_pos=1` so the open M1 bar is excluded:

```text
> startcheck --config-dir Config --migrations-dir Migrations
```

If `startcheck` stops at the bridge step, follow the terminal message: start MT5, attach the compiled EA, verify the host/port, then rerun the same command. For known IC Markets MT5 servers, `startcheck` and `backfill` can insert identity-bound historical GMT+2/GMT+3 broker policy segments automatically after the EA live snapshot matches the policy. Unknown brokers or mismatched policies still fail closed and require audited `broker_time_offsets` rows for the exact MT5 company/server/account shown by the EA.

8. Confirm symbol mappings and broker digits:

```text
> symbol-check --config-dir Config
```

9. Run the initial historical backfill:

```text
> backfill --config-dir Config --symbols all
```

10. Run verification without random MT5 ranges first, then with MT5 random ranges when the bridge is connected:

```text
> verify --config-dir Config --random-ranges 0
> verify --config-dir Config --random-ranges 20
```

11. Start the dedicated FXBacktest API v1 when FXBacktest needs historical data:

```text
> fxbacktest-api --config-dir Config --api-host 127.0.0.1 --api-port 5066
```

Leave this command running while FXBacktest loads M1 OHLC data. The API server is the only supported external data path for FXBacktest.

12. Start live updates only after backfill is complete or intentionally resumed:

```text
> live --config-dir Config
```

For production operation, prefer the supervised runtime instead of running `live` alone:

```text
> supervise --config-dir Config
```

If the first historical import should be owned by the supervisor, start it explicitly:

```text
> supervise --config-dir Config --with-backfill
```

## Configuration

Copy the sample configs:

```bash
mkdir -p Config
cp ConfigSamples/app.sample.json Config/app.json
cp ConfigSamples/clickhouse.sample.json Config/clickhouse.json
cp ConfigSamples/mt5_bridge.sample.json Config/mt5_bridge.json
cp ConfigSamples/symbols.sample.json Config/symbols.json
cp ConfigSamples/history_data.sample.json Config/history_data.json
```

Edit the local files before production use. There is intentionally no `broker_time.json`; FXExport discovers the connected MT5 company/server/account through the EA and registers the broker source in ClickHouse.

ClickHouse credentials belong only in local ignored files under `Config/`. The Swift HTTP client sends credentials with an HTTP Basic Authorization header and does not put the password into the request URL. For remote ClickHouse, prefer `passwordEnvironmentVariable` so the password is supplied by the process environment instead of a config file; plaintext remote passwords are rejected unless `allowPlaintextRemotePassword` is explicitly enabled.

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

FXExport no longer uses a local `Config/broker_time.json`. On MT5-backed commands, Swift asks the EA for terminal identity, derives a stable `broker_source_id` such as `raw-trading-ltd-icmarkets-sc-mt5-4-account-12345678`, and records the identity in ClickHouse table `broker_sources`. If an identity already maps to exactly one active broker source, that ID is reused; ambiguous identity rows stop the command.

Canonical UTC conversion loads active, verified offset segments from ClickHouse table `broker_time_offsets`, filtered by:

- `broker_source_id`
- `mt5_company`
- `mt5_server`
- `mt5_account_login`
- `confidence = 'verified'`
- `is_active = 1`

For the current live MT5 server day, FXExport can automatically record a verified offset segment from the EA's `GET_SERVER_TIME_SNAPSHOT` when no verified segment covers the live server timestamp. For known IC Markets MT5 servers, FXExport can also auto-create non-overlapping historical GMT+2/GMT+3 broker-policy segments for the discovered MT5 history range, but only after the EA live snapshot matches that policy. Unknown brokers, broker policy mismatches, overlapping DB authority, or uncovered ranges stop canonical ingestion.

UTC conversion is:

```text
UTC = MT5_SERVER_TIME - OFFSET_SECONDS
```

Do not mark offset data as verified unless it is either generated by FXExport's code-owned broker policy after EA live-snapshot verification, or you have independently audited the broker offset for that exact MT5 company/server/account and historical segment. Inferred or unresolved offsets are allowed to exist in audit state, but canonical ingestion will not load them and canonical insert builders reject non-verified rows.

Manual verified offset rows are only for brokers without a code-owned policy:

```sql
INSERT INTO fxexport.broker_time_offsets
(
  broker_source_id, mt5_company, mt5_server, mt5_account_login,
  valid_from_mt5_server_ts, valid_to_mt5_server_ts, offset_seconds,
  source, confidence, verification_evidence, is_active, created_at_utc
)
VALUES
(
  'raw-trading-ltd-icmarkets-sc-mt5-4-account-12345678', 'REPLACE_WITH_BRIDGE_CHECK_COMPANY', 'ICMarketsSC-MT5-4', 12345678,
  1672531200, 1688169600, 7200,
  'manual', 'verified', 'Verified against broker server/GMT snapshot and known DST schedule', 1, 1700000000
);
```

This strictness is intentional. Brokers can change server timezone policy, daylight-saving behavior, server names, or account routing. A broad inferred offset is useful for planning but is not safe enough for canonical backtesting data.

For IC Markets server `ICMarketsSC-MT5-4`, the automatic broker source id is `raw-trading-ltd-icmarkets-sc-mt5-4-account-12345678`; code-owned policy accepts only `7200` and `10800` second offsets and generates historical GMT+2/GMT+3 segments around US daylight-saving transitions after the live EA snapshot agrees with the policy. IC Markets documents that its MT4/MT5 server time is GMT+2 or GMT+3 when daylight saving is in effect ([trading hours](https://www.icmarkets.com/global/en/trading-pricing/trading-hours/)), and its 2026 notice says the server changed from GMT+2 to GMT+3 on 2026-03-08 ([2026 server time notice](https://www.icmarkets.com.au/blog/us-daylight-savings-server-time-changing-to-gmt3-2026/)).

## ClickHouse Migrations

Run:

```text
> migrate
```

This creates:

- `mt5_ohlc_m1_raw`
- `ohlc_m1_canonical`
- `ohlc_m1_conflicts`
- `broker_sources`
- `broker_time_offsets`
- `ingest_state`
- `ingest_operations`
- `ohlc_m1_verified_coverage`
- `verification_results`
- `repair_log`
- `runtime_agent_events`
- `runtime_agent_state`
- `data_certificates`

Raw audit data is append-only. Repairs only target canonical data and must preserve conflicts, repair logs, ingest operation history, and verified coverage records.

## ClickHouse Transport Safety

FXExport uses ClickHouse's HTTP interface from Swift. This is intentional for now: the correctness guarantees come from typed validation, deterministic batch stages, canonical readback, SHA-256 chunk evidence, and checkpoint ordering, not from the wire protocol alone. For a centralized remote ClickHouse server, configure HTTPS. Plain `http://` is allowed by default only for `localhost`, `127.0.0.1`, or `::1`. A remote `http://` endpoint is rejected unless `allowInsecureRemoteHTTP` is explicitly set for a private tunnel you already trust.

The HTTP client sends SQL by POST, reads the full response body, parses ClickHouse exceptions in the body, uses `wait_end_of_query=1`, adds a per-attempt `query_id`, and never retries non-idempotent writes. Credentials must be in `username` plus either `password` or `passwordEnvironmentVariable`, not embedded in the URL. Remote deployments should use HTTPS plus `passwordEnvironmentVariable`; a remote plaintext password in config is rejected unless explicitly allowed for a private deployment.

Canonical ingestion is replace-by-range for the affected broker/source/symbol range. Swift first asks the EA for a manifest-backed MT5 range, reads it twice, and only accepts it when the source hash, row count, first/last timestamps, closed-bar boundary, and history synchronization metadata are stable. Every accepted chunk now also carries SHA-256 evidence: the semantic MT5 source response, the verified broker-offset authority snapshot, and the canonical ClickHouse readback digest after insert. Before replacing a canonical range, Swift reads existing canonical rows for the same UTC identities and writes `ohlc_m1_conflicts` rows for any differing OHLC/hash values. The delete predicate then covers both the raw MT5 server-time envelope and the converted UTC identity range before reinserting verified rows. After the replacement insert, Swift reads the full requested MT5 server-time envelope back from ClickHouse and verifies row count, unique MT5 and UTC timestamp counts, single MT5 symbol/timeframe/digits identity, zero non-verified UTC offset rows, the exact MT5 timestamp/UTC/OHLC/digits/hash sequence, and matching canonical SHA-256 before advancing the checkpoint. Each chunk writes an `ingest_operations` trail and then a SHA-256-backed verified coverage certificate split at broker UTC-offset segment boundaries, so DST/server-offset changes cannot over-certify UTC coverage. The data certification agent then creates valid data certificates over verified coverage, and the history-data API refuses ranges without both verified coverage and data certificates. This prevents duplicate canonical bars after a crash between insert and checkpoint update, catches older rows written under a wrong UTC mapping, blocks consumers after unfinished or non-SHA-protected writes, and preserves conflicting canonical versions for audit. Raw audit rows remain append-only.

## Crash Or Abort Recovery

If the first backfill is interrupted by a crash, reboot, terminal close, MT5 disconnect, or ClickHouse outage, do not drop tables and do not manually edit `ingest_state`.

Safe recovery sequence:

```text
> migrate --config-dir Config --migrations-dir Migrations
> verify --config-dir Config --random-ranges 0
> backfill --config-dir Config --symbols all
> verify --config-dir Config --random-ranges 20
> supervise --config-dir Config
```

Backfill is designed to be rerun. A checkpoint is advanced only after MT5 source double-read verification, raw insert, canonical range replacement, canonical insert, canonical readback verification, SHA-256 chunk evidence, and verified coverage write all succeed. If the process crashes before that point, rerunning backfill starts again from the last verified checkpoint. Canonical rows for the retried range are deleted and reinserted by both MT5 server-time range and UTC identity range, so duplicate canonical bars should not accumulate. Until the retried deterministic batch reaches a terminal status in `ingest_operations`, `data-check` and the read-only history API block the affected database from being used for backtests.

Do not run `backfill`, `live`, `repair`, or `supervise` in parallel for the same `broker_source_id`. The command runtime enforces this with a broker-level runtime lock under `/tmp`; if another writer or supervisor is active, the second command exits before touching MT5, canonical data, or checkpoints.

Before oldest/latest discovery for each symbol, backfill asks the EA for MT5 M1 history status and waits up to 60 seconds for synchronization. If MT5 has not synchronized local history, backfill stops for that symbol instead of snapshotting a partial oldest/latest range.

If MT5 exposes older historical bars after the first partial run, backfill now detects that the newly discovered oldest MT5 bar is earlier than the stored checkpoint oldest and reprocesses from the new oldest bar. This is conservative but prevents silent holes at the beginning of history.

If the checkpoint references a different configured MT5 symbol, or the checkpoint is newer than MT5's latest closed bar, ingestion stops for that symbol. Treat that as a broker/source identity problem and inspect config, MT5 account/server, and `broker_time_offsets` before continuing.

Raw audit rows are append-only. A crash/retry may leave repeated raw audit attempts with the same deterministic `batch_id`, but canonical history data is rewritten and verified before the checkpoint moves. Empty MT5 source ranges, such as weekends, are not treated as data gaps; they are accepted only after the EA returns a stable empty manifest, the matching canonical MT5 server-time range is proven empty in ClickHouse, and FXExport records SHA-256-backed verified coverage for that source gap.

## MT5 EA Setup

Keep `EA/FXExport.mq5` under the MT5 `MQL5/Experts` tree and let `startcheck` compile it through MetaEditor:

```text
> startcheck --config-dir Config --migrations-dir Migrations --skip-bridge
```

If your package is outside the MT5 Experts tree, copy `EA/FXExport.mq5` into `MQL5/Experts` first or set `MT5RESEARCH_METAEDITOR`, `MT5RESEARCH_WINE`, and `MT5RESEARCH_WINEPREFIX` so the terminal compile check can find the MT5 toolchain.

Attach it to a chart and enable socket/network permissions required by your MT5/Wine setup. Configure:

- `SwiftHost = 127.0.0.1`
- `SwiftPort = 5055`

Then run the needed command from the already-open `>` prompt so Swift listens for the EA.

## Interactive Commands

FXExport is now a resident terminal app. Start it without launch-time input:

```bash
swift run FXExport
```

The app does not execute commands passed at launch. Paste command text into the internal `>` prompt instead:

```text
> migrate
> bridge-check
> symbol-check
> backfill --symbols all
> backfill --symbols EURUSD,USDJPY
> live
> supervise
> supervise --with-backfill
> supervise --supervisor-cycles 1
> startcheck
> failure-guide
> verify
> verify --random-ranges 20
> repair --symbol EURUSD --from 2020-01-01 --to 2020-02-01
> data-check --config Config/history_data.json
> fxbacktest-api --api-host 127.0.0.1 --api-port 5066
> health-api --api-host 127.0.0.1 --api-port 5067
```

Command options:

```text
--config-dir Config
--migrations-dir Migrations
--config Config/history_data.json   # data-check only
--api-host 127.0.0.1                # fxbacktest-api / health-api
--api-port 5066                     # fxbacktest-api / health-api
--verbose
--debug
```

Commands that require ClickHouse first run a local readiness check. If local ClickHouse is stopped, the program tries to start it and waits for the HTTP endpoint before continuing. Commands that require MT5 print action-oriented bridge setup guidance when the EA/socket is not ready instead of leaving the user with only a low-level socket error.

Shell control commands:

```text
status   show the active command
stop     gracefully cancel the active command and wait for shutdown
wait     wait until the active command finishes
help     show shell help
exit     gracefully stop the active command and close the shell
```

If a new app command is pasted while `live`, `supervise`, `backfill`, or another active command is running, the shell first requests graceful cancellation and waits for the command task to return. It does not mark unfinished ingest work complete; checkpoints still advance only through the normal validated insert and canonical readback path.

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

`repair --from` and `--to` are UTC dates in `YYYY-MM-DD` format. Repair first verifies the requested range against MT5 through the stable source-read path, repairs canonical rows only when the mismatch is unambiguous and UTC mapping is verified, writes `repair_log`, records repair stages in `ingest_operations`, writes fresh verified coverage after canonical readback, and then verifies the range again. Raw audit rows are never deleted.

`data-check` is an operator diagnostic inside FXExport. It runs the same safety gate used by `fxbacktest-api`, then reads verified canonical M1 bars through FXExport's internal ClickHouse-backed pipeline. External backtest applications must not copy this internal path or connect to ClickHouse directly. If `"use_metal": true` is set in the config, the diagnostic also prepares read-only Metal buffers from the same arrays. No local cache is written.

`export-cache`, `backtest`, and `optimize` intentionally fail closed. FXExport is the history-data provider; strategy execution, optimization sweeps, durable optimizer jobs, and result persistence belong in the external Swift backtest application.

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

`data-check` and `fxbacktest-api` run the database safety gate before loading canonical bars. FXBacktest reaches that gate only through the API, not through direct database access. The gate blocks when:

- Any configured symbol has no checkpoint, a non-`live` ingest status, or a checkpoint mapped to a different configured MT5 symbol.
- The requested data end is beyond the target symbol's latest verified checkpoint.
- Any ingest/live/repair batch is unfinished or failed according to `ingest_operations`.
- The requested UTC range is not fully covered by SHA-256-backed `ohlc_m1_verified_coverage`.
- The requested UTC range is not fully covered by valid `data_certificates`.
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
| `data_certification` | max(7200s, 2x backup interval) | Confirms verified coverage has SHA-256 data certificates for downstream audit/export safety. |

This means a freshly loaded database is not considered safe for external backtests until the supervisor has run the safety agents successfully. If a first import was interrupted, `ingest_state.status` remains `backfilling` or `ingest_operations` contains a non-terminal batch, so history-data reads for backtesting stay blocked until backfill is rerun, all deterministic batches are terminal, and all configured symbols reach `live`.

## FXBacktest API v1

FXBacktest reads historical M1 OHLC data and pre-run MT5 execution snapshots only through the dedicated FXBacktest API v1. The API server runs inside FXExport, so FXExport remains the only process that knows ClickHouse credentials, database names, canonical table layout, readiness gates, repair/verification rules, and MT5 EA bridge details.

Start the API from the resident FXExport prompt:

```text
> fxbacktest-api --config-dir Config --api-host 127.0.0.1 --api-port 5066
```

The public SwiftPM product for clients is:

```swift
.product(name: "FXExportFXBacktestAPI", package: "MT5Research")
```

The client-side module contains only shared DTOs, the v1 constants, validation, and a small HTTP client. It does not expose ClickHouse, `BacktestCore`, FXExport's internal data provider, or Metal buffer helpers.

API identity:

| Field | Value |
| --- | --- |
| Version | `fxexport.fxbacktest.history.v1` |
| Status | `GET /v1/status` |
| M1 history | `POST /v1/history/m1` |
| Execution snapshot | `POST /v1/execution/spec` |
| Transport | Local HTTP, default `http://127.0.0.1:5066` |

Example history request:

```json
{
  "api_version": "fxexport.fxbacktest.history.v1",
  "broker_source_id": "raw-trading-ltd-icmarkets-sc-mt5-4-account-12345678",
  "logical_symbol": "EURUSD",
  "utc_start_inclusive": 1577836800,
  "utc_end_exclusive": 1735689600,
  "expected_mt5_symbol": "EURUSD",
  "expected_digits": 5,
  "maximum_rows": 5000000
}
```

Example history response shape:

```json
{
  "api_version": "fxexport.fxbacktest.history.v1",
  "metadata": {
    "broker_source_id": "raw-trading-ltd-icmarkets-sc-mt5-4-account-12345678",
    "logical_symbol": "EURUSD",
    "mt5_symbol": "EURUSD",
    "timeframe": "M1",
    "digits": 5,
    "requested_utc_start": 1577836800,
    "requested_utc_end_exclusive": 1735689600,
    "first_utc": 1577836800,
    "last_utc": 1735689540,
    "row_count": 1000
  },
  "utc_timestamps": [1577836800],
  "open": [108000],
  "high": [108020],
  "low": [107990],
  "close": [108010]
}
```

All OHLC arrays use scaled integer prices. The request validator rejects `maximum_rows` values above 5,000,000 so clients must split larger ranges. The response validator rejects mismatched column lengths, non-M1 metadata, non-minute timestamps, timestamps outside the requested range, unsorted timestamps, invalid OHLC invariants, and mismatched first/last metadata.

Before a non-demo FXBacktest run starts, FXBacktest calls `POST /v1/execution/spec` with the broker source and each loaded logical symbol. FXExport queries the live MT5 terminal through the EA bridge and returns bid/ask, spread, floating-spread flag, contract size, min/step/max lots, swap fields, margin estimates from `OrderCalcMargin`, tick values, trade mode, account currency, and leverage. The API response always marks the FXBacktest account model as `hedging`.

MT5 does not expose a reliable static symbol commission or Strategy Tester slippage model through `SymbolInfo*`, so the execution snapshot carries source fields. Current defaults are `commission_per_lot_per_side = null` with source `not_exposed_by_mt5_symbol_info`, and `slippage_points = 0` with source `deterministic_zero_default`.

Server-side loading still uses FXExport's internal read-only ClickHouse-backed provider after the readiness gate passes. That internal provider is not a supported integration surface for FXBacktest. This prevents direct DB shortcuts and keeps API v1 as the single versioned contract between the projects.

## Operational Health API

For external monitoring, start the lightweight read-only health API from the resident prompt:

```text
> health-api --api-host 127.0.0.1 --api-port 5067
```

It serves `GET /v1/health` with ClickHouse reachability, broker source count, canonical row count, unfinished ingest operation count, warning/failed agent counts, valid data certificate count, and latest canonical UTC. This endpoint is operational status only; it does not serve OHLC data.

## Production Supervisor Agents

`supervise` runs eleven operational agents through one sequential supervisor. The supervisor owns the single MT5 bridge connection, uses the same broker runtime lock as standalone writer commands, records agent events in ClickHouse, and keeps retryable failures from advancing ingestion checkpoints.

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
| 85 | `data_certification` | 3600s | Creates SHA-256 data certificates from verified coverage and canonical readback evidence. |
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
- Automatic MT5 broker source discovery, DB-backed verified broker offset authority, live-day offset auto-observation, and explicit UTC conversion.
- M1 OHLC validation.
- Deterministic framed JSON protocol.
- TCP socket transport in Swift with separate connect/accept timeout and request read/write timeout.
- ClickHouse HTTP client and migrations.
- Backfill/live update agents with MT5 history synchronization checks, double-read source completeness proofs, conflict recording, verified coverage certificates, checkpoint-after-canonical-readback flow, and canonical range replacement.
- Production supervisor with eleven operational agents, priority/supersedence rules, broker runtime lock, and runtime event/state tables.
- SHA-256 data certification agent and `data_certificates` table for verified coverage audit evidence.
- Read-only operational health API at `/v1/health`.
- Operational failure guide command with action-oriented recovery advice for unattended operation.
- Alerting agent checks for failed/stale safety agents, MT5 bridge outage state, ClickHouse/local disk pressure, unresolved verification mismatches, and failed repair outcomes. Heavy canonical duplicate/OHLC/offset scans stay owned by the verifier agent instead of running every alert cycle.
- Resilient live updater that reconnects the MT5 bridge and retries local ClickHouse recovery with backoff without advancing checkpoints on failed batches.
- History-data readiness gate that blocks on incomplete first-run ingest, unfinished ingest/repair batches, missing verified coverage, missing data certificates, damaged canonical data, unresolved verification/repair state, or stale safety-agent OK state.
- `startcheck` go-live gate with ClickHouse checks, MetaEditor EA compile, MT5 terminal identity validation, safe known-broker historical offset authority setup, verified broker UTC offset coverage, and `GET_RATES_FROM_POSITION` smoke testing.
- Startup verifier checks plus random historical MT5-vs-ClickHouse range comparison.
- Canonical-only repair command with verify -> repair -> reverify flow.
- Dedicated FXBacktest API v1 with shared DTO/client module, local HTTP server, strict request/response validation, and readiness-gated M1 history loading.
- Internal optional Metal availability and OHLC buffer-preparation utility for diagnostics.
- MQL5 EA bridge skeleton.
- XCTest coverage for critical domain/protocol/validation/time/checkpoint/history-data logic.

Intentionally not implemented in FXExport:

- Local cache export. Caches can become stale after verifier/repair activity.
- Strategy execution and EA-clone backtesting.
- Long-running optimizer jobs.
- Metal strategy kernels or optimization-sweep execution.

Those responsibilities belong in FXBacktest or another external Swift research application that consumes the dedicated FXBacktest API v1.

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
