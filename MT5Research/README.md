# MT5Research

MT5Research is a macOS terminal Swift project for exporting MetaTrader 5 historical M1 OHLC data into ClickHouse, verifying it against MT5, repairing canonical ranges only when safe, and preparing the data path for deterministic CPU backtesting and optional Metal acceleration on Apple Silicon.

The implementation is intentionally defensive:

- MT5 server timestamps and UTC timestamps are different Swift types.
- Prices are stored as scaled integers, never canonical floating-point values.
- Raw MT5 timestamps are always preserved.
- Canonical ingestion requires verified broker UTC offset segments.
- The current open M1 bar must never be ingested.
- ClickHouse primary keys are not treated as uniqueness constraints.
- The CPU backtest engine is the correctness reference.
- Metal acceleration is optional and must be verified against CPU results.

## Architecture

The system has two parts:

1. `EA/HistoryBridgeEA.mq5`
   - Runs inside MetaTrader 5.
   - Connects to the Swift process over localhost TCP.
   - Uses MT5 history APIs such as `CopyRates`.
   - Sends M1 OHLC only.
   - Does not make database, checkpoint, UTC, verification, or repair decisions.

2. `mt5research`
   - Swift terminal executable.
   - Listens for or connects to the MT5 bridge.
   - Validates and converts MT5 data.
   - Writes raw audit rows and canonical OHLC rows to ClickHouse.
   - Runs backfill, live updates, MT5 cross-check verification, canonical-only repair, and backtest scaffolds.

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
.build/release/mt5research
```

## First Run Quickstart

1. Start ClickHouse and confirm the HTTP endpoint is reachable.
2. Build the release binary:

```bash
swift build -c release
```

3. Copy and edit the local configs in `Config/`. Keep the ClickHouse password only in `Config/clickhouse.json`; this directory is ignored by Git.
4. Run the database and EA preflight. This applies idempotent migrations, verifies required tables, runs DB-only integrity checks, and compiles `HistoryBridgeEA.mq5` through MetaEditor from the terminal:

```bash
.build/release/mt5research startcheck --config-dir Config --migrations-dir Migrations --skip-bridge
```

5. Attach the compiled `HistoryBridgeEA` in MT5. Set `SwiftHost = 127.0.0.1` and `SwiftPort = 5055`, then allow localhost sockets in MT5/Wine if prompted.
6. Run the full go-live gate. This repeats the DB and EA compile checks, waits for the EA socket, verifies the connected MT5 terminal identity, checks the live server offset through the EA, proves verified offset coverage for the configured MT5 history, and tests `GET_RATES_FROM_POSITION` with `start_pos=1` so the open M1 bar is excluded:

```bash
.build/release/mt5research startcheck --config-dir Config --migrations-dir Migrations
```

If `startcheck` stops at the bridge step, follow the terminal message: start MT5, attach the compiled EA, verify the host/port, then rerun the same command. If it reports missing `broker_time_offsets`, insert active, verified historical offset authority for the exact MT5 company/server/account shown by the EA. Do not calculate the current live offset by hand; `startcheck`, `bridge-check`, `backfill`, `live`, and MT5-backed `verify` check the live offset automatically through the EA.

7. Confirm symbol mappings and broker digits:

```bash
.build/release/mt5research symbol-check --config-dir Config
```

8. Run the initial historical backfill:

```bash
.build/release/mt5research backfill --config-dir Config --symbols all
```

9. Run verification without random MT5 ranges first, then with MT5 random ranges when the bridge is connected:

```bash
.build/release/mt5research verify --config-dir Config --random-ranges 0
.build/release/mt5research verify --config-dir Config --random-ranges 20
```

10. Start live updates only after backfill is complete or intentionally resumed:

```bash
.build/release/mt5research live --config-dir Config
```

For production operation, prefer the supervised runtime instead of running `live` alone:

```bash
.build/release/mt5research supervise --config-dir Config
```

If the first historical import should be owned by the supervisor, start it explicitly:

```bash
.build/release/mt5research supervise --config-dir Config --with-backfill
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
```

Edit every file before production use.

ClickHouse credentials belong only in local ignored files under `Config/`. The Swift HTTP client sends credentials with an HTTP Basic Authorization header and does not put the password into the request URL.

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
INSERT INTO mt5research.broker_time_offsets
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
swift run mt5research migrate
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

Canonical ingestion is replace-by-range for the affected broker/source/symbol range. Before replacing a canonical range, Swift reads existing canonical rows for the same UTC identities and writes `ohlc_m1_conflicts` rows for any differing OHLC/hash values. The delete predicate then covers both the raw MT5 server-time range and the converted UTC identity range before reinserting verified rows. After the replacement insert, Swift reads the canonical range back from ClickHouse and verifies row count, unique MT5 and UTC timestamp counts, and the exact MT5 timestamp/UTC/hash sequence before advancing the checkpoint. This prevents duplicate canonical bars after a crash between insert and checkpoint update, catches older rows written under a wrong UTC mapping, and preserves conflicting canonical versions for audit. Raw audit rows remain append-only.

## Crash Or Abort Recovery

If the first backfill is interrupted by a crash, reboot, terminal close, MT5 disconnect, or ClickHouse outage, do not drop tables and do not manually edit `ingest_state`.

Safe recovery sequence:

```bash
.build/release/mt5research migrate --config-dir Config --migrations-dir Migrations
.build/release/mt5research verify --config-dir Config --random-ranges 0
.build/release/mt5research backfill --config-dir Config --symbols all
.build/release/mt5research verify --config-dir Config --random-ranges 20
.build/release/mt5research supervise --config-dir Config
```

Backfill is designed to be rerun. A checkpoint is advanced only after raw insert, canonical range replacement, canonical insert, and canonical readback verification all succeed. If the process crashes before that point, rerunning backfill starts again from the last verified checkpoint. Canonical rows for the retried range are deleted and reinserted by both MT5 server-time range and UTC identity range, so duplicate canonical bars should not accumulate.

Before oldest/latest discovery for each symbol, backfill asks the EA for MT5 M1 history status and waits up to 60 seconds for synchronization. If MT5 has not synchronized local history, backfill stops for that symbol instead of snapshotting a partial oldest/latest range.

If MT5 exposes older historical bars after the first partial run, backfill now detects that the newly discovered oldest MT5 bar is earlier than the stored checkpoint oldest and reprocesses from the new oldest bar. This is conservative but prevents silent holes at the beginning of history.

If the checkpoint references a different configured MT5 symbol, or the checkpoint is newer than MT5's latest closed bar, ingestion stops for that symbol. Treat that as a broker/source identity problem and inspect config, MT5 account/server, and `broker_time_offsets` before continuing.

Raw audit rows are append-only. A crash/retry may leave repeated raw audit attempts with the same deterministic `batch_id`, but canonical backtesting data is rewritten and verified before the checkpoint moves.

## MT5 EA Setup

Keep `EA/HistoryBridgeEA.mq5` under the MT5 `MQL5/Experts` tree and let `startcheck` compile it through MetaEditor:

```bash
.build/release/mt5research startcheck --config-dir Config --migrations-dir Migrations --skip-bridge
```

If your package is outside the MT5 Experts tree, copy `EA/HistoryBridgeEA.mq5` into `MQL5/Experts` first or set `MT5RESEARCH_METAEDITOR`, `MT5RESEARCH_WINE`, and `MT5RESEARCH_WINEPREFIX` so the terminal compile check can find the MT5 toolchain.

Attach it to a chart and enable socket/network permissions required by your MT5/Wine setup. Configure:

- `SwiftHost = 127.0.0.1`
- `SwiftPort = 5055`

Then start a Swift command that listens for the EA.

## CLI Commands

```bash
swift run mt5research migrate
swift run mt5research bridge-check
swift run mt5research symbol-check
swift run mt5research backfill --symbols all
swift run mt5research backfill --symbols EURUSD,USDJPY
swift run mt5research live
swift run mt5research supervise
swift run mt5research supervise --with-backfill
swift run mt5research supervise --supervisor-cycles 1
swift run mt5research startcheck
swift run mt5research -startcheck
swift run mt5research verify
swift run mt5research verify --random-ranges 20
swift run mt5research repair --symbol EURUSD --from 2020-01-01 --to 2020-02-01
swift run mt5research export-cache --symbol EURUSD --from 2020-01-01 --to 2025-01-01
swift run mt5research backtest --config Config/backtest.json
swift run mt5research optimize --config Config/optimize.json
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

`symbol-check` returns a non-zero validation exit code if any configured symbol is missing, not selected, or has different digits than `Config/symbols.json`.

`verify --random-ranges 0` runs DB-only duplicate/OHLC/UTC-confidence checks without opening the MT5 bridge. If random ranges are requested, the command connects to the EA and fails the command if any MT5 cross-check mismatches canonical data.

`repair --from` and `--to` are UTC dates in `YYYY-MM-DD` format. Repair first verifies the requested range against MT5, repairs canonical rows only when the mismatch is unambiguous and UTC mapping is verified, writes `repair_log`, and then verifies the range again. Raw audit rows are never deleted.

## Production Supervisor Agents

`supervise` runs ten operational agents through one sequential supervisor. The supervisor owns the single MT5 bridge connection, uses a per-broker lock so two supervisors cannot run against the same broker source, records agent events in ClickHouse, and keeps retryable failures from advancing ingestion checkpoints.

The agents are:

1. `history_importer` - optional first-run/resume backfill, enabled with `--with-backfill` or `supervisor.run_backfill_on_start`.
2. `live_m1_updater` - 10 second closed-M1 ingestion loop.
3. `database_verifier_repairer` - DB checks plus MT5 random range cross-checks and canonical-only repair when unambiguous.
4. `utc_time_authority` - verifies live broker server offset against DB-backed verified offset segments for the exact terminal identity.
5. `health_monitor` - checks ClickHouse and MT5 bridge reachability.
6. `supervisor_coordinator` - confirms single bridge ownership and sequential MT5 access are active.
7. `symbol_metadata_drift` - checks configured MT5 symbols and digit metadata for drift.
8. `checkpoint_gap_auditor` - checks checkpoint/canonical consistency and warns when live ingestion falls behind MT5.
9. `backup_readiness` - verifies canonical data exists for backup/export workflows.
10. `alerting` - summarizes recent supervisor warnings/errors from `runtime_agent_events`.

Supervisor intervals are configured under `supervisor` in `Config/app.json`. The default production stance is to leave backfill disabled in the supervisor and run it deliberately, then use `supervise` for ongoing operation.

## Current Implementation Status

Implemented:

- Swift Package Manager project.
- Strong domain types.
- ANSI terminal logger with `NO_COLOR` support.
- JSON config loading.
- DB-backed verified broker offset authority and explicit UTC conversion.
- M1 OHLC validation.
- Deterministic framed JSON protocol.
- TCP socket transport in Swift with separate connect/accept timeout and request read/write timeout.
- ClickHouse HTTP client and migrations.
- Backfill/live update agents with MT5 history synchronization checks, conflict recording, checkpoint-after-canonical-readback flow, and canonical range replacement.
- Production supervisor with ten operational agents and runtime event/state tables.
- `startcheck` go-live gate with ClickHouse checks, MetaEditor EA compile, MT5 terminal identity validation, verified broker UTC offset coverage, and `GET_RATES_FROM_POSITION` smoke testing.
- Startup verifier checks plus random historical MT5-vs-ClickHouse range comparison.
- Canonical-only repair command with verify -> repair -> reverify flow.
- CPU backtest scaffold using columnar arrays.
- Optional Metal availability scaffold.
- MQL5 EA bridge skeleton.
- XCTest coverage for critical domain/protocol/validation/time/checkpoint/backtest logic.

Still intentionally scaffolded:

- Export-cache command.
- Strategy loading and real EA-clone backtest logic.
- Metal compute pipeline execution.

Those TODOs are isolated and actionable; they are not hidden in hot-path validation.

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
- Never make GPU results authoritative without CPU verification.
