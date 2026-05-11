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
   - Runs backfill, live updates, verification scaffolds, repair scaffolds, and backtest scaffolds.

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
  'demo-broker-mt5', 'Broker Ltd', 'Broker-Server', 12345678,
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

If these values are provided, backfill/live commands verify the connected terminal before ingestion. Even when they are omitted, the actual MT5 terminal identity is still used for the DB-backed offset lookup.

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

Raw audit data is append-only. Repairs only target canonical data and must preserve conflicts and repair logs.

Canonical ingestion is replace-by-range for the affected broker/source/symbol range. The delete predicate covers both the raw MT5 server-time range and the converted UTC identity range before reinserting verified rows. After the replacement insert, Swift reads the canonical range back from ClickHouse and verifies row count plus unique MT5 and UTC timestamp counts before advancing the checkpoint. This prevents duplicate canonical bars after a crash between insert and checkpoint update, and also catches older rows written under a wrong UTC mapping. Raw audit rows remain append-only.

## MT5 EA Setup

Copy `EA/HistoryBridgeEA.mq5` into your MT5 `MQL5/Experts` folder and compile it in MetaEditor.

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

## Current Implementation Status

Implemented:

- Swift Package Manager project.
- Strong domain types.
- ANSI terminal logger with `NO_COLOR` support.
- JSON config loading.
- DB-backed verified broker offset authority and explicit UTC conversion.
- M1 OHLC validation.
- Deterministic framed JSON protocol.
- TCP socket transport in Swift.
- ClickHouse HTTP client and migrations.
- Backfill/live update scaffolds with checkpoint-after-canonical-readback flow and canonical range replacement.
- Verifier/repair decision scaffolds.
- CPU backtest scaffold using columnar arrays.
- Optional Metal availability scaffold.
- MQL5 EA bridge skeleton.
- XCTest coverage for critical domain/protocol/validation/time/checkpoint/backtest logic.

Still intentionally scaffolded:

- Full typed ClickHouse historical range readback for random MT5-vs-database verification.
- Full random MT5-vs-ClickHouse repair execution.
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
