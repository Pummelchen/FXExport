# FXExport

FXExport contains `MT5Research`, a Swift Package Manager terminal project for moving MetaTrader 5 M1 OHLC history into ClickHouse with defensive validation, audit storage, verification/repair scaffolding, and future CPU/GPU backtesting support.

Start here:

- [MT5Research README](MT5Research/README.md)
- Swift package: `MT5Research/Package.swift`
- MT5 bridge EA: `MT5Research/EA/HistoryBridgeEA.mq5`
- ClickHouse migrations: `MT5Research/Migrations/`

Validation commands:

```bash
cd MT5Research
swift test
swift build -c release
```

Git safety:

- Local runtime configs under `MT5Research/Config/` stay ignored and are excluded from source archives.
- The repository hooks block pushes if tracked content contains the local ClickHouse password or personal home-directory paths.
- Use relative paths in tracked docs, configs, and release notes.
