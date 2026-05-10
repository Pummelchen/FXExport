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
