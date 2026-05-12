import ClickHouse
import Config
import Domain
import Foundation
import FXBacktestAPI
import Ingestion
import MT5Bridge

public actor FXExportBacktestExecutionService: FXBacktestExecutionProviding {
    private let config: ConfigBundle
    private let clickHouse: ClickHouseClientProtocol
    private let bridgeConnector: @Sendable () throws -> MT5BridgeClient

    public init(
        config: ConfigBundle,
        clickHouse: ClickHouseClientProtocol,
        bridgeConnector: @escaping @Sendable () throws -> MT5BridgeClient
    ) {
        self.config = config
        self.clickHouse = clickHouse
        self.bridgeConnector = bridgeConnector
    }

    public func loadExecutionSpec(_ request: FXBacktestExecutionSpecRequest) async throws -> FXBacktestExecutionSpecResponse {
        try request.validate()
        let brokerSourceId: BrokerSourceId
        do {
            brokerSourceId = try BrokerSourceId(request.brokerSourceId)
        } catch {
            throw FXBacktestAPIServiceError.invalidRequest(String(describing: error))
        }

        guard config.brokerTime.isAutomatic || brokerSourceId == config.brokerTime.brokerSourceId else {
            throw FXBacktestAPIServiceError.brokerMismatch(
                expected: config.brokerTime.brokerSourceId.rawValue,
                actual: brokerSourceId.rawValue
            )
        }

        let bridge: MT5BridgeClient
        do {
            bridge = try bridgeConnector()
        } catch {
            throw FXBacktestAPIServiceError.executionUnavailable("MT5 bridge connection failed: \(error)")
        }
        defer { bridge.close() }

        let terminal: TerminalInfoDTO
        do {
            terminal = try bridge.terminalInfo()
        } catch {
            throw FXBacktestAPIServiceError.executionUnavailable("Could not query MT5 account metadata: \(error)")
        }

        let identity: BrokerServerIdentity
        do {
            identity = try terminal.brokerServerIdentity()
        } catch {
            throw FXBacktestAPIServiceError.executionUnavailable("MT5 terminal identity is invalid: \(error)")
        }

        if config.brokerTime.isAutomatic {
            let activeIds: [BrokerSourceId]
            do {
                activeIds = try await BrokerSourceRegistry(
                    client: clickHouse,
                    database: config.clickHouse.database
                ).knownActiveBrokerSources(for: identity)
            } catch {
                throw FXBacktestAPIServiceError.executionUnavailable("Could not verify MT5 broker source identity in ClickHouse: \(error)")
            }
            guard activeIds.contains(brokerSourceId) else {
                let expected = activeIds.map(\.rawValue).sorted().joined(separator: ",")
                throw FXBacktestAPIServiceError.brokerMismatch(
                    expected: expected.isEmpty ? "registered broker source for \(identity)" : expected,
                    actual: brokerSourceId.rawValue
                )
            }
        }

        guard let accountCurrency = terminal.accountCurrency, !accountCurrency.isEmpty,
              let leverage = terminal.accountLeverage, leverage > 0 else {
            throw FXBacktestAPIServiceError.executionUnavailable("FXExport EA must be recompiled; terminal account currency/leverage fields are missing.")
        }

        var symbolSpecs: [FXBacktestExecutionSymbolSpec] = []
        symbolSpecs.reserveCapacity(request.symbols.count)
        for symbolRequest in request.symbols {
            let logicalSymbol: LogicalSymbol
            do {
                logicalSymbol = try LogicalSymbol(symbolRequest.logicalSymbol)
            } catch {
                throw FXBacktestAPIServiceError.invalidRequest(String(describing: error))
            }
            guard let mapping = config.symbols.mapping(for: logicalSymbol) else {
                throw FXBacktestAPIServiceError.unconfiguredSymbol(logicalSymbol.rawValue)
            }
            if let expectedMT5Symbol = symbolRequest.expectedMT5Symbol, expectedMT5Symbol != mapping.mt5Symbol.rawValue {
                throw FXBacktestAPIServiceError.mt5SymbolMismatch(
                    expected: mapping.mt5Symbol.rawValue,
                    actual: expectedMT5Symbol
                )
            }
            if let expectedDigits = symbolRequest.expectedDigits, expectedDigits != mapping.digits.rawValue {
                throw FXBacktestAPIServiceError.digitsMismatch(
                    expected: mapping.digits.rawValue,
                    actual: expectedDigits
                )
            }

            let info: SymbolInfoDTO
            do {
                info = try bridge.prepareSymbol(mapping.mt5Symbol)
            } catch {
                throw FXBacktestAPIServiceError.executionUnavailable("\(logicalSymbol.rawValue): MT5 symbol metadata request failed: \(error)")
            }
            let spec = try makeSymbolSpec(
                logicalSymbol: logicalSymbol.rawValue,
                mappingMT5Symbol: mapping.mt5Symbol.rawValue,
                mappingDigits: mapping.digits.rawValue,
                info: info
            )
            symbolSpecs.append(spec)
        }

        let response = FXBacktestExecutionSpecResponse(
            brokerSourceId: brokerSourceId.rawValue,
            capturedAtUtc: Int64(Date().timeIntervalSince1970),
            accountCurrency: accountCurrency,
            accountLeverage: Double(leverage),
            accountMode: "hedging",
            mt5AccountMarginMode: terminal.accountMarginMode,
            symbols: symbolSpecs
        )
        try response.validate()
        return response
    }

    private func makeSymbolSpec(
        logicalSymbol: String,
        mappingMT5Symbol: String,
        mappingDigits: Int,
        info: SymbolInfoDTO
    ) throws -> FXBacktestExecutionSymbolSpec {
        guard info.mt5Symbol == mappingMT5Symbol else {
            throw FXBacktestAPIServiceError.mt5SymbolMismatch(expected: mappingMT5Symbol, actual: info.mt5Symbol)
        }
        guard info.digits == mappingDigits else {
            throw FXBacktestAPIServiceError.digitsMismatch(expected: mappingDigits, actual: info.digits)
        }
        guard let bid = info.bid,
              let ask = info.ask,
              let point = info.point,
              let spread = info.spread,
              let spreadFloat = info.spreadFloat,
              let contractSize = info.contractSize,
              let volumeMin = info.volumeMin,
              let volumeStep = info.volumeStep,
              let volumeMax = info.volumeMax,
              let swapLong = info.swapLong,
              let swapShort = info.swapShort,
              let swapMode = info.swapMode,
              let marginCalcLots = info.marginCalcLots,
              let tradeCalcMode = info.tradeCalcMode,
              let tradeMode = info.tradeMode,
              let tickSize = info.tickSize,
              let tickValue = info.tickValue else {
            throw FXBacktestAPIServiceError.executionUnavailable("\(logicalSymbol): FXExport EA must be recompiled; extended symbol execution fields are missing.")
        }

        let marginBuyPerLot = Self.perLot(info.marginBuy, lots: marginCalcLots)
        let marginSellPerLot = Self.perLot(info.marginSell, lots: marginCalcLots)
        let marginInitialPerLot = Self.firstPositive([
            info.marginInitial,
            marginBuyPerLot,
            marginSellPerLot
        ])

        return FXBacktestExecutionSymbolSpec(
            logicalSymbol: logicalSymbol,
            mt5Symbol: info.mt5Symbol,
            selected: info.selected,
            digits: info.digits,
            bid: bid,
            ask: ask,
            point: point,
            spreadPoints: spread,
            spreadFloat: spreadFloat,
            contractSize: contractSize,
            volumeMin: volumeMin,
            volumeStep: volumeStep,
            volumeMax: volumeMax,
            swapLongPerLot: swapLong,
            swapShortPerLot: swapShort,
            swapMode: swapMode,
            marginInitialPerLot: marginInitialPerLot,
            marginMaintenancePerLot: info.marginMaintenance,
            marginBuyPerLot: marginBuyPerLot,
            marginSellPerLot: marginSellPerLot,
            marginCalcLots: marginCalcLots,
            tradeCalcMode: tradeCalcMode,
            tradeMode: tradeMode,
            tickSize: tickSize,
            tickValue: tickValue,
            tickValueProfit: info.tickValueProfit,
            tickValueLoss: info.tickValueLoss,
            commissionPerLotPerSide: nil,
            commissionSource: "not_exposed_by_mt5_symbol_info",
            slippagePoints: 0,
            slippageSource: "deterministic_zero_default"
        )
    }

    private static func perLot(_ value: Double?, lots: Double) -> Double? {
        guard let value, value.isFinite, lots.isFinite, lots > 0 else { return nil }
        return value / lots
    }

    private static func firstPositive(_ values: [Double?]) -> Double? {
        values.compactMap { value -> Double? in
            guard let value, value.isFinite, value > 0 else { return nil }
            return value
        }.first
    }
}
