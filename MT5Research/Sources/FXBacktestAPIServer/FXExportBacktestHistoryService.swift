import BacktestCore
import ClickHouse
import Config
import Domain
import Foundation
import FXBacktestAPI
import Operations

public struct FXExportBacktestHistoryService: FXBacktestHistoryProviding {
    private let config: ConfigBundle
    private let clickHouse: ClickHouseClientProtocol

    public init(config: ConfigBundle, clickHouse: ClickHouseClientProtocol) {
        self.config = config
        self.clickHouse = clickHouse
    }

    public func loadM1History(_ request: FXBacktestM1HistoryRequest) async throws -> FXBacktestM1HistoryResponse {
        try request.validate()
        let brokerSourceId: BrokerSourceId
        let logicalSymbol: LogicalSymbol
        do {
            brokerSourceId = try BrokerSourceId(request.brokerSourceId)
            logicalSymbol = try LogicalSymbol(request.logicalSymbol)
        } catch {
            throw FXBacktestAPIServiceError.invalidRequest(String(describing: error))
        }

        guard brokerSourceId == config.brokerTime.brokerSourceId else {
            throw FXBacktestAPIServiceError.brokerMismatch(
                expected: config.brokerTime.brokerSourceId.rawValue,
                actual: brokerSourceId.rawValue
            )
        }
        guard let mapping = config.symbols.mapping(for: logicalSymbol) else {
            throw FXBacktestAPIServiceError.unconfiguredSymbol(logicalSymbol.rawValue)
        }
        if let expectedMT5Symbol = request.expectedMT5Symbol, expectedMT5Symbol != mapping.mt5Symbol.rawValue {
            throw FXBacktestAPIServiceError.mt5SymbolMismatch(
                expected: mapping.mt5Symbol.rawValue,
                actual: expectedMT5Symbol
            )
        }
        if let expectedDigits = request.expectedDigits, expectedDigits != mapping.digits.rawValue {
            throw FXBacktestAPIServiceError.digitsMismatch(
                expected: mapping.digits.rawValue,
                actual: expectedDigits
            )
        }

        let utcStart = UtcSecond(rawValue: request.utcStartInclusive)
        let utcEnd = UtcSecond(rawValue: request.utcEndExclusive)
        do {
            try await BacktestReadinessGate(config: config, clickHouse: clickHouse).assertReady(BacktestReadinessRequest(
                brokerSourceId: brokerSourceId,
                logicalSymbol: logicalSymbol,
                utcStart: utcStart,
                utcEndExclusive: utcEnd
            ))
        } catch {
            throw FXBacktestAPIServiceError.readinessBlocked(String(describing: error))
        }

        do {
            let series = try await ClickHouseHistoricalOhlcDataProvider(
                client: clickHouse,
                database: config.clickHouse.database
            ).loadM1Ohlc(HistoricalOhlcRequest(
                brokerSourceId: brokerSourceId,
                logicalSymbol: logicalSymbol,
                utcStartInclusive: utcStart,
                utcEndExclusive: utcEnd,
                expectedMT5Symbol: mapping.mt5Symbol,
                expectedDigits: mapping.digits,
                maximumRows: request.maximumRows
            ))
            let response = FXBacktestM1HistoryResponse(
                metadata: FXBacktestM1HistoryMetadata(
                    brokerSourceId: series.metadata.brokerSourceId.rawValue,
                    logicalSymbol: series.metadata.logicalSymbol.rawValue,
                    mt5Symbol: mapping.mt5Symbol.rawValue,
                    timeframe: series.metadata.timeframe.rawValue,
                    digits: series.metadata.digits.rawValue,
                    requestedUtcStart: request.utcStartInclusive,
                    requestedUtcEndExclusive: request.utcEndExclusive,
                    firstUtc: series.metadata.firstUtc?.rawValue,
                    lastUtc: series.metadata.lastUtc?.rawValue,
                    rowCount: series.count
                ),
                utcTimestamps: series.utcTimestamps,
                open: series.open,
                high: series.high,
                low: series.low,
                close: series.close
            )
            try response.validate()
            return response
        } catch let error as FXBacktestAPIServiceError {
            throw error
        } catch {
            throw FXBacktestAPIServiceError.historyUnavailable(String(describing: error))
        }
    }
}
