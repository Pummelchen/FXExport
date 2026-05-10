import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import MT5Bridge
import TimeMapping
import Validation

public enum IngestError: Error, CustomStringConvertible, Sendable {
    case symbolMissing(String)
    case digitsMismatch(symbol: String, expected: Int, actual: Int)
    case emptyMT5Range(String)
    case invalidChunk(String)

    public var description: String {
        switch self {
        case .symbolMissing(let symbol):
            return "Configured MT5 symbol '\(symbol)' is missing or cannot be selected."
        case .digitsMismatch(let symbol, let expected, let actual):
            return "\(symbol) digits mismatch. Config expected \(expected), MT5 reported \(actual)."
        case .emptyMT5Range(let symbol):
            return "\(symbol) has no closed M1 history available in MT5."
        case .invalidChunk(let reason):
            return "Invalid ingest chunk: \(reason)"
        }
    }
}

public struct BackfillAgent: Sendable {
    private let config: ConfigBundle
    private let bridge: MT5BridgeClient
    private let clickHouse: ClickHouseClientProtocol
    private let checkpointStore: CheckpointStore
    private let logger: Logger

    public init(
        config: ConfigBundle,
        bridge: MT5BridgeClient,
        clickHouse: ClickHouseClientProtocol,
        checkpointStore: CheckpointStore,
        logger: Logger
    ) {
        self.config = config
        self.bridge = bridge
        self.clickHouse = clickHouse
        self.checkpointStore = checkpointStore
        self.logger = logger
    }

    public func run(selectedSymbols: [LogicalSymbol]?) async throws {
        let selected = selectedSymbols.map(Set.init)
        let mappings = config.symbols.symbols.filter { mapping in
            selected?.contains(mapping.logicalSymbol) ?? true
        }
        let offsetMap = BrokerOffsetMap(config: config.brokerTime)
        let validator = OhlcValidator(timeConverter: TimeConverter(offsetMap: offsetMap))
        let insertBuilder = ClickHouseInsertBuilder(database: config.clickHouse.database)
        let batchBuilder = BatchBuilder(chunkSize: config.app.chunkSize)

        for mapping in mappings {
            do {
                try await backfill(
                    mapping: mapping,
                    validator: validator,
                    insertBuilder: insertBuilder,
                    batchBuilder: batchBuilder
                )
            } catch {
                logger.error("\(mapping.logicalSymbol.rawValue): \(error)")
                if config.app.strictSymbolFailures { throw error }
            }
        }
    }

    private func backfill(
        mapping: SymbolMapping,
        validator: OhlcValidator,
        insertBuilder: ClickHouseInsertBuilder,
        batchBuilder: BatchBuilder
    ) async throws {
        logger.info("\(mapping.logicalSymbol.rawValue): preparing MT5 symbol \(mapping.mt5Symbol.rawValue)")
        let symbolInfo = try bridge.prepareSymbol(mapping.mt5Symbol)
        guard symbolInfo.selected else { throw IngestError.symbolMissing(mapping.mt5Symbol.rawValue) }
        guard symbolInfo.digits == mapping.digits.rawValue else {
            throw IngestError.digitsMismatch(symbol: mapping.mt5Symbol.rawValue, expected: mapping.digits.rawValue, actual: symbolInfo.digits)
        }

        logger.info("\(mapping.logicalSymbol.rawValue): discovering oldest available M1 bar")
        let oldest = MT5ServerSecond(rawValue: try bridge.oldestM1BarTime(mapping.mt5Symbol).mt5ServerTime)
        let latestClosed = MT5ServerSecond(rawValue: try bridge.latestClosedM1Bar(mapping.mt5Symbol).mt5ServerTime)
        guard oldest.rawValue <= latestClosed.rawValue else {
            throw IngestError.emptyMT5Range(mapping.logicalSymbol.rawValue)
        }
        logger.ok("\(mapping.logicalSymbol.rawValue): oldest \(oldest.rawValue), latest closed \(latestClosed.rawValue) server time")

        let existingState = try await checkpointStore.latestState(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol
        )
        var cursor = existingState.map {
            MT5ServerSecond(rawValue: $0.latestIngestedClosedMT5ServerTime.rawValue + Timeframe.m1.seconds)
        } ?? oldest

        while cursor.rawValue <= latestClosed.rawValue {
            let range = batchBuilder.nextRange(start: cursor, endInclusive: latestClosed)
            let batchId = BatchId.deterministic(
                brokerSourceId: config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol,
                start: range.from,
                end: range.toExclusive
            )
            logger.info("\(mapping.logicalSymbol.rawValue): backfilling \(config.app.chunkSize)-bar chunk from \(range.from.rawValue)")

            let response = try bridge.ratesRange(
                mt5Symbol: mapping.mt5Symbol,
                from: range.from,
                toExclusive: range.toExclusive,
                maxBars: config.app.chunkSize
            )
            let closedBars = try response.rates.map {
                try $0.toClosedM1Bar(logicalSymbol: mapping.logicalSymbol, mt5Symbol: mapping.mt5Symbol, digits: mapping.digits)
            }
            guard !closedBars.isEmpty else { break }

            let now = UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))
            let context = OhlcValidationContext(
                brokerSourceId: config.brokerTime.brokerSourceId,
                expectedLogicalSymbol: mapping.logicalSymbol,
                expectedMT5Symbol: mapping.mt5Symbol,
                expectedDigits: mapping.digits,
                latestClosedMT5ServerTime: latestClosed,
                batchId: batchId,
                ingestedAtUtc: now
            )
            let validated = try validator.validateBatch(closedBars, context: context)
            try await insertValidatedBars(validated, insertBuilder: insertBuilder)

            guard let last = validated.last else { throw IngestError.invalidChunk("validated chunk unexpectedly empty") }
            let state = IngestState(
                brokerSourceId: config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol,
                mt5Symbol: mapping.mt5Symbol,
                oldestMT5ServerTime: oldest,
                latestIngestedClosedMT5ServerTime: last.mt5ServerTime,
                latestIngestedClosedUtcTime: last.utcTime,
                status: .backfilling,
                lastBatchId: batchId,
                updatedAtUtc: now
            )
            try await checkpointStore.save(state)
            logger.ok("\(mapping.logicalSymbol.rawValue): inserted \(validated.count) validated M1 bars")
            cursor = MT5ServerSecond(rawValue: last.mt5ServerTime.rawValue + Timeframe.m1.seconds)
        }
    }

    private func insertValidatedBars(_ bars: [ValidatedBar], insertBuilder: ClickHouseInsertBuilder) async throws {
        guard !bars.isEmpty else { return }
        _ = try await clickHouse.execute(insertBuilder.rawBarsInsert(bars))
        _ = try await clickHouse.execute(insertBuilder.canonicalBarsInsert(bars))
    }
}
