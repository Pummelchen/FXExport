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
    case selectedSymbolNotConfigured(String)
    case digitsMismatch(symbol: String, expected: Int, actual: Int)
    case emptyMT5Range(String)
    case invalidChunk(String)
    case invalidBridgeResponse(String)
    case canonicalInsertVerificationFailed(String)
    case terminalIdentityMismatch(String)

    public var description: String {
        switch self {
        case .symbolMissing(let symbol):
            return "Configured MT5 symbol '\(symbol)' is missing or cannot be selected."
        case .selectedSymbolNotConfigured(let symbol):
            return "Requested symbol '\(symbol)' is not configured in symbols.json."
        case .digitsMismatch(let symbol, let expected, let actual):
            return "\(symbol) digits mismatch. Config expected \(expected), MT5 reported \(actual)."
        case .emptyMT5Range(let symbol):
            return "\(symbol) has no closed M1 history available in MT5."
        case .invalidChunk(let reason):
            return "Invalid ingest chunk: \(reason)"
        case .invalidBridgeResponse(let reason):
            return "Invalid MT5 bridge response: \(reason)"
        case .canonicalInsertVerificationFailed(let reason):
            return "Canonical insert verification failed: \(reason)"
        case .terminalIdentityMismatch(let reason):
            return "MT5 terminal identity mismatch: \(reason)"
        }
    }
}

public struct BackfillAgent: Sendable {
    private let config: ConfigBundle
    private let bridge: MT5BridgeClient
    private let clickHouse: ClickHouseClientProtocol
    private let checkpointStore: CheckpointStore
    private let offsetStore: BrokerOffsetStore
    private let logger: Logger

    public init(
        config: ConfigBundle,
        bridge: MT5BridgeClient,
        clickHouse: ClickHouseClientProtocol,
        checkpointStore: CheckpointStore,
        offsetStore: BrokerOffsetStore,
        logger: Logger
    ) {
        self.config = config
        self.bridge = bridge
        self.clickHouse = clickHouse
        self.checkpointStore = checkpointStore
        self.offsetStore = offsetStore
        self.logger = logger
    }

    public func run(selectedSymbols: [LogicalSymbol]?) async throws {
        let terminalIdentity = try loadTerminalIdentity()
        let selected = selectedSymbols.map(Set.init)
        if let selectedSymbols {
            let configured = Set(config.symbols.symbols.map(\.logicalSymbol))
            if let missing = selectedSymbols.first(where: { !configured.contains($0) }) {
                throw IngestError.selectedSymbolNotConfigured(missing.rawValue)
            }
        }
        let mappings = config.symbols.symbols.filter { mapping in
            selected?.contains(mapping.logicalSymbol) ?? true
        }
        let offsetMap = try await offsetStore.loadVerifiedOffsetMap(
            brokerSourceId: config.brokerTime.brokerSourceId,
            terminalIdentity: terminalIdentity
        )
        logger.ok("Loaded \(offsetMap.segments.count) verified broker UTC offset segment(s) from ClickHouse for \(terminalIdentity)")
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
        let oldestResponse = try bridge.oldestM1BarTime(mapping.mt5Symbol)
        try validateSingleTimeResponse(oldestResponse, expectedMT5Symbol: mapping.mt5Symbol)
        let latestClosedResponse = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
        try validateSingleTimeResponse(latestClosedResponse, expectedMT5Symbol: mapping.mt5Symbol)
        let oldest = MT5ServerSecond(rawValue: oldestResponse.mt5ServerTime)
        let latestClosed = MT5ServerSecond(rawValue: latestClosedResponse.mt5ServerTime)
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
            try validateRatesResponse(response, expectedMT5Symbol: mapping.mt5Symbol)
            let closedBars = try response.rates.map {
                try $0.toClosedM1Bar(logicalSymbol: mapping.logicalSymbol, mt5Symbol: mapping.mt5Symbol, digits: mapping.digits)
            }
            guard !closedBars.isEmpty else {
                logger.warn("\(mapping.logicalSymbol.rawValue): no MT5 bars in server-time range \(range.from.rawValue)..<\(range.toExclusive.rawValue); advancing over source gap")
                cursor = range.toExclusive
                continue
            }

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
        _ = try await clickHouse.execute(try insertBuilder.canonicalRangeDelete(bars))
        _ = try await clickHouse.execute(try insertBuilder.canonicalBarsInsert(bars))
        try await CanonicalInsertVerifier(clickHouse: clickHouse, insertBuilder: insertBuilder).verify(bars)
    }

    private func validateSingleTimeResponse(_ response: SingleTimeResponseDTO, expectedMT5Symbol: MT5Symbol) throws {
        guard response.mt5Symbol == expectedMT5Symbol.rawValue else {
            throw IngestError.invalidBridgeResponse("expected \(expectedMT5Symbol.rawValue), got \(response.mt5Symbol)")
        }
        guard MT5ServerSecond(rawValue: response.mt5ServerTime).isMinuteAligned else {
            throw IngestError.invalidBridgeResponse("MT5 timestamp \(response.mt5ServerTime) is not minute-aligned")
        }
    }

    private func validateRatesResponse(_ response: RatesResponseDTO, expectedMT5Symbol: MT5Symbol) throws {
        guard response.mt5Symbol == expectedMT5Symbol.rawValue else {
            throw IngestError.invalidBridgeResponse("expected rates for \(expectedMT5Symbol.rawValue), got \(response.mt5Symbol)")
        }
        guard response.timeframe == Timeframe.m1.rawValue else {
            throw IngestError.invalidBridgeResponse("expected M1 rates, got \(response.timeframe)")
        }
    }

    private func loadTerminalIdentity() throws -> BrokerServerIdentity {
        let actual = try bridge.terminalInfo()
        let identity = try actual.brokerServerIdentity()
        guard let expected = config.brokerTime.expectedTerminalIdentity, !expected.isEmpty else {
            logger.warn("No expected MT5 terminal identity configured for broker_source_id \(config.brokerTime.brokerSourceId.rawValue); using actual terminal identity \(identity) for DB-backed offset lookup")
            return identity
        }
        if let company = expected.company, company != actual.company {
            throw IngestError.terminalIdentityMismatch("expected company '\(company)', got '\(actual.company)'")
        }
        if let server = expected.server, server != actual.server {
            throw IngestError.terminalIdentityMismatch("expected server '\(server)', got '\(actual.server)'")
        }
        if let accountLogin = expected.accountLogin, accountLogin != actual.accountLogin {
            throw IngestError.terminalIdentityMismatch("expected account \(accountLogin), got \(actual.accountLogin)")
        }
        logger.ok("MT5 terminal identity verified for broker_source_id \(config.brokerTime.brokerSourceId.rawValue): \(identity)")
        return identity
    }
}
