import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import MT5Bridge
import TimeMapping
import Validation

public struct LiveUpdateAgent: Sendable {
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

    public func runForever() async throws {
        logger.info("Starting live updater; scan interval \(config.app.liveScanIntervalSeconds)s")
        let terminalIdentity = try loadTerminalIdentity()
        while !Task.isCancelled {
            do {
                try await runOnce(terminalIdentity: terminalIdentity)
            } catch {
                logger.warn("Live update cycle failed safely: \(error)")
            }
            do {
                try await Task.sleep(nanoseconds: UInt64(config.app.liveScanIntervalSeconds) * 1_000_000_000)
            } catch {
                return
            }
        }
    }

    public func runOnce() async throws {
        try await runOnce(terminalIdentity: loadTerminalIdentity())
    }

    private func runOnce(terminalIdentity: BrokerServerIdentity) async throws {
        let offsetMap = try await offsetStore.loadVerifiedOffsetMap(
            brokerSourceId: config.brokerTime.brokerSourceId,
            terminalIdentity: terminalIdentity
        )
        try BrokerOffsetRuntimeVerifier().verify(
            snapshot: bridge.serverTimeSnapshot(),
            offsetMap: offsetMap,
            acceptedLiveOffsetSeconds: config.brokerTime.acceptedLiveOffsetSeconds,
            logger: logger
        )
        let validator = OhlcValidator(timeConverter: TimeConverter(offsetMap: offsetMap))
        let insertBuilder = ClickHouseInsertBuilder(database: config.clickHouse.database)
        var failureCount = 0
        for mapping in config.symbols.symbols {
            do {
                try await update(mapping: mapping, validator: validator, insertBuilder: insertBuilder)
            } catch let error as MT5BridgeError {
                throw error
            } catch let error as ProtocolError {
                throw error
            } catch let error as ClickHouseError {
                throw error
            } catch {
                failureCount += 1
                logger.warn("\(mapping.logicalSymbol.rawValue): live update skipped safely: \(error)")
            }
        }
        if failureCount == config.symbols.symbols.count {
            throw IngestError.invalidChunk("all configured symbols failed during the live update cycle")
        }
    }

    private func update(mapping: SymbolMapping, validator: OhlcValidator, insertBuilder: ClickHouseInsertBuilder) async throws {
        let latestResponse = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
        guard latestResponse.mt5Symbol == mapping.mt5Symbol.rawValue else {
            throw IngestError.invalidBridgeResponse("expected \(mapping.mt5Symbol.rawValue), got \(latestResponse.mt5Symbol)")
        }
        let latestClosed = MT5ServerSecond(rawValue: latestResponse.mt5ServerTime)
        guard latestClosed.isMinuteAligned else {
            throw IngestError.invalidBridgeResponse("latest closed M1 timestamp \(latestResponse.mt5ServerTime) is not minute-aligned")
        }
        guard let state = try await checkpointStore.latestState(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol
        ) else {
            logger.warn("\(mapping.logicalSymbol.rawValue) - no checkpoint exists; run historical import before live M1 updates")
            return
        }
        guard state.mt5Symbol == mapping.mt5Symbol else {
            throw IngestError.checkpointSymbolMismatch(
                logicalSymbol: mapping.logicalSymbol.rawValue,
                expected: mapping.mt5Symbol.rawValue,
                actual: state.mt5Symbol.rawValue
            )
        }

        guard latestClosed.rawValue >= state.latestIngestedClosedMT5ServerTime.rawValue else {
            throw IngestError.checkpointAheadOfMT5(
                logicalSymbol: mapping.logicalSymbol.rawValue,
                checkpoint: state.latestIngestedClosedMT5ServerTime.rawValue,
                latestClosed: latestClosed.rawValue
            )
        }
        guard latestClosed.rawValue > state.latestIngestedClosedMT5ServerTime.rawValue else {
            logger.debug("\(mapping.logicalSymbol.rawValue) - no new closed M1 bar; checkpoint is current at \(state.latestIngestedClosedMT5ServerTime.rawValue) server time")
            return
        }

        let from = try addOneMinute(to: state.latestIngestedClosedMT5ServerTime)
        let toExclusive = try addOneMinute(to: latestClosed)
        let rangeLabel = OperatorStatusText.monthRangeLabel(start: from, endExclusive: toExclusive)
        let batchId = BatchId.deterministic(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol,
            start: from,
            end: toExclusive
        )
        logger.info("\(mapping.logicalSymbol.rawValue) - pulling closed M1 OHLC for \(rangeLabel)")
        let response = try bridge.ratesRange(mt5Symbol: mapping.mt5Symbol, from: from, toExclusive: toExclusive, maxBars: config.app.chunkSize)
        guard response.mt5Symbol == mapping.mt5Symbol.rawValue else {
            throw IngestError.invalidBridgeResponse("expected rates for \(mapping.mt5Symbol.rawValue), got \(response.mt5Symbol)")
        }
        guard response.timeframe == Timeframe.m1.rawValue else {
            throw IngestError.invalidBridgeResponse("expected M1 rates, got \(response.timeframe)")
        }
        let closedBars = try response.rates.map {
            try $0.toClosedM1Bar(logicalSymbol: mapping.logicalSymbol, mt5Symbol: mapping.mt5Symbol, digits: mapping.digits)
        }
        try validateClosedBarsInRange(closedBars, from: from, toExclusive: toExclusive)
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
        logger.info("\(mapping.logicalSymbol.rawValue) - validating \(rangeLabel) for OHLC integrity and verified UTC conversion")
        let validated = try validator.validateBatch(closedBars, context: context)
        guard !validated.isEmpty else {
            logger.warn("\(mapping.logicalSymbol.rawValue) - MT5 returned no closed M1 bars for \(rangeLabel); checkpoint was not advanced")
            return
        }
        let rawInsert = insertBuilder.rawBarsInsert(validated)
        let canonicalDelete = try insertBuilder.canonicalRangeDelete(validated)
        let canonicalInsert = try insertBuilder.canonicalBarsInsert(validated)
        try await CanonicalConflictRecorder(clickHouse: clickHouse, insertBuilder: insertBuilder)
            .recordConflictsBeforeCanonicalReplace(validated, detectedAtUtc: now)
        _ = try await clickHouse.execute(rawInsert)
        _ = try await clickHouse.execute(canonicalDelete)
        _ = try await clickHouse.execute(canonicalInsert)
        try await CanonicalInsertVerifier(clickHouse: clickHouse, insertBuilder: insertBuilder).verify(validated)
        guard let first = validated.first, let last = validated.last else { return }
        try await checkpointStore.save(IngestState(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol,
            mt5Symbol: mapping.mt5Symbol,
            oldestMT5ServerTime: state.oldestMT5ServerTime,
            latestIngestedClosedMT5ServerTime: last.mt5ServerTime,
            latestIngestedClosedUtcTime: last.utcTime,
            status: .live,
            lastBatchId: batchId,
            updatedAtUtc: now
        ))
        let verifiedRangeLabel = OperatorStatusText.monthRangeLabel(
            start: first.utcTime,
            endExclusive: try addOneMinute(to: last.utcTime)
        )
        logger.ok("\(mapping.logicalSymbol.rawValue) - \(verifiedRangeLabel) pulled, verified, UTC correct and canonical data clean (\(validated.count) new closed M1 bars)")
    }

    private func validateClosedBarsInRange(_ bars: [ClosedM1Bar], from: MT5ServerSecond, toExclusive: MT5ServerSecond) throws {
        for bar in bars {
            guard bar.mt5ServerTime.rawValue >= from.rawValue,
                  bar.mt5ServerTime.rawValue < toExclusive.rawValue else {
                throw IngestError.invalidBridgeResponse("MT5 bar \(bar.mt5ServerTime.rawValue) is outside requested range \(from.rawValue)..<\(toExclusive.rawValue)")
            }
        }
    }

    private func loadTerminalIdentity() throws -> BrokerServerIdentity {
        let actual = try bridge.terminalInfo()
        do {
            return try TerminalIdentityPolicy().resolve(
                actual: actual,
                brokerSourceId: config.brokerTime.brokerSourceId,
                expected: config.brokerTime.expectedTerminalIdentity,
                logger: logger
            )
        } catch let error as TerminalIdentityPolicyError {
            throw IngestError.terminalIdentityMismatch(error.description)
        }
    }

    private func addOneMinute(to value: MT5ServerSecond) throws -> MT5ServerSecond {
        let result = value.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw IngestError.invalidChunk("MT5 server timestamp overflow while advancing one M1 bar")
        }
        return MT5ServerSecond(rawValue: result.partialValue)
    }

    private func addOneMinute(to value: UtcSecond) throws -> UtcSecond {
        let result = value.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw IngestError.invalidChunk("UTC timestamp overflow while advancing one M1 bar")
        }
        return UtcSecond(rawValue: result.partialValue)
    }
}
