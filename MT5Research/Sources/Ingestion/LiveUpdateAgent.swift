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

    public func runForever() async {
        logger.info("Starting live updater; scan interval \(config.app.liveScanIntervalSeconds)s")
        while !Task.isCancelled {
            do {
                try await runOnce()
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
        let offsetMap = BrokerOffsetMap(config: config.brokerTime)
        let validator = OhlcValidator(timeConverter: TimeConverter(offsetMap: offsetMap))
        let insertBuilder = ClickHouseInsertBuilder(database: config.clickHouse.database)
        for mapping in config.symbols.symbols {
            try await update(mapping: mapping, validator: validator, insertBuilder: insertBuilder)
        }
    }

    private func update(mapping: SymbolMapping, validator: OhlcValidator, insertBuilder: ClickHouseInsertBuilder) async throws {
        let latestClosed = MT5ServerSecond(rawValue: try bridge.latestClosedM1Bar(mapping.mt5Symbol).mt5ServerTime)
        guard let state = try await checkpointStore.latestState(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol
        ) else {
            logger.warn("\(mapping.logicalSymbol.rawValue): no checkpoint exists; run backfill first")
            return
        }

        guard latestClosed.rawValue > state.latestIngestedClosedMT5ServerTime.rawValue else { return }

        let from = MT5ServerSecond(rawValue: state.latestIngestedClosedMT5ServerTime.rawValue + Timeframe.m1.seconds)
        let toExclusive = MT5ServerSecond(rawValue: latestClosed.rawValue + Timeframe.m1.seconds)
        let batchId = BatchId.deterministic(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol,
            start: from,
            end: toExclusive
        )
        let response = try bridge.ratesRange(mt5Symbol: mapping.mt5Symbol, from: from, toExclusive: toExclusive, maxBars: config.app.chunkSize)
        let closedBars = try response.rates.map {
            try $0.toClosedM1Bar(logicalSymbol: mapping.logicalSymbol, mt5Symbol: mapping.mt5Symbol, digits: mapping.digits)
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
        guard !validated.isEmpty else { return }
        _ = try await clickHouse.execute(insertBuilder.rawBarsInsert(validated))
        _ = try await clickHouse.execute(insertBuilder.canonicalBarsInsert(validated))
        guard let last = validated.last else { return }
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
        logger.ok("\(mapping.logicalSymbol.rawValue): live update inserted \(validated.count) closed M1 bars")
    }
}
