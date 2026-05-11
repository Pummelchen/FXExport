import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import Ingestion
import MT5Bridge
import TimeMapping
import Validation

public struct VerificationAgent: Sendable {
    private let config: ConfigBundle
    private let bridge: MT5BridgeClient?
    private let clickHouse: ClickHouseClientProtocol
    private let logger: Logger

    public init(config: ConfigBundle, bridge: MT5BridgeClient?, clickHouse: ClickHouseClientProtocol, logger: Logger) {
        self.config = config
        self.bridge = bridge
        self.clickHouse = clickHouse
        self.logger = logger
    }

    public func startupChecks(randomRanges: Int) async throws {
        guard randomRanges >= 0 else {
            throw VerificationError.invalidRandomRangeCount(randomRanges)
        }
        var integrityIssues: [String] = []

        logger.verify("Running duplicate-key check")
        let duplicateRows = try await clickHouse.execute(.select("""
        SELECT broker_source_id, logical_symbol, ts_utc, count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        GROUP BY broker_source_id, logical_symbol, ts_utc
        HAVING count() > 1
        LIMIT 20
        """))
        if !duplicateRows.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            let issue = "Duplicate canonical keys found:\n\(duplicateRows)"
            integrityIssues.append(issue)
            logger.warn(issue)
        }

        logger.verify("Running OHLC invariant check")
        let invariantCount = try await clickHouse.execute(.select("""
        SELECT count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        WHERE open_scaled <= 0 OR high_scaled < open_scaled OR high_scaled < close_scaled OR high_scaled < low_scaled OR low_scaled > open_scaled OR low_scaled > close_scaled
        """))
        if invariantCount.trimmingCharacters(in: .whitespacesAndNewlines) != "0" {
            let issue = "OHLC invariant violations found: \(invariantCount.trimmingCharacters(in: .whitespacesAndNewlines))"
            integrityIssues.append(issue)
            logger.warn(issue)
        }

        logger.verify("Running canonical UTC offset confidence check")
        let unverifiedCount = try await clickHouse.execute(.select("""
        SELECT count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        WHERE offset_confidence != 'verified'
        """))
        if unverifiedCount.trimmingCharacters(in: .whitespacesAndNewlines) != "0" {
            let issue = "Canonical rows with non-verified UTC offsets found: \(unverifiedCount.trimmingCharacters(in: .whitespacesAndNewlines))"
            integrityIssues.append(issue)
            logger.warn(issue)
        }

        guard integrityIssues.isEmpty else {
            throw VerificationError.databaseIntegrityFailed(integrityIssues)
        }

        guard randomRanges > 0 else {
            logger.verify("Random historical cross-check disabled for this run")
            return
        }
        guard let bridge else {
            logger.warn("Random MT5 cross-check skipped because no MT5 bridge connection is active")
            return
        }
        let terminal = try bridge.terminalInfo()
        let terminalIdentity = try TerminalIdentityPolicy().resolve(
            actual: terminal,
            brokerSourceId: config.brokerTime.brokerSourceId,
            expected: config.brokerTime.expectedTerminalIdentity,
            logger: logger
        )
        let offsetStore = ClickHouseBrokerOffsetStore(client: clickHouse, database: config.clickHouse.database)
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
        let verifier = HistoricalRangeVerifier(
            config: config,
            bridge: bridge,
            clickHouse: clickHouse,
            offsetMap: offsetMap,
            logger: logger
        )
        let checkpointStore = ClickHouseCheckpointStore(
            client: clickHouse,
            insertBuilder: ClickHouseInsertBuilder(database: config.clickHouse.database),
            database: config.clickHouse.database
        )
        var generator = SystemRandomNumberGenerator()
        let selector = RandomRangeSelector()
        for index in 1...randomRanges {
            guard let mapping = config.symbols.symbols.randomElement(using: &generator) else { return }
            guard let state = try await checkpointStore.latestState(
                brokerSourceId: config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol
            ) else {
                logger.warn("\(mapping.logicalSymbol.rawValue): random verification skipped because no checkpoint exists")
                continue
            }
            let range = try selector.selectMonth(
                brokerSourceId: config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol,
                oldest: state.oldestMT5ServerTime,
                latestClosed: state.latestIngestedClosedMT5ServerTime,
                random: &generator
            )
            logger.verify("Random MT5 cross-check \(index)/\(randomRanges): \(range.logicalSymbol.rawValue) \(range.mt5Start.rawValue)..<\(range.mt5EndExclusive.rawValue)")
            let outcome = try await verifier.verify(range: range)
            guard outcome.result.isClean else {
                throw VerificationError.randomCrossCheckFailed(
                    symbol: range.logicalSymbol,
                    mismatchCount: outcome.result.mismatches.count
                )
            }
        }
    }
}

public enum VerificationError: Error, CustomStringConvertible, Sendable {
    case invalidRandomRangeCount(Int)
    case databaseIntegrityFailed([String])
    case randomCrossCheckFailed(symbol: LogicalSymbol, mismatchCount: Int)

    public var description: String {
        switch self {
        case .invalidRandomRangeCount(let count):
            return "Random verification range count must not be negative; got \(count)."
        case .databaseIntegrityFailed(let issues):
            return "Database integrity checks failed: \(issues.joined(separator: " | "))"
        case .randomCrossCheckFailed(let symbol, let mismatchCount):
            return "\(symbol.rawValue): random MT5 cross-check found \(mismatchCount) mismatch(es)."
        }
    }
}
