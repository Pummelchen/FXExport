import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import MT5Bridge
import TimeMapping
import Validation

public enum VerificationError: Error, CustomStringConvertible, Sendable {
    case notImplemented(String)

    public var description: String {
        switch self {
        case .notImplemented(let detail):
            return "Verification capability is scaffolded but not fully wired: \(detail)"
        }
    }
}

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
        logger.verify("Running duplicate-key check")
        let duplicateRows = try await clickHouse.execute(.select("""
        SELECT broker_source_id, logical_symbol, ts_utc, count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        GROUP BY broker_source_id, logical_symbol, ts_utc
        HAVING count() > 1
        LIMIT 20
        """))
        if !duplicateRows.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            logger.warn("Duplicate canonical keys found:\n\(duplicateRows)")
        }

        logger.verify("Running OHLC invariant check")
        let invariantCount = try await clickHouse.execute(.select("""
        SELECT count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        WHERE open_scaled <= 0 OR high_scaled < open_scaled OR high_scaled < close_scaled OR high_scaled < low_scaled OR low_scaled > open_scaled OR low_scaled > close_scaled
        """))
        if invariantCount.trimmingCharacters(in: .whitespacesAndNewlines) != "0" {
            logger.warn("OHLC invariant violations found: \(invariantCount.trimmingCharacters(in: .whitespacesAndNewlines))")
        }

        logger.verify("Running unresolved-time-offset check")
        let unresolvedCount = try await clickHouse.execute(.select("""
        SELECT count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        WHERE offset_confidence = 'unresolved'
        """))
        if unresolvedCount.trimmingCharacters(in: .whitespacesAndNewlines) != "0" {
            logger.warn("Canonical rows with unresolved offsets found: \(unresolvedCount.trimmingCharacters(in: .whitespacesAndNewlines))")
        }

        guard bridge != nil else {
            logger.warn("Random MT5 cross-check skipped because no MT5 bridge connection is active")
            return
        }
        logger.verify("Random historical cross-check scaffold ready for \(randomRanges) range(s)")
        // TODO: wire typed ClickHouse range decoding, then compare with MT5 source bars using VerificationComparator.
    }
}
