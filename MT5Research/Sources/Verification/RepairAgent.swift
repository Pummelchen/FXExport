import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import Ingestion

public enum RepairError: Error, CustomStringConvertible, Sendable {
    case refused(String)

    public var description: String {
        switch self {
        case .refused(let reason):
            return "Repair refused: \(reason)"
        }
    }
}

public struct RepairAgent: Sendable {
    private let clickHouse: ClickHouseClientProtocol
    private let database: String
    private let logger: Logger

    public init(clickHouse: ClickHouseClientProtocol, database: String, logger: Logger) {
        self.clickHouse = clickHouse
        self.database = database
        self.logger = logger
    }

    public func repairCanonicalRange(range: VerificationRange, replacementBars: [ValidatedBar], decision: RepairDecision) async throws {
        switch decision {
        case .noRepairNeeded:
            return
        case .refuse(let reason):
            throw RepairError.refused(reason)
        case .repairCanonicalOnly(let reason):
            logger.repair("\(range.logicalSymbol.rawValue): \(reason)")
            guard !replacementBars.isEmpty else {
                throw RepairError.refused("replacement range is empty")
            }
            for bar in replacementBars {
                guard bar.brokerSourceId == range.brokerSourceId,
                      bar.logicalSymbol == range.logicalSymbol,
                      bar.mt5ServerTime.rawValue >= range.mt5Start.rawValue,
                      bar.mt5ServerTime.rawValue < range.mt5EndExclusive.rawValue else {
                    throw RepairError.refused("replacement bars do not match the requested repair range")
                }
            }
            let brokerSourceId = Self.sqlLiteral(range.brokerSourceId.rawValue)
            let symbol = Self.sqlLiteral(range.logicalSymbol.rawValue)
            let deleteSQL = """
            ALTER TABLE \(database).ohlc_m1_canonical DELETE
            WHERE broker_source_id = '\(brokerSourceId)'
              AND logical_symbol = '\(symbol)'
              AND mt5_server_ts_raw >= \(range.mt5Start.rawValue)
              AND mt5_server_ts_raw < \(range.mt5EndExclusive.rawValue)
            SETTINGS mutations_sync = 1
            """
            _ = try await clickHouse.execute(.mutation(deleteSQL, idempotent: true))
            let insertBuilder = ClickHouseInsertBuilder(database: database)
            _ = try await clickHouse.execute(insertBuilder.canonicalBarsInsert(replacementBars))
            try await CanonicalInsertVerifier(clickHouse: clickHouse, insertBuilder: insertBuilder).verify(replacementBars)
        }
    }

    private static func sqlLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "\\", with: "\\\\").replacingOccurrences(of: "'", with: "\\'")
    }
}
