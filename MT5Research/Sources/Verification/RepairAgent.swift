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
                      bar.offsetConfidence == .verified,
                      bar.mt5ServerTime.rawValue >= range.mt5Start.rawValue,
                      bar.mt5ServerTime.rawValue < range.mt5EndExclusive.rawValue else {
                    throw RepairError.refused("replacement bars do not match the requested repair range or contain non-verified UTC offsets")
                }
            }
            let insertBuilder = ClickHouseInsertBuilder(database: database)
            let insertQuery = try insertBuilder.canonicalBarsInsert(replacementBars)
            guard let first = replacementBars.first, let last = replacementBars.last else {
                throw RepairError.refused("replacement range is empty")
            }
            let brokerSourceId = Self.sqlLiteral(range.brokerSourceId.rawValue)
            let symbol = Self.sqlLiteral(range.logicalSymbol.rawValue)
            let deleteSQL = """
            ALTER TABLE \(database).ohlc_m1_canonical DELETE
            WHERE broker_source_id = '\(brokerSourceId)'
              AND logical_symbol = '\(symbol)'
              AND (
                  (mt5_server_ts_raw >= \(range.mt5Start.rawValue)
                   AND mt5_server_ts_raw < \(range.mt5EndExclusive.rawValue))
                  OR
                  (ts_utc >= \(first.utcTime.rawValue)
                   AND ts_utc <= \(last.utcTime.rawValue))
              )
            SETTINGS mutations_sync = 1
            """
            do {
                try await CanonicalConflictRecorder(clickHouse: clickHouse, insertBuilder: insertBuilder)
                    .recordConflictsBeforeCanonicalReplace(replacementBars, detectedAtUtc: UtcSecond(rawValue: Int64(Date().timeIntervalSince1970)))
                _ = try await clickHouse.execute(.mutation(deleteSQL, idempotent: true))
                _ = try await clickHouse.execute(insertQuery)
                try await CanonicalInsertVerifier(clickHouse: clickHouse, insertBuilder: insertBuilder).verify(replacementBars)
                try await writeRepairLog(
                    range: range,
                    decision: "repair_canonical_only",
                    outcome: "success",
                    details: reason,
                    batchId: first.batchId
                )
            } catch {
                let repairError = error
                do {
                    try await writeRepairLog(
                        range: range,
                        decision: "repair_canonical_only",
                        outcome: "failed",
                        details: String(describing: repairError),
                        batchId: first.batchId
                    )
                } catch {
                    logger.warn("\(range.logicalSymbol.rawValue): failed to write repair_log failure row: \(error)")
                }
                throw repairError
            }
        }
    }

    private func writeRepairLog(
        range: VerificationRange,
        decision: String,
        outcome: String,
        details: String,
        batchId: BatchId
    ) async throws {
        let row = [
            Self.tsv(range.brokerSourceId.rawValue),
            Self.tsv(range.logicalSymbol.rawValue),
            String(range.mt5Start.rawValue),
            String(range.mt5EndExclusive.rawValue),
            Self.tsv(decision),
            Self.tsv(outcome),
            Self.tsv(details),
            Self.tsv(batchId.rawValue),
            String(Int64(Date().timeIntervalSince1970))
        ].joined(separator: "\t")
        let sql = """
        INSERT INTO \(database).repair_log (
            broker_source_id, logical_symbol, range_start_mt5_server_ts,
            range_end_mt5_server_ts, decision, outcome, details, batch_id, created_at_utc
        ) FORMAT TabSeparated
        \(row)
        """
        _ = try await clickHouse.execute(.mutation(sql, idempotent: false))
    }

    private static func sqlLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "\\", with: "\\\\").replacingOccurrences(of: "'", with: "\\'")
    }

    private static func tsv(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\t", with: "\\t")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }
}
