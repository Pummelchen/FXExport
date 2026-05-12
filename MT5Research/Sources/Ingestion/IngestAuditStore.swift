import ClickHouse
import Domain
import Foundation

public enum IngestOperationType: String, Sendable {
    case backfill
    case live
    case verification
    case repair
}

public enum IngestOperationStatus: String, Sendable {
    case started
    case sourceVerified = "source_verified"
    case rawWritten = "raw_written"
    case canonicalDeleted = "canonical_deleted"
    case canonicalWritten = "canonical_written"
    case readbackVerified = "readback_verified"
    case emptyCoverageVerified = "empty_coverage_verified"
    case repairVerified = "repair_verified"
    case checkpointed
    case failed
}

public struct VerifiedCoverageRecord: Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let mt5Symbol: MT5Symbol
    public let timeframe: Timeframe
    public let mt5Start: MT5ServerSecond
    public let mt5EndExclusive: MT5ServerSecond
    public let utcStart: UtcSecond
    public let utcEndExclusive: UtcSecond
    public let sourceBarCount: Int
    public let canonicalRowCount: Int
    public let sourceHash: String
    public let verificationMethod: String
    public let batchId: BatchId
    public let verifiedAtUtc: UtcSecond
}

public struct IngestAuditStore: Sendable {
    private let clickHouse: ClickHouseClientProtocol
    private let database: String

    public init(clickHouse: ClickHouseClientProtocol, database: String) {
        self.clickHouse = clickHouse
        self.database = database
    }

    public func recordOperation(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        operationType: IngestOperationType,
        batchId: BatchId,
        mt5Start: MT5ServerSecond,
        mt5EndExclusive: MT5ServerSecond,
        status: IngestOperationStatus,
        stage: String,
        sourceBarCount: Int?,
        canonicalRowCount: Int?,
        sourceHash: String?,
        errorMessage: String? = nil,
        eventAtUtc: UtcSecond = UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))
    ) async throws {
        var fields: [String] = []
        fields.reserveCapacity(16)
        fields.append(tsv(brokerSourceId.rawValue))
        fields.append(tsv(logicalSymbol.rawValue))
        fields.append(tsv(mt5Symbol.rawValue))
        fields.append(tsv(Timeframe.m1.rawValue))
        fields.append(tsv(operationType.rawValue))
        fields.append(tsv(batchId.rawValue))
        fields.append(String(mt5Start.rawValue))
        fields.append(String(mt5EndExclusive.rawValue))
        fields.append(tsv(status.rawValue))
        fields.append(String(Self.rank(status)))
        fields.append(tsv(stage))
        fields.append(sourceBarCount.map { String($0) } ?? "\\N")
        fields.append(canonicalRowCount.map { String($0) } ?? "\\N")
        fields.append(sourceHash.map(tsv) ?? "\\N")
        fields.append(errorMessage.map(tsv) ?? "\\N")
        fields.append(String(eventAtUtc.rawValue))
        let row = fields.joined(separator: "\t")
        let sql = """
        INSERT INTO \(database).ingest_operations (
            broker_source_id, logical_symbol, mt5_symbol, timeframe, operation_type,
            batch_id, mt5_range_start, mt5_range_end_exclusive,
            status, status_rank, stage, source_bar_count, canonical_row_count, source_hash,
            error_message, event_at_utc
        ) FORMAT TabSeparated
        \(row)
        """
        _ = try await clickHouse.execute(.mutation(sql, idempotent: false))
    }

    public func recordVerifiedCoverage(_ record: VerifiedCoverageRecord) async throws {
        let row = [
            tsv(record.brokerSourceId.rawValue),
            tsv(record.logicalSymbol.rawValue),
            tsv(record.mt5Symbol.rawValue),
            tsv(record.timeframe.rawValue),
            String(record.mt5Start.rawValue),
            String(record.mt5EndExclusive.rawValue),
            String(record.utcStart.rawValue),
            String(record.utcEndExclusive.rawValue),
            String(record.sourceBarCount),
            String(record.canonicalRowCount),
            tsv(record.sourceHash),
            tsv(record.verificationMethod),
            tsv(record.batchId.rawValue),
            String(record.verifiedAtUtc.rawValue)
        ].joined(separator: "\t")
        let sql = """
        INSERT INTO \(database).ohlc_m1_verified_coverage (
            broker_source_id, logical_symbol, mt5_symbol, timeframe,
            mt5_range_start, mt5_range_end_exclusive,
            utc_range_start, utc_range_end_exclusive,
            source_bar_count, canonical_row_count, source_hash,
            verification_method, batch_id, verified_at_utc
        ) FORMAT TabSeparated
        \(row)
        """
        _ = try await clickHouse.execute(.mutation(sql, idempotent: false))
    }

    private func tsv(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\t", with: "\\t")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }

    private static func rank(_ status: IngestOperationStatus) -> UInt8 {
        switch status {
        case .started: return 10
        case .sourceVerified: return 20
        case .rawWritten: return 30
        case .canonicalDeleted: return 40
        case .canonicalWritten: return 50
        case .readbackVerified: return 60
        case .failed: return 110
        case .emptyCoverageVerified, .repairVerified, .checkpointed: return 120
        }
    }
}
