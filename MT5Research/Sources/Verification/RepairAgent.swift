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

    public func repairCanonicalRange(
        range: VerificationRange,
        replacementBars: [ValidatedBar],
        decision: RepairDecision,
        sourceComplete: Bool,
        verifiedCoverage: [VerifiedCoverageRecord]
    ) async throws {
        switch decision {
        case .noRepairNeeded:
            return
        case .refuse(let reason):
            throw RepairError.refused(reason)
        case .repairCanonicalOnly(let reason):
            guard sourceComplete else {
                throw RepairError.refused("MT5 source range completeness is not proven")
            }
            for coverage in verifiedCoverage {
                guard coverage.brokerSourceId == range.brokerSourceId,
                      coverage.logicalSymbol == range.logicalSymbol,
                      coverage.mt5Start.rawValue >= range.mt5Start.rawValue,
                      coverage.mt5EndExclusive.rawValue <= range.mt5EndExclusive.rawValue else {
                    throw RepairError.refused("verified coverage records do not match the requested repair range")
                }
            }
            guard !verifiedCoverage.isEmpty else {
                throw RepairError.refused("repair requires SHA-256 verified MT5 coverage records")
            }
            let offsetAuthorityDigests = Set(verifiedCoverage.map(\.offsetAuthoritySHA256))
            guard offsetAuthorityDigests.count == 1, let offsetAuthoritySHA256 = offsetAuthorityDigests.first else {
                throw RepairError.refused("repair coverage was produced from multiple broker UTC offset authority snapshots")
            }
            let mt5SourceSHA256 = ChunkHashing.combinedSHA256(
                namespace: "repair_mt5_source_coverage",
                values: verifiedCoverage.map(\.mt5SourceSHA256)
            )
            let rangeLabel = OperatorStatusText.monthRangeLabel(start: range.mt5Start, endExclusive: range.mt5EndExclusive)
            logger.repair("\(range.logicalSymbol.rawValue) - repairing canonical M1 OHLC for \(rangeLabel): \(reason)")
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
            let auditStore = IngestAuditStore(clickHouse: clickHouse, database: database)
            let sourceHash = Self.repairSourceHash(replacementBars)
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
                try await recordRepairOperation(
                    auditStore: auditStore,
                    range: range,
                    mt5Symbol: first.mt5Symbol,
                    batchId: first.batchId,
                    status: .started,
                    stage: "repair_started",
                    sourceBarCount: replacementBars.count,
                    canonicalRowCount: nil,
                    sourceHash: sourceHash,
                    mt5SourceSHA256: mt5SourceSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                try await recordRepairOperation(
                    auditStore: auditStore,
                    range: range,
                    mt5Symbol: first.mt5Symbol,
                    batchId: first.batchId,
                    status: .sourceVerified,
                    stage: "mt5_source_complete",
                    sourceBarCount: replacementBars.count,
                    canonicalRowCount: nil,
                    sourceHash: sourceHash,
                    mt5SourceSHA256: mt5SourceSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                try await CanonicalConflictRecorder(clickHouse: clickHouse, insertBuilder: insertBuilder)
                    .recordConflictsBeforeCanonicalReplace(replacementBars, detectedAtUtc: UtcSecond(rawValue: Int64(Date().timeIntervalSince1970)))
                _ = try await clickHouse.execute(.mutation(deleteSQL, idempotent: true))
                try await recordRepairOperation(
                    auditStore: auditStore,
                    range: range,
                    mt5Symbol: first.mt5Symbol,
                    batchId: first.batchId,
                    status: .canonicalDeleted,
                    stage: "canonical_range_deleted",
                    sourceBarCount: replacementBars.count,
                    canonicalRowCount: nil,
                    sourceHash: sourceHash,
                    mt5SourceSHA256: mt5SourceSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                _ = try await clickHouse.execute(insertQuery)
                try await recordRepairOperation(
                    auditStore: auditStore,
                    range: range,
                    mt5Symbol: first.mt5Symbol,
                    batchId: first.batchId,
                    status: .canonicalWritten,
                    stage: "canonical_written",
                    sourceBarCount: replacementBars.count,
                    canonicalRowCount: replacementBars.count,
                    sourceHash: sourceHash,
                    mt5SourceSHA256: mt5SourceSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                let canonicalVerification = try await CanonicalInsertVerifier(clickHouse: clickHouse, insertBuilder: insertBuilder).verify(
                    replacementBars,
                    mt5Start: range.mt5Start,
                    mt5EndExclusive: range.mt5EndExclusive
                )
                for coverage in verifiedCoverage {
                    try await auditStore.recordVerifiedCoverage(coverage)
                }
                try await recordRepairOperation(
                    auditStore: auditStore,
                    range: range,
                    mt5Symbol: first.mt5Symbol,
                    batchId: first.batchId,
                    status: .readbackVerified,
                    stage: "canonical_readback_verified",
                    sourceBarCount: replacementBars.count,
                    canonicalRowCount: replacementBars.count,
                    sourceHash: sourceHash,
                    mt5SourceSHA256: mt5SourceSHA256,
                    canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                try await recordRepairOperation(
                    auditStore: auditStore,
                    range: range,
                    mt5Symbol: first.mt5Symbol,
                    batchId: first.batchId,
                    status: .repairVerified,
                    stage: "repair_verified",
                    sourceBarCount: replacementBars.count,
                    canonicalRowCount: replacementBars.count,
                    sourceHash: sourceHash,
                    mt5SourceSHA256: mt5SourceSHA256,
                    canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                try await writeRepairLog(
                    range: range,
                    decision: "repair_canonical_only",
                    outcome: "success",
                    details: reason,
                    batchId: first.batchId
                )
                logger.repair("\(range.logicalSymbol.rawValue) - \(rangeLabel) repair written and canonical readback clean")
            } catch {
                let repairError = error
                do {
                    try await recordRepairOperation(
                        auditStore: auditStore,
                        range: range,
                        mt5Symbol: first.mt5Symbol,
                        batchId: first.batchId,
                        status: .failed,
                        stage: "repair_failed",
                        sourceBarCount: replacementBars.count,
                        canonicalRowCount: nil,
                        sourceHash: sourceHash,
                        mt5SourceSHA256: mt5SourceSHA256,
                        offsetAuthoritySHA256: offsetAuthoritySHA256,
                        errorMessage: String(describing: repairError)
                    )
                } catch {
                    logger.warn("\(range.logicalSymbol.rawValue): failed to write repair ingest operation failure row: \(error)")
                }
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

    private func recordRepairOperation(
        auditStore: IngestAuditStore,
        range: VerificationRange,
        mt5Symbol: MT5Symbol,
        batchId: BatchId,
        status: IngestOperationStatus,
        stage: String,
        sourceBarCount: Int?,
        canonicalRowCount: Int?,
        sourceHash: String?,
        mt5SourceSHA256: SHA256DigestHex? = nil,
        canonicalReadbackSHA256: SHA256DigestHex? = nil,
        offsetAuthoritySHA256: SHA256DigestHex? = nil,
        errorMessage: String? = nil
    ) async throws {
        let hasSHA256Evidence = mt5SourceSHA256 != nil || canonicalReadbackSHA256 != nil || offsetAuthoritySHA256 != nil
        try await auditStore.recordOperation(
            brokerSourceId: range.brokerSourceId,
            logicalSymbol: range.logicalSymbol,
            mt5Symbol: mt5Symbol,
            operationType: .repair,
            batchId: batchId,
            mt5Start: range.mt5Start,
            mt5EndExclusive: range.mt5EndExclusive,
            status: status,
            stage: stage,
            sourceBarCount: sourceBarCount,
            canonicalRowCount: canonicalRowCount,
            sourceHash: sourceHash,
            hashSchemaVersion: hasSHA256Evidence ? ChunkHashing.schemaVersion : nil,
            mt5SourceSHA256: mt5SourceSHA256,
            canonicalReadbackSHA256: canonicalReadbackSHA256,
            offsetAuthoritySHA256: offsetAuthoritySHA256,
            errorMessage: errorMessage
        )
    }

    private static func repairSourceHash(_ bars: [ValidatedBar]) -> String {
        var hasher = FNV1a64()
        for bar in bars {
            hasher.append(bar.barHash.description)
        }
        return "fnv64:" + String(format: "%016llx", hasher.value)
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
