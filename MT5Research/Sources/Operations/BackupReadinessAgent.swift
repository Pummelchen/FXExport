import Domain
import Foundation

public struct BackupReadinessAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .backupReadiness,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let database = context.config.clickHouse.database
        let sql = """
        SELECT
            count(),
            if(count() = 0, 0, min(ts_utc)),
            if(count() = 0, 0, max(ts_utc))
        FROM \(database).ohlc_m1_canonical
        WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
        FORMAT TabSeparated
        """
        let body = try await context.clickHouse.execute(.select(sql))
        let fields = body.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\t", omittingEmptySubsequences: false)
        let rowCount = fields.first.flatMap { UInt64($0) } ?? 0
        let minTs = fields.count > 1 ? String(fields[1]) : "0"
        let maxTs = fields.count > 2 ? String(fields[2]) : "0"
        let certificateCountBody = try await context.clickHouse.execute(.select("""
        SELECT count()
        FROM \(database).data_certificates
        WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
          AND certificate_status = 'valid'
          AND hash_schema_version = '\(SQLText.literal(ChunkHashSchemaVersion.sha256V1))'
          AND length(certificate_sha256) = 64
        FORMAT TabSeparated
        """))
        let certificateCount = UInt64(certificateCountBody.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        guard rowCount > 0 else {
            return factory.warning("No canonical OHLC rows are available for backup/export validation")
        }
        guard certificateCount > 0 else {
            return factory.warning("Canonical OHLC data exists, but no valid data certificates are available yet")
        }
        return factory.ok(
            "Canonical OHLC data and data certificates are present for backup/export workflows",
            details: "rows=\(rowCount); certificates=\(certificateCount); min_ts_utc=\(minTs); max_ts_utc=\(maxTs)"
        )
    }
}
