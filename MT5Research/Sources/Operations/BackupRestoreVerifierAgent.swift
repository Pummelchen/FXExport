import Domain
import Foundation

public struct BackupRestoreVerifierAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .backupRestoreVerifier,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let database = context.config.clickHouse.database
        let broker = SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue)
        let certificateBody = try await context.clickHouse.execute(.select("""
        SELECT
            count(),
            if(count() = 0, 0, sum(coverage_source_bar_count)),
            if(count() = 0, 0, sum(coverage_canonical_row_count)),
            if(count() = 0, 0, min(first_covered_utc)),
            if(count() = 0, 0, max(last_covered_utc))
        FROM \(database).data_certificates
        WHERE broker_source_id = '\(broker)'
          AND certificate_status = 'valid'
          AND hash_schema_version = '\(SQLText.literal(ChunkHashSchemaVersion.sha256V1))'
          AND length(certificate_sha256) = 64
          AND length(mt5_source_sha256_aggregate) = 64
          AND length(canonical_readback_sha256_aggregate) = 64
          AND length(offset_authority_sha256_aggregate) = 64
        FORMAT TabSeparated
        """))
        let certificate = try parseCertificateSummary(certificateBody)
        let invalidCertificates = try await scalar(context: context, sql: """
        SELECT count()
        FROM \(database).data_certificates
        WHERE broker_source_id = '\(broker)'
          AND (
              certificate_status != 'valid'
              OR hash_schema_version != '\(SQLText.literal(ChunkHashSchemaVersion.sha256V1))'
              OR length(certificate_sha256) != 64
              OR length(mt5_source_sha256_aggregate) != 64
              OR length(canonical_readback_sha256_aggregate) != 64
              OR length(offset_authority_sha256_aggregate) != 64
          )
        FORMAT TabSeparated
        """)
        let unfinished = try await scalar(context: context, sql: """
        SELECT count()
        FROM (
            SELECT operation_type, batch_id, argMax(status, tuple(event_at_utc, status_rank)) AS latest_status
            FROM \(database).ingest_operations
            WHERE broker_source_id = '\(broker)'
            GROUP BY operation_type, batch_id
        )
        WHERE NOT (
            (operation_type IN ('backfill', 'live') AND latest_status IN ('checkpointed', 'empty_coverage_verified'))
            OR (operation_type = 'repair' AND latest_status = 'repair_verified')
        )
        FORMAT TabSeparated
        """)

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        guard unfinished == 0 else {
            return factory.warning(
                "Restore evidence is blocked by unfinished ingest or repair batches",
                details: "unfinished_batches=\(unfinished)"
            )
        }
        guard invalidCertificates == 0 else {
            return factory.warning(
                "Restore evidence contains invalid or obsolete data certificates",
                details: "invalid_certificates=\(invalidCertificates)"
            )
        }
        guard certificate.count > 0 else {
            return factory.warning("No valid data certificates exist for restore verification")
        }
        guard certificate.sourceBars == certificate.canonicalRows else {
            return factory.warning(
                "Restore evidence has source/canonical count mismatch",
                details: "source_bars=\(certificate.sourceBars); canonical_rows=\(certificate.canonicalRows)"
            )
        }

        return factory.ok(
            "Backup restore evidence is internally verifiable",
            details: "certificates=\(certificate.count); rows=\(certificate.canonicalRows); covered_utc=\(certificate.firstUtc)..\(certificate.lastUtc); external_restore_target=not_configured"
        )
    }

    private func parseCertificateSummary(_ body: String) throws -> CertificateSummary {
        let fields = body.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\t", omittingEmptySubsequences: false)
        guard fields.count == 5,
              let count = UInt64(fields[0]) else {
            throw ProductionAgentError.invariant("backup restore verifier received invalid certificate summary: \(body)")
        }
        if count == 0 {
            return CertificateSummary(count: 0, sourceBars: 0, canonicalRows: 0, firstUtc: 0, lastUtc: 0)
        }
        guard let sourceBars = UInt64(fields[1]),
              let canonicalRows = UInt64(fields[2]),
              let firstUtc = Int64(fields[3]),
              let lastUtc = Int64(fields[4]) else {
            throw ProductionAgentError.invariant("backup restore verifier received invalid certificate totals: \(body)")
        }
        return CertificateSummary(
            count: count,
            sourceBars: sourceBars,
            canonicalRows: canonicalRows,
            firstUtc: firstUtc,
            lastUtc: lastUtc
        )
    }

    private func scalar(context: AgentRuntimeContext, sql: String) async throws -> Int64 {
        let body = try await context.clickHouse.execute(.select(sql))
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let value = Int64(trimmed) else {
            throw ProductionAgentError.invariant("backup restore verifier scalar query returned invalid value: \(trimmed)")
        }
        return value
    }
}

private struct CertificateSummary: Sendable {
    let count: UInt64
    let sourceBars: UInt64
    let canonicalRows: UInt64
    let firstUtc: Int64
    let lastUtc: Int64
}
