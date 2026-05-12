import Domain
import Foundation

public struct DataCertificationAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .dataCertification,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let store = DataCertificateStore(clickHouse: context.clickHouse, database: context.config.clickHouse.database)
        let ranges = try await pendingRanges(context: context, limit: 20)
        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        guard !ranges.isEmpty else {
            return factory.ok("All verified coverage ranges have current data certificates")
        }

        var certified = 0
        for range in ranges {
            try await store.certify(
                brokerSourceId: range.brokerSourceId,
                logicalSymbol: range.logicalSymbol,
                utcStart: range.utcStart,
                utcEndExclusive: range.utcEndExclusive
            )
            certified += 1
        }
        return factory.ok("Data certificates created", details: "certified_ranges=\(certified)")
    }

    private func pendingRanges(context: AgentRuntimeContext, limit: Int) async throws -> [CertificationRange] {
        let body = try await context.clickHouse.execute(.select("""
        SELECT coverage.broker_source_id, coverage.logical_symbol,
               min(coverage.utc_range_start), max(coverage.utc_range_end_exclusive)
        FROM \(context.config.clickHouse.database).ohlc_m1_verified_coverage AS coverage
        LEFT JOIN
        (
            SELECT broker_source_id, logical_symbol, utc_range_start, utc_range_end_exclusive,
                   argMax(certificate_status, created_at_utc) AS latest_status
            FROM \(context.config.clickHouse.database).data_certificates
            GROUP BY broker_source_id, logical_symbol, utc_range_start, utc_range_end_exclusive
        ) AS certs
        ON coverage.broker_source_id = certs.broker_source_id
           AND coverage.logical_symbol = certs.logical_symbol
           AND coverage.utc_range_start = certs.utc_range_start
           AND coverage.utc_range_end_exclusive = certs.utc_range_end_exclusive
        WHERE coverage.broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
          AND coverage.hash_schema_version = '\(SQLText.literal(ChunkHashSchemaVersion.sha256V1))'
          AND length(coverage.mt5_source_sha256) = 64
          AND length(coverage.canonical_readback_sha256) = 64
          AND length(coverage.offset_authority_sha256) = 64
          AND (certs.latest_status IS NULL OR certs.latest_status != 'valid')
        GROUP BY coverage.broker_source_id, coverage.logical_symbol, coverage.utc_range_start, coverage.utc_range_end_exclusive
        ORDER BY min(coverage.utc_range_start) ASC
        LIMIT \(limit)
        FORMAT TabSeparated
        """))
        return try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try CertificationRange.parse(String($0)) }
    }
}

private struct CertificationRange {
    let brokerSourceId: BrokerSourceId
    let logicalSymbol: LogicalSymbol
    let utcStart: UtcSecond
    let utcEndExclusive: UtcSecond

    static func parse(_ row: String) throws -> CertificationRange {
        let fields = row.split(separator: "\t", omittingEmptySubsequences: false)
        guard fields.count == 4,
              let start = Int64(fields[2]),
              let end = Int64(fields[3]) else {
            throw DataCertificateError.invalidCoverageRow(row)
        }
        return CertificationRange(
            brokerSourceId: try BrokerSourceId(String(fields[0])),
            logicalSymbol: try LogicalSymbol(String(fields[1])),
            utcStart: UtcSecond(rawValue: start),
            utcEndExclusive: UtcSecond(rawValue: end)
        )
    }
}
