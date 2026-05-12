import ClickHouse
import Config
import Domain
import Foundation

public struct OperationalHealthSnapshot: Codable, Sendable, Equatable {
    public let service: String
    public let status: String
    public let checkedAtUtc: Int64
    public let clickHouseOk: Bool
    public let brokerSourceCount: Int64
    public let canonicalRows: Int64
    public let unfinishedIngestOperations: Int64
    public let warningAgentCount: Int64
    public let failedAgentCount: Int64
    public let validDataCertificateCount: Int64
    public let latestCanonicalUtc: Int64?

    enum CodingKeys: String, CodingKey {
        case service
        case status
        case checkedAtUtc = "checked_at_utc"
        case clickHouseOk = "clickhouse_ok"
        case brokerSourceCount = "broker_source_count"
        case canonicalRows = "canonical_rows"
        case unfinishedIngestOperations = "unfinished_ingest_operations"
        case warningAgentCount = "warning_agent_count"
        case failedAgentCount = "failed_agent_count"
        case validDataCertificateCount = "valid_data_certificate_count"
        case latestCanonicalUtc = "latest_canonical_utc"
    }
}

public struct OperationalHealthService: Sendable {
    private let config: ConfigBundle
    private let clickHouse: ClickHouseClientProtocol

    public init(config: ConfigBundle, clickHouse: ClickHouseClientProtocol) {
        self.config = config
        self.clickHouse = clickHouse
    }

    public func snapshot() async -> OperationalHealthSnapshot {
        let now = utcNow().rawValue
        do {
            _ = try await clickHouse.execute(.select("SELECT 1", databaseOverride: "default"))
            let brokerSources = try await scalar("SELECT count() FROM \(config.clickHouse.database).broker_sources WHERE is_active = 1 FORMAT TabSeparated")
            let canonical = try await scalar("SELECT count() FROM \(config.clickHouse.database).ohlc_m1_canonical FORMAT TabSeparated")
            let latest = try await optionalScalar("SELECT if(count() = 0, NULL, max(ts_utc)) FROM \(config.clickHouse.database).ohlc_m1_canonical FORMAT TabSeparated")
            let unfinished = try await scalar("""
            SELECT count()
            FROM (
                SELECT operation_type, batch_id, argMax(status, tuple(event_at_utc, status_rank)) AS latest_status
                FROM \(config.clickHouse.database).ingest_operations
                GROUP BY operation_type, batch_id
            )
            WHERE NOT (
                (operation_type IN ('backfill', 'live') AND latest_status IN ('checkpointed', 'empty_coverage_verified'))
                OR (operation_type = 'repair' AND latest_status = 'repair_verified')
            )
            FORMAT TabSeparated
            """)
            let warningAgents = try await scalar("SELECT count() FROM \(config.clickHouse.database).runtime_agent_state FINAL WHERE status = 'warning' FORMAT TabSeparated")
            let failedAgents = try await scalar("SELECT count() FROM \(config.clickHouse.database).runtime_agent_state FINAL WHERE status = 'failed' FORMAT TabSeparated")
            let certificates = try await scalar("SELECT count() FROM \(config.clickHouse.database).data_certificates WHERE certificate_status = 'valid' FORMAT TabSeparated")
            let status = unfinished == 0 && failedAgents == 0 ? "ok" : "attention_required"
            return OperationalHealthSnapshot(
                service: "FXExport",
                status: status,
                checkedAtUtc: now,
                clickHouseOk: true,
                brokerSourceCount: brokerSources,
                canonicalRows: canonical,
                unfinishedIngestOperations: unfinished,
                warningAgentCount: warningAgents,
                failedAgentCount: failedAgents,
                validDataCertificateCount: certificates,
                latestCanonicalUtc: latest
            )
        } catch {
            return OperationalHealthSnapshot(
                service: "FXExport",
                status: "clickhouse_unavailable",
                checkedAtUtc: now,
                clickHouseOk: false,
                brokerSourceCount: 0,
                canonicalRows: 0,
                unfinishedIngestOperations: 0,
                warningAgentCount: 0,
                failedAgentCount: 0,
                validDataCertificateCount: 0,
                latestCanonicalUtc: nil
            )
        }
    }

    private func scalar(_ sql: String) async throws -> Int64 {
        let body = try await clickHouse.execute(.select(sql))
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let value = Int64(trimmed) else {
            throw OperationalHealthServiceError.invalidScalar(trimmed)
        }
        return value
    }

    private func optionalScalar(_ sql: String) async throws -> Int64? {
        let body = try await clickHouse.execute(.select(sql))
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, trimmed.uppercased() != "\\N" else { return nil }
        return Int64(trimmed)
    }
}

public enum OperationalHealthServiceError: Error, CustomStringConvertible, Sendable {
    case invalidScalar(String)

    public var description: String {
        switch self {
        case .invalidScalar(let value):
            return "Operational health query returned an invalid scalar value: \(value)"
        }
    }
}
