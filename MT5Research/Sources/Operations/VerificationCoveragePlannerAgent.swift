import Domain
import Foundation

public struct VerificationCoveragePlannerAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .verificationCoveragePlanner,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        var missingCoverage: [String] = []
        var coverageCountMismatches: [String] = []
        var missingCleanVerification: [String] = []
        var summaries: [String] = []

        for mapping in context.config.symbols.symbols {
            let canonical = try await canonicalSummary(context: context, logicalSymbol: mapping.logicalSymbol.rawValue)
            let coverage = try await coverageSummary(context: context, logicalSymbol: mapping.logicalSymbol.rawValue)
            if coverage.count == 0 {
                missingCoverage.append(mapping.logicalSymbol.rawValue)
            } else {
                summaries.append("\(mapping.logicalSymbol.rawValue):coverage_rows=\(coverage.count),canonical_rows=\(coverage.canonicalRows),utc=\(coverage.minUtc)..<\(coverage.maxUtc)")
            }
            if canonical.rows > 0, coverage.canonicalRows != canonical.rows {
                coverageCountMismatches.append("\(mapping.logicalSymbol.rawValue):canonical=\(canonical.rows),covered=\(coverage.canonicalRows)")
            }

            let cleanVerifications = try await cleanVerificationCount(context: context, logicalSymbol: mapping.logicalSymbol.rawValue)
            if cleanVerifications == 0 {
                missingCleanVerification.append(mapping.logicalSymbol.rawValue)
            }
        }

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        let details = [
            "missing_coverage=\(missingCoverage.joined(separator: ","))",
            "coverage_count_mismatches=\(coverageCountMismatches.joined(separator: " | "))",
            "missing_clean_verification=\(missingCleanVerification.joined(separator: ","))",
            "coverage=\(summaries.joined(separator: " | "))"
        ].joined(separator: "; ")

        if !missingCoverage.isEmpty || !coverageCountMismatches.isEmpty || !missingCleanVerification.isEmpty {
            return factory.warning("Verification coverage plan has incomplete symbols", details: details)
        }
        return factory.ok(
            "Verification coverage plan is current for configured symbols",
            details: "symbols=\(context.config.symbols.symbols.count)"
        )
    }

    private func canonicalSummary(context: AgentRuntimeContext, logicalSymbol: String) async throws -> CanonicalSummary {
        let body = try await context.clickHouse.execute(.select("""
        SELECT
            count(),
            if(count() = 0, 0, min(ts_utc)),
            if(count() = 0, 0, max(ts_utc))
        FROM \(context.config.clickHouse.database).ohlc_m1_canonical
        WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
          AND logical_symbol = '\(SQLText.literal(logicalSymbol))'
          AND timeframe = 'M1'
          AND offset_confidence = 'verified'
        FORMAT TabSeparated
        """))
        let fields = body.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\t", omittingEmptySubsequences: false)
        guard fields.count == 3,
              let rows = UInt64(fields[0]),
              let minUtc = Int64(fields[1]),
              let maxUtc = Int64(fields[2]) else {
            throw ProductionAgentError.invariant("verification coverage planner received invalid canonical summary: \(body)")
        }
        return CanonicalSummary(rows: rows, minUtc: minUtc, maxUtc: maxUtc)
    }

    private func coverageSummary(context: AgentRuntimeContext, logicalSymbol: String) async throws -> CoverageSummary {
        let body = try await context.clickHouse.execute(.select("""
        SELECT
            count(),
            if(count() = 0, 0, min(utc_range_start)),
            if(count() = 0, 0, max(utc_range_end_exclusive)),
            if(count() = 0, 0, sum(source_bar_count)),
            if(count() = 0, 0, sum(canonical_row_count))
        FROM \(context.config.clickHouse.database).ohlc_m1_verified_coverage
        WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
          AND logical_symbol = '\(SQLText.literal(logicalSymbol))'
          AND timeframe = 'M1'
          AND hash_schema_version = '\(SQLText.literal(ChunkHashSchemaVersion.sha256V1))'
          AND length(mt5_source_sha256) = 64
          AND length(canonical_readback_sha256) = 64
          AND length(offset_authority_sha256) = 64
        FORMAT TabSeparated
        """))
        let fields = body.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\t", omittingEmptySubsequences: false)
        guard fields.count == 5,
              let count = UInt64(fields[0]),
              let minUtc = Int64(fields[1]),
              let maxUtc = Int64(fields[2]),
              let sourceRows = UInt64(fields[3]),
              let canonicalRows = UInt64(fields[4]) else {
            throw ProductionAgentError.invariant("verification coverage planner received invalid coverage summary: \(body)")
        }
        return CoverageSummary(count: count, minUtc: minUtc, maxUtc: maxUtc, sourceRows: sourceRows, canonicalRows: canonicalRows)
    }

    private func cleanVerificationCount(context: AgentRuntimeContext, logicalSymbol: String) async throws -> Int64 {
        let body = try await context.clickHouse.execute(.select("""
        SELECT count()
        FROM (
            SELECT range_start_mt5_server_ts, range_end_mt5_server_ts,
                   argMax(result, checked_at_utc) AS latest_result
            FROM \(context.config.clickHouse.database).verification_results
            WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
              AND logical_symbol = '\(SQLText.literal(logicalSymbol))'
            GROUP BY range_start_mt5_server_ts, range_end_mt5_server_ts
        )
        WHERE latest_result = 'clean'
        FORMAT TabSeparated
        """))
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let value = Int64(trimmed) else {
            throw ProductionAgentError.invariant("verification coverage planner received invalid clean verification count: \(trimmed)")
        }
        return value
    }
}

private struct CanonicalSummary: Sendable {
    let rows: UInt64
    let minUtc: Int64
    let maxUtc: Int64
}

private struct CoverageSummary: Sendable {
    let count: UInt64
    let minUtc: Int64
    let maxUtc: Int64
    let sourceRows: UInt64
    let canonicalRows: UInt64
}
