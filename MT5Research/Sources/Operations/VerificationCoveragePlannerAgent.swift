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
        var coverageSpanGaps: [String] = []
        var missingCleanVerification: [String] = []
        var summaries: [String] = []

        for mapping in context.config.symbols.symbols {
            let canonical = try await canonicalSummary(context: context, logicalSymbol: mapping.logicalSymbol.rawValue)
            let coverage = try await coverageIntervals(context: context, logicalSymbol: mapping.logicalSymbol.rawValue)
            if canonical.rows == 0 {
                missingCoverage.append("\(mapping.logicalSymbol.rawValue):no_canonical_rows")
            } else if coverage.isEmpty {
                missingCoverage.append(mapping.logicalSymbol.rawValue)
            } else {
                let canonicalEndExclusive = try addOneMinute(canonical.maxUtc)
                if let gap = firstUncoveredRange(from: canonical.minUtc, toExclusive: canonicalEndExclusive, intervals: coverage) {
                    coverageSpanGaps.append("\(mapping.logicalSymbol.rawValue):uncovered_utc=\(gap.start)..<\(gap.end)")
                }
                summaries.append("\(mapping.logicalSymbol.rawValue):coverage_rows=\(coverage.count),canonical_rows=\(canonical.rows),utc=\(canonical.minUtc)..<\(canonicalEndExclusive)")
            }

            let cleanVerifications = try await cleanVerificationCount(context: context, logicalSymbol: mapping.logicalSymbol.rawValue)
            if cleanVerifications == 0 {
                missingCleanVerification.append(mapping.logicalSymbol.rawValue)
            }
        }

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        let details = [
            "missing_coverage=\(missingCoverage.joined(separator: ","))",
            "coverage_span_gaps=\(coverageSpanGaps.joined(separator: " | "))",
            "missing_clean_verification=\(missingCleanVerification.joined(separator: ","))",
            "coverage=\(summaries.joined(separator: " | "))"
        ].joined(separator: "; ")

        if !missingCoverage.isEmpty || !coverageSpanGaps.isEmpty || !missingCleanVerification.isEmpty {
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

    private func coverageIntervals(context: AgentRuntimeContext, logicalSymbol: String) async throws -> [CoverageInterval] {
        let body = try await context.clickHouse.execute(.select("""
        SELECT utc_range_start, utc_range_end_exclusive
        FROM \(context.config.clickHouse.database).ohlc_m1_verified_coverage
        WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
          AND logical_symbol = '\(SQLText.literal(logicalSymbol))'
          AND timeframe = 'M1'
          AND hash_schema_version = '\(SQLText.literal(ChunkHashSchemaVersion.sha256V1))'
          AND length(mt5_source_sha256) = 64
          AND length(canonical_readback_sha256) = 64
          AND length(offset_authority_sha256) = 64
        ORDER BY utc_range_start ASC, utc_range_end_exclusive DESC
        FORMAT TabSeparated
        """))
        return try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { line in
                let fields = line.split(separator: "\t", omittingEmptySubsequences: false)
                guard fields.count == 2,
                      let start = Int64(fields[0]),
                      let end = Int64(fields[1]),
                      start < end else {
                    throw ProductionAgentError.invariant("verification coverage planner received invalid coverage interval: \(line)")
                }
                return CoverageInterval(start: start, end: end)
            }
    }

    private func firstUncoveredRange(from start: Int64, toExclusive end: Int64, intervals: [CoverageInterval]) -> CoverageInterval? {
        var cursor = start
        for interval in intervals where interval.end > cursor {
            if interval.start > cursor {
                return CoverageInterval(start: cursor, end: min(interval.start, end))
            }
            cursor = max(cursor, interval.end)
            if cursor >= end {
                return nil
            }
        }
        return CoverageInterval(start: cursor, end: end)
    }

    private func addOneMinute(_ value: Int64) throws -> Int64 {
        let result = value.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw ProductionAgentError.invariant("verification coverage planner UTC end overflowed for \(value)")
        }
        return result.partialValue
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

private struct CoverageInterval: Sendable {
    let start: Int64
    let end: Int64
}
