import ClickHouse
import Domain
import Foundation
import Ingestion

public struct CheckpointGapAuditAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .checkpointGapAuditor,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let checkpointStore = context.checkpointStore()
        var missingCheckpoints: [String] = []
        var incompleteStates: [String] = []
        var mt5SymbolMismatches: [String] = []
        var missingCanonical: [String] = []
        var missingCoverage: [String] = []
        var staleSymbols: [String] = []
        var checked = 0

        for mapping in context.config.symbols.symbols {
            guard let state = try await checkpointStore.latestState(
                brokerSourceId: context.config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol
            ) else {
                missingCheckpoints.append(mapping.logicalSymbol.rawValue)
                continue
            }
            guard state.mt5Symbol == mapping.mt5Symbol else {
                mt5SymbolMismatches.append("\(mapping.logicalSymbol.rawValue):checkpoint=\(state.mt5Symbol.rawValue),configured=\(mapping.mt5Symbol.rawValue)")
                continue
            }
            if state.status != .live {
                incompleteStates.append("\(mapping.logicalSymbol.rawValue):status=\(state.status.rawValue)")
            }
            checked += 1
            let count = try await canonicalCheckpointCount(context: context, state: state)
            if count != 1 {
                missingCanonical.append("\(mapping.logicalSymbol.rawValue):checkpoint_count=\(count)")
            }
            let coverageCount = try await verifiedCoverageCount(context: context, state: state)
            if coverageCount < 1 {
                missingCoverage.append("\(mapping.logicalSymbol.rawValue):checkpoint_utc=\(state.latestIngestedClosedUtcTime.rawValue)")
            }
            if let bridge = context.bridge {
                let latest = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
                if latest.mt5Symbol == mapping.mt5Symbol.rawValue {
                    let lag = latest.mt5ServerTime - state.latestIngestedClosedMT5ServerTime.rawValue
                    if lag > Int64(context.config.app.supervisor.staleLiveWarningSeconds) {
                        staleSymbols.append("\(mapping.logicalSymbol.rawValue):lag_seconds=\(lag)")
                    }
                }
            }
        }

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        let details = [
            "configured=\(context.config.symbols.symbols.count)",
            "checked=\(checked)",
            "missing_checkpoints=\(missingCheckpoints.joined(separator: "; "))",
            "incomplete=\(incompleteStates.joined(separator: "; "))",
            "mt5_symbol_mismatch=\(mt5SymbolMismatches.joined(separator: "; "))",
            "canonical_mismatch=\(missingCanonical.joined(separator: "; "))",
            "coverage_missing=\(missingCoverage.joined(separator: "; "))",
            "stale=\(staleSymbols.joined(separator: "; "))"
        ].joined(separator: "; ")
        if !missingCheckpoints.isEmpty {
            return factory.warning("Configured symbols are missing ingest checkpoints", details: details)
        }
        if !incompleteStates.isEmpty {
            return factory.warning("Configured symbols are not live after backfill", details: details)
        }
        if !mt5SymbolMismatches.isEmpty {
            return factory.warning("Checkpoint MT5 symbol mapping does not match config", details: details)
        }
        if !missingCanonical.isEmpty {
            return factory.warning("Checkpoint canonical row mismatch found", details: details)
        }
        if !missingCoverage.isEmpty {
            return factory.warning("Checkpoint verified coverage is missing", details: details)
        }
        if !staleSymbols.isEmpty {
            return factory.warning("Live checkpoint lag exceeds configured threshold", details: details)
        }
        return factory.ok("Checkpoint and gap audit completed", details: "checked=\(checked)")
    }

    private func canonicalCheckpointCount(context: AgentRuntimeContext, state: IngestState) async throws -> Int {
        let sql = """
        SELECT count()
        FROM \(context.config.clickHouse.database).ohlc_m1_canonical
        WHERE broker_source_id = '\(SQLText.literal(state.brokerSourceId.rawValue))'
          AND logical_symbol = '\(SQLText.literal(state.logicalSymbol.rawValue))'
          AND mt5_server_ts_raw = \(state.latestIngestedClosedMT5ServerTime.rawValue)
          AND ts_utc = \(state.latestIngestedClosedUtcTime.rawValue)
        FORMAT TabSeparated
        """
        let body = try await context.clickHouse.execute(.select(sql))
        return Int(body.trimmingCharacters(in: .whitespacesAndNewlines)) ?? -1
    }

    private func verifiedCoverageCount(context: AgentRuntimeContext, state: IngestState) async throws -> Int {
        let sql = """
        SELECT count()
        FROM \(context.config.clickHouse.database).ohlc_m1_verified_coverage
        WHERE broker_source_id = '\(SQLText.literal(state.brokerSourceId.rawValue))'
          AND logical_symbol = '\(SQLText.literal(state.logicalSymbol.rawValue))'
          AND timeframe = 'M1'
          AND hash_schema_version = '\(SQLText.literal(ChunkHashSchemaVersion.sha256V1))'
          AND length(mt5_source_sha256) = 64
          AND length(canonical_readback_sha256) = 64
          AND length(offset_authority_sha256) = 64
          AND utc_range_start <= \(state.latestIngestedClosedUtcTime.rawValue)
          AND utc_range_end_exclusive > \(state.latestIngestedClosedUtcTime.rawValue)
        FORMAT TabSeparated
        """
        let body = try await context.clickHouse.execute(.select(sql))
        return Int(body.trimmingCharacters(in: .whitespacesAndNewlines)) ?? -1
    }
}
