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
        var missingCanonical: [String] = []
        var staleSymbols: [String] = []
        var checked = 0

        for mapping in context.config.symbols.symbols {
            guard let state = try await checkpointStore.latestState(
                brokerSourceId: context.config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol
            ) else {
                continue
            }
            checked += 1
            let count = try await canonicalCheckpointCount(context: context, state: state)
            if count != 1 {
                missingCanonical.append("\(mapping.logicalSymbol.rawValue):checkpoint_count=\(count)")
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
        let details = "checked=\(checked); missing=\(missingCanonical.joined(separator: "; ")); stale=\(staleSymbols.joined(separator: "; "))"
        if !missingCanonical.isEmpty {
            return factory.warning("Checkpoint canonical row mismatch found", details: details)
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
}
