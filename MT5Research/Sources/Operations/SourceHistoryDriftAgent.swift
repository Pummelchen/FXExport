import Domain
import Foundation
import Ingestion

public struct SourceHistoryDriftAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .sourceHistoryDrift,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: true
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let bridge = try context.requireBridge(for: descriptor.kind)
        let checkpointStore = context.checkpointStore()
        var checked = 0
        var warnings: [String] = []
        var failures: [String] = []

        for mapping in context.config.symbols.symbols {
            let status = try bridge.historyStatus(mapping.mt5Symbol)
            guard status.mt5Symbol == mapping.mt5Symbol.rawValue else {
                failures.append("\(mapping.logicalSymbol.rawValue): history status returned \(status.mt5Symbol)")
                continue
            }
            guard status.synchronized, status.bars > 0 else {
                warnings.append("\(mapping.logicalSymbol.rawValue): MT5 history not synchronized")
                continue
            }

            let oldest = try bridge.oldestM1BarTime(mapping.mt5Symbol)
            let latest = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
            guard oldest.mt5Symbol == mapping.mt5Symbol.rawValue,
                  latest.mt5Symbol == mapping.mt5Symbol.rawValue else {
                failures.append("\(mapping.logicalSymbol.rawValue): oldest/latest response symbol mismatch")
                continue
            }
            checked += 1

            guard let state = try await checkpointStore.latestState(
                brokerSourceId: context.config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol
            ) else {
                warnings.append("\(mapping.logicalSymbol.rawValue): no checkpoint yet; first import still required")
                continue
            }
            if state.mt5Symbol != mapping.mt5Symbol {
                failures.append("\(mapping.logicalSymbol.rawValue): checkpoint MT5 symbol \(state.mt5Symbol.rawValue) != configured \(mapping.mt5Symbol.rawValue)")
                continue
            }
            if latest.mt5ServerTime < state.latestIngestedClosedMT5ServerTime.rawValue {
                failures.append("\(mapping.logicalSymbol.rawValue): MT5 latest closed \(latest.mt5ServerTime) is older than checkpoint \(state.latestIngestedClosedMT5ServerTime.rawValue)")
            }
            if oldest.mt5ServerTime > state.oldestMT5ServerTime.rawValue {
                failures.append("\(mapping.logicalSymbol.rawValue): MT5 oldest \(oldest.mt5ServerTime) is newer than checkpoint oldest \(state.oldestMT5ServerTime.rawValue)")
            }
            if oldest.mt5ServerTime < state.oldestMT5ServerTime.rawValue {
                warnings.append("\(mapping.logicalSymbol.rawValue): MT5 now has older history from \(oldest.mt5ServerTime); rerun backfill to extend the beginning")
            }
        }

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        let details = "configured=\(context.config.symbols.symbols.count); checked=\(checked); warnings=\(warnings.joined(separator: " | ")); failures=\(failures.joined(separator: " | "))"
        if !failures.isEmpty {
            return factory.failed("MT5 source history drift requires operator review", details: details)
        }
        if !warnings.isEmpty {
            return factory.warning("MT5 source history changed or is not fully imported", details: details)
        }
        return factory.ok("MT5 source history boundaries match checkpoints", details: "checked=\(checked)")
    }
}
