import Foundation
import Ingestion

public struct SymbolMetadataDriftAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .symbolMetadataDrift,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: true
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let bridge = try context.requireBridge(for: descriptor.kind)
        var warnings: [String] = []
        var failures: [String] = []
        for mapping in context.config.symbols.symbols {
            let info = try bridge.symbolInfo(mapping.mt5Symbol)
            if !info.selected {
                warnings.append("\(mapping.mt5Symbol.rawValue) is not selected")
            }
            if info.digits != mapping.digits.rawValue {
                failures.append("\(mapping.mt5Symbol.rawValue) digits expected=\(mapping.digits.rawValue) actual=\(info.digits)")
            }
        }
        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        if !failures.isEmpty {
            return factory.failed("Symbol metadata drift found", details: failures.joined(separator: "; "))
        }
        if warnings.isEmpty {
            return factory.ok("Symbol metadata matches configured mappings")
        }
        return factory.warning("Symbol metadata warnings found", details: warnings.joined(separator: "; "))
    }
}
