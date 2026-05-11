import ClickHouse
import Foundation

public struct HealthMonitorProductionAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .healthMonitor,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        _ = try await context.clickHouse.execute(.select("SELECT 1", databaseOverride: "default"))
        if let bridge = context.bridge {
            _ = try bridge.ping()
            let terminal = try bridge.terminalInfo()
            return AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
                .ok("ClickHouse and MT5 bridge are healthy", details: "\(terminal.server) account \(terminal.accountLogin)")
        }
        return AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
            .warning("ClickHouse is healthy, MT5 bridge is not connected")
    }
}
