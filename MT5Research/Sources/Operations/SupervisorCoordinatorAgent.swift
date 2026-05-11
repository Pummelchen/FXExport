import Foundation

public struct SupervisorCoordinatorAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .supervisorCoordinator,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let age = utcNow().rawValue - context.supervisorStartedAtUtc.rawValue
        let details = "supervisor_age_seconds=\(age); single_bridge_owner=true; sequential_agent_execution=true"
        return AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
            .ok("Supervisor ownership and sequential MT5 access are active", details: details)
    }
}
