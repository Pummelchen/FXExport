import Foundation

public struct UTCTimeAuthorityProductionAgent: ProductionAgent {
    public let descriptor: AgentDescriptor
    private let authority = BrokerAuthority()

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .utcTimeAuthority,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: true
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let bridge = try context.requireBridge(for: descriptor.kind)
        let offsetMap = try await authority.verifyLiveOffset(context: context, bridge: bridge)
        return AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
            .ok("Broker UTC offset authority verified", details: "segments=\(offsetMap.segments.count)")
    }
}
