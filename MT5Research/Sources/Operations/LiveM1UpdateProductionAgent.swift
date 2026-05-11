import Foundation
import Ingestion

public struct LiveM1UpdateProductionAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .liveM1Updater,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: true
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let bridge = try context.requireBridge(for: descriptor.kind)
        try await LiveUpdateAgent(
            config: context.config,
            bridge: bridge,
            clickHouse: context.clickHouse,
            checkpointStore: context.checkpointStore(),
            offsetStore: context.offsetStore(),
            logger: context.logger
        ).runOnce()
        return AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
            .ok("Live M1 update cycle completed")
    }
}
