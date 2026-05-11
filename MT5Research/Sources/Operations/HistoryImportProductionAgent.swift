import Config
import Foundation
import Ingestion

public struct HistoryImportProductionAgent: ProductionAgent {
    public let descriptor: AgentDescriptor
    private let enabled: Bool

    public init(intervalSeconds: Int, enabled: Bool) {
        self.enabled = enabled
        self.descriptor = AgentDescriptor(
            kind: .historyImporter,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: enabled,
            runOnStart: enabled,
            runOnlyOnce: true
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let outcome = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        guard enabled else {
            return outcome.skipped("Backfill-on-start is disabled")
        }
        let bridge = try context.requireBridge(for: descriptor.kind)
        let agent = BackfillAgent(
            config: context.config,
            bridge: bridge,
            clickHouse: context.clickHouse,
            checkpointStore: context.checkpointStore(),
            offsetStore: context.offsetStore(),
            logger: context.logger
        )
        try await agent.run(selectedSymbols: nil)
        return outcome.ok("Initial/resume backfill completed")
    }
}
