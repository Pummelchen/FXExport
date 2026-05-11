import Foundation

public struct AlertingAgent: ProductionAgent {
    public let descriptor: AgentDescriptor
    private let lookbackSeconds: Int

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .alerting,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
        self.lookbackSeconds = max(300, intervalSeconds * 10)
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let since = utcNow().rawValue - Int64(lookbackSeconds)
        let sql = """
        SELECT
            agent_name,
            severity,
            message,
            finished_at_utc
        FROM \(context.config.clickHouse.database).runtime_agent_events
        WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
          AND finished_at_utc >= \(since)
          AND severity IN ('warning', 'error')
          AND agent_name != '\(ProductionAgentKind.alerting.rawValue)'
        ORDER BY finished_at_utc DESC
        LIMIT 20
        FORMAT TabSeparated
        """
        let body = try await context.clickHouse.execute(.select(sql))
        let rows = body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map(String.init)

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        guard !rows.isEmpty else {
            return factory.ok("No recent supervisor warnings or errors")
        }

        let details = rows.joined(separator: " | ")
        return factory.warning(
            "Recent supervisor warnings/errors require attention",
            details: "events=\(rows.count); \(details)"
        )
    }
}
