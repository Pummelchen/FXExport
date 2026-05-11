import Foundation

public struct AgentScheduler: Sendable {
    private var lastFinished: [ProductionAgentKind: Date] = [:]
    private var completedOnce: Set<ProductionAgentKind> = []

    public init() {}

    public mutating func dueAgents(from agents: [any ProductionAgent], now: Date) -> [any ProductionAgent] {
        agents.filter { agent in
            let descriptor = agent.descriptor
            if descriptor.runOnlyOnce && completedOnce.contains(descriptor.kind) {
                return false
            }
            guard let last = lastFinished[descriptor.kind] else {
                return descriptor.runOnStart
            }
            return now.timeIntervalSince(last) >= Double(descriptor.intervalSeconds)
        }
    }

    public mutating func markFinished(_ kind: ProductionAgentKind, at date: Date, runOnlyOnce: Bool) {
        lastFinished[kind] = date
        if runOnlyOnce {
            completedOnce.insert(kind)
        }
    }
}
