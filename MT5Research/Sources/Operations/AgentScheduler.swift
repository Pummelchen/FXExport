import Foundation

public struct AgentScheduler: Sendable {
    private var lastFinished: [ProductionAgentKind: Date] = [:]
    private var completedOnce: Set<ProductionAgentKind> = []
    private var deferredUntil: [ProductionAgentKind: Date] = [:]

    public init() {}

    public mutating func dueAgents(from agents: [any ProductionAgent], now: Date) -> [any ProductionAgent] {
        agents.filter { agent in
            let descriptor = agent.descriptor
            if descriptor.runOnlyOnce && completedOnce.contains(descriptor.kind) {
                return false
            }
            if let deferred = deferredUntil[descriptor.kind], now < deferred {
                return false
            }
            guard let last = lastFinished[descriptor.kind] else {
                return descriptor.runOnStart
            }
            return now.timeIntervalSince(last) >= Double(descriptor.intervalSeconds)
        }.sorted {
            if $0.descriptor.kind.priorityRank == $1.descriptor.kind.priorityRank {
                return $0.descriptor.kind.rawValue < $1.descriptor.kind.rawValue
            }
            return $0.descriptor.kind.priorityRank < $1.descriptor.kind.priorityRank
        }
    }

    public mutating func markFinished(_ kind: ProductionAgentKind, at date: Date, runOnlyOnce: Bool) {
        lastFinished[kind] = date
        deferredUntil[kind] = nil
        if runOnlyOnce {
            completedOnce.insert(kind)
        }
    }

    public mutating func markDeferred(_ kind: ProductionAgentKind, at date: Date, retryAfterSeconds: Int) {
        deferredUntil[kind] = date.addingTimeInterval(Double(max(1, retryAfterSeconds)))
    }
}
