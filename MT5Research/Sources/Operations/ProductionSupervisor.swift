import AppCore
import ClickHouse
import Config
import Foundation
import MT5Bridge

public struct ProductionSupervisor: Sendable {
    public typealias BridgeConnector = @Sendable () throws -> MT5BridgeClient

    private let config: ConfigBundle
    private let clickHouse: ClickHouseClientProtocol
    private let eventStore: AgentEventStore
    private let logger: Logger
    private let bridgeConnector: BridgeConnector
    private let runBackfillOnStart: Bool

    public init(
        config: ConfigBundle,
        clickHouse: ClickHouseClientProtocol,
        eventStore: AgentEventStore,
        logger: Logger,
        bridgeConnector: @escaping BridgeConnector,
        runBackfillOnStart: Bool
    ) {
        self.config = config
        self.clickHouse = clickHouse
        self.eventStore = eventStore
        self.logger = logger
        self.bridgeConnector = bridgeConnector
        self.runBackfillOnStart = runBackfillOnStart
    }

    public func run(maxCycles: Int? = nil) async throws {
        let agents = ProductionAgentFactory().makeAgents(
            config: config,
            runBackfillOnStart: runBackfillOnStart
        )
        var scheduler = AgentScheduler()
        let executionPolicy = AgentExecutionPolicy()
        var bridge: MT5BridgeClient?
        var persistentBlocksBySource: [ProductionAgentKind: [ProductionAgentKind: String]] = [:]
        let supervisorStarted = utcNow()
        var cycle = 0

        logger.info("Supervisor started with \(agents.count) production agents")
        while !Task.isCancelled {
            cycle += 1
            var bridgeUnavailableDetails: String?
            let now = Date()
            let dueAgents = scheduler.dueAgents(from: agents, now: now)
            if dueAgents.isEmpty {
                logger.debug("Supervisor cycle \(cycle): no agents due")
            }
            let staticBlocks = executionPolicy.staticSupersedence(for: Set(dueAgents.map(\.descriptor.kind)))
            var blockedKinds = Self.flattenBlocks(persistentBlocksBySource, staticBlocks: staticBlocks)

            for agent in dueAgents {
                let startedAt = Date()
                if let reason = blockedKinds[agent.descriptor.kind] {
                    let outcome = AgentOutcomeFactory(kind: agent.descriptor.kind, startedAt: startedAt)
                        .skipped("Agent superseded by higher-priority safety rule", details: reason)
                    await recordAndLog(outcome)
                    scheduler.markDeferred(
                        agent.descriptor.kind,
                        at: Date(),
                        retryAfterSeconds: config.app.supervisor.cycleSeconds
                    )
                    continue
                }
                if agent.descriptor.requiresMT5Bridge && bridge == nil {
                    if let bridgeUnavailableDetails {
                        let outcome = AgentOutcomeFactory(kind: agent.descriptor.kind, startedAt: startedAt)
                            .failed("MT5 bridge unavailable", details: bridgeUnavailableDetails)
                        await recordAndLog(outcome)
                        Self.updatePersistentBlocks(
                            &persistentBlocksBySource,
                            after: outcome,
                            policy: executionPolicy
                        )
                        blockedKinds = Self.flattenBlocks(persistentBlocksBySource, staticBlocks: staticBlocks)
                        scheduler.markFinished(
                            agent.descriptor.kind,
                            at: Date(),
                            runOnlyOnce: false
                        )
                        continue
                    }
                    do {
                        logger.db("Connecting MT5 bridge for \(agent.descriptor.kind.rawValue)")
                        bridge = try bridgeConnector()
                        logger.ok("MT5 bridge connected for supervised agents")
                    } catch {
                        bridgeUnavailableDetails = String(describing: error)
                        let outcome = AgentOutcomeFactory(kind: agent.descriptor.kind, startedAt: startedAt)
                            .failed("MT5 bridge unavailable", details: bridgeUnavailableDetails ?? "")
                        await recordAndLog(outcome)
                        Self.updatePersistentBlocks(
                            &persistentBlocksBySource,
                            after: outcome,
                            policy: executionPolicy
                        )
                        blockedKinds = Self.flattenBlocks(persistentBlocksBySource, staticBlocks: staticBlocks)
                        scheduler.markFinished(
                            agent.descriptor.kind,
                            at: Date(),
                            runOnlyOnce: false
                        )
                        continue
                    }
                }

                let context = AgentRuntimeContext(
                    config: config,
                    clickHouse: clickHouse,
                    bridge: bridge,
                    eventStore: eventStore,
                    logger: logger,
                    supervisorStartedAtUtc: supervisorStarted,
                    repairOnVerifierMismatch: config.app.supervisor.repairOnVerifierMismatch
                )

                do {
                    let outcome = try await agent.run(context: context, startedAt: startedAt)
                    await recordAndLog(outcome)
                    Self.updatePersistentBlocks(
                        &persistentBlocksBySource,
                        after: outcome,
                        policy: executionPolicy
                    )
                    blockedKinds = Self.flattenBlocks(persistentBlocksBySource, staticBlocks: staticBlocks)
                    scheduler.markFinished(
                        agent.descriptor.kind,
                        at: Date(),
                        runOnlyOnce: agent.descriptor.runOnlyOnce && (outcome.status == .ok || outcome.status == .skipped)
                    )
                } catch {
                    if agent.descriptor.requiresMT5Bridge || Self.isBridgeRelated(error) {
                        bridge = nil
                        bridgeUnavailableDetails = String(describing: error)
                    }
                    await recoverClickHouseIfNeeded(after: error)
                    let outcome = AgentOutcomeFactory(kind: agent.descriptor.kind, startedAt: startedAt)
                        .failed("Agent failed", details: String(describing: error))
                    await recordAndLog(outcome)
                    Self.updatePersistentBlocks(
                        &persistentBlocksBySource,
                        after: outcome,
                        policy: executionPolicy
                    )
                    blockedKinds = Self.flattenBlocks(persistentBlocksBySource, staticBlocks: staticBlocks)
                    scheduler.markFinished(
                        agent.descriptor.kind,
                        at: Date(),
                        runOnlyOnce: false
                    )
                }
            }

            if let maxCycles, cycle >= maxCycles {
                logger.info("Supervisor stopped after \(cycle) cycle(s)")
                return
            }
            try await Task.sleep(nanoseconds: UInt64(config.app.supervisor.cycleSeconds) * 1_000_000_000)
        }
    }

    private func recordAndLog(_ outcome: AgentOutcome) async {
        do {
            try await eventStore.record(outcome, brokerSourceId: config.brokerTime.brokerSourceId)
        } catch {
            logger.warn("Could not write supervisor event for \(outcome.agent.rawValue): \(error)")
            await recoverClickHouseIfNeeded(after: error)
        }

        let message = "\(outcome.agent.rawValue): \(outcome.message)"
        switch outcome.status {
        case .ok:
            logger.ok(message)
        case .warning:
            logger.alert(message, details: outcome.details)
        case .failed:
            logger.alert(message, details: outcome.details)
        case .skipped:
            logger.info(message)
        }
    }

    private static func isBridgeRelated(_ error: Error) -> Bool {
        error is MT5BridgeError || error is ProtocolError || error is ProductionAgentError
    }

    private func recoverClickHouseIfNeeded(after error: Error) async {
        guard error is ClickHouseError else { return }
        do {
            try await ClickHouseStartupManager(
                config: config.clickHouse,
                client: clickHouse,
                logger: logger
            ).ensureReady()
        } catch {
            logger.warn("ClickHouse automatic recovery failed inside supervisor")
            logger.verbose(OperationalFailureGuide.advice(for: error).formatted)
        }
    }

    private static func updatePersistentBlocks(
        _ persistentBlocksBySource: inout [ProductionAgentKind: [ProductionAgentKind: String]],
        after outcome: AgentOutcome,
        policy: AgentExecutionPolicy
    ) {
        if outcome.status == .ok {
            persistentBlocksBySource[outcome.agent] = nil
            return
        }
        let blocks = policy.dynamicSupersedence(after: outcome)
        if blocks.isEmpty {
            persistentBlocksBySource[outcome.agent] = nil
            return
        }
        persistentBlocksBySource[outcome.agent] = blocks
    }

    private static func flattenBlocks(
        _ persistentBlocksBySource: [ProductionAgentKind: [ProductionAgentKind: String]],
        staticBlocks: [ProductionAgentKind: String]
    ) -> [ProductionAgentKind: String] {
        var merged: [ProductionAgentKind: String] = [:]
        for source in persistentBlocksBySource.keys.sorted(by: { $0.priorityRank < $1.priorityRank }) {
            guard let blocks = persistentBlocksBySource[source] else { continue }
            merged.merge(blocks) { current, _ in current }
        }
        merged.merge(staticBlocks) { current, _ in current }
        return merged
    }
}
