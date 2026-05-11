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
        var bridge: MT5BridgeClient?
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

            for agent in dueAgents {
                let startedAt = Date()
                if agent.descriptor.requiresMT5Bridge && bridge == nil {
                    if let bridgeUnavailableDetails {
                        let outcome = AgentOutcomeFactory(kind: agent.descriptor.kind, startedAt: startedAt)
                            .failed("MT5 bridge unavailable", details: bridgeUnavailableDetails)
                        await recordAndLog(outcome)
                        scheduler.markFinished(
                            agent.descriptor.kind,
                            at: Date(),
                            runOnlyOnce: agent.descriptor.runOnlyOnce
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
                        scheduler.markFinished(
                            agent.descriptor.kind,
                            at: Date(),
                            runOnlyOnce: agent.descriptor.runOnlyOnce
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
                } catch {
                    if agent.descriptor.requiresMT5Bridge || Self.isBridgeRelated(error) {
                        bridge = nil
                        bridgeUnavailableDetails = String(describing: error)
                    }
                    let outcome = AgentOutcomeFactory(kind: agent.descriptor.kind, startedAt: startedAt)
                        .failed("Agent failed", details: String(describing: error))
                    await recordAndLog(outcome)
                }
                scheduler.markFinished(
                    agent.descriptor.kind,
                    at: Date(),
                    runOnlyOnce: agent.descriptor.runOnlyOnce
                )
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
        }

        let message = "\(outcome.agent.rawValue): \(outcome.message)"
        switch outcome.status {
        case .ok:
            logger.ok(message)
        case .warning:
            logger.warn(message)
            if !outcome.details.isEmpty {
                logger.verbose(outcome.details)
            }
        case .failed:
            logger.error(message)
            if !outcome.details.isEmpty {
                logger.verbose(outcome.details)
            }
        case .skipped:
            logger.info(message)
        }
    }

    private static func isBridgeRelated(_ error: Error) -> Bool {
        error is MT5BridgeError || error is ProtocolError || error is ProductionAgentError
    }
}
