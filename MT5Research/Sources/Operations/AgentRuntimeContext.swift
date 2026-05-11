import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import Ingestion
import MT5Bridge

public struct AgentRuntimeContext: Sendable {
    public let config: ConfigBundle
    public let clickHouse: ClickHouseClientProtocol
    public let bridge: MT5BridgeClient?
    public let eventStore: AgentEventStore
    public let logger: Logger
    public let supervisorStartedAtUtc: UtcSecond
    public let repairOnVerifierMismatch: Bool

    public init(
        config: ConfigBundle,
        clickHouse: ClickHouseClientProtocol,
        bridge: MT5BridgeClient?,
        eventStore: AgentEventStore,
        logger: Logger,
        supervisorStartedAtUtc: UtcSecond,
        repairOnVerifierMismatch: Bool
    ) {
        self.config = config
        self.clickHouse = clickHouse
        self.bridge = bridge
        self.eventStore = eventStore
        self.logger = logger
        self.supervisorStartedAtUtc = supervisorStartedAtUtc
        self.repairOnVerifierMismatch = repairOnVerifierMismatch
    }

    public func requireBridge(for kind: ProductionAgentKind) throws -> MT5BridgeClient {
        guard let bridge else { throw ProductionAgentError.mt5BridgeUnavailable(kind) }
        return bridge
    }

    public func checkpointStore() -> ClickHouseCheckpointStore {
        ClickHouseCheckpointStore(
            client: clickHouse,
            insertBuilder: ClickHouseInsertBuilder(database: config.clickHouse.database),
            database: config.clickHouse.database
        )
    }

    public func offsetStore() -> ClickHouseBrokerOffsetStore {
        ClickHouseBrokerOffsetStore(client: clickHouse, database: config.clickHouse.database)
    }

    public func insertBuilder() -> ClickHouseInsertBuilder {
        ClickHouseInsertBuilder(database: config.clickHouse.database)
    }
}
