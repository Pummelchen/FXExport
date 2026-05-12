import Config
import Domain
import Foundation
import Ingestion
import MT5Bridge
import TimeMapping

struct BrokerAuthority: Sendable {
    func terminalIdentity(context: AgentRuntimeContext, bridge: MT5BridgeClient) throws -> BrokerServerIdentity {
        let terminal = try bridge.terminalInfo()
        return try TerminalIdentityPolicy().resolve(
            actual: terminal,
            brokerSourceId: context.config.brokerTime.brokerSourceId,
            expected: context.config.brokerTime.expectedTerminalIdentity,
            logger: context.logger
        )
    }

    func verifiedOffsetMap(context: AgentRuntimeContext, bridge: MT5BridgeClient) async throws -> BrokerOffsetMap {
        let terminal = try bridge.terminalInfo()
        let resolved = try await BrokerSourceRegistry(
            client: context.clickHouse,
            database: context.config.clickHouse.database
        ).resolve(terminalInfo: terminal)
        let brokerSourceId = context.config.brokerTime.isAutomatic ? resolved.brokerSourceId : context.config.brokerTime.brokerSourceId
        let identity = try TerminalIdentityPolicy().resolve(
            actual: terminal,
            brokerSourceId: brokerSourceId,
            expected: context.config.brokerTime.expectedTerminalIdentity,
            logger: context.logger
        )
        try await BrokerOffsetAutoAuthority(
            clickHouse: context.clickHouse,
            database: context.config.clickHouse.database,
            logger: context.logger
        ).ensureLiveSegmentIfMissing(
            brokerSourceId: brokerSourceId,
            terminalIdentity: identity,
            snapshot: bridge.serverTimeSnapshot()
        )
        return try await context.offsetStore().loadVerifiedOffsetMap(
            brokerSourceId: brokerSourceId,
            terminalIdentity: identity
        )
    }

    func verifyLiveOffset(context: AgentRuntimeContext, bridge: MT5BridgeClient) async throws -> BrokerOffsetMap {
        let offsetMap = try await verifiedOffsetMap(context: context, bridge: bridge)
        try BrokerOffsetRuntimeVerifier().verify(
            snapshot: bridge.serverTimeSnapshot(),
            offsetMap: offsetMap,
            acceptedLiveOffsetSeconds: context.config.brokerTime.acceptedLiveOffsetSeconds,
            logger: context.logger
        )
        return offsetMap
    }
}
