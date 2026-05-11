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
        let identity = try terminalIdentity(context: context, bridge: bridge)
        return try await context.offsetStore().loadVerifiedOffsetMap(
            brokerSourceId: context.config.brokerTime.brokerSourceId,
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
