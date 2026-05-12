import Foundation
import Ingestion
import MT5Bridge

public struct BridgeVersionGuardAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .bridgeVersionGuard,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: true
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let bridge = try context.requireBridge(for: descriptor.kind)
        let hello = try bridge.hello()
        let terminal = try bridge.terminalInfo()
        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)

        guard hello.bridgeName == "FXExport" else {
            return factory.failed(
                "Unexpected MT5 bridge attached",
                details: "bridge_name=\(hello.bridgeName); expected=FXExport"
            )
        }
        guard hello.schemaVersion == FramedProtocolCodec.schemaVersion else {
            return factory.failed(
                "MT5 bridge protocol schema mismatch",
                details: "schema=\(hello.schemaVersion); expected=\(FramedProtocolCodec.schemaVersion)"
            )
        }
        guard !hello.bridgeVersion.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return factory.failed("MT5 bridge version is empty")
        }

        _ = try TerminalIdentityPolicy().resolve(
            actual: terminal,
            brokerSourceId: context.config.brokerTime.brokerSourceId,
            expected: context.config.brokerTime.expectedTerminalIdentity,
            logger: context.logger
        )

        return factory.ok(
            "MT5 bridge version and identity are correct",
            details: "bridge=\(hello.bridgeName); version=\(hello.bridgeVersion); schema=\(hello.schemaVersion); server=\(terminal.server); account=\(terminal.accountLogin)"
        )
    }
}
