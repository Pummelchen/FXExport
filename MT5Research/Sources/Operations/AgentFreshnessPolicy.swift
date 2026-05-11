import Config
import Foundation

public struct AgentFreshnessPolicy: Sendable {
    private let config: ConfigBundle

    public init(config: ConfigBundle) {
        self.config = config
    }

    public func maxOkAgeSeconds(for agent: ProductionAgentKind) -> Int64 {
        let supervisor = config.app.supervisor
        let seconds: Int
        switch agent {
        case .utcTimeAuthority:
            seconds = max(180, supervisor.utcCheckIntervalSeconds * 3)
        case .symbolMetadataDrift:
            seconds = max(900, supervisor.symbolMetadataCheckIntervalSeconds * 3)
        case .liveM1Updater:
            seconds = max(120, config.app.liveScanIntervalSeconds * 6)
        case .databaseVerifierRepairer:
            seconds = max(7_200, supervisor.verificationIntervalSeconds * 2)
        case .checkpointGapAuditor:
            seconds = max(900, supervisor.checkpointAuditIntervalSeconds * 3)
        case .supervisorCoordinator, .healthMonitor, .historyImporter, .backupReadiness, .alerting:
            seconds = 0
        }
        return Int64(seconds)
    }
}
