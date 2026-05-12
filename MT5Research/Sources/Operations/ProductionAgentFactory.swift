import Config
import Foundation

public struct ProductionAgentFactory: Sendable {
    public init() {}

    public func makeAgents(config: ConfigBundle, runBackfillOnStart: Bool) -> [any ProductionAgent] {
        let supervisor = config.app.supervisor
        return [
            HistoryImportProductionAgent(
                intervalSeconds: max(60, supervisor.checkpointAuditIntervalSeconds),
                enabled: runBackfillOnStart
            ),
            LiveM1UpdateProductionAgent(intervalSeconds: config.app.liveScanIntervalSeconds),
            DatabaseVerifierRepairProductionAgent(intervalSeconds: supervisor.verificationIntervalSeconds),
            UTCTimeAuthorityProductionAgent(intervalSeconds: supervisor.utcCheckIntervalSeconds),
            HealthMonitorProductionAgent(intervalSeconds: supervisor.healthCheckIntervalSeconds),
            SupervisorCoordinatorAgent(intervalSeconds: supervisor.healthCheckIntervalSeconds),
            SymbolMetadataDriftAgent(intervalSeconds: supervisor.symbolMetadataCheckIntervalSeconds),
            CheckpointGapAuditAgent(intervalSeconds: supervisor.checkpointAuditIntervalSeconds),
            DataCertificationAgent(intervalSeconds: supervisor.backupCheckIntervalSeconds),
            BackupReadinessAgent(intervalSeconds: supervisor.backupCheckIntervalSeconds),
            AlertingAgent(intervalSeconds: supervisor.alertIntervalSeconds)
        ]
    }
}
