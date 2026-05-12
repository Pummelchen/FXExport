import Config
import Foundation

public struct ProductionAgentFactory: Sendable {
    public init() {}

    public func makeAgents(config: ConfigBundle, runBackfillOnStart: Bool) -> [any ProductionAgent] {
        let supervisor = config.app.supervisor
        return [
            SupervisorCoordinatorAgent(intervalSeconds: supervisor.healthCheckIntervalSeconds),
            HealthMonitorProductionAgent(intervalSeconds: supervisor.healthCheckIntervalSeconds),
            SchemaDriftGuardAgent(intervalSeconds: supervisor.healthCheckIntervalSeconds),
            BridgeVersionGuardAgent(intervalSeconds: supervisor.healthCheckIntervalSeconds),
            UTCTimeAuthorityProductionAgent(intervalSeconds: supervisor.utcCheckIntervalSeconds),
            SymbolMetadataDriftAgent(intervalSeconds: supervisor.symbolMetadataCheckIntervalSeconds),
            SourceHistoryDriftAgent(intervalSeconds: supervisor.checkpointAuditIntervalSeconds),
            HistoryImportProductionAgent(
                intervalSeconds: max(60, supervisor.checkpointAuditIntervalSeconds),
                enabled: runBackfillOnStart
            ),
            LiveM1UpdateProductionAgent(intervalSeconds: config.app.liveScanIntervalSeconds),
            DatabaseVerifierRepairProductionAgent(intervalSeconds: supervisor.verificationIntervalSeconds),
            VerificationCoveragePlannerAgent(intervalSeconds: supervisor.verificationIntervalSeconds),
            CheckpointGapAuditAgent(intervalSeconds: supervisor.checkpointAuditIntervalSeconds),
            DataCertificationAgent(intervalSeconds: supervisor.backupCheckIntervalSeconds),
            BackupReadinessAgent(intervalSeconds: supervisor.backupCheckIntervalSeconds),
            BackupRestoreVerifierAgent(intervalSeconds: supervisor.backupCheckIntervalSeconds),
            AlertingAgent(intervalSeconds: supervisor.alertIntervalSeconds)
        ]
    }
}
