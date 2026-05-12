import Foundation

public struct AgentExecutionPolicy: Sendable {
    public init() {}

    public func staticSupersedence(for dueKinds: Set<ProductionAgentKind>) -> [ProductionAgentKind: String] {
        var blocked: [ProductionAgentKind: String] = [:]
        if dueKinds.contains(.historyImporter) {
            block(
                [.liveM1Updater, .sourceHistoryDrift, .databaseVerifierRepairer, .verificationCoveragePlanner, .checkpointGapAuditor, .dataCertification, .backupReadiness, .backupRestoreVerifier],
                reason: "history_importer owns first-run/resume canonical writes this cycle",
                into: &blocked
            )
        }
        if dueKinds.contains(.databaseVerifierRepairer) {
            block(
                [.verificationCoveragePlanner, .dataCertification, .backupReadiness, .backupRestoreVerifier],
                reason: "database_verifier_repairer must settle data quality before certification or backup readiness",
                into: &blocked
            )
        }
        if dueKinds.contains(.verificationCoveragePlanner) {
            block(
                [.dataCertification, .backupReadiness, .backupRestoreVerifier],
                reason: "verification_coverage_planner must settle historical verification coverage before certification or backup readiness",
                into: &blocked
            )
        }
        if dueKinds.contains(.checkpointGapAuditor) {
            block(
                [.dataCertification, .backupReadiness, .backupRestoreVerifier],
                reason: "checkpoint_gap_auditor must validate checkpoint/canonical consistency before certification or backup readiness",
                into: &blocked
            )
        }
        if dueKinds.contains(.dataCertification) {
            block(
                [.backupReadiness, .backupRestoreVerifier],
                reason: "data_certification must write cryptographic certificates before backup readiness",
                into: &blocked
            )
        }
        if dueKinds.contains(.backupReadiness) {
            block(
                [.backupRestoreVerifier],
                reason: "backup_readiness must confirm backup inputs before restore evidence can be checked",
                into: &blocked
            )
        }
        return blocked
    }

    public func dynamicSupersedence(after outcome: AgentOutcome) -> [ProductionAgentKind: String] {
        guard outcome.status == .failed || outcome.status == .warning else { return [:] }
        let reason = "\(outcome.agent.rawValue) reported \(outcome.status.rawValue): \(outcome.message)"
        var blocked: [ProductionAgentKind: String] = [:]
        switch outcome.agent {
        case .healthMonitor:
            if outcome.status == .failed {
                block(
                    [.schemaDriftGuard, .bridgeVersionGuard, .utcTimeAuthority, .symbolMetadataDrift, .sourceHistoryDrift, .historyImporter, .liveM1Updater, .databaseVerifierRepairer, .verificationCoveragePlanner, .checkpointGapAuditor, .dataCertification, .backupReadiness, .backupRestoreVerifier],
                    reason: reason,
                    into: &blocked
                )
            }
        case .schemaDriftGuard:
            block([.bridgeVersionGuard, .utcTimeAuthority, .symbolMetadataDrift, .sourceHistoryDrift, .historyImporter, .liveM1Updater, .databaseVerifierRepairer, .verificationCoveragePlanner, .checkpointGapAuditor, .dataCertification, .backupReadiness, .backupRestoreVerifier], reason: reason, into: &blocked)
        case .bridgeVersionGuard:
            block([.utcTimeAuthority, .symbolMetadataDrift, .sourceHistoryDrift, .historyImporter, .liveM1Updater, .databaseVerifierRepairer], reason: reason, into: &blocked)
        case .utcTimeAuthority:
            block([.historyImporter, .liveM1Updater, .databaseVerifierRepairer], reason: reason, into: &blocked)
        case .symbolMetadataDrift:
            if outcome.status == .failed {
                block([.sourceHistoryDrift, .historyImporter, .liveM1Updater, .databaseVerifierRepairer], reason: reason, into: &blocked)
            }
        case .sourceHistoryDrift:
            block([.dataCertification, .backupReadiness, .backupRestoreVerifier], reason: reason, into: &blocked)
        case .historyImporter:
            block([.liveM1Updater, .sourceHistoryDrift, .databaseVerifierRepairer, .verificationCoveragePlanner, .checkpointGapAuditor, .dataCertification, .backupReadiness, .backupRestoreVerifier], reason: reason, into: &blocked)
        case .databaseVerifierRepairer:
            block([.verificationCoveragePlanner, .dataCertification, .backupReadiness, .backupRestoreVerifier], reason: reason, into: &blocked)
        case .verificationCoveragePlanner:
            block([.dataCertification, .backupReadiness, .backupRestoreVerifier], reason: reason, into: &blocked)
        case .checkpointGapAuditor:
            block([.dataCertification, .backupReadiness, .backupRestoreVerifier], reason: reason, into: &blocked)
        case .dataCertification:
            block([.backupReadiness, .backupRestoreVerifier], reason: reason, into: &blocked)
        case .liveM1Updater:
            if outcome.status == .failed {
                block([.backupReadiness, .backupRestoreVerifier], reason: reason, into: &blocked)
            }
        case .backupReadiness:
            block([.backupRestoreVerifier], reason: reason, into: &blocked)
        case .supervisorCoordinator, .backupRestoreVerifier, .alerting:
            break
        }
        return blocked
    }

    public static let backtestBlockingAgentKinds: Set<ProductionAgentKind> = [
        .schemaDriftGuard,
        .bridgeVersionGuard,
        .historyImporter,
        .liveM1Updater,
        .databaseVerifierRepairer,
        .utcTimeAuthority,
        .symbolMetadataDrift,
        .sourceHistoryDrift,
        .verificationCoveragePlanner,
        .checkpointGapAuditor,
        .dataCertification
    ]

    public static let backtestRequiredOkAgentKinds: Set<ProductionAgentKind> = [
        .schemaDriftGuard,
        .bridgeVersionGuard,
        .utcTimeAuthority,
        .symbolMetadataDrift,
        .sourceHistoryDrift,
        .liveM1Updater,
        .databaseVerifierRepairer,
        .verificationCoveragePlanner,
        .checkpointGapAuditor,
        .dataCertification
    ]

    private func block(
        _ kinds: [ProductionAgentKind],
        reason: String,
        into blocked: inout [ProductionAgentKind: String]
    ) {
        for kind in kinds where blocked[kind] == nil {
            blocked[kind] = reason
        }
    }
}
