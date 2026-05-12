import Foundation

public struct AgentExecutionPolicy: Sendable {
    public init() {}

    public func staticSupersedence(for dueKinds: Set<ProductionAgentKind>) -> [ProductionAgentKind: String] {
        var blocked: [ProductionAgentKind: String] = [:]
        if dueKinds.contains(.historyImporter) {
            block(
                [.liveM1Updater, .databaseVerifierRepairer, .checkpointGapAuditor, .dataCertification, .backupReadiness],
                reason: "history_importer owns first-run/resume canonical writes this cycle",
                into: &blocked
            )
        }
        if dueKinds.contains(.databaseVerifierRepairer) {
            block(
                [.dataCertification, .backupReadiness],
                reason: "database_verifier_repairer must settle data quality before certification or backup readiness",
                into: &blocked
            )
        }
        if dueKinds.contains(.checkpointGapAuditor) {
            block(
                [.dataCertification, .backupReadiness],
                reason: "checkpoint_gap_auditor must validate checkpoint/canonical consistency before certification or backup readiness",
                into: &blocked
            )
        }
        if dueKinds.contains(.dataCertification) {
            block(
                [.backupReadiness],
                reason: "data_certification must write cryptographic certificates before backup readiness",
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
                    [.utcTimeAuthority, .symbolMetadataDrift, .historyImporter, .liveM1Updater, .databaseVerifierRepairer, .checkpointGapAuditor, .dataCertification, .backupReadiness],
                    reason: reason,
                    into: &blocked
                )
            }
        case .utcTimeAuthority:
            block([.historyImporter, .liveM1Updater, .databaseVerifierRepairer], reason: reason, into: &blocked)
        case .symbolMetadataDrift:
            if outcome.status == .failed {
                block([.historyImporter, .liveM1Updater, .databaseVerifierRepairer], reason: reason, into: &blocked)
            }
        case .historyImporter:
            block([.liveM1Updater, .databaseVerifierRepairer, .checkpointGapAuditor, .dataCertification, .backupReadiness], reason: reason, into: &blocked)
        case .databaseVerifierRepairer:
            block([.dataCertification, .backupReadiness], reason: reason, into: &blocked)
        case .checkpointGapAuditor:
            block([.dataCertification, .backupReadiness], reason: reason, into: &blocked)
        case .dataCertification:
            block([.backupReadiness], reason: reason, into: &blocked)
        case .liveM1Updater:
            if outcome.status == .failed {
                block([.backupReadiness], reason: reason, into: &blocked)
            }
        case .supervisorCoordinator, .backupReadiness, .alerting:
            break
        }
        return blocked
    }

    public static let backtestBlockingAgentKinds: Set<ProductionAgentKind> = [
        .historyImporter,
        .liveM1Updater,
        .databaseVerifierRepairer,
        .utcTimeAuthority,
        .symbolMetadataDrift,
        .checkpointGapAuditor,
        .dataCertification
    ]

    public static let backtestRequiredOkAgentKinds: Set<ProductionAgentKind> = [
        .utcTimeAuthority,
        .symbolMetadataDrift,
        .liveM1Updater,
        .databaseVerifierRepairer,
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
