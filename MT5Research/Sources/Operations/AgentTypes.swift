import AppCore
import Domain
import Foundation

public enum ProductionAgentKind: String, CaseIterable, Sendable {
    case schemaDriftGuard = "schema_drift_guard"
    case bridgeVersionGuard = "bridge_version_guard"
    case historyImporter = "history_importer"
    case liveM1Updater = "live_m1_updater"
    case databaseVerifierRepairer = "database_verifier_repairer"
    case utcTimeAuthority = "utc_time_authority"
    case healthMonitor = "health_monitor"
    case supervisorCoordinator = "supervisor_coordinator"
    case symbolMetadataDrift = "symbol_metadata_drift"
    case sourceHistoryDrift = "source_history_drift"
    case verificationCoveragePlanner = "verification_coverage_planner"
    case checkpointGapAuditor = "checkpoint_gap_auditor"
    case dataCertification = "data_certification"
    case backupReadiness = "backup_readiness"
    case backupRestoreVerifier = "backup_restore_verifier"
    case alerting = "alerting"

    public var priorityRank: Int {
        switch self {
        case .supervisorCoordinator: return 10
        case .healthMonitor: return 20
        case .schemaDriftGuard: return 25
        case .bridgeVersionGuard: return 30
        case .utcTimeAuthority: return 40
        case .symbolMetadataDrift: return 50
        case .sourceHistoryDrift: return 55
        case .historyImporter: return 60
        case .liveM1Updater: return 70
        case .databaseVerifierRepairer: return 80
        case .verificationCoveragePlanner: return 85
        case .checkpointGapAuditor: return 90
        case .dataCertification: return 95
        case .backupReadiness: return 100
        case .backupRestoreVerifier: return 105
        case .alerting: return 110
        }
    }

    public var displayName: String {
        switch self {
        case .schemaDriftGuard:
            return "Schema Guard"
        case .bridgeVersionGuard:
            return "Bridge Guard"
        case .historyImporter:
            return "History Importer"
        case .liveM1Updater:
            return "M1 Updater"
        case .databaseVerifierRepairer:
            return "Database Cleaner"
        case .utcTimeAuthority:
            return "UTC Time Agent"
        case .healthMonitor:
            return "Health Monitor"
        case .supervisorCoordinator:
            return "Supervisor"
        case .symbolMetadataDrift:
            return "Symbol Guard"
        case .sourceHistoryDrift:
            return "Source History Guard"
        case .verificationCoveragePlanner:
            return "Verification Planner"
        case .checkpointGapAuditor:
            return "Gap Auditor"
        case .dataCertification:
            return "Data Certifier"
        case .backupReadiness:
            return "Backup Readiness"
        case .backupRestoreVerifier:
            return "Restore Verifier"
        case .alerting:
            return "Alert Monitor"
        }
    }

    public var startMessage: String {
        switch self {
        case .schemaDriftGuard:
            return "Checking ClickHouse schema and migration drift"
        case .bridgeVersionGuard:
            return "Checking MT5 bridge version and protocol identity"
        case .historyImporter:
            return "Starting or resuming historical M1 OHLC import"
        case .liveM1Updater:
            return "Checking for newly closed M1 OHLC bars"
        case .databaseVerifierRepairer:
            return "Checking canonical data cleanliness and repair safety"
        case .utcTimeAuthority:
            return "Checking broker UTC offset authority"
        case .healthMonitor:
            return "Checking ClickHouse and MT5 bridge health"
        case .supervisorCoordinator:
            return "Checking supervisor ownership and schedule"
        case .symbolMetadataDrift:
            return "Checking MT5 symbols and digits"
        case .sourceHistoryDrift:
            return "Checking MT5 source history boundaries"
        case .verificationCoveragePlanner:
            return "Checking historical verification coverage plan"
        case .checkpointGapAuditor:
            return "Checking checkpoints, gaps and live lag"
        case .dataCertification:
            return "Creating cryptographic data certificates for verified history"
        case .backupReadiness:
            return "Checking canonical history readiness for backup"
        case .backupRestoreVerifier:
            return "Checking backup restore evidence"
        case .alerting:
            return "Checking runtime alerts and disk pressure"
        }
    }

    public var terminalColor: TerminalColor {
        switch self {
        case .supervisorCoordinator:
            return .brightCyan
        case .healthMonitor:
            return .brightGreen
        case .schemaDriftGuard:
            return .white
        case .bridgeVersionGuard:
            return .brightBlue
        case .utcTimeAuthority:
            return .brightMagenta
        case .symbolMetadataDrift:
            return .brightBlue
        case .sourceHistoryDrift:
            return .cyan
        case .historyImporter:
            return .green
        case .liveM1Updater:
            return .cyan
        case .databaseVerifierRepairer:
            return .magenta
        case .verificationCoveragePlanner:
            return .brightYellow
        case .checkpointGapAuditor:
            return .yellow
        case .dataCertification:
            return .brightYellow
        case .backupReadiness:
            return .blue
        case .backupRestoreVerifier:
            return .brightGreen
        case .alerting:
            return .gray
        }
    }
}

public enum AgentStatus: String, Sendable {
    case ok
    case warning
    case failed
    case skipped
}

public enum AgentSeverity: String, Sendable {
    case info
    case warning
    case error
}

public struct AgentDescriptor: Sendable, Equatable {
    public let kind: ProductionAgentKind
    public let intervalSeconds: Int
    public let requiresMT5Bridge: Bool
    public let runOnStart: Bool
    public let runOnlyOnce: Bool

    public init(
        kind: ProductionAgentKind,
        intervalSeconds: Int,
        requiresMT5Bridge: Bool,
        runOnStart: Bool = true,
        runOnlyOnce: Bool = false
    ) {
        self.kind = kind
        self.intervalSeconds = intervalSeconds
        self.requiresMT5Bridge = requiresMT5Bridge
        self.runOnStart = runOnStart
        self.runOnlyOnce = runOnlyOnce
    }
}

public struct AgentOutcome: Sendable, Equatable {
    public let agent: ProductionAgentKind
    public let status: AgentStatus
    public let severity: AgentSeverity
    public let message: String
    public let details: String
    public let startedAtUtc: UtcSecond
    public let finishedAtUtc: UtcSecond
    public let durationMilliseconds: UInt64

    public init(
        agent: ProductionAgentKind,
        status: AgentStatus,
        severity: AgentSeverity,
        message: String,
        details: String = "",
        startedAtUtc: UtcSecond,
        finishedAtUtc: UtcSecond,
        durationMilliseconds: UInt64
    ) {
        self.agent = agent
        self.status = status
        self.severity = severity
        self.message = message
        self.details = details
        self.startedAtUtc = startedAtUtc
        self.finishedAtUtc = finishedAtUtc
        self.durationMilliseconds = durationMilliseconds
    }
}

public protocol ProductionAgent: Sendable {
    var descriptor: AgentDescriptor { get }
    func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome
}

public enum ProductionAgentError: Error, CustomStringConvertible, Sendable {
    case mt5BridgeUnavailable(ProductionAgentKind)
    case missingCheckpoint(LogicalSymbol)
    case invariant(String)

    public var description: String {
        switch self {
        case .mt5BridgeUnavailable(let kind):
            return "\(kind.rawValue) requires MT5 bridge, but no bridge is connected."
        case .missingCheckpoint(let symbol):
            return "\(symbol.rawValue) has no checkpoint."
        case .invariant(let reason):
            return reason
        }
    }
}

public func utcNow() -> UtcSecond {
    UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))
}

func millisecondsBetween(_ start: Date, _ end: Date) -> UInt64 {
    UInt64(max(0, (end.timeIntervalSince(start) * 1000).rounded()))
}
