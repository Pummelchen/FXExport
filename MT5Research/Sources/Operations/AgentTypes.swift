import AppCore
import Domain
import Foundation

public enum ProductionAgentKind: String, CaseIterable, Sendable {
    case historyImporter = "history_importer"
    case liveM1Updater = "live_m1_updater"
    case databaseVerifierRepairer = "database_verifier_repairer"
    case utcTimeAuthority = "utc_time_authority"
    case healthMonitor = "health_monitor"
    case supervisorCoordinator = "supervisor_coordinator"
    case symbolMetadataDrift = "symbol_metadata_drift"
    case checkpointGapAuditor = "checkpoint_gap_auditor"
    case backupReadiness = "backup_readiness"
    case alerting = "alerting"

    public var priorityRank: Int {
        switch self {
        case .supervisorCoordinator: return 10
        case .healthMonitor: return 20
        case .utcTimeAuthority: return 30
        case .symbolMetadataDrift: return 40
        case .historyImporter: return 50
        case .liveM1Updater: return 60
        case .databaseVerifierRepairer: return 70
        case .checkpointGapAuditor: return 80
        case .backupReadiness: return 90
        case .alerting: return 100
        }
    }

    public var displayName: String {
        switch self {
        case .historyImporter:
            return "First-run MT5 history importer"
        case .liveM1Updater:
            return "M1 update agent"
        case .databaseVerifierRepairer:
            return "Database verifier and cleaner"
        case .utcTimeAuthority:
            return "UTC time authority"
        case .healthMonitor:
            return "Database and MT5 health monitor"
        case .supervisorCoordinator:
            return "Supervisor coordinator"
        case .symbolMetadataDrift:
            return "Symbol metadata drift checker"
        case .checkpointGapAuditor:
            return "Checkpoint and gap auditor"
        case .backupReadiness:
            return "Backup readiness checker"
        case .alerting:
            return "Monitoring and alerting agent"
        }
    }

    public var terminalColor: TerminalColor {
        switch self {
        case .supervisorCoordinator:
            return .brightCyan
        case .healthMonitor:
            return .brightGreen
        case .utcTimeAuthority:
            return .brightMagenta
        case .symbolMetadataDrift:
            return .brightBlue
        case .historyImporter:
            return .green
        case .liveM1Updater:
            return .cyan
        case .databaseVerifierRepairer:
            return .magenta
        case .checkpointGapAuditor:
            return .yellow
        case .backupReadiness:
            return .blue
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
