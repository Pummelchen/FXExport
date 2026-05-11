import Domain
import Foundation

struct AgentOutcomeFactory {
    let kind: ProductionAgentKind
    let startedAt: Date

    func ok(_ message: String, details: String = "") -> AgentOutcome {
        make(status: .ok, severity: .info, message: message, details: details)
    }

    func warning(_ message: String, details: String = "") -> AgentOutcome {
        make(status: .warning, severity: .warning, message: message, details: details)
    }

    func skipped(_ message: String, details: String = "") -> AgentOutcome {
        make(status: .skipped, severity: .info, message: message, details: details)
    }

    func failed(_ message: String, details: String = "") -> AgentOutcome {
        make(status: .failed, severity: .error, message: message, details: details)
    }

    private func make(status: AgentStatus, severity: AgentSeverity, message: String, details: String) -> AgentOutcome {
        let finishedAt = Date()
        return AgentOutcome(
            agent: kind,
            status: status,
            severity: severity,
            message: message,
            details: details,
            startedAtUtc: UtcSecond(rawValue: Int64(startedAt.timeIntervalSince1970)),
            finishedAtUtc: UtcSecond(rawValue: Int64(finishedAt.timeIntervalSince1970)),
            durationMilliseconds: millisecondsBetween(startedAt, finishedAt)
        )
    }
}
