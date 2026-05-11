import ClickHouse
import Domain
import Foundation

public protocol AgentEventStore: Sendable {
    func record(_ outcome: AgentOutcome, brokerSourceId: BrokerSourceId) async throws
}

public enum AgentEventStoreError: Error, CustomStringConvertible, Sendable {
    case invalidRuntimeStateRow(String)

    public var description: String {
        switch self {
        case .invalidRuntimeStateRow(let row):
            return "Invalid runtime_agent_state row: \(row)"
        }
    }
}

public struct ClickHouseAgentEventStore: AgentEventStore {
    private let clickHouse: ClickHouseClientProtocol
    private let database: String

    public init(clickHouse: ClickHouseClientProtocol, database: String) {
        self.clickHouse = clickHouse
        self.database = database
    }

    public func record(_ outcome: AgentOutcome, brokerSourceId: BrokerSourceId) async throws {
        let eventRow = [
            Self.tsv(brokerSourceId.rawValue),
            Self.tsv(outcome.agent.rawValue),
            Self.tsv(outcome.status.rawValue),
            Self.tsv(outcome.severity.rawValue),
            Self.tsv(outcome.message),
            Self.tsv(outcome.details),
            String(outcome.startedAtUtc.rawValue),
            String(outcome.finishedAtUtc.rawValue),
            String(outcome.durationMilliseconds)
        ].joined(separator: "\t")
        let eventSQL = """
        INSERT INTO \(database).runtime_agent_events (
            broker_source_id, agent_name, status, severity, message, details,
            started_at_utc, finished_at_utc, duration_ms
        ) FORMAT TabSeparated
        \(eventRow)
        """
        _ = try await clickHouse.execute(.mutation(eventSQL, idempotent: false))

        let previous = try await previousState(
            brokerSourceId: brokerSourceId,
            agent: outcome.agent
        )
        var lastOk = previous?.lastOkAtUtc ?? 0
        var lastError = previous?.lastErrorAtUtc ?? 0
        if outcome.status == .ok {
            lastOk = outcome.finishedAtUtc.rawValue
        }
        if outcome.status == .failed {
            lastError = outcome.finishedAtUtc.rawValue
        }
        let stateRow = [
            Self.tsv(brokerSourceId.rawValue),
            Self.tsv(outcome.agent.rawValue),
            Self.tsv(outcome.status.rawValue),
            Self.tsv(outcome.message),
            String(lastOk),
            String(lastError),
            String(outcome.finishedAtUtc.rawValue)
        ].joined(separator: "\t")
        let stateSQL = """
        INSERT INTO \(database).runtime_agent_state (
            broker_source_id, agent_name, status, last_message,
            last_ok_at_utc, last_error_at_utc, updated_at_utc
        ) FORMAT TabSeparated
        \(stateRow)
        """
        _ = try await clickHouse.execute(.mutation(stateSQL, idempotent: true))
    }

    private func previousState(
        brokerSourceId: BrokerSourceId,
        agent: ProductionAgentKind
    ) async throws -> (lastOkAtUtc: Int64, lastErrorAtUtc: Int64)? {
        let sql = """
        SELECT last_ok_at_utc, last_error_at_utc
        FROM \(database).runtime_agent_state FINAL
        WHERE broker_source_id = '\(SQLText.literal(brokerSourceId.rawValue))'
          AND agent_name = '\(SQLText.literal(agent.rawValue))'
        ORDER BY updated_at_utc DESC
        LIMIT 1
        FORMAT TabSeparated
        """
        let body = try await clickHouse.execute(.select(sql))
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        let fields = trimmed.split(separator: "\t", omittingEmptySubsequences: false)
        guard fields.count >= 2,
              let lastOk = Int64(fields[0]),
              let lastError = Int64(fields[1]) else {
            throw AgentEventStoreError.invalidRuntimeStateRow(trimmed)
        }
        return (lastOk, lastError)
    }

    private static func tsv(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\t", with: "\\t")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }
}

public actor InMemoryAgentEventStore: AgentEventStore {
    public private(set) var outcomes: [AgentOutcome] = []

    public init() {}

    public func record(_ outcome: AgentOutcome, brokerSourceId: BrokerSourceId) async throws {
        outcomes.append(outcome)
    }
}
