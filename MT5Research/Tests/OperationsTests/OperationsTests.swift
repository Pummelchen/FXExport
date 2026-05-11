import ClickHouse
import Config
import Domain
@testable import Operations
import XCTest

final class OperationsTests: XCTestCase {
    func testSupervisorConfigDefaultsWhenOmitted() throws {
        let data = """
        {
          "chunk_size": 50000,
          "live_scan_interval_seconds": 10,
          "log_level": "normal",
          "strict_symbol_failures": false,
          "verifier_random_ranges": 3
        }
        """.data(using: .utf8)!

        let config = try JSONDecoder().decode(AppConfigFile.self, from: data)

        XCTAssertEqual(config.supervisor, .default)
    }

    func testAgentSchedulerRunsStartupAgentsAndHonorsRunOnlyOnce() throws {
        let importer = StubAgent(
            descriptor: AgentDescriptor(
                kind: .historyImporter,
                intervalSeconds: 60,
                requiresMT5Bridge: true,
                runOnStart: true,
                runOnlyOnce: true
            )
        )
        let live = StubAgent(
            descriptor: AgentDescriptor(
                kind: .liveM1Updater,
                intervalSeconds: 10,
                requiresMT5Bridge: true,
                runOnStart: true
            )
        )
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        var scheduler = AgentScheduler()

        let firstDue = scheduler.dueAgents(from: [importer, live], now: now).map(\.descriptor.kind)
        XCTAssertEqual(firstDue, [.historyImporter, .liveM1Updater])

        scheduler.markFinished(.historyImporter, at: now, runOnlyOnce: true)
        scheduler.markFinished(.liveM1Updater, at: now, runOnlyOnce: false)

        let afterFiveSeconds = now.addingTimeInterval(5)
        XCTAssertTrue(scheduler.dueAgents(from: [importer, live], now: afterFiveSeconds).isEmpty)

        let afterElevenSeconds = now.addingTimeInterval(11)
        let laterDue = scheduler.dueAgents(from: [importer, live], now: afterElevenSeconds).map(\.descriptor.kind)
        XCTAssertEqual(laterDue, [.liveM1Updater])
    }

    func testInMemoryAgentEventStoreRecordsOutcomes() async throws {
        let store = InMemoryAgentEventStore()
        let broker = try BrokerSourceId("unit-test")
        let outcome = AgentOutcome(
            agent: .healthMonitor,
            status: .ok,
            severity: .info,
            message: "ok",
            startedAtUtc: UtcSecond(rawValue: 1),
            finishedAtUtc: UtcSecond(rawValue: 2),
            durationMilliseconds: 1
        )

        try await store.record(outcome, brokerSourceId: broker)

        let outcomes = await store.outcomes
        XCTAssertEqual(outcomes, [outcome])
    }

    func testClickHouseAgentStatePreservesPreviousOkTimestampOnWarning() async throws {
        let clickHouse = RecordingClickHouse(selectBodies: ["", "10\t0\n"])
        let store = ClickHouseAgentEventStore(clickHouse: clickHouse, database: "db")
        let broker = try BrokerSourceId("unit-test")
        let ok = AgentOutcome(
            agent: .healthMonitor,
            status: .ok,
            severity: .info,
            message: "ok",
            startedAtUtc: UtcSecond(rawValue: 9),
            finishedAtUtc: UtcSecond(rawValue: 10),
            durationMilliseconds: 1
        )
        let warning = AgentOutcome(
            agent: .healthMonitor,
            status: .warning,
            severity: .warning,
            message: "warn",
            startedAtUtc: UtcSecond(rawValue: 19),
            finishedAtUtc: UtcSecond(rawValue: 20),
            durationMilliseconds: 1
        )

        try await store.record(ok, brokerSourceId: broker)
        try await store.record(warning, brokerSourceId: broker)

        let stateInserts = await clickHouse.queries
            .map(\.sql)
            .filter { $0.contains("INSERT INTO db.runtime_agent_state") }
        XCTAssertEqual(stateInserts.count, 2)
        XCTAssertTrue(stateInserts[0].contains("\thealth_monitor\tok\tok\t10\t0\t10"))
        XCTAssertTrue(stateInserts[1].contains("\thealth_monitor\twarning\twarn\t10\t0\t20"))
    }
}

private struct StubAgent: ProductionAgent {
    let descriptor: AgentDescriptor

    func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt).ok("ok")
    }
}

private actor RecordingClickHouse: ClickHouseClientProtocol {
    private var selectBodies: [String]
    private(set) var queries: [ClickHouseQuery] = []

    init(selectBodies: [String]) {
        self.selectBodies = selectBodies
    }

    func execute(_ query: ClickHouseQuery) async throws -> String {
        queries.append(query)
        if query.sql.contains("SELECT last_ok_at_utc") {
            return selectBodies.isEmpty ? "" : selectBodies.removeFirst()
        }
        return ""
    }
}
