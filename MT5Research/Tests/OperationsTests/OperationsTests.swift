import AppCore
import ClickHouse
import Config
import Domain
import TimeMapping
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

    func testAgentSchedulerSortsByPriority() {
        let backup = StubAgent(descriptor: AgentDescriptor(kind: .backupReadiness, intervalSeconds: 60, requiresMT5Bridge: false))
        let health = StubAgent(descriptor: AgentDescriptor(kind: .healthMonitor, intervalSeconds: 60, requiresMT5Bridge: false))
        let utc = StubAgent(descriptor: AgentDescriptor(kind: .utcTimeAuthority, intervalSeconds: 60, requiresMT5Bridge: true))
        var scheduler = AgentScheduler()

        let due = scheduler.dueAgents(
            from: [backup, utc, health],
            now: Date(timeIntervalSince1970: 1_700_000_000)
        ).map(\.descriptor.kind)

        XCTAssertEqual(due, [.healthMonitor, .utcTimeAuthority, .backupReadiness])
    }

    func testAgentSchedulerDeferredAgentRetriesAfterShortDelay() {
        let verifier = StubAgent(descriptor: AgentDescriptor(kind: .databaseVerifierRepairer, intervalSeconds: 3600, requiresMT5Bridge: false))
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        var scheduler = AgentScheduler()

        scheduler.markDeferred(.databaseVerifierRepairer, at: now, retryAfterSeconds: 10)

        XCTAssertTrue(scheduler.dueAgents(from: [verifier], now: now.addingTimeInterval(9)).isEmpty)
        XCTAssertEqual(
            scheduler.dueAgents(from: [verifier], now: now.addingTimeInterval(11)).map(\.descriptor.kind),
            [.databaseVerifierRepairer]
        )
    }

    func testDisabledHistoryImporterDoesNotSupersedeLiveStartup() throws {
        let config = try makeConfig()
        let agents = ProductionAgentFactory().makeAgents(config: config, runBackfillOnStart: false)
        var scheduler = AgentScheduler()

        let due = scheduler.dueAgents(
            from: agents,
            now: Date(timeIntervalSince1970: 1_700_000_000)
        ).map(\.descriptor.kind)

        XCTAssertFalse(due.contains(.historyImporter))
        XCTAssertTrue(due.contains(.liveM1Updater))
    }

    func testEnabledHistoryImporterUsesCheckpointAuditRetryInterval() throws {
        let config = try makeConfig()
        let agents = ProductionAgentFactory().makeAgents(config: config, runBackfillOnStart: true)
        let importer = try XCTUnwrap(agents.first { $0.descriptor.kind == .historyImporter })

        XCTAssertEqual(importer.descriptor.intervalSeconds, config.app.supervisor.checkpointAuditIntervalSeconds)
    }

    func testClickHouseStartupManagerRunsStartCommandForLocalTransportFailure() async throws {
        let config = try makeConfig()
        let client = StartupClickHouse(failuresBeforeSuccess: 1)
        let command = SystemCommandRequest(
            executable: URL(fileURLWithPath: "/bin/echo"),
            arguments: ["start-clickhouse"],
            timeoutSeconds: 1
        )
        let runner = RecordingCommandRunner(resultExitCode: 0)
        let manager = ClickHouseStartupManager(
            config: config.clickHouse,
            client: client,
            logger: Logger(level: .quiet),
            commandRunner: runner,
            startCommands: [command],
            startupWaitSeconds: 0.1,
            pollIntervalNanoseconds: 1_000_000
        )

        try await manager.ensureReady()

        let commands = await runner.executedCommands()
        XCTAssertEqual(commands, ["/bin/echo start-clickhouse"])
    }

    func testClickHouseStartupManagerDoesNotStartRemoteEndpoint() async throws {
        var config = try makeConfig()
        config = ConfigBundle(
            app: config.app,
            clickHouse: ClickHouseConfig(
                url: try XCTUnwrap(URL(string: "http://db.example.com:8123")),
                database: config.clickHouse.database,
                username: nil,
                password: nil,
                requestTimeoutSeconds: 1,
                retryCount: 0
            ),
            mt5Bridge: config.mt5Bridge,
            brokerTime: config.brokerTime,
            symbols: config.symbols
        )
        let client = StartupClickHouse(failuresBeforeSuccess: Int.max)
        let runner = RecordingCommandRunner(resultExitCode: 0)
        let manager = ClickHouseStartupManager(
            config: config.clickHouse,
            client: client,
            logger: Logger(level: .quiet),
            commandRunner: runner,
            startCommands: [SystemCommandRequest(executable: URL(fileURLWithPath: "/bin/echo"), arguments: ["start"], timeoutSeconds: 1)]
        )

        await XCTAssertThrowsErrorAsync(try await manager.ensureReady()) { error in
            guard case ClickHouseStartupError.notAutoStartable = error else {
                XCTFail("Expected notAutoStartable, got \(error)")
                return
            }
        }
        let commands = await runner.executedCommands()
        XCTAssertTrue(commands.isEmpty)
    }

    func testOperationalFailureGuideCatalogCoversCoreDataSafetyFailures() {
        let text = OperationalFailureGuide.catalogText()

        XCTAssertTrue(text.contains("ClickHouse HTTP endpoint is down"))
        XCTAssertTrue(text.contains("MT5 bridge disconnects during live run"))
        XCTAssertTrue(text.contains("MetaEditor EA compile or toolchain failure"))
        XCTAssertTrue(text.contains("Missing verified broker UTC offsets"))
        XCTAssertTrue(text.contains("Canonical insert readback verification failed"))
        XCTAssertTrue(text.contains("Backtest data readiness blocked"))
        XCTAssertTrue(text.contains("Disk full or ClickHouse storage pressure"))
        XCTAssertTrue(text.contains("Computer sleep, shutdown, or process interruption"))
    }

    func testOperationalFailureGuideMapsBacktestBlockToStopAdvice() {
        let advice = OperationalFailureGuide.advice(for: BacktestReadinessError.duplicateCanonicalKeys(2))

        XCTAssertEqual(advice.code, "BACKTEST-001")
        XCTAssertEqual(advice.severity, RecoverySeverity.stop)
        XCTAssertTrue(advice.dataSafety.contains("Research never runs"))
    }

    func testAgentExecutionPolicySupersedesConflictingAgents() {
        let policy = AgentExecutionPolicy()
        let staticBlocked = policy.staticSupersedence(for: [.historyImporter, .liveM1Updater, .databaseVerifierRepairer, .backupReadiness])
        XCTAssertEqual(staticBlocked[.liveM1Updater], "history_importer owns first-run/resume canonical writes this cycle")
        XCTAssertEqual(staticBlocked[.databaseVerifierRepairer], "history_importer owns first-run/resume canonical writes this cycle")
        XCTAssertEqual(staticBlocked[.backupReadiness], "history_importer owns first-run/resume canonical writes this cycle")

        let outcome = AgentOutcome(
            agent: .utcTimeAuthority,
            status: .failed,
            severity: .error,
            message: "offset mismatch",
            startedAtUtc: UtcSecond(rawValue: 1),
            finishedAtUtc: UtcSecond(rawValue: 2),
            durationMilliseconds: 1
        )
        let dynamicBlocked = policy.dynamicSupersedence(after: outcome)
        XCTAssertTrue(dynamicBlocked.keys.contains(.historyImporter))
        XCTAssertTrue(dynamicBlocked.keys.contains(.liveM1Updater))
        XCTAssertTrue(dynamicBlocked.keys.contains(.databaseVerifierRepairer))
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

    func testMetaEditorWinePathArgumentUsesZDriveAbsolutePath() {
        let url = URL(fileURLWithPath: "/tmp/Project/EA/HistoryBridgeEA.mq5")
        XCTAssertEqual(
            MetaEditorToolchain.winePathArgument(url),
            "Z:\\tmp\\Project\\EA\\HistoryBridgeEA.mq5"
        )
    }

    func testStartCheckDetectsOffsetCoverageGaps() throws {
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker Ltd", server: "Broker-Server", accountLogin: 1)
        let map = try BrokerOffsetMap(
            brokerSourceId: broker,
            terminalIdentity: identity,
            segments: [
                BrokerOffsetSegment(
                    brokerSourceId: broker,
                    terminalIdentity: identity,
                    validFrom: MT5ServerSecond(rawValue: 0),
                    validTo: MT5ServerSecond(rawValue: 120),
                    offset: OffsetSeconds(rawValue: 7200),
                    source: .manual,
                    confidence: .verified
                ),
                BrokerOffsetSegment(
                    brokerSourceId: broker,
                    terminalIdentity: identity,
                    validFrom: MT5ServerSecond(rawValue: 240),
                    validTo: MT5ServerSecond(rawValue: 360),
                    offset: OffsetSeconds(rawValue: 7200),
                    source: .manual,
                    confidence: .verified
                )
            ]
        )

        let gaps = StartCheckRunner.coverageGaps(
            in: map,
            from: MT5ServerSecond(rawValue: 60),
            toExclusive: MT5ServerSecond(rawValue: 300)
        )

        XCTAssertEqual(gaps, ["120..<240"])
    }

    func testBacktestReadinessGatePassesWhenDataAndAgentsAreClean() async throws {
        let config = try makeConfig()
        let clickHouse = BacktestGateClickHouse(mode: .clean)
        let gate = BacktestReadinessGate(config: config, clickHouse: clickHouse)

        try await gate.assertReady(BacktestReadinessRequest(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStart: UtcSecond(rawValue: 60),
            utcEndExclusive: UtcSecond(rawValue: 120)
        ))
    }

    func testBacktestReadinessGateBlocksInterruptedBackfill() async throws {
        let config = try makeConfig()
        let clickHouse = BacktestGateClickHouse(mode: .interruptedBackfill)
        let gate = BacktestReadinessGate(config: config, clickHouse: clickHouse)

        await XCTAssertThrowsErrorAsync(try await gate.assertReady(BacktestReadinessRequest(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStart: UtcSecond(rawValue: 60),
            utcEndExclusive: UtcSecond(rawValue: 120)
        ))) { error in
            guard case BacktestReadinessError.incompleteIngest = error else {
                XCTFail("Expected incompleteIngest, got \(error)")
                return
            }
        }
    }

    func testBacktestReadinessGateBlocksFailedAgentState() async throws {
        let config = try makeConfig()
        let clickHouse = BacktestGateClickHouse(mode: .failedAgentState)
        let gate = BacktestReadinessGate(config: config, clickHouse: clickHouse)

        await XCTAssertThrowsErrorAsync(try await gate.assertReady(BacktestReadinessRequest(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStart: UtcSecond(rawValue: 60),
            utcEndExclusive: UtcSecond(rawValue: 120)
        ))) { error in
            guard case BacktestReadinessError.blockingAgentState = error else {
                XCTFail("Expected blockingAgentState, got \(error)")
                return
            }
        }
    }

    func testBacktestReadinessGateBlocksMissingRequiredAgentState() async throws {
        let config = try makeConfig()
        let clickHouse = BacktestGateClickHouse(mode: .missingRequiredAgentState)
        let gate = BacktestReadinessGate(config: config, clickHouse: clickHouse)

        await XCTAssertThrowsErrorAsync(try await gate.assertReady(BacktestReadinessRequest(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStart: UtcSecond(rawValue: 60),
            utcEndExclusive: UtcSecond(rawValue: 120)
        ))) { error in
            guard case BacktestReadinessError.missingRequiredAgentState = error else {
                XCTFail("Expected missingRequiredAgentState, got \(error)")
                return
            }
        }
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

private enum BacktestGateMode {
    case clean
    case interruptedBackfill
    case failedAgentState
    case missingRequiredAgentState
}

private actor BacktestGateClickHouse: ClickHouseClientProtocol {
    private let mode: BacktestGateMode

    init(mode: BacktestGateMode) {
        self.mode = mode
    }

    func execute(_ query: ClickHouseQuery) async throws -> String {
        let sql = query.sql
        if sql.contains("FROM db.ingest_state") {
            let status = mode == .interruptedBackfill && sql.contains("logical_symbol = 'EURUSD'") ? "backfilling" : "live"
            if sql.contains("logical_symbol = 'EURUSD'") {
                return "demo\tEURUSD\tEURUSD\t0\t180\t180\t\(status)\tbatch\t200\n"
            }
            if sql.contains("logical_symbol = 'USDJPY'") {
                return "demo\tUSDJPY\tUSDJPY\t0\t180\t180\tlive\tbatch\t200\n"
            }
            return ""
        }
        if sql.contains("runtime_agent_state") {
            if sql.contains("status IN") {
                return mode == .failedAgentState ? "database_verifier_repairer\tfailed\tverification mismatch\n" : ""
            }
            guard mode != .missingRequiredAgentState else { return "" }
            let now = Int64(Date().timeIntervalSince1970)
            return """
            utc_time_authority\t\(now)
            symbol_metadata_drift\t\(now)
            live_m1_updater\t\(now)
            database_verifier_repairer\t\(now)
            checkpoint_gap_auditor\t\(now)

            """
        }
        if sql.contains("ohlc_m1_canonical") && sql.contains("ts_utc >= 60") && sql.contains("ts_utc < 120") {
            return "10\n"
        }
        return "0\n"
    }
}

private actor StartupClickHouse: ClickHouseClientProtocol {
    private var failuresBeforeSuccess: Int

    init(failuresBeforeSuccess: Int) {
        self.failuresBeforeSuccess = failuresBeforeSuccess
    }

    func execute(_ query: ClickHouseQuery) async throws -> String {
        if failuresBeforeSuccess > 0 {
            failuresBeforeSuccess -= 1
            throw ClickHouseError.transport("connection refused")
        }
        return "1\n"
    }
}

private actor RecordingCommandRunner: SystemCommandRunning {
    private var commands: [String] = []
    private let resultExitCode: Int32

    init(resultExitCode: Int32) {
        self.resultExitCode = resultExitCode
    }

    func run(_ request: SystemCommandRequest) async throws -> SystemCommandResult {
        commands.append(request.display)
        return SystemCommandResult(
            request: request,
            exitCode: resultExitCode,
            stdout: "",
            stderr: ""
        )
    }

    func executedCommands() -> [String] {
        commands
    }
}

private func makeConfig() throws -> ConfigBundle {
    let appData = """
    {
      "chunk_size": 50000,
      "live_scan_interval_seconds": 10,
      "log_level": "normal",
      "strict_symbol_failures": false,
      "verifier_random_ranges": 0
    }
    """.data(using: .utf8)!
    return ConfigBundle(
        app: try JSONDecoder().decode(AppConfigFile.self, from: appData),
        clickHouse: ClickHouseConfig(
            url: URL(string: "http://localhost:8123")!,
            database: "db",
            username: nil,
            password: nil,
            requestTimeoutSeconds: 10,
            retryCount: 0
        ),
        mt5Bridge: MT5BridgeConfig(
            mode: .listen,
            host: "127.0.0.1",
            port: 5055,
            connectTimeoutSeconds: 10,
            requestTimeoutSeconds: 10
        ),
        brokerTime: BrokerTimeConfig(
            brokerSourceId: try BrokerSourceId("demo"),
            offsetSegments: []
        ),
        symbols: SymbolConfig(symbols: [
            SymbolMapping(logicalSymbol: try LogicalSymbol("EURUSD"), mt5Symbol: try MT5Symbol("EURUSD"), digits: try Digits(5)),
            SymbolMapping(logicalSymbol: try LogicalSymbol("USDJPY"), mt5Symbol: try MT5Symbol("USDJPY"), digits: try Digits(3))
        ])
    )
}

private func XCTAssertThrowsErrorAsync<T>(
    _ expression: @autoclosure () async throws -> T,
    file: StaticString = #filePath,
    line: UInt = #line,
    _ errorHandler: (Error) -> Void = { _ in }
) async {
    do {
        _ = try await expression()
        XCTFail("Expected async expression to throw", file: file, line: line)
    } catch {
        errorHandler(error)
    }
}
