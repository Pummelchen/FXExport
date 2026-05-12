import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import Ingestion
import MT5Bridge
import TimeMapping
import Verification

public struct StartCheckOptions: Sendable {
    public let migrationsDirectory: URL
    public let workingDirectory: URL
    public let compileEA: Bool
    public let bridgeChecks: Bool
    public let compileTimeoutSeconds: TimeInterval

    public init(
        migrationsDirectory: URL,
        workingDirectory: URL,
        compileEA: Bool = true,
        bridgeChecks: Bool = true,
        compileTimeoutSeconds: TimeInterval = 120
    ) {
        self.migrationsDirectory = migrationsDirectory
        self.workingDirectory = workingDirectory
        self.compileEA = compileEA
        self.bridgeChecks = bridgeChecks
        self.compileTimeoutSeconds = compileTimeoutSeconds
    }
}

public struct StartCheckRunner: Sendable {
    public typealias BridgeConnector = @Sendable () throws -> MT5BridgeClient

    private let config: ConfigBundle
    private let clickHouse: ClickHouseClientProtocol
    private let logger: Logger
    private let bridgeConnector: BridgeConnector
    private let options: StartCheckOptions

    public init(
        config: ConfigBundle,
        clickHouse: ClickHouseClientProtocol,
        logger: Logger,
        bridgeConnector: @escaping BridgeConnector,
        options: StartCheckOptions
    ) {
        self.config = config
        self.clickHouse = clickHouse
        self.logger = logger
        self.bridgeConnector = bridgeConnector
        self.options = options
    }

    public func run() async -> Bool {
        var result = StartCheckResult()
        logger.info("STARTCHECK: validating the production path before go-live")
        logger.info("STARTCHECK: this command applies idempotent migrations, checks the EA, checks MT5, and verifies UTC authority")

        await step("1/7 ClickHouse connectivity and migrations", result: &result) {
            _ = try await clickHouse.execute(.select("SELECT 1", databaseOverride: "default"))
            try await ClickHouseMigrator(client: clickHouse, config: config.clickHouse, logger: logger)
                .migrate(migrationsDirectory: options.migrationsDirectory)
        }

        await step("2/7 Required ClickHouse tables", result: &result) {
            try await verifyRequiredTables()
        }

        await step("3/7 Database-only data integrity checks", result: &result) {
            try await VerificationAgent(config: config, bridge: nil, clickHouse: clickHouse, logger: logger)
                .startupChecks(randomRanges: 0)
        }

        if options.compileEA {
            await step("4/7 MetaEditor EA compile check", result: &result) {
                let compiler = MetaEditorCompiler(workingDirectory: options.workingDirectory)
                let compile = try await compiler.compileHistoryBridgeEA(timeoutSeconds: options.compileTimeoutSeconds)
                logger.ok("EA compiled: \(compile.outputPath.path)")
                if !compile.logSummary.isEmpty {
                    logger.verbose(compile.logSummary)
                }
            }
        } else {
            logger.warn("STARTCHECK 4/7 MetaEditor EA compile check skipped by option")
            result.warningCount += 1
        }

        guard options.bridgeChecks else {
            logger.warn("STARTCHECK: MT5 bridge checks skipped by option")
            result.warningCount += 1
            return result.finish(logger: logger)
        }

        var bridge: MT5BridgeClient?
        await step("5/7 MT5 bridge connection and terminal identity", result: &result) {
            logger.info("Action needed if this waits: start MT5, attach HistoryBridgeEA, allow localhost sockets, and set SwiftHost/SwiftPort from Config/mt5_bridge.json")
            let connectedBridge = try bridgeConnector()
            let hello = try connectedBridge.hello()
            guard hello.schemaVersion == FramedProtocolCodec.schemaVersion else {
                throw StartCheckError.invalidBridge("bridge schema \(hello.schemaVersion), expected \(FramedProtocolCodec.schemaVersion)")
            }
            let terminal = try connectedBridge.terminalInfo()
            _ = try TerminalIdentityPolicy().resolve(
                actual: terminal,
                brokerSourceId: config.brokerTime.brokerSourceId,
                expected: config.brokerTime.expectedTerminalIdentity,
                logger: logger
            )
            bridge = connectedBridge
            logger.ok("MT5 bridge connected: \(hello.bridgeName) \(hello.bridgeVersion), server \(terminal.server), account \(terminal.accountLogin)")
        }

        guard let bridge else {
            logger.error("STARTCHECK stopped before MT5-backed checks. Next action: fix the EA/socket setup, then rerun startcheck.")
            return result.finish(logger: logger)
        }

        await step("6/7 Broker UTC authority and historical coverage", result: &result) {
            let terminal = try bridge.terminalInfo()
            let identity = try TerminalIdentityPolicy().resolve(
                actual: terminal,
                brokerSourceId: config.brokerTime.brokerSourceId,
                expected: config.brokerTime.expectedTerminalIdentity,
                logger: logger
            )
            let offsetMap = try await ClickHouseBrokerOffsetStore(client: clickHouse, database: config.clickHouse.database)
                .loadVerifiedOffsetMap(
                    brokerSourceId: config.brokerTime.brokerSourceId,
                    terminalIdentity: identity
                )
            try BrokerOffsetRuntimeVerifier().verify(
                snapshot: bridge.serverTimeSnapshot(),
                offsetMap: offsetMap,
                acceptedLiveOffsetSeconds: config.brokerTime.acceptedLiveOffsetSeconds,
                logger: logger
            )
            try verifyOffsetCoverage(bridge: bridge, offsetMap: offsetMap)
        }

        await step("7/7 Symbol, latest closed bar, and position API smoke check", result: &result) {
            try verifySymbolsAndBridgeCommands(bridge: bridge)
        }

        return result.finish(logger: logger)
    }

    private func step(_ title: String, result: inout StartCheckResult, operation: () async throws -> Void) async {
        logger.info("STARTCHECK \(title)")
        do {
            try await operation()
            logger.ok("STARTCHECK \(title) passed")
        } catch {
            result.failureCount += 1
            logger.error("STARTCHECK \(title) failed: \(error)")
            logger.info(StartCheckGuidance.guidance(for: error))
        }
    }

    private func verifyRequiredTables() async throws {
        let requiredTables = [
            "mt5_ohlc_m1_raw",
            "ohlc_m1_canonical",
            "ohlc_m1_conflicts",
            "broker_time_offsets",
            "ingest_state",
            "verification_results",
            "repair_log",
            "runtime_agent_events",
            "runtime_agent_state"
        ]
        let quoted = requiredTables.map { "'\(SQLText.literal($0))'" }.joined(separator: ",")
        let sql = """
        SELECT name
        FROM system.tables
        WHERE database = '\(SQLText.literal(config.clickHouse.database))'
          AND name IN (\(quoted))
        FORMAT TabSeparated
        """
        let body = try await clickHouse.execute(.select(sql, databaseOverride: "default"))
        let found = Set(body.split(separator: "\n", omittingEmptySubsequences: true).map(String.init))
        let missing = requiredTables.filter { !found.contains($0) }
        guard missing.isEmpty else {
            throw StartCheckError.missingTables(missing)
        }
    }

    private func verifyOffsetCoverage(bridge: MT5BridgeClient, offsetMap: BrokerOffsetMap) throws {
        var failures: [String] = []
        for mapping in config.symbols.symbols {
            let info = try bridge.prepareSymbol(mapping.mt5Symbol)
            guard info.selected, info.digits == mapping.digits.rawValue else {
                throw StartCheckError.invalidBridge("\(mapping.mt5Symbol.rawValue) is not prepared with configured digits before offset coverage check")
            }
            let status = try bridge.historyStatus(mapping.mt5Symbol)
            guard status.mt5Symbol == mapping.mt5Symbol.rawValue,
                  status.synchronized,
                  status.bars > 0 else {
                throw StartCheckError.invalidBridge("\(mapping.mt5Symbol.rawValue) M1 history is not synchronized in MT5")
            }
            let oldest = try bridge.oldestM1BarTime(mapping.mt5Symbol)
            let latest = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
            guard oldest.mt5Symbol == mapping.mt5Symbol.rawValue,
                  latest.mt5Symbol == mapping.mt5Symbol.rawValue else {
                throw StartCheckError.invalidBridge("symbol mismatch while checking offset coverage for \(mapping.logicalSymbol.rawValue)")
            }
            let oldestTime = MT5ServerSecond(rawValue: oldest.mt5ServerTime)
            let latestExclusive = try Self.addOneMinute(latest.mt5ServerTime)
            let gaps = Self.coverageGaps(in: offsetMap, from: oldestTime, toExclusive: latestExclusive)
            if !gaps.isEmpty {
                failures.append("\(mapping.logicalSymbol.rawValue): \(gaps.prefix(3).joined(separator: ", "))")
            }
        }
        guard failures.isEmpty else {
            throw StartCheckError.offsetCoverageGaps(failures)
        }
    }

    private func verifySymbolsAndBridgeCommands(bridge: MT5BridgeClient) throws {
        guard let first = config.symbols.symbols.first else {
            throw StartCheckError.invalidBridge("no symbols configured")
        }
        for mapping in config.symbols.symbols {
            let info = try bridge.prepareSymbol(mapping.mt5Symbol)
            guard info.selected else {
                throw StartCheckError.invalidBridge("\(mapping.mt5Symbol.rawValue) is not selected")
            }
            guard info.digits == mapping.digits.rawValue else {
                throw StartCheckError.invalidBridge("\(mapping.mt5Symbol.rawValue) digits expected \(mapping.digits.rawValue), got \(info.digits)")
            }
        }
        let latest = try bridge.latestClosedM1Bar(first.mt5Symbol)
        let positional = try bridge.ratesFromPosition(mt5Symbol: first.mt5Symbol, startPosition: 1, count: 1)
        guard positional.mt5Symbol == first.mt5Symbol.rawValue,
              positional.timeframe == Timeframe.m1.rawValue,
              positional.rates.count == 1,
              let positionalRate = positional.rates.first else {
            throw StartCheckError.invalidBridge("GET_RATES_FROM_POSITION did not return exactly one closed M1 bar")
        }
        guard positionalRate.mt5ServerTime == latest.mt5ServerTime else {
            throw StartCheckError.invalidBridge("GET_RATES_FROM_POSITION start_pos=1 did not match latest closed M1 bar")
        }
    }

    static func coverageGaps(in offsetMap: BrokerOffsetMap, from: MT5ServerSecond, toExclusive: MT5ServerSecond) -> [String] {
        guard from.rawValue < toExclusive.rawValue else { return [] }
        var cursor = from.rawValue
        var gaps: [String] = []
        for segment in offsetMap.segments {
            guard segment.validTo.rawValue > cursor else { continue }
            if segment.validFrom.rawValue > cursor {
                let gapEnd = min(segment.validFrom.rawValue, toExclusive.rawValue)
                gaps.append("\(cursor)..<\(gapEnd)")
                cursor = gapEnd
            }
            if segment.validFrom.rawValue <= cursor {
                cursor = max(cursor, min(segment.validTo.rawValue, toExclusive.rawValue))
            }
            if cursor >= toExclusive.rawValue { break }
        }
        if cursor < toExclusive.rawValue {
            gaps.append("\(cursor)..<\(toExclusive.rawValue)")
        }
        return gaps
    }

    private static func addOneMinute(_ rawTimestamp: Int64) throws -> MT5ServerSecond {
        let result = rawTimestamp.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw StartCheckError.invalidBridge("timestamp overflow while computing latest closed exclusive bound")
        }
        return MT5ServerSecond(rawValue: result.partialValue)
    }
}

private struct StartCheckResult {
    var failureCount = 0
    var warningCount = 0

    func finish(logger: Logger) -> Bool {
        if failureCount == 0 {
            if warningCount == 0 {
                logger.ok("STARTCHECK PASSED: production prerequisites are satisfied")
            } else {
                logger.warn("STARTCHECK PASSED WITH WARNINGS: \(warningCount) warning(s)")
            }
            return true
        }
        logger.error("STARTCHECK FAILED: \(failureCount) required check(s) failed")
        return false
    }
}

public enum StartCheckError: Error, CustomStringConvertible, Sendable {
    case missingTables([String])
    case metaEditorNotFound(String)
    case eaSourceNotFound(String)
    case eaCompileFailed(String)
    case invalidBridge(String)
    case offsetCoverageGaps([String])

    public var description: String {
        switch self {
        case .missingTables(let tables):
            return "Missing ClickHouse tables: \(tables.joined(separator: ", "))"
        case .metaEditorNotFound(let message):
            return message
        case .eaSourceNotFound(let message):
            return message
        case .eaCompileFailed(let message):
            return message
        case .invalidBridge(let message):
            return message
        case .offsetCoverageGaps(let gaps):
            return "Verified broker UTC offset segments do not cover MT5 history: \(gaps.joined(separator: "; "))"
        }
    }
}

private enum StartCheckGuidance {
    static func guidance(for error: Error) -> String {
        if case StartCheckError.missingTables = error {
            return "Next action: rerun `FXExport migrate --config-dir Config --migrations-dir Migrations`, then rerun `FXExport startcheck --config-dir Config`."
        }
        if case StartCheckError.metaEditorNotFound = error {
            return "Next action: confirm `/Applications/MetaTrader 5.app` exists, or set MT5RESEARCH_WINE and MT5RESEARCH_METAEDITOR before running startcheck."
        }
        if case StartCheckError.eaCompileFailed = error {
            return "Next action: open MetaEditor, compile `MT5Research/EA/HistoryBridgeEA.mq5`, fix reported errors, then rerun startcheck."
        }
        if case StartCheckError.offsetCoverageGaps = error {
            return "Next action: insert active `confidence='verified'` rows in broker_time_offsets for the exact MT5 company/server/account and every historical server-time segment before backfill."
        }
        if error is MT5BridgeError || error is ProtocolError {
            return "Next action: start MT5, attach the compiled HistoryBridgeEA to a chart, enable localhost sockets, then rerun startcheck."
        }
        if error is BrokerOffsetStoreError || error is BrokerOffsetRuntimeError || error is TimeMappingError {
            return "Next action: inspect broker_time_offsets for the connected MT5 identity and verify live server offset through the EA."
        }
        return "Next action: fix the reported problem and rerun startcheck."
    }
}

public struct MetaEditorCompileResult: Sendable {
    public let outputPath: URL
    public let logPath: URL
    public let logSummary: String
}

public struct MetaEditorCompiler: Sendable {
    private let workingDirectory: URL

    public init(workingDirectory: URL) {
        self.workingDirectory = workingDirectory
    }

    public func compileHistoryBridgeEA(timeoutSeconds: TimeInterval) async throws -> MetaEditorCompileResult {
        let source = try locateHistoryBridgeSource()
        let toolchain = try MetaEditorToolchain.locate(from: source)
        let stage = try Self.createCompileStage(for: source)
        let logPath = stage.source.deletingLastPathComponent().appendingPathComponent("compile.log")
        let startedAt = Date()
        let liveOutput = source.deletingPathExtension().appendingPathExtension("ex5")
        let stageOutput = stage.source.deletingPathExtension().appendingPathExtension("ex5")
        let stdoutPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("FXExport-HistoryBridgeEA-stdout-\(UUID().uuidString).log")
        guard FileManager.default.createFile(atPath: stdoutPath.path, contents: nil) else {
            throw StartCheckError.eaCompileFailed("Could not create MetaEditor output log at \(stdoutPath.path)")
        }
        let stdoutHandle = try FileHandle(forWritingTo: stdoutPath)
        var cleanupSuccessfulCompile = false
        defer {
            do {
                try stdoutHandle.close()
            } catch {
                // Nothing useful can be recovered after the compile process has completed.
            }
            if cleanupSuccessfulCompile {
                Self.removeTemporaryFile(stdoutPath)
                Self.removeTemporaryDirectory(stage.directory)
            }
        }

        var environment = ProcessInfo.processInfo.environment
        environment["WINEPREFIX"] = toolchain.winePrefix.path

        let process = Process()
        process.executableURL = toolchain.wine
        process.arguments = [
            toolchain.metaEditor.path,
            "/compile:\(MetaEditorToolchain.winePathArgument(stage.source))",
            "/log:\(MetaEditorToolchain.winePathArgument(logPath))"
        ]
        process.environment = environment
        process.standardOutput = stdoutHandle
        process.standardError = stdoutHandle
        try process.run()

        let deadline = Date().addingTimeInterval(timeoutSeconds)
        var lastLog = ""
        while Date() < deadline {
            if let text = Self.readCompileLog(logPath), !text.isEmpty {
                lastLog = text
            }
            if Self.logIsClean(lastLog), Self.outputWasBuilt(stageOutput, after: startedAt) {
                if process.isRunning {
                    process.terminate()
                }
                try Self.replaceLiveOutput(from: stageOutput, to: liveOutput)
                cleanupSuccessfulCompile = true
                return MetaEditorCompileResult(
                    outputPath: liveOutput,
                    logPath: logPath,
                    logSummary: Self.summary(from: lastLog)
                )
            }
            if !process.isRunning {
                break
            }
            try await Task.sleep(nanoseconds: 500_000_000)
        }

        if process.isRunning {
            process.terminate()
        }
        let outputText = Self.readPlainText(stdoutPath) ?? ""
        let logText = Self.readCompileLog(logPath) ?? lastLog
        let detail = Self.summary(from: logText).isEmpty ? outputText : Self.summary(from: logText)
        throw StartCheckError.eaCompileFailed("MetaEditor compile did not produce a clean HistoryBridgeEA.ex5. Compile log: \(logPath.path). \(detail)")
    }

    private func locateHistoryBridgeSource() throws -> URL {
        let candidates = [
            workingDirectory.appendingPathComponent("EA/HistoryBridgeEA.mq5"),
            workingDirectory.appendingPathComponent("MT5Research/EA/HistoryBridgeEA.mq5")
        ]
        if let candidate = candidates.first(where: { FileManager.default.fileExists(atPath: $0.path) }) {
            return candidate
        }
        throw StartCheckError.eaSourceNotFound("Could not find HistoryBridgeEA.mq5 from \(workingDirectory.path)")
    }

    private static func createCompileStage(for source: URL) throws -> (directory: URL, source: URL) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("FXExport-HistoryBridgeEA-stage-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let stagedSource = directory.appendingPathComponent(source.lastPathComponent)
        try FileManager.default.copyItem(at: source, to: stagedSource)
        return (directory, stagedSource)
    }

    private static func removeTemporaryFile(_ url: URL) {
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        do {
            try FileManager.default.removeItem(at: url)
        } catch {
            // Best-effort cleanup of successful compile artifacts only.
        }
    }

    private static func removeTemporaryDirectory(_ url: URL) {
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        do {
            try FileManager.default.removeItem(at: url)
        } catch {
            // Best-effort cleanup of successful compile artifacts only.
        }
    }

    private static func replaceLiveOutput(from stagedOutput: URL, to liveOutput: URL) throws {
        let fileManager = FileManager.default
        let temporaryOutput = liveOutput.deletingLastPathComponent()
            .appendingPathComponent(".\(liveOutput.lastPathComponent).\(UUID().uuidString).tmp")
        try fileManager.copyItem(at: stagedOutput, to: temporaryOutput)
        if fileManager.fileExists(atPath: liveOutput.path) {
            let replaced = try fileManager.replaceItemAt(liveOutput, withItemAt: temporaryOutput, backupItemName: nil)
            guard replaced != nil else {
                throw StartCheckError.eaCompileFailed("Compiled EA could not replace existing output at \(liveOutput.path)")
            }
        } else {
            try fileManager.moveItem(at: temporaryOutput, to: liveOutput)
        }
    }

    private static func readCompileLog(_ url: URL) -> String? {
        guard FileManager.default.fileExists(atPath: url.path) else {
            return nil
        }
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            return nil
        }
        if let text = String(data: data, encoding: .utf16LittleEndian) {
            return text
        }
        if let text = String(data: data, encoding: .utf16) {
            return text
        }
        return String(data: data, encoding: .utf8)
    }

    private static func readPlainText(_ url: URL) -> String? {
        guard FileManager.default.fileExists(atPath: url.path) else {
            return nil
        }
        do {
            return try String(contentsOf: url, encoding: .utf8)
        } catch {
            return nil
        }
    }

    private static func logIsClean(_ text: String) -> Bool {
        text.localizedCaseInsensitiveContains("0 errors, 0 warnings")
    }

    private static func outputWasBuilt(_ url: URL, after startedAt: Date) -> Bool {
        guard FileManager.default.fileExists(atPath: url.path) else {
            return false
        }
        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
            guard let modifiedAt = attributes[.modificationDate] as? Date else {
                return false
            }
            return modifiedAt >= startedAt.addingTimeInterval(-2)
        } catch {
            return false
        }
    }

    private static func summary(from text: String) -> String {
        text.components(separatedBy: .newlines)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .suffix(5)
            .joined(separator: " | ")
    }
}

public struct MetaEditorToolchain: Sendable {
    public let wine: URL
    public let metaEditor: URL
    public let winePrefix: URL

    public static func locate(from source: URL) throws -> MetaEditorToolchain {
        let environment = ProcessInfo.processInfo.environment
        let wine = URL(fileURLWithPath: environment["MT5RESEARCH_WINE"] ?? environment["FXAI_WINE"] ?? "/Applications/MetaTrader 5.app/Contents/SharedSupport/wine/bin/wine64")
        guard FileManager.default.fileExists(atPath: wine.path) else {
            throw StartCheckError.metaEditorNotFound("Wine binary not found at \(wine.path)")
        }

        let mt5Root = findMetaTraderRoot(from: source)
        let metaEditor = URL(fileURLWithPath: environment["MT5RESEARCH_METAEDITOR"] ?? environment["FXAI_METAEDITOR"] ?? mt5Root.appendingPathComponent("MetaEditor64.exe").path)
        guard FileManager.default.fileExists(atPath: metaEditor.path) else {
            throw StartCheckError.metaEditorNotFound("MetaEditor64.exe not found at \(metaEditor.path)")
        }

        let winePrefix = URL(fileURLWithPath: environment["WINEPREFIX"] ?? environment["MT5RESEARCH_WINEPREFIX"] ?? mt5Root.deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().path)
        return MetaEditorToolchain(wine: wine, metaEditor: metaEditor, winePrefix: winePrefix)
    }

    public static func winePathArgument(_ url: URL) -> String {
        "Z:\\" + url.path.replacingOccurrences(of: "/", with: "\\").trimmingCharacters(in: CharacterSet(charactersIn: "\\"))
    }

    private static func findMetaTraderRoot(from source: URL) -> URL {
        var cursor = source.deletingLastPathComponent()
        while cursor.path != "/" {
            if FileManager.default.fileExists(atPath: cursor.appendingPathComponent("MetaEditor64.exe").path) {
                return cursor
            }
            cursor.deleteLastPathComponent()
        }
        return FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/net.metaquotes.wine.metatrader5/drive_c/Program Files/MetaTrader 5")
    }
}
