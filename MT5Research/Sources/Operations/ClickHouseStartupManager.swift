import AppCore
import ClickHouse
import Config
import Foundation

public struct ClickHouseStartupManager: Sendable {
    private let config: ClickHouseConfig
    private let client: ClickHouseClientProtocol
    private let logger: Logger
    private let commandRunner: SystemCommandRunning
    private let configuredStartCommands: [SystemCommandRequest]?
    private let startupWaitSeconds: TimeInterval
    private let pollIntervalNanoseconds: UInt64

    public init(
        config: ClickHouseConfig,
        client: ClickHouseClientProtocol,
        logger: Logger,
        commandRunner: SystemCommandRunning = ProcessCommandRunner(),
        startCommands: [SystemCommandRequest]? = nil,
        startupWaitSeconds: TimeInterval = 20,
        pollIntervalNanoseconds: UInt64 = 1_000_000_000
    ) {
        self.config = config
        self.client = client
        self.logger = logger
        self.commandRunner = commandRunner
        self.configuredStartCommands = startCommands
        self.startupWaitSeconds = startupWaitSeconds
        self.pollIntervalNanoseconds = pollIntervalNanoseconds
    }

    public func ensureReady() async throws {
        logger.db("Startup check: ClickHouse HTTP endpoint \(config.url.absoluteString)")
        do {
            try await ping()
            logger.ok("ClickHouse is responding")
            return
        } catch {
            guard Self.shouldAttemptLocalStart(for: config.url, error: error) else {
                throw ClickHouseStartupError.notAutoStartable(url: config.url, cause: String(describing: error))
            }
            logger.warn("ClickHouse is not responding; attempting to start the local service")
        }

        let commands = startCommands()
        guard !commands.isEmpty else {
            throw ClickHouseStartupError.noStartCommand(url: config.url)
        }

        var attempts: [ClickHouseStartupAttempt] = []
        for command in commands {
            logger.db("Running: \(command.display)")
            do {
                let result = try await commandRunner.run(command)
                attempts.append(ClickHouseStartupAttempt(command: command.display, exitCode: result.exitCode, output: result.combinedOutput))
                guard result.exitCode == 0 else {
                    logger.warn("Start command exited with code \(result.exitCode); trying the next safe option")
                    continue
                }
                logger.db("Start command returned; waiting for ClickHouse HTTP to become ready")
                if try await waitUntilReady() {
                    logger.ok("ClickHouse started and is responding at \(config.url.absoluteString)")
                    return
                }
            } catch {
                attempts.append(ClickHouseStartupAttempt(command: command.display, exitCode: nil, output: String(describing: error)))
                logger.warn("Start command failed: \(error)")
            }
        }

        throw ClickHouseStartupError.startFailed(url: config.url, attempts: attempts)
    }

    private func ping() async throws {
        _ = try await client.execute(.select("SELECT 1", databaseOverride: "default"))
    }

    private func waitUntilReady() async throws -> Bool {
        let deadline = Date().addingTimeInterval(max(0, startupWaitSeconds))
        var lastError: Error?
        repeat {
            do {
                try await ping()
                return true
            } catch {
                lastError = error
            }
            if Date() >= deadline {
                break
            }
            try await Task.sleep(nanoseconds: pollIntervalNanoseconds)
        } while true
        if let lastError {
            logger.verbose("ClickHouse still not ready: \(lastError)")
        }
        return false
    }

    private func startCommands() -> [SystemCommandRequest] {
        if let configuredStartCommands {
            return configuredStartCommands
        }

        var commands: [SystemCommandRequest] = []
        if let brew = Self.locateExecutable(candidates: ["/opt/homebrew/bin/brew", "/usr/local/bin/brew"]) {
            commands.append(SystemCommandRequest(executable: brew, arguments: ["services", "start", "clickhouse"], timeoutSeconds: 45))
            commands.append(SystemCommandRequest(executable: brew, arguments: ["services", "start", "clickhouse-server"], timeoutSeconds: 45))
        }
        if let clickHouse = Self.locateExecutable(candidates: ["/opt/homebrew/bin/clickhouse", "/usr/local/bin/clickhouse"]) {
            commands.append(SystemCommandRequest(executable: clickHouse, arguments: ["start"], timeoutSeconds: 45))
        }
        return commands
    }

    private static func shouldAttemptLocalStart(for url: URL, error: Error) -> Bool {
        guard isLocalEndpoint(url) else { return false }
        if case ClickHouseError.transport = error {
            return true
        }
        return false
    }

    private static func isLocalEndpoint(_ url: URL) -> Bool {
        guard let host = url.host?.lowercased() else { return false }
        return host == "localhost" || host == "127.0.0.1" || host == "::1"
    }

    private static func locateExecutable(candidates: [String]) -> URL? {
        for candidate in candidates where FileManager.default.isExecutableFile(atPath: candidate) {
            return URL(fileURLWithPath: candidate)
        }
        return nil
    }
}

public struct ClickHouseStartupAttempt: Sendable, Equatable {
    public let command: String
    public let exitCode: Int32?
    public let output: String
}

public enum ClickHouseStartupError: Error, CustomStringConvertible, Sendable {
    case notAutoStartable(url: URL, cause: String)
    case noStartCommand(url: URL)
    case startFailed(url: URL, attempts: [ClickHouseStartupAttempt])

    public var description: String {
        switch self {
        case .notAutoStartable(let url, let cause):
            return """
            ClickHouse is not ready at \(url.absoluteString), and this endpoint cannot be safely auto-started.
            Reason: \(cause)
            Next steps:
              1. If this should be local Homebrew ClickHouse, set Config/clickhouse.json url to http://localhost:8123.
              2. If this is a remote ClickHouse server, start it on the remote host or fix network/authentication.
              3. Rerun: FXExport startcheck --config-dir Config --migrations-dir Migrations
            """
        case .noStartCommand(let url):
            return """
            ClickHouse is not responding at \(url.absoluteString), and no safe local start command was found.
            Next steps:
              1. Confirm Homebrew is installed: command -v brew
              2. Confirm ClickHouse is installed: command -v clickhouse
              3. Try manually: brew services start clickhouse
              4. If your install uses the standalone daemon, try: clickhouse start
              5. Rerun: FXExport startcheck --config-dir Config --migrations-dir Migrations
            """
        case .startFailed(let url, let attempts):
            let attemptText = attempts
                .map { attempt in
                    let exit = attempt.exitCode.map(String.init) ?? "not started"
                    let output = Self.trim(attempt.output)
                    return "  - \(attempt.command) -> \(exit)\(output.isEmpty ? "" : " | \(output)")"
                }
                .joined(separator: "\n")
            return """
            ClickHouse could not be started automatically or did not become ready at \(url.absoluteString).
            Attempted:
            \(attemptText)
            Next steps:
              1. Run: brew services list | grep clickhouse
              2. Run: brew services start clickhouse
              3. If that fails, run: clickhouse start
              4. Check the ClickHouse log/config for your install.
              5. Rerun: FXExport startcheck --config-dir Config --migrations-dir Migrations
            """
        }
    }

    private static func trim(_ value: String) -> String {
        let normalized = value
            .replacingOccurrences(of: "\n", with: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard normalized.count > 700 else { return normalized }
        return String(normalized.prefix(700)) + "..."
    }
}
