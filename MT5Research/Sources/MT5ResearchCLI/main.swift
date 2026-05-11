import AppCore
import BacktestCore
import ClickHouse
import Config
import Domain
import Foundation
import Ingestion
import MetalAccel
import MT5Bridge
import Operations
import TimeMapping
import Verification

@main
struct MT5ResearchCLI {
    static func main() async {
        let result = await run(arguments: Array(CommandLine.arguments.dropFirst()))
        Darwin.exit(result.rawValue)
    }

    static func run(arguments: [String]) async -> ExitCode {
        do {
            let options = try CLIOptions(arguments: arguments)
            if options.command == .help {
                printUsage()
                return .success
            }

            let loader = ConfigLoader()
            let config = try loader.loadBundle(configDirectory: options.configDirectory)
            let logger = Logger(level: options.overrideLogLevel ?? config.app.logLevel)
            let clickHouse = ClickHouseHTTPClient(config: config.clickHouse, logger: logger)

            switch options.command {
            case .migrate:
                logger.db("Connecting to ClickHouse at \(config.clickHouse.url.absoluteString)")
                _ = try await clickHouse.execute(.select("SELECT 1", databaseOverride: "default"))
                logger.ok("ClickHouse connection verified")
                try await ClickHouseMigrator(client: clickHouse, config: config.clickHouse, logger: logger)
                    .migrate(migrationsDirectory: options.migrationsDirectory)
                return .success

            case .bridgeCheck:
                let bridge = try connectBridge(config: config, logger: logger)
                let hello = try bridge.hello()
                logger.ok("MT5 bridge connected: \(hello.bridgeName) \(hello.bridgeVersion)")
                let terminal = try bridge.terminalInfo()
                logger.ok("MT5 terminal: \(terminal.terminalName), server \(terminal.server), account \(terminal.accountLogin)")
                _ = try await verifyLiveBrokerOffset(
                    bridge: bridge,
                    clickHouse: clickHouse,
                    config: config,
                    terminal: terminal,
                    logger: logger
                )
                return .success

            case .symbolCheck:
                let bridge = try connectBridge(config: config, logger: logger)
                var failureCount = 0
                for mapping in config.symbols.symbols {
                    do {
                        let info = try bridge.prepareSymbol(mapping.mt5Symbol)
                        if info.selected && info.digits == mapping.digits.rawValue {
                            logger.ok("\(mapping.logicalSymbol.rawValue): \(mapping.mt5Symbol.rawValue) selected, digits \(info.digits)")
                        } else if info.selected {
                            failureCount += 1
                            logger.warn("\(mapping.logicalSymbol.rawValue): digits mismatch, config \(mapping.digits.rawValue), MT5 \(info.digits)")
                        } else {
                            failureCount += 1
                            logger.error("\(mapping.logicalSymbol.rawValue): symbol \(mapping.mt5Symbol.rawValue) not selected in MT5")
                        }
                    } catch {
                        failureCount += 1
                        logger.error("\(mapping.logicalSymbol.rawValue): \(error)")
                    }
                }
                return failureCount == 0 ? .success : .validation

            case .backfill:
                let bridge = try connectBridge(config: config, logger: logger)
                let checkpointStore = ClickHouseCheckpointStore(
                    client: clickHouse,
                    insertBuilder: ClickHouseInsertBuilder(database: config.clickHouse.database),
                    database: config.clickHouse.database
                )
                let offsetStore = ClickHouseBrokerOffsetStore(client: clickHouse, database: config.clickHouse.database)
                let agent = BackfillAgent(
                    config: config,
                    bridge: bridge,
                    clickHouse: clickHouse,
                    checkpointStore: checkpointStore,
                    offsetStore: offsetStore,
                    logger: logger
                )
                let symbols = try selectedSymbols(from: options.symbolsArgument)
                try await agent.run(selectedSymbols: symbols)
                return .success

            case .live:
                let bridge = try connectBridge(config: config, logger: logger)
                let checkpointStore = ClickHouseCheckpointStore(
                    client: clickHouse,
                    insertBuilder: ClickHouseInsertBuilder(database: config.clickHouse.database),
                    database: config.clickHouse.database
                )
                let offsetStore = ClickHouseBrokerOffsetStore(client: clickHouse, database: config.clickHouse.database)
                try await LiveUpdateAgent(
                    config: config,
                    bridge: bridge,
                    clickHouse: clickHouse,
                    checkpointStore: checkpointStore,
                    offsetStore: offsetStore,
                    logger: logger
                ).runForever()
                return .success

            case .supervise:
                let lock = try SupervisorLock.acquireDefault(
                    brokerSourceId: config.brokerTime.brokerSourceId.rawValue
                )
                logger.ok("Supervisor lock acquired: \(lock.path)")
                let eventStore = ClickHouseAgentEventStore(
                    clickHouse: clickHouse,
                    database: config.clickHouse.database
                )
                let supervisor = ProductionSupervisor(
                    config: config,
                    clickHouse: clickHouse,
                    eventStore: eventStore,
                    logger: logger,
                    bridgeConnector: {
                        try connectBridge(config: config, logger: logger)
                    },
                    runBackfillOnStart: options.runBackfillOnStart ?? config.app.supervisor.runBackfillOnStart
                )
                try await supervisor.run(maxCycles: options.supervisorCycles)
                _ = lock
                return .success

            case .startcheck:
                let runner = StartCheckRunner(
                    config: config,
                    clickHouse: clickHouse,
                    logger: logger,
                    bridgeConnector: {
                        try connectBridge(config: config, logger: logger)
                    },
                    options: StartCheckOptions(
                        migrationsDirectory: options.migrationsDirectory,
                        workingDirectory: URL(fileURLWithPath: FileManager.default.currentDirectoryPath),
                        compileEA: options.compileEA,
                        bridgeChecks: options.bridgeChecks,
                        compileTimeoutSeconds: options.compileTimeoutSeconds
                    )
                )
                return await runner.run() ? .success : .verification

            case .verify:
                let randomRangeCount = options.randomRanges ?? config.app.verifierRandomRanges
                let bridge: MT5BridgeClient?
                if options.shouldConnectBridgeForVerify(randomRangeCount: randomRangeCount) {
                    bridge = try connectBridge(config: config, logger: logger)
                } else {
                    bridge = nil
                }
                try await VerificationAgent(config: config, bridge: bridge, clickHouse: clickHouse, logger: logger)
                    .startupChecks(randomRanges: randomRangeCount)
                return .success

            case .repair:
                try await runRepair(options: options, config: config, clickHouse: clickHouse, logger: logger)
                return .success

            case .exportCache:
                logger.warn("Export-cache CLI is scaffolded. It will read canonical bars in bulk and write a typed local cache format.")
                return .success

            case .backtest:
                guard let commandConfigPath = options.commandConfigPath else {
                    throw CLIError.missingValue("--config")
                }
                guard FileManager.default.fileExists(atPath: commandConfigPath.path) else {
                    throw CLIError.invalidValue("--config")
                }
                let availability = MetalAvailability()
                if availability.isAvailable {
                    logger.ok("Metal available: \(availability.deviceName ?? "unknown device")")
                } else {
                    logger.warn("Metal unavailable; CPU reference engine will be used")
                }
                logger.warn("Backtest CLI scaffold is ready; strategy loading is not implemented yet. Config: \(commandConfigPath.path)")
                return .success

            case .optimize:
                guard let commandConfigPath = options.commandConfigPath else {
                    throw CLIError.missingValue("--config")
                }
                guard FileManager.default.fileExists(atPath: commandConfigPath.path) else {
                    throw CLIError.invalidValue("--config")
                }
                logger.warn("Optimize CLI scaffold is ready; CPU reference and optional Metal sweep hooks are in place. Config: \(commandConfigPath.path)")
                return .success

            case .help:
                printUsage()
                return .success
            }
        } catch let error as CLIError {
            print("[ERROR] \(error.description)")
            printUsage()
            return .usage
        } catch let error as ConfigError {
            print("[ERROR] \(error.description)")
            return .configuration
        } catch let error as TerminalIdentityPolicyError {
            print("[ERROR] \(error.description)")
            return .configuration
        } catch let error as BrokerOffsetRuntimeError {
            print("[ERROR] Broker UTC offset: \(error.description)")
            return .configuration
        } catch let error as SupervisorError {
            print("[ERROR] Supervisor: \(error.description)")
            return .configuration
        } catch let error as TimeMappingError {
            print("[ERROR] Broker UTC offset: \(error.description)")
            return .configuration
        } catch let error as VerificationError {
            print("[ERROR] Verification: \(error.description)")
            return .verification
        } catch let error as MT5BridgeError {
            print("[ERROR] MT5 bridge: \(error.description)")
            return .mt5Bridge
        } catch let error as ClickHouseError {
            print("[ERROR] ClickHouse: \(error.description)")
            return .clickHouse
        } catch {
            print("[ERROR] \(error)")
            return .unknown
        }
    }

    private static func connectBridge(config: ConfigBundle, logger: Logger) throws -> MT5BridgeClient {
        switch config.mt5Bridge.mode {
        case .listen:
            logger.db("Waiting for MT5 EA bridge at \(config.mt5Bridge.host):\(config.mt5Bridge.port)")
            return try MT5BridgeClient.listen(
                host: config.mt5Bridge.host,
                port: config.mt5Bridge.port,
                connectTimeoutSeconds: config.mt5Bridge.connectTimeoutSeconds,
                requestTimeoutSeconds: config.mt5Bridge.requestTimeoutSeconds
            )
        case .connect:
            logger.db("Connecting to MT5 bridge at \(config.mt5Bridge.host):\(config.mt5Bridge.port)")
            return try MT5BridgeClient.connect(
                host: config.mt5Bridge.host,
                port: config.mt5Bridge.port,
                connectTimeoutSeconds: config.mt5Bridge.connectTimeoutSeconds,
                requestTimeoutSeconds: config.mt5Bridge.requestTimeoutSeconds
            )
        }
    }

    private static func selectedSymbols(from argument: String?) throws -> [LogicalSymbol]? {
        guard let argument, argument.lowercased() != "all" else { return nil }
        return try argument.split(separator: ",").map { try LogicalSymbol(String($0)) }
    }

    private static func verifyLiveBrokerOffset(
        bridge: MT5BridgeClient,
        clickHouse: ClickHouseClientProtocol,
        config: ConfigBundle,
        terminal: TerminalInfoDTO,
        logger: Logger
    ) async throws -> BrokerOffsetMap {
        let terminalIdentity = try TerminalIdentityPolicy().resolve(
            actual: terminal,
            brokerSourceId: config.brokerTime.brokerSourceId,
            expected: config.brokerTime.expectedTerminalIdentity,
            logger: logger
        )
        let offsetMap = try await ClickHouseBrokerOffsetStore(
            client: clickHouse,
            database: config.clickHouse.database
        ).loadVerifiedOffsetMap(
            brokerSourceId: config.brokerTime.brokerSourceId,
            terminalIdentity: terminalIdentity
        )
        logger.ok("Loaded \(offsetMap.segments.count) verified broker UTC offset segment(s) from ClickHouse for \(terminalIdentity)")
        try BrokerOffsetRuntimeVerifier().verify(
            snapshot: bridge.serverTimeSnapshot(),
            offsetMap: offsetMap,
            acceptedLiveOffsetSeconds: config.brokerTime.acceptedLiveOffsetSeconds,
            logger: logger
        )
        return offsetMap
    }

    private static func runRepair(
        options: CLIOptions,
        config: ConfigBundle,
        clickHouse: ClickHouseClientProtocol,
        logger: Logger
    ) async throws {
        guard let symbol = options.repairSymbol else {
            throw CLIError.missingValue("--symbol")
        }
        guard let from = options.fromUtcDay else {
            throw CLIError.missingValue("--from")
        }
        guard let to = options.toUtcDay else {
            throw CLIError.missingValue("--to")
        }
        guard config.symbols.mapping(for: symbol) != nil else {
            throw CLIError.invalidValue("--symbol")
        }

        let bridge = try connectBridge(config: config, logger: logger)
        let terminal = try bridge.terminalInfo()
        let offsetMap = try await verifyLiveBrokerOffset(
            bridge: bridge,
            clickHouse: clickHouse,
            config: config,
            terminal: terminal,
            logger: logger
        )
        let ranges = try RepairRangePlanner().mt5Ranges(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: symbol,
            utcStart: from,
            utcEndExclusive: to,
            offsetMap: offsetMap
        )
        let verifier = HistoricalRangeVerifier(
            config: config,
            bridge: bridge,
            clickHouse: clickHouse,
            offsetMap: offsetMap,
            logger: logger
        )
        let repairAgent = RepairAgent(
            clickHouse: clickHouse,
            database: config.clickHouse.database,
            logger: logger
        )
        let policy = RepairPolicy()
        for range in ranges {
            let outcome = try await verifier.verify(range: range)
            let decision = policy.decide(
                verification: outcome.result,
                mt5Available: !outcome.mt5Bars.isEmpty,
                utcMappingAmbiguous: false
            )
            try await repairAgent.repairCanonicalRange(
                range: range,
                replacementBars: outcome.mt5Bars,
                decision: decision
            )
            if case .noRepairNeeded = decision {
                continue
            } else {
                let recheck = try await verifier.verify(range: range)
                guard recheck.result.isClean else {
                    throw RepairError.refused("post-repair verification still reports \(recheck.result.mismatches.count) mismatch(es)")
                }
            }
        }
        logger.ok("\(symbol.rawValue): repair command completed for UTC range \(from.rawValue)..<\(to.rawValue)")
    }

    fileprivate static func parseUtcDay(_ value: String) throws -> UtcSecond {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        guard let date = formatter.date(from: value) else {
            throw CLIError.invalidValue(value)
        }
        return UtcSecond(rawValue: Int64(date.timeIntervalSince1970))
    }

    private static func printUsage() {
        print("""
        mt5research <command> [options]

        Commands:
          migrate
          bridge-check
          symbol-check
          backfill --symbols all
          backfill --symbols EURUSD,USDJPY
          live
          supervise [--with-backfill] [--supervisor-cycles N]
          startcheck
          verify
          verify --random-ranges 20
          repair --symbol EURUSD --from 2020-01-01 --to 2020-02-01
          export-cache --symbol EURUSD --from 2020-01-01 --to 2025-01-01
          backtest --config Config/backtest.json
          optimize --config Config/optimize.json

        Global options:
          --config-dir Config
          --migrations-dir Migrations
          --config Config/backtest.json   # backtest/optimize only
          --verbose
          --debug
        """)
    }
}

enum Command: Equatable {
    case migrate
    case bridgeCheck
    case symbolCheck
    case backfill
    case live
    case supervise
    case startcheck
    case verify
    case repair
    case exportCache
    case backtest
    case optimize
    case help
}

struct CLIOptions {
    let command: Command
    let configDirectory: URL
    let migrationsDirectory: URL
    let overrideLogLevel: LogLevel?
    let symbolsArgument: String?
    let randomRanges: Int?
    let repairSymbol: LogicalSymbol?
    let fromUtcDay: UtcSecond?
    let toUtcDay: UtcSecond?
    let noBridgeRequested: Bool
    let runBackfillOnStart: Bool?
    let supervisorCycles: Int?
    let compileEA: Bool
    let bridgeChecks: Bool
    let compileTimeoutSeconds: TimeInterval
    let commandConfigPath: URL?

    init(arguments: [String]) throws {
        guard let first = arguments.first else {
            self.command = .help
            self.configDirectory = URL(fileURLWithPath: "Config")
            self.migrationsDirectory = URL(fileURLWithPath: "Migrations")
            self.overrideLogLevel = nil
            self.symbolsArgument = nil
            self.randomRanges = nil
            self.repairSymbol = nil
            self.fromUtcDay = nil
            self.toUtcDay = nil
            self.noBridgeRequested = false
            self.runBackfillOnStart = nil
            self.supervisorCycles = nil
            self.compileEA = true
            self.bridgeChecks = true
            self.compileTimeoutSeconds = 120
            self.commandConfigPath = nil
            return
        }

        self.command = try Self.parseCommand(first)
        var configDirectory = URL(fileURLWithPath: "Config")
        var migrationsDirectory = URL(fileURLWithPath: "Migrations")
        var overrideLogLevel: LogLevel?
        var symbolsArgument: String?
        var randomRanges: Int?
        var repairSymbol: LogicalSymbol?
        var fromUtcDay: UtcSecond?
        var toUtcDay: UtcSecond?
        var noBridgeRequested = false
        var runBackfillOnStart: Bool?
        var supervisorCycles: Int?
        var compileEA = true
        var bridgeChecks = true
        var compileTimeoutSeconds: TimeInterval = 120
        var commandConfigPath: URL?

        var index = 1
        while index < arguments.count {
            let arg = arguments[index]
            switch arg {
            case "--config-dir":
                index += 1
                guard index < arguments.count else { throw CLIError.missingValue(arg) }
                configDirectory = URL(fileURLWithPath: arguments[index])
            case "--migrations-dir":
                index += 1
                guard index < arguments.count else { throw CLIError.missingValue(arg) }
                migrationsDirectory = URL(fileURLWithPath: arguments[index])
            case "--symbols":
                index += 1
                guard index < arguments.count else { throw CLIError.missingValue(arg) }
                symbolsArgument = arguments[index]
            case "--random-ranges":
                index += 1
                guard index < arguments.count, let value = Int(arguments[index]), value >= 0 else {
                    throw CLIError.invalidValue(arg)
                }
                randomRanges = value
            case "--symbol":
                index += 1
                guard index < arguments.count else { throw CLIError.missingValue(arg) }
                repairSymbol = try LogicalSymbol(arguments[index])
            case "--from":
                index += 1
                guard index < arguments.count else { throw CLIError.missingValue(arg) }
                fromUtcDay = try MT5ResearchCLI.parseUtcDay(arguments[index])
            case "--to":
                index += 1
                guard index < arguments.count else { throw CLIError.missingValue(arg) }
                toUtcDay = try MT5ResearchCLI.parseUtcDay(arguments[index])
            case "--no-bridge":
                noBridgeRequested = true
            case "--with-backfill":
                runBackfillOnStart = true
            case "--without-backfill":
                runBackfillOnStart = false
            case "--supervisor-cycles":
                index += 1
                guard index < arguments.count, let value = Int(arguments[index]), value > 0 else {
                    throw CLIError.invalidValue(arg)
                }
                supervisorCycles = value
            case "--skip-ea-compile":
                compileEA = false
            case "--skip-bridge":
                bridgeChecks = false
            case "--compile-timeout-seconds":
                index += 1
                guard index < arguments.count, let value = TimeInterval(arguments[index]), value > 0, value <= 1800 else {
                    throw CLIError.invalidValue(arg)
                }
                compileTimeoutSeconds = value
            case "--verbose":
                overrideLogLevel = .verbose
            case "--debug":
                overrideLogLevel = .debug
            case "--config":
                index += 1
                guard index < arguments.count else { throw CLIError.missingValue(arg) }
                commandConfigPath = URL(fileURLWithPath: arguments[index])
            default:
                throw CLIError.unknownOption(arg)
            }
            index += 1
        }

        if command == .repair, let fromUtcDay, let toUtcDay, fromUtcDay.rawValue >= toUtcDay.rawValue {
            throw CLIError.invalidValue("--from/--to")
        }
        if commandConfigPath != nil && command != .backtest && command != .optimize {
            throw CLIError.invalidValue("--config")
        }

        self.configDirectory = configDirectory
        self.migrationsDirectory = migrationsDirectory
        self.overrideLogLevel = overrideLogLevel
        self.symbolsArgument = symbolsArgument
        self.randomRanges = randomRanges
        self.repairSymbol = repairSymbol
        self.fromUtcDay = fromUtcDay
        self.toUtcDay = toUtcDay
        self.noBridgeRequested = noBridgeRequested
        self.runBackfillOnStart = runBackfillOnStart
        self.supervisorCycles = supervisorCycles
        self.compileEA = compileEA
        self.bridgeChecks = bridgeChecks
        self.compileTimeoutSeconds = compileTimeoutSeconds
        self.commandConfigPath = commandConfigPath
    }

    private static func parseCommand(_ value: String) throws -> Command {
        switch value {
        case "migrate": return .migrate
        case "bridge-check": return .bridgeCheck
        case "symbol-check": return .symbolCheck
        case "backfill": return .backfill
        case "live": return .live
        case "supervise": return .supervise
        case "startcheck", "-startcheck", "--startcheck": return .startcheck
        case "verify": return .verify
        case "repair": return .repair
        case "export-cache": return .exportCache
        case "backtest": return .backtest
        case "optimize": return .optimize
        case "help", "--help", "-h": return .help
        default: throw CLIError.unknownCommand(value)
        }
    }

    func shouldConnectBridgeForVerify(randomRangeCount: Int) -> Bool {
        !noBridgeRequested && randomRangeCount > 0
    }
}

enum CLIError: Error, CustomStringConvertible {
    case unknownCommand(String)
    case unknownOption(String)
    case missingValue(String)
    case invalidValue(String)

    var description: String {
        switch self {
        case .unknownCommand(let value):
            return "Unknown command '\(value)'."
        case .unknownOption(let value):
            return "Unknown option '\(value)'."
        case .missingValue(let option):
            return "Missing value for \(option)."
        case .invalidValue(let option):
            return "Invalid value for \(option)."
        }
    }
}
