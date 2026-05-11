import AppCore
import BacktestCore
import ClickHouse
import Config
import Domain
import Foundation
import Ingestion
import MetalAccel
import MT5Bridge
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
                return .success

            case .symbolCheck:
                let bridge = try connectBridge(config: config, logger: logger)
                for mapping in config.symbols.symbols {
                    do {
                        let info = try bridge.prepareSymbol(mapping.mt5Symbol)
                        if info.selected && info.digits == mapping.digits.rawValue {
                            logger.ok("\(mapping.logicalSymbol.rawValue): \(mapping.mt5Symbol.rawValue) selected, digits \(info.digits)")
                        } else if info.selected {
                            logger.warn("\(mapping.logicalSymbol.rawValue): digits mismatch, config \(mapping.digits.rawValue), MT5 \(info.digits)")
                        } else {
                            logger.error("\(mapping.logicalSymbol.rawValue): symbol \(mapping.mt5Symbol.rawValue) not selected in MT5")
                        }
                    } catch {
                        logger.error("\(mapping.logicalSymbol.rawValue): \(error)")
                    }
                }
                return .success

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

            case .verify:
                let bridge: MT5BridgeClient?
                if options.requiresBridge {
                    bridge = try connectBridge(config: config, logger: logger)
                } else {
                    bridge = nil
                }
                try await VerificationAgent(config: config, bridge: bridge, clickHouse: clickHouse, logger: logger)
                    .startupChecks(randomRanges: options.randomRanges ?? config.app.verifierRandomRanges)
                return .success

            case .repair:
                logger.warn("Repair CLI is scaffolded. Use verifier output to implement a precise canonical-only range repair command.")
                return .success

            case .exportCache:
                logger.warn("Export-cache CLI is scaffolded. It will read canonical bars in bulk and write a typed local cache format.")
                return .success

            case .backtest:
                let availability = MetalAvailability()
                if availability.isAvailable {
                    logger.ok("Metal available: \(availability.deviceName ?? "unknown device")")
                } else {
                    logger.warn("Metal unavailable; CPU reference engine will be used")
                }
                logger.warn("Backtest CLI scaffold is ready; strategy loading is not implemented yet.")
                return .success

            case .optimize:
                logger.warn("Optimize CLI scaffold is ready; CPU reference and optional Metal sweep hooks are in place.")
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
                timeoutSeconds: config.mt5Bridge.connectTimeoutSeconds
            )
        case .connect:
            logger.db("Connecting to MT5 bridge at \(config.mt5Bridge.host):\(config.mt5Bridge.port)")
            return try MT5BridgeClient.connect(
                host: config.mt5Bridge.host,
                port: config.mt5Bridge.port,
                timeoutSeconds: config.mt5Bridge.connectTimeoutSeconds
            )
        }
    }

    private static func selectedSymbols(from argument: String?) throws -> [LogicalSymbol]? {
        guard let argument, argument.lowercased() != "all" else { return nil }
        return try argument.split(separator: ",").map { try LogicalSymbol(String($0)) }
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
          verify
          verify --random-ranges 20
          repair --symbol EURUSD --from 2020-01-01 --to 2020-02-01
          export-cache --symbol EURUSD --from 2020-01-01 --to 2025-01-01
          backtest --config Config/backtest.json
          optimize --config Config/optimize.json

        Global options:
          --config-dir Config
          --migrations-dir Migrations
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
    let requiresBridge: Bool

    init(arguments: [String]) throws {
        guard let first = arguments.first else {
            self.command = .help
            self.configDirectory = URL(fileURLWithPath: "Config")
            self.migrationsDirectory = URL(fileURLWithPath: "Migrations")
            self.overrideLogLevel = nil
            self.symbolsArgument = nil
            self.randomRanges = nil
            self.requiresBridge = false
            return
        }

        self.command = try Self.parseCommand(first)
        var configDirectory = URL(fileURLWithPath: "Config")
        var migrationsDirectory = URL(fileURLWithPath: "Migrations")
        var overrideLogLevel: LogLevel?
        var symbolsArgument: String?
        var randomRanges: Int?
        var requiresBridge = command == .verify
        var noBridgeRequested = false

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
            case "--no-bridge":
                noBridgeRequested = true
            case "--verbose":
                overrideLogLevel = .verbose
            case "--debug":
                overrideLogLevel = .debug
            case "--config", "--symbol", "--from", "--to":
                index += 1
                guard index < arguments.count else { throw CLIError.missingValue(arg) }
            default:
                throw CLIError.unknownOption(arg)
            }
            index += 1
        }

        if command == .verify {
            requiresBridge = !noBridgeRequested && (randomRanges.map { $0 > 0 } ?? true)
        }

        self.configDirectory = configDirectory
        self.migrationsDirectory = migrationsDirectory
        self.overrideLogLevel = overrideLogLevel
        self.symbolsArgument = symbolsArgument
        self.randomRanges = randomRanges
        self.requiresBridge = requiresBridge
    }

    private static func parseCommand(_ value: String) throws -> Command {
        switch value {
        case "migrate": return .migrate
        case "bridge-check": return .bridgeCheck
        case "symbol-check": return .symbolCheck
        case "backfill": return .backfill
        case "live": return .live
        case "verify": return .verify
        case "repair": return .repair
        case "export-cache": return .exportCache
        case "backtest": return .backtest
        case "optimize": return .optimize
        case "help", "--help", "-h": return .help
        default: throw CLIError.unknownCommand(value)
        }
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
