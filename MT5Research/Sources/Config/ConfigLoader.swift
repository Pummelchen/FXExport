import AppCore
import Domain
import Foundation

public struct ConfigBundle: Sendable {
    public let app: AppConfigFile
    public let clickHouse: ClickHouseConfig
    public let mt5Bridge: MT5BridgeConfig
    public let brokerTime: BrokerTimeConfig
    public let symbols: SymbolConfig

    public init(
        app: AppConfigFile,
        clickHouse: ClickHouseConfig,
        mt5Bridge: MT5BridgeConfig,
        brokerTime: BrokerTimeConfig,
        symbols: SymbolConfig
    ) {
        self.app = app
        self.clickHouse = clickHouse
        self.mt5Bridge = mt5Bridge
        self.brokerTime = brokerTime
        self.symbols = symbols
    }
}

public enum ConfigError: Error, CustomStringConvertible, Sendable {
    case missingFile(URL)
    case invalidFile(URL, String)
    case invalidValue(String)

    public var description: String {
        switch self {
        case .missingFile(let url):
            return "Missing config file: \(url.path)"
        case .invalidFile(let url, let reason):
            return "Invalid config file \(url.path): \(reason)"
        case .invalidValue(let reason):
            return "Invalid config value: \(reason)"
        }
    }
}

public struct ConfigLoader: Sendable {
    private let decoder: JSONDecoder

    public init() {
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .useDefaultKeys
        self.decoder = decoder
    }

    public func loadBundle(configDirectory: URL) throws -> ConfigBundle {
        let app: AppConfigFile = try load("app.json", from: configDirectory)
        let clickHouse: ClickHouseConfig = try load("clickhouse.json", from: configDirectory)
        let mt5Bridge: MT5BridgeConfig = try load("mt5_bridge.json", from: configDirectory)
        let brokerTime: BrokerTimeConfig = try load("broker_time.json", from: configDirectory)
        let symbols: SymbolConfig = try load("symbols.json", from: configDirectory)
        try validate(app: app, clickHouse: clickHouse, mt5Bridge: mt5Bridge, brokerTime: brokerTime, symbols: symbols)
        return ConfigBundle(app: app, clickHouse: clickHouse, mt5Bridge: mt5Bridge, brokerTime: brokerTime, symbols: symbols)
    }

    public func load<T: Decodable>(_ fileName: String, from directory: URL) throws -> T {
        let url = directory.appendingPathComponent(fileName)
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw ConfigError.missingFile(url)
        }
        do {
            let data = try Data(contentsOf: url)
            return try decoder.decode(T.self, from: data)
        } catch let error as ConfigError {
            throw error
        } catch {
            throw ConfigError.invalidFile(url, error.localizedDescription)
        }
    }

    private func validate(
        app: AppConfigFile,
        clickHouse: ClickHouseConfig,
        mt5Bridge: MT5BridgeConfig,
        brokerTime: BrokerTimeConfig,
        symbols: SymbolConfig
    ) throws {
        guard app.chunkSize > 0 else { throw ConfigError.invalidValue("chunk_size must be greater than zero") }
        guard app.chunkSize <= 50_000 else {
            throw ConfigError.invalidValue("chunk_size must be 50,000 or lower to stay within the EA protocol response bound")
        }
        guard app.liveScanIntervalSeconds > 0 else {
            throw ConfigError.invalidValue("live_scan_interval_seconds must be greater than zero")
        }
        guard app.verifierRandomRanges >= 0 else {
            throw ConfigError.invalidValue("verifier_random_ranges must not be negative")
        }
        try validateSupervisor(app.supervisor)
        guard !clickHouse.database.isEmpty else { throw ConfigError.invalidValue("ClickHouse database is empty") }
        guard Self.isSafeClickHouseIdentifier(clickHouse.database) else {
            throw ConfigError.invalidValue("ClickHouse database must contain only letters, digits, and underscores, and must not start with a digit")
        }
        guard clickHouse.url.scheme == "http" || clickHouse.url.scheme == "https" else {
            throw ConfigError.invalidValue("ClickHouse URL must use http or https")
        }
        guard Self.isReasonableTimeout(clickHouse.requestTimeoutSeconds) else {
            throw ConfigError.invalidValue("ClickHouse requestTimeoutSeconds must be finite and between 0 and 3600 seconds")
        }
        guard (0...10).contains(clickHouse.retryCount) else {
            throw ConfigError.invalidValue("ClickHouse retryCount must be between 0 and 10")
        }
        guard !mt5Bridge.host.isEmpty else { throw ConfigError.invalidValue("MT5 bridge host is empty") }
        guard Self.isReasonableTimeout(mt5Bridge.connectTimeoutSeconds) else {
            throw ConfigError.invalidValue("MT5 bridge connectTimeoutSeconds must be finite and between 0 and 3600 seconds")
        }
        guard Self.isReasonableTimeout(mt5Bridge.requestTimeoutSeconds) else {
            throw ConfigError.invalidValue("MT5 bridge requestTimeoutSeconds must be finite and between 0 and 3600 seconds")
        }
        guard !symbols.symbols.isEmpty else { throw ConfigError.invalidValue("No symbols configured") }
        if let expected = brokerTime.expectedTerminalIdentity {
            if let company = expected.company, company.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                throw ConfigError.invalidValue("expected_terminal_identity.company must not be empty when provided")
            }
            if let server = expected.server, server.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                throw ConfigError.invalidValue("expected_terminal_identity.server must not be empty when provided")
            }
            if let accountLogin = expected.accountLogin, accountLogin <= 0 {
                throw ConfigError.invalidValue("expected_terminal_identity.account_login must be positive when provided")
            }
        }
        for offset in brokerTime.acceptedLiveOffsetSeconds {
            guard offset.rawValue % 60 == 0 else {
                throw ConfigError.invalidValue("accepted_live_offset_seconds values must be minute-aligned")
            }
            guard Int64(Int32.min)...Int64(Int32.max) ~= offset.rawValue else {
                throw ConfigError.invalidValue("accepted_live_offset_seconds values must fit ClickHouse Int32 storage")
            }
        }

        let logicalSymbols = symbols.symbols.map(\.logicalSymbol)
        guard Set(logicalSymbols).count == logicalSymbols.count else {
            throw ConfigError.invalidValue("Duplicate logical symbols in symbols.json")
        }
        let mt5Symbols = symbols.symbols.map(\.mt5Symbol)
        guard Set(mt5Symbols).count == mt5Symbols.count else {
            throw ConfigError.invalidValue("Duplicate MT5 symbols in symbols.json")
        }

        var sortedSegments = brokerTime.offsetSegments.sorted { $0.validFromMT5ServerTs < $1.validFromMT5ServerTs }
        for segment in sortedSegments {
            guard segment.validFromMT5ServerTs.rawValue < segment.validToMT5ServerTs.rawValue else {
                throw ConfigError.invalidValue("Broker time offset segment has non-positive duration")
            }
            guard segment.validFromMT5ServerTs.isMinuteAligned && segment.validToMT5ServerTs.isMinuteAligned else {
                throw ConfigError.invalidValue("Broker time offset segment boundaries must be minute-aligned")
            }
            guard segment.offsetSeconds.rawValue % 60 == 0 else {
                throw ConfigError.invalidValue("Broker UTC offset seconds must be minute-aligned")
            }
            guard Int64(Int32.min)...Int64(Int32.max) ~= segment.offsetSeconds.rawValue else {
                throw ConfigError.invalidValue("Broker UTC offset seconds must fit ClickHouse Int32 storage")
            }
        }
        while sortedSegments.count >= 2 {
            let first = sortedSegments.removeFirst()
            let second = sortedSegments[0]
            guard first.validToMT5ServerTs.rawValue <= second.validFromMT5ServerTs.rawValue else {
                throw ConfigError.invalidValue("Broker time offset segments overlap")
            }
        }
    }

    private static func isSafeClickHouseIdentifier(_ value: String) -> Bool {
        guard let first = value.first, first == "_" || first.isLetter else { return false }
        return value.allSatisfy { character in
            character == "_" || character.isLetter || character.isNumber
        }
    }

    private func validateSupervisor(_ supervisor: SupervisorConfig) throws {
        let intervals = [
            ("supervisor.cycle_seconds", supervisor.cycleSeconds),
            ("supervisor.health_check_interval_seconds", supervisor.healthCheckIntervalSeconds),
            ("supervisor.utc_check_interval_seconds", supervisor.utcCheckIntervalSeconds),
            ("supervisor.verification_interval_seconds", supervisor.verificationIntervalSeconds),
            ("supervisor.symbol_metadata_check_interval_seconds", supervisor.symbolMetadataCheckIntervalSeconds),
            ("supervisor.checkpoint_audit_interval_seconds", supervisor.checkpointAuditIntervalSeconds),
            ("supervisor.backup_check_interval_seconds", supervisor.backupCheckIntervalSeconds),
            ("supervisor.alert_interval_seconds", supervisor.alertIntervalSeconds),
            ("supervisor.stale_live_warning_seconds", supervisor.staleLiveWarningSeconds)
        ]
        for (name, value) in intervals {
            guard value > 0 else {
                throw ConfigError.invalidValue("\(name) must be greater than zero")
            }
        }
    }

    private static func isReasonableTimeout(_ value: Double) -> Bool {
        value.isFinite && value > 0 && value <= 3600
    }
}
