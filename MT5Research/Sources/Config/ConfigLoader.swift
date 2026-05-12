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
        try validateLogging(app.logging)
        guard !clickHouse.database.isEmpty else { throw ConfigError.invalidValue("ClickHouse database is empty") }
        guard Self.isSafeClickHouseIdentifier(clickHouse.database) else {
            throw ConfigError.invalidValue("ClickHouse database must contain only letters, digits, and underscores, and must not start with a digit")
        }
        guard clickHouse.url.scheme == "http" || clickHouse.url.scheme == "https" else {
            throw ConfigError.invalidValue("ClickHouse URL must use http or https")
        }
        guard !Self.urlContainsCredentials(clickHouse.url) else {
            throw ConfigError.invalidValue("ClickHouse credentials must be configured with username/password fields, not embedded in the URL")
        }
        guard !Self.urlContainsCredentialQueryItems(clickHouse.url) else {
            throw ConfigError.invalidValue("ClickHouse URL query must not contain credential fields")
        }
        if clickHouse.usesInsecureRemoteHTTP && !clickHouse.allowInsecureRemoteHTTP {
            throw ConfigError.invalidValue("Remote ClickHouse endpoints must use https. Set allowInsecureRemoteHTTP only for an explicitly accepted private tunnel.")
        }
        guard !clickHouse.queryIdPrefix.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw ConfigError.invalidValue("ClickHouse queryIdPrefix must not be empty")
        }
        guard clickHouse.queryIdPrefix.utf8.count <= 64 else {
            throw ConfigError.invalidValue("ClickHouse queryIdPrefix must be 64 bytes or shorter")
        }
        guard Self.isSafeQueryIdPrefix(clickHouse.queryIdPrefix) else {
            throw ConfigError.invalidValue("ClickHouse queryIdPrefix may contain only ASCII letters, digits, '-' and '_'")
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

    private static func urlContainsCredentials(_ url: URL) -> Bool {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false) else { return false }
        return components.user != nil || components.password != nil
    }

    private static func urlContainsCredentialQueryItems(_ url: URL) -> Bool {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              let queryItems = components.queryItems else { return false }
        let blocked = Set(["user", "username", "password", "passwd"])
        return queryItems.contains { item in
            blocked.contains(item.name.lowercased())
        }
    }

    private static func isSafeQueryIdPrefix(_ value: String) -> Bool {
        value.utf8.allSatisfy { byte in
            (byte >= 48 && byte <= 57) ||
            (byte >= 65 && byte <= 90) ||
            (byte >= 97 && byte <= 122) ||
            byte == 45 ||
            byte == 95
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
            ("supervisor.stale_live_warning_seconds", supervisor.staleLiveWarningSeconds),
            ("supervisor.mt5_bridge_down_alert_seconds", supervisor.mt5BridgeDownAlertSeconds)
        ]
        for (name, value) in intervals {
            guard value > 0 else {
                throw ConfigError.invalidValue("\(name) must be greater than zero")
            }
        }
        guard supervisor.minimumFreeDiskBytes > 0 else {
            throw ConfigError.invalidValue("supervisor.minimum_free_disk_bytes must be greater than zero")
        }
        guard supervisor.clickHouseDiskFreeAlertBytes > 0 else {
            throw ConfigError.invalidValue("supervisor.clickhouse_disk_free_alert_bytes must be greater than zero")
        }
    }

    private func validateLogging(_ logging: OperationalLoggingConfig) throws {
        guard logging.fileLoggingEnabled else { return }
        guard logging.maxFileBytes >= 1024 else {
            throw ConfigError.invalidValue("logging.max_file_bytes must be at least 1024")
        }
        guard (0...100).contains(logging.maxRotatedFiles) else {
            throw ConfigError.invalidValue("logging.max_rotated_files must be between 0 and 100")
        }
        guard !logging.logFilePath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw ConfigError.invalidValue("logging.log_file_path must not be empty")
        }
        guard !logging.alertFilePath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw ConfigError.invalidValue("logging.alert_file_path must not be empty")
        }
    }

    private static func isReasonableTimeout(_ value: Double) -> Bool {
        value.isFinite && value > 0 && value <= 3600
    }
}
