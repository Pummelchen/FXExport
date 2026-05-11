import AppCore
import Foundation

public struct AppConfigFile: Codable, Sendable {
    public let chunkSize: Int
    public let liveScanIntervalSeconds: Int
    public let logLevel: LogLevel
    public let strictSymbolFailures: Bool
    public let verifierRandomRanges: Int
    public let supervisor: SupervisorConfig
    public let logging: OperationalLoggingConfig

    enum CodingKeys: String, CodingKey {
        case chunkSize = "chunk_size"
        case liveScanIntervalSeconds = "live_scan_interval_seconds"
        case logLevel = "log_level"
        case strictSymbolFailures = "strict_symbol_failures"
        case verifierRandomRanges = "verifier_random_ranges"
        case supervisor
        case logging
    }

    public init(
        chunkSize: Int,
        liveScanIntervalSeconds: Int,
        logLevel: LogLevel,
        strictSymbolFailures: Bool,
        verifierRandomRanges: Int,
        supervisor: SupervisorConfig = .default,
        logging: OperationalLoggingConfig = .default
    ) {
        self.chunkSize = chunkSize
        self.liveScanIntervalSeconds = liveScanIntervalSeconds
        self.logLevel = logLevel
        self.strictSymbolFailures = strictSymbolFailures
        self.verifierRandomRanges = verifierRandomRanges
        self.supervisor = supervisor
        self.logging = logging
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.chunkSize = try container.decode(Int.self, forKey: .chunkSize)
        self.liveScanIntervalSeconds = try container.decode(Int.self, forKey: .liveScanIntervalSeconds)
        self.logLevel = try container.decode(LogLevel.self, forKey: .logLevel)
        self.strictSymbolFailures = try container.decode(Bool.self, forKey: .strictSymbolFailures)
        self.verifierRandomRanges = try container.decode(Int.self, forKey: .verifierRandomRanges)
        self.supervisor = try container.decodeIfPresent(SupervisorConfig.self, forKey: .supervisor) ?? .default
        self.logging = try container.decodeIfPresent(OperationalLoggingConfig.self, forKey: .logging) ?? .default
    }

    public var runtimeSettings: RuntimeSettings {
        RuntimeSettings(
            chunkSize: chunkSize,
            liveScanIntervalSeconds: liveScanIntervalSeconds,
            logLevel: logLevel,
            strictSymbolFailures: strictSymbolFailures
        )
    }
}

public struct OperationalLoggingConfig: Codable, Sendable, Equatable {
    public let fileLoggingEnabled: Bool
    public let logFilePath: String
    public let alertFilePath: String
    public let maxFileBytes: UInt64
    public let maxRotatedFiles: Int

    enum CodingKeys: String, CodingKey {
        case fileLoggingEnabled = "file_logging_enabled"
        case logFilePath = "log_file_path"
        case alertFilePath = "alert_file_path"
        case maxFileBytes = "max_file_bytes"
        case maxRotatedFiles = "max_rotated_files"
    }

    public static let `default` = OperationalLoggingConfig(
        fileLoggingEnabled: true,
        logFilePath: "Logs/mt5research.log",
        alertFilePath: "Logs/alerts.jsonl",
        maxFileBytes: 10 * 1024 * 1024,
        maxRotatedFiles: 5
    )

    public init(
        fileLoggingEnabled: Bool,
        logFilePath: String,
        alertFilePath: String,
        maxFileBytes: UInt64,
        maxRotatedFiles: Int
    ) {
        self.fileLoggingEnabled = fileLoggingEnabled
        self.logFilePath = logFilePath
        self.alertFilePath = alertFilePath
        self.maxFileBytes = maxFileBytes
        self.maxRotatedFiles = maxRotatedFiles
    }
}

public struct SupervisorConfig: Codable, Sendable, Equatable {
    public let cycleSeconds: Int
    public let healthCheckIntervalSeconds: Int
    public let utcCheckIntervalSeconds: Int
    public let verificationIntervalSeconds: Int
    public let symbolMetadataCheckIntervalSeconds: Int
    public let checkpointAuditIntervalSeconds: Int
    public let backupCheckIntervalSeconds: Int
    public let alertIntervalSeconds: Int
    public let staleLiveWarningSeconds: Int
    public let mt5BridgeDownAlertSeconds: Int
    public let minimumFreeDiskBytes: Int64
    public let clickHouseDiskFreeAlertBytes: Int64
    public let runBackfillOnStart: Bool
    public let repairOnVerifierMismatch: Bool

    enum CodingKeys: String, CodingKey {
        case cycleSeconds = "cycle_seconds"
        case healthCheckIntervalSeconds = "health_check_interval_seconds"
        case utcCheckIntervalSeconds = "utc_check_interval_seconds"
        case verificationIntervalSeconds = "verification_interval_seconds"
        case symbolMetadataCheckIntervalSeconds = "symbol_metadata_check_interval_seconds"
        case checkpointAuditIntervalSeconds = "checkpoint_audit_interval_seconds"
        case backupCheckIntervalSeconds = "backup_check_interval_seconds"
        case alertIntervalSeconds = "alert_interval_seconds"
        case staleLiveWarningSeconds = "stale_live_warning_seconds"
        case mt5BridgeDownAlertSeconds = "mt5_bridge_down_alert_seconds"
        case minimumFreeDiskBytes = "minimum_free_disk_bytes"
        case clickHouseDiskFreeAlertBytes = "clickhouse_disk_free_alert_bytes"
        case runBackfillOnStart = "run_backfill_on_start"
        case repairOnVerifierMismatch = "repair_on_verifier_mismatch"
    }

    public static let `default` = SupervisorConfig(
        cycleSeconds: 10,
        healthCheckIntervalSeconds: 30,
        utcCheckIntervalSeconds: 60,
        verificationIntervalSeconds: 3600,
        symbolMetadataCheckIntervalSeconds: 300,
        checkpointAuditIntervalSeconds: 300,
        backupCheckIntervalSeconds: 3600,
        alertIntervalSeconds: 30,
        staleLiveWarningSeconds: 180,
        mt5BridgeDownAlertSeconds: 180,
        minimumFreeDiskBytes: 10 * 1024 * 1024 * 1024,
        clickHouseDiskFreeAlertBytes: 10 * 1024 * 1024 * 1024,
        runBackfillOnStart: false,
        repairOnVerifierMismatch: true
    )

    public init(
        cycleSeconds: Int,
        healthCheckIntervalSeconds: Int,
        utcCheckIntervalSeconds: Int,
        verificationIntervalSeconds: Int,
        symbolMetadataCheckIntervalSeconds: Int,
        checkpointAuditIntervalSeconds: Int,
        backupCheckIntervalSeconds: Int,
        alertIntervalSeconds: Int,
        staleLiveWarningSeconds: Int,
        mt5BridgeDownAlertSeconds: Int,
        minimumFreeDiskBytes: Int64,
        clickHouseDiskFreeAlertBytes: Int64,
        runBackfillOnStart: Bool,
        repairOnVerifierMismatch: Bool
    ) {
        self.cycleSeconds = cycleSeconds
        self.healthCheckIntervalSeconds = healthCheckIntervalSeconds
        self.utcCheckIntervalSeconds = utcCheckIntervalSeconds
        self.verificationIntervalSeconds = verificationIntervalSeconds
        self.symbolMetadataCheckIntervalSeconds = symbolMetadataCheckIntervalSeconds
        self.checkpointAuditIntervalSeconds = checkpointAuditIntervalSeconds
        self.backupCheckIntervalSeconds = backupCheckIntervalSeconds
        self.alertIntervalSeconds = alertIntervalSeconds
        self.staleLiveWarningSeconds = staleLiveWarningSeconds
        self.mt5BridgeDownAlertSeconds = mt5BridgeDownAlertSeconds
        self.minimumFreeDiskBytes = minimumFreeDiskBytes
        self.clickHouseDiskFreeAlertBytes = clickHouseDiskFreeAlertBytes
        self.runBackfillOnStart = runBackfillOnStart
        self.repairOnVerifierMismatch = repairOnVerifierMismatch
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let defaults = Self.default
        self.cycleSeconds = try container.decodeIfPresent(Int.self, forKey: .cycleSeconds) ?? defaults.cycleSeconds
        self.healthCheckIntervalSeconds = try container.decodeIfPresent(Int.self, forKey: .healthCheckIntervalSeconds) ?? defaults.healthCheckIntervalSeconds
        self.utcCheckIntervalSeconds = try container.decodeIfPresent(Int.self, forKey: .utcCheckIntervalSeconds) ?? defaults.utcCheckIntervalSeconds
        self.verificationIntervalSeconds = try container.decodeIfPresent(Int.self, forKey: .verificationIntervalSeconds) ?? defaults.verificationIntervalSeconds
        self.symbolMetadataCheckIntervalSeconds = try container.decodeIfPresent(Int.self, forKey: .symbolMetadataCheckIntervalSeconds) ?? defaults.symbolMetadataCheckIntervalSeconds
        self.checkpointAuditIntervalSeconds = try container.decodeIfPresent(Int.self, forKey: .checkpointAuditIntervalSeconds) ?? defaults.checkpointAuditIntervalSeconds
        self.backupCheckIntervalSeconds = try container.decodeIfPresent(Int.self, forKey: .backupCheckIntervalSeconds) ?? defaults.backupCheckIntervalSeconds
        self.alertIntervalSeconds = try container.decodeIfPresent(Int.self, forKey: .alertIntervalSeconds) ?? defaults.alertIntervalSeconds
        self.staleLiveWarningSeconds = try container.decodeIfPresent(Int.self, forKey: .staleLiveWarningSeconds) ?? defaults.staleLiveWarningSeconds
        self.mt5BridgeDownAlertSeconds = try container.decodeIfPresent(Int.self, forKey: .mt5BridgeDownAlertSeconds) ?? defaults.mt5BridgeDownAlertSeconds
        self.minimumFreeDiskBytes = try container.decodeIfPresent(Int64.self, forKey: .minimumFreeDiskBytes) ?? defaults.minimumFreeDiskBytes
        self.clickHouseDiskFreeAlertBytes = try container.decodeIfPresent(Int64.self, forKey: .clickHouseDiskFreeAlertBytes) ?? defaults.clickHouseDiskFreeAlertBytes
        self.runBackfillOnStart = try container.decodeIfPresent(Bool.self, forKey: .runBackfillOnStart) ?? defaults.runBackfillOnStart
        self.repairOnVerifierMismatch = try container.decodeIfPresent(Bool.self, forKey: .repairOnVerifierMismatch) ?? defaults.repairOnVerifierMismatch
    }
}
