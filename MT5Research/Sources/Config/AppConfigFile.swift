import AppCore
import Foundation

public struct AppConfigFile: Codable, Sendable {
    public let chunkSize: Int
    public let liveScanIntervalSeconds: Int
    public let logLevel: LogLevel
    public let strictSymbolFailures: Bool
    public let verifierRandomRanges: Int
    public let supervisor: SupervisorConfig

    enum CodingKeys: String, CodingKey {
        case chunkSize = "chunk_size"
        case liveScanIntervalSeconds = "live_scan_interval_seconds"
        case logLevel = "log_level"
        case strictSymbolFailures = "strict_symbol_failures"
        case verifierRandomRanges = "verifier_random_ranges"
        case supervisor
    }

    public init(
        chunkSize: Int,
        liveScanIntervalSeconds: Int,
        logLevel: LogLevel,
        strictSymbolFailures: Bool,
        verifierRandomRanges: Int,
        supervisor: SupervisorConfig = .default
    ) {
        self.chunkSize = chunkSize
        self.liveScanIntervalSeconds = liveScanIntervalSeconds
        self.logLevel = logLevel
        self.strictSymbolFailures = strictSymbolFailures
        self.verifierRandomRanges = verifierRandomRanges
        self.supervisor = supervisor
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.chunkSize = try container.decode(Int.self, forKey: .chunkSize)
        self.liveScanIntervalSeconds = try container.decode(Int.self, forKey: .liveScanIntervalSeconds)
        self.logLevel = try container.decode(LogLevel.self, forKey: .logLevel)
        self.strictSymbolFailures = try container.decode(Bool.self, forKey: .strictSymbolFailures)
        self.verifierRandomRanges = try container.decode(Int.self, forKey: .verifierRandomRanges)
        self.supervisor = try container.decodeIfPresent(SupervisorConfig.self, forKey: .supervisor) ?? .default
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
        self.runBackfillOnStart = runBackfillOnStart
        self.repairOnVerifierMismatch = repairOnVerifierMismatch
    }
}
