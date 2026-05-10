import AppCore
import Foundation

public struct AppConfigFile: Codable, Sendable {
    public let chunkSize: Int
    public let liveScanIntervalSeconds: Int
    public let logLevel: LogLevel
    public let strictSymbolFailures: Bool
    public let verifierRandomRanges: Int

    enum CodingKeys: String, CodingKey {
        case chunkSize = "chunk_size"
        case liveScanIntervalSeconds = "live_scan_interval_seconds"
        case logLevel = "log_level"
        case strictSymbolFailures = "strict_symbol_failures"
        case verifierRandomRanges = "verifier_random_ranges"
    }

    public init(
        chunkSize: Int,
        liveScanIntervalSeconds: Int,
        logLevel: LogLevel,
        strictSymbolFailures: Bool,
        verifierRandomRanges: Int
    ) {
        self.chunkSize = chunkSize
        self.liveScanIntervalSeconds = liveScanIntervalSeconds
        self.logLevel = logLevel
        self.strictSymbolFailures = strictSymbolFailures
        self.verifierRandomRanges = verifierRandomRanges
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
