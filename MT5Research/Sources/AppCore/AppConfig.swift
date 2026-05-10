import Foundation

public struct RuntimeSettings: Codable, Sendable {
    public let chunkSize: Int
    public let liveScanIntervalSeconds: Int
    public let logLevel: LogLevel
    public let strictSymbolFailures: Bool

    public init(
        chunkSize: Int,
        liveScanIntervalSeconds: Int,
        logLevel: LogLevel,
        strictSymbolFailures: Bool
    ) {
        self.chunkSize = chunkSize
        self.liveScanIntervalSeconds = liveScanIntervalSeconds
        self.logLevel = logLevel
        self.strictSymbolFailures = strictSymbolFailures
    }
}
