import Domain
import Foundation

public enum IngestStatus: String, Codable, Sendable {
    case new
    case backfilling
    case live
    case warning
    case failed
}

public struct IngestState: Codable, Hashable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let mt5Symbol: MT5Symbol
    public let oldestMT5ServerTime: MT5ServerSecond
    public let latestIngestedClosedMT5ServerTime: MT5ServerSecond
    public let latestIngestedClosedUtcTime: UtcSecond
    public let status: IngestStatus
    public let lastBatchId: BatchId
    public let updatedAtUtc: UtcSecond

    public init(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        oldestMT5ServerTime: MT5ServerSecond,
        latestIngestedClosedMT5ServerTime: MT5ServerSecond,
        latestIngestedClosedUtcTime: UtcSecond,
        status: IngestStatus,
        lastBatchId: BatchId,
        updatedAtUtc: UtcSecond
    ) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.mt5Symbol = mt5Symbol
        self.oldestMT5ServerTime = oldestMT5ServerTime
        self.latestIngestedClosedMT5ServerTime = latestIngestedClosedMT5ServerTime
        self.latestIngestedClosedUtcTime = latestIngestedClosedUtcTime
        self.status = status
        self.lastBatchId = lastBatchId
        self.updatedAtUtc = updatedAtUtc
    }
}
