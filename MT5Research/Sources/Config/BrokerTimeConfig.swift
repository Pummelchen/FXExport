import Domain
import Foundation

public struct BrokerTimeOffsetConfig: Codable, Hashable, Sendable {
    public let validFromMT5ServerTs: MT5ServerSecond
    public let validToMT5ServerTs: MT5ServerSecond
    public let offsetSeconds: OffsetSeconds
    public let source: OffsetSource
    public let confidence: OffsetConfidence

    enum CodingKeys: String, CodingKey {
        case validFromMT5ServerTs = "valid_from_mt5_server_ts"
        case validToMT5ServerTs = "valid_to_mt5_server_ts"
        case offsetSeconds = "offset_seconds"
        case source
        case confidence
    }

    public init(
        validFromMT5ServerTs: MT5ServerSecond,
        validToMT5ServerTs: MT5ServerSecond,
        offsetSeconds: OffsetSeconds,
        source: OffsetSource,
        confidence: OffsetConfidence
    ) {
        self.validFromMT5ServerTs = validFromMT5ServerTs
        self.validToMT5ServerTs = validToMT5ServerTs
        self.offsetSeconds = offsetSeconds
        self.source = source
        self.confidence = confidence
    }
}

public struct BrokerTimeConfig: Codable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let offsetSegments: [BrokerTimeOffsetConfig]

    enum CodingKeys: String, CodingKey {
        case brokerSourceId = "broker_source_id"
        case offsetSegments = "offset_segments"
    }

    public init(brokerSourceId: BrokerSourceId, offsetSegments: [BrokerTimeOffsetConfig]) {
        self.brokerSourceId = brokerSourceId
        self.offsetSegments = offsetSegments
    }
}
