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
    public let isAutomatic: Bool
    /// Optional bootstrap/reference segments. Canonical ingestion never trusts this array;
    /// it loads verified offset authority from ClickHouse `broker_time_offsets`.
    public let offsetSegments: [BrokerTimeOffsetConfig]
    public let expectedTerminalIdentity: ExpectedTerminalIdentity?
    public let acceptedLiveOffsetSeconds: [OffsetSeconds]

    enum CodingKeys: String, CodingKey {
        case brokerSourceId = "broker_source_id"
        case isAutomatic = "is_automatic"
        case offsetSegments = "offset_segments"
        case expectedTerminalIdentity = "expected_terminal_identity"
        case acceptedLiveOffsetSeconds = "accepted_live_offset_seconds"
    }

    public init(
        brokerSourceId: BrokerSourceId,
        isAutomatic: Bool = false,
        offsetSegments: [BrokerTimeOffsetConfig],
        expectedTerminalIdentity: ExpectedTerminalIdentity? = nil,
        acceptedLiveOffsetSeconds: [OffsetSeconds] = []
    ) {
        self.brokerSourceId = brokerSourceId
        self.isAutomatic = isAutomatic
        self.offsetSegments = offsetSegments
        self.expectedTerminalIdentity = expectedTerminalIdentity
        self.acceptedLiveOffsetSeconds = acceptedLiveOffsetSeconds
    }

    public static func automatic() throws -> BrokerTimeConfig {
        BrokerTimeConfig(
            brokerSourceId: try BrokerSourceId("auto"),
            isAutomatic: true,
            offsetSegments: [],
            expectedTerminalIdentity: nil,
            acceptedLiveOffsetSeconds: []
        )
    }

    public func resolving(brokerSourceId: BrokerSourceId) -> BrokerTimeConfig {
        BrokerTimeConfig(
            brokerSourceId: brokerSourceId,
            isAutomatic: false,
            offsetSegments: offsetSegments,
            expectedTerminalIdentity: expectedTerminalIdentity,
            acceptedLiveOffsetSeconds: acceptedLiveOffsetSeconds
        )
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.brokerSourceId = try container.decode(BrokerSourceId.self, forKey: .brokerSourceId)
        self.isAutomatic = try container.decodeIfPresent(Bool.self, forKey: .isAutomatic) ?? false
        self.offsetSegments = try container.decodeIfPresent([BrokerTimeOffsetConfig].self, forKey: .offsetSegments) ?? []
        self.expectedTerminalIdentity = try container.decodeIfPresent(ExpectedTerminalIdentity.self, forKey: .expectedTerminalIdentity)
        self.acceptedLiveOffsetSeconds = try container.decodeIfPresent([OffsetSeconds].self, forKey: .acceptedLiveOffsetSeconds) ?? []
    }
}

public struct ExpectedTerminalIdentity: Codable, Hashable, Sendable {
    public let company: String?
    public let server: String?
    public let accountLogin: Int64?

    enum CodingKeys: String, CodingKey {
        case company
        case server
        case accountLogin = "account_login"
    }

    public init(company: String?, server: String?, accountLogin: Int64?) {
        self.company = company
        self.server = server
        self.accountLogin = accountLogin
    }

    public var isEmpty: Bool {
        company == nil && server == nil && accountLogin == nil
    }
}
