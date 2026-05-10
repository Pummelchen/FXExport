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
    /// Optional bootstrap/reference segments. Canonical ingestion never trusts this array;
    /// it loads verified offset authority from ClickHouse `broker_time_offsets`.
    public let offsetSegments: [BrokerTimeOffsetConfig]
    public let expectedTerminalIdentity: ExpectedTerminalIdentity?

    enum CodingKeys: String, CodingKey {
        case brokerSourceId = "broker_source_id"
        case offsetSegments = "offset_segments"
        case expectedTerminalIdentity = "expected_terminal_identity"
    }

    public init(
        brokerSourceId: BrokerSourceId,
        offsetSegments: [BrokerTimeOffsetConfig],
        expectedTerminalIdentity: ExpectedTerminalIdentity? = nil
    ) {
        self.brokerSourceId = brokerSourceId
        self.offsetSegments = offsetSegments
        self.expectedTerminalIdentity = expectedTerminalIdentity
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.brokerSourceId = try container.decode(BrokerSourceId.self, forKey: .brokerSourceId)
        self.offsetSegments = try container.decodeIfPresent([BrokerTimeOffsetConfig].self, forKey: .offsetSegments) ?? []
        self.expectedTerminalIdentity = try container.decodeIfPresent(ExpectedTerminalIdentity.self, forKey: .expectedTerminalIdentity)
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
