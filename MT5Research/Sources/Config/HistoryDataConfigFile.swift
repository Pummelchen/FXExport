import Domain
import Foundation

public struct HistoryDataConfigFile: Codable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let fromUtc: UtcSecond
    public let toUtc: UtcSecond
    public let useMetal: Bool

    enum CodingKeys: String, CodingKey {
        case brokerSourceId = "broker_source_id"
        case logicalSymbol = "logical_symbol"
        case fromUtc = "from_utc"
        case toUtc = "to_utc"
        case useMetal = "use_metal"
    }

    public init(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        fromUtc: UtcSecond,
        toUtc: UtcSecond,
        useMetal: Bool
    ) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.fromUtc = fromUtc
        self.toUtc = toUtc
        self.useMetal = useMetal
    }
}

@available(*, deprecated, renamed: "HistoryDataConfigFile")
public typealias BacktestConfigFile = HistoryDataConfigFile
