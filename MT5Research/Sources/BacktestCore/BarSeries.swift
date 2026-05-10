import Domain
import Foundation

public struct BarSeriesMetadata: Hashable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let digits: Digits

    public init(brokerSourceId: BrokerSourceId, logicalSymbol: LogicalSymbol, digits: Digits) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.digits = digits
    }
}

public protocol BarSeries: Sendable {
    var metadata: BarSeriesMetadata { get }
    var count: Int { get }
}
