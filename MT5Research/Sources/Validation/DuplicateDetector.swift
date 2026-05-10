import Domain
import Foundation

public struct DuplicateKey: Hashable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let utcTime: UtcSecond

    public init(brokerSourceId: BrokerSourceId, logicalSymbol: LogicalSymbol, utcTime: UtcSecond) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.utcTime = utcTime
    }
}

public struct DuplicateDetector: Sendable {
    public init() {}

    public func duplicates(in bars: [ValidatedBar]) -> [DuplicateKey] {
        var seen = Set<DuplicateKey>()
        var duplicates = Set<DuplicateKey>()
        for bar in bars {
            let key = DuplicateKey(brokerSourceId: bar.brokerSourceId, logicalSymbol: bar.logicalSymbol, utcTime: bar.utcTime)
            if !seen.insert(key).inserted {
                duplicates.insert(key)
            }
        }
        return duplicates.sorted { lhs, rhs in
            if lhs.logicalSymbol != rhs.logicalSymbol { return lhs.logicalSymbol < rhs.logicalSymbol }
            return lhs.utcTime < rhs.utcTime
        }
    }
}
