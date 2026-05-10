import Domain
import Foundation

public struct BatchBuilder: Sendable {
    public let chunkSize: Int

    public init(chunkSize: Int) {
        self.chunkSize = chunkSize
    }

    public func nextRange(start: MT5ServerSecond, endInclusive: MT5ServerSecond) -> (from: MT5ServerSecond, toExclusive: MT5ServerSecond) {
        let maxEndExclusive = start.rawValue + Int64(chunkSize) * Timeframe.m1.seconds
        let requestedEndExclusive = min(maxEndExclusive, endInclusive.rawValue + Timeframe.m1.seconds)
        return (start, MT5ServerSecond(rawValue: requestedEndExclusive))
    }
}
