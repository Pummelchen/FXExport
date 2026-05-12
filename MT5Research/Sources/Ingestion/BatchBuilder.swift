import Domain
import Foundation

public struct BatchBuilder: Sendable {
    public let chunkSize: Int

    public init(chunkSize: Int) {
        self.chunkSize = chunkSize
    }

    public func nextRange(start: MT5ServerSecond, endInclusive: MT5ServerSecond) -> (from: MT5ServerSecond, toExclusive: MT5ServerSecond) {
        let chunkDuration = Int64(chunkSize).multipliedReportingOverflow(by: Timeframe.m1.seconds)
        let maxEnd = chunkDuration.overflow
            ? (partialValue: Int64.max, overflow: true)
            : start.rawValue.addingReportingOverflow(chunkDuration.partialValue)
        let inclusiveEnd = endInclusive.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        let maxEndExclusive = maxEnd.overflow ? Int64.max : maxEnd.partialValue
        let requestedEndCap = inclusiveEnd.overflow ? Int64.max : inclusiveEnd.partialValue
        let requestedEndExclusive = min(maxEndExclusive, requestedEndCap)
        return (start, MT5ServerSecond(rawValue: requestedEndExclusive))
    }
}
