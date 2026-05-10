import Domain
import Foundation

public struct Gap: Hashable, Sendable {
    public let previous: MT5ServerSecond
    public let next: MT5ServerSecond

    public init(previous: MT5ServerSecond, next: MT5ServerSecond) {
        self.previous = previous
        self.next = next
    }
}

public struct GapDetector: Sendable {
    public init() {}

    public func gapsPresentInMT5ButMissingFromDatabase(mt5Bars: [ClosedM1Bar], databaseBars: [ValidatedBar]) -> [MT5ServerSecond] {
        let databaseTimes = Set(databaseBars.map(\.mt5ServerTime))
        return mt5Bars.map(\.mt5ServerTime).filter { !databaseTimes.contains($0) }
    }

    public func calendarMinuteGaps(in bars: [ClosedM1Bar]) -> [Gap] {
        guard bars.count >= 2 else { return [] }
        var gaps: [Gap] = []
        for index in 1..<bars.count {
            let previous = bars[index - 1].mt5ServerTime
            let current = bars[index].mt5ServerTime
            if current.rawValue - previous.rawValue > Timeframe.m1.seconds {
                gaps.append(Gap(previous: previous, next: current))
            }
        }
        return gaps
    }
}
