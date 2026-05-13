import Domain
import Foundation

public enum HistoryMonthRangeBuilderError: Error, CustomStringConvertible, Sendable {
    case latestClosedBeforeUnixEpoch(Int64)
    case calendarComputationFailed(Int64)
    case overflow(Int64)

    public var description: String {
        switch self {
        case .latestClosedBeforeUnixEpoch(let value):
            return "Latest closed MT5 server timestamp \(value) is before 1970-01-01 00:00:00."
        case .calendarComputationFailed(let value):
            return "Could not compute a Gregorian month boundary for MT5 server timestamp \(value)."
        case .overflow(let value):
            return "Timestamp overflow while computing monthly history range near \(value)."
        }
    }
}

public struct HistoryMonthRange: Sendable, Equatable {
    public let from: MT5ServerSecond
    public let toExclusive: MT5ServerSecond

    public init(from: MT5ServerSecond, toExclusive: MT5ServerSecond) {
        self.from = from
        self.toExclusive = toExclusive
    }
}

public struct HistoryMonthRangeBuilder: Sendable {
    public static let firstSupportedMonthStart = MT5ServerSecond(rawValue: 0)
    public static let maximumM1BarsInCalendarMonth = 31 * 24 * 60
    public static let recommendedMonthlyFetchMaxBars = 50_000

    private var calendar: Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0) ?? TimeZone(identifier: "UTC") ?? TimeZone.autoupdatingCurrent
        return calendar
    }

    public init() {}

    public func rangesFromUnixEpoch(through latestClosed: MT5ServerSecond) throws -> [HistoryMonthRange] {
        guard latestClosed.rawValue >= Self.firstSupportedMonthStart.rawValue else {
            throw HistoryMonthRangeBuilderError.latestClosedBeforeUnixEpoch(latestClosed.rawValue)
        }
        let endExclusive = try addOneMinute(to: latestClosed)
        var ranges: [HistoryMonthRange] = []
        var cursor = Self.firstSupportedMonthStart
        while cursor.rawValue < endExclusive.rawValue {
            let next = try nextRange(start: cursor, endInclusive: latestClosed)
            ranges.append(next)
            cursor = next.toExclusive
        }
        return ranges
    }

    public func nextRange(start: MT5ServerSecond, endInclusive: MT5ServerSecond) throws -> HistoryMonthRange {
        guard start.rawValue >= Self.firstSupportedMonthStart.rawValue else {
            throw HistoryMonthRangeBuilderError.latestClosedBeforeUnixEpoch(start.rawValue)
        }
        let requestedEndExclusive = try addOneMinute(to: endInclusive)
        let nextBoundary = try nextMonthBoundary(afterOrAt: start)
        let end = min(nextBoundary.rawValue, requestedEndExclusive.rawValue)
        guard end > start.rawValue else {
            throw HistoryMonthRangeBuilderError.calendarComputationFailed(start.rawValue)
        }
        return HistoryMonthRange(from: start, toExclusive: MT5ServerSecond(rawValue: end))
    }

    private func nextMonthBoundary(afterOrAt value: MT5ServerSecond) throws -> MT5ServerSecond {
        let date = Date(timeIntervalSince1970: TimeInterval(value.rawValue))
        let calendar = calendar
        let components = calendar.dateComponents([.year, .month], from: date)
        guard let year = components.year, let month = components.month else {
            throw HistoryMonthRangeBuilderError.calendarComputationFailed(value.rawValue)
        }
        var monthStartComponents = DateComponents()
        monthStartComponents.calendar = calendar
        monthStartComponents.timeZone = calendar.timeZone
        monthStartComponents.year = year
        monthStartComponents.month = month
        monthStartComponents.day = 1
        monthStartComponents.hour = 0
        monthStartComponents.minute = 0
        monthStartComponents.second = 0
        guard let monthStart = calendar.date(from: monthStartComponents),
              let nextMonthStart = calendar.date(byAdding: .month, value: 1, to: monthStart) else {
            throw HistoryMonthRangeBuilderError.calendarComputationFailed(value.rawValue)
        }
        let nextEpoch = Int64(nextMonthStart.timeIntervalSince1970)
        guard nextEpoch > value.rawValue else {
            throw HistoryMonthRangeBuilderError.calendarComputationFailed(value.rawValue)
        }
        return MT5ServerSecond(rawValue: nextEpoch)
    }

    private func addOneMinute(to value: MT5ServerSecond) throws -> MT5ServerSecond {
        let result = value.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw HistoryMonthRangeBuilderError.overflow(value.rawValue)
        }
        return MT5ServerSecond(rawValue: result.partialValue)
    }
}
