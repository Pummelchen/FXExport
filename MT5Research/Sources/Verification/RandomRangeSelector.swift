import Domain
import Foundation

public enum RandomRangeSelectorError: Error, CustomStringConvertible, Sendable {
    case calendarCalculationFailed(Int64)
    case latestClosedOverflow(MT5ServerSecond)

    public var description: String {
        switch self {
        case .calendarCalculationFailed(let timestamp):
            return "Could not calculate a calendar month boundary for MT5 server timestamp \(timestamp)."
        case .latestClosedOverflow(let timestamp):
            return "Latest closed MT5 server timestamp \(timestamp.rawValue) overflows while building verification range."
        }
    }
}

public struct VerificationRange: Hashable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let mt5Start: MT5ServerSecond
    public let mt5EndExclusive: MT5ServerSecond

    public init(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Start: MT5ServerSecond,
        mt5EndExclusive: MT5ServerSecond
    ) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.mt5Start = mt5Start
        self.mt5EndExclusive = mt5EndExclusive
    }
}

public struct RandomRangeSelector: Sendable {
    public init() {}

    public func selectMonth<R: RandomNumberGenerator>(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        oldest: MT5ServerSecond,
        latestClosed: MT5ServerSecond,
        random: inout R
    ) throws -> VerificationRange {
        let latestExclusive = try Self.latestClosedExclusive(latestClosed)
        guard latestExclusive.rawValue > oldest.rawValue else {
            return VerificationRange(
                brokerSourceId: brokerSourceId,
                logicalSymbol: logicalSymbol,
                mt5Start: oldest,
                mt5EndExclusive: latestExclusive
            )
        }
        let monthStarts = try Self.calendarMonthStarts(from: oldest.rawValue, through: latestClosed.rawValue)
        guard !monthStarts.isEmpty else {
            return VerificationRange(
                brokerSourceId: brokerSourceId,
                logicalSymbol: logicalSymbol,
                mt5Start: oldest,
                mt5EndExclusive: latestExclusive
            )
        }
        let selectedStart = monthStarts[Int.random(in: 0..<monthStarts.count, using: &random)]
        let selectedEnd = try Self.nextMonthStart(after: selectedStart)
        let clippedStart = max(oldest.rawValue, selectedStart)
        let clippedEnd = min(latestExclusive.rawValue, selectedEnd)
        guard clippedStart < clippedEnd else {
            return VerificationRange(
                brokerSourceId: brokerSourceId,
                logicalSymbol: logicalSymbol,
                mt5Start: oldest,
                mt5EndExclusive: latestExclusive
            )
        }
        return VerificationRange(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            mt5Start: MT5ServerSecond(rawValue: clippedStart),
            mt5EndExclusive: MT5ServerSecond(rawValue: clippedEnd)
        )
    }

    private static func latestClosedExclusive(_ latestClosed: MT5ServerSecond) throws -> MT5ServerSecond {
        let result = latestClosed.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw RandomRangeSelectorError.latestClosedOverflow(latestClosed)
        }
        return MT5ServerSecond(rawValue: result.partialValue)
    }

    private static func calendarMonthStarts(from oldest: Int64, through latest: Int64) throws -> [Int64] {
        var starts: [Int64] = []
        let calendar = utcCalendar()
        let firstStart = try monthStart(for: oldest, calendar: calendar)
        let lastStart = try monthStart(for: latest, calendar: calendar)
        var current = Date(timeIntervalSince1970: TimeInterval(firstStart))
        let last = Date(timeIntervalSince1970: TimeInterval(lastStart))
        while current <= last {
            starts.append(Int64(current.timeIntervalSince1970))
            guard let next = calendar.date(byAdding: .month, value: 1, to: current) else {
                throw RandomRangeSelectorError.calendarCalculationFailed(Int64(current.timeIntervalSince1970))
            }
            current = next
        }
        return starts
    }

    private static func monthStart(for epochSeconds: Int64, calendar: Calendar) throws -> Int64 {
        let date = Date(timeIntervalSince1970: TimeInterval(epochSeconds))
        let components = calendar.dateComponents([.year, .month], from: date)
        guard let monthStart = calendar.date(from: components) else {
            throw RandomRangeSelectorError.calendarCalculationFailed(epochSeconds)
        }
        return Int64(monthStart.timeIntervalSince1970)
    }

    private static func nextMonthStart(after epochSeconds: Int64) throws -> Int64 {
        let calendar = utcCalendar()
        let start = Date(timeIntervalSince1970: TimeInterval(epochSeconds))
        guard let next = calendar.date(byAdding: .month, value: 1, to: start) else {
            throw RandomRangeSelectorError.calendarCalculationFailed(epochSeconds)
        }
        return Int64(next.timeIntervalSince1970)
    }

    private static func utcCalendar() -> Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0) ?? TimeZone(identifier: "UTC") ?? TimeZone.autoupdatingCurrent
        return calendar
    }
}
