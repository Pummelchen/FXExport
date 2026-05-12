import Domain
import Foundation

public enum OperatorStatusText {
    private static var utcTimeZone: TimeZone {
        TimeZone(secondsFromGMT: 0) ?? TimeZone(identifier: "UTC") ?? TimeZone.autoupdatingCurrent
    }

    public static func monthRangeLabel(startEpochSeconds: Int64, endExclusiveEpochSeconds: Int64) -> String {
        let lastEpochSecond = max(startEpochSeconds, endExclusiveEpochSeconds - Timeframe.m1.seconds)
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = utcTimeZone
        let startDate = Date(timeIntervalSince1970: TimeInterval(startEpochSeconds))
        let endDate = Date(timeIntervalSince1970: TimeInterval(lastEpochSecond))
        let startComponents = calendar.dateComponents([.year, .month], from: startDate)
        let endComponents = calendar.dateComponents([.year, .month], from: endDate)
        guard startComponents.year == endComponents.year,
              startComponents.month == endComponents.month else {
            return "\(monthYearLabel(startEpochSeconds))-\(monthYearLabel(lastEpochSecond))"
        }
        return monthYearLabel(startEpochSeconds)
    }

    public static func monthRangeLabel(start: MT5ServerSecond, endExclusive: MT5ServerSecond) -> String {
        monthRangeLabel(startEpochSeconds: start.rawValue, endExclusiveEpochSeconds: endExclusive.rawValue)
    }

    public static func monthRangeLabel(start: UtcSecond, endExclusive: UtcSecond) -> String {
        monthRangeLabel(startEpochSeconds: start.rawValue, endExclusiveEpochSeconds: endExclusive.rawValue)
    }

    public static func monthYearLabel(_ epochSeconds: Int64) -> String {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = utcTimeZone
        formatter.dateFormat = "LLLL yyyy"
        return formatter.string(from: Date(timeIntervalSince1970: TimeInterval(epochSeconds)))
    }
}
