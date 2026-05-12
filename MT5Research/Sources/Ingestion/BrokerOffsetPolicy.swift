import Domain
import Foundation
import TimeMapping

public enum BrokerOffsetPolicy {
    /// Code-owned broker defaults replace fragile local broker-time configuration.
    /// Unknown brokers are not restricted here; they are still protected by the
    /// EA-observed live snapshot and audited ClickHouse offset authority.
    public static func acceptedLiveOffsets(for identity: BrokerServerIdentity) -> [OffsetSeconds] {
        let server = identity.server.lowercased()
        if server.contains("icmarkets") && server.contains("mt5") {
            return [
                OffsetSeconds(rawValue: 7_200),
                OffsetSeconds(rawValue: 10_800)
            ]
        }
        return []
    }

    public static func hasAutomaticHistoricalPolicy(for identity: BrokerServerIdentity) -> Bool {
        isICMarketsMT5(identity)
    }

    public static func historicalSegments(
        for identity: BrokerServerIdentity,
        brokerSourceId: BrokerSourceId,
        covering requiredFrom: MT5ServerSecond,
        to requiredToExclusive: MT5ServerSecond
    ) throws -> [BrokerOffsetSegment] {
        guard requiredFrom.rawValue < requiredToExclusive.rawValue else { return [] }
        guard isICMarketsMT5(identity) else { return [] }

        let years = try serverCalendarYearRange(from: requiredFrom, to: requiredToExclusive)
        var segments: [BrokerOffsetSegment] = []
        for year in years {
            let yearStart = try serverTimestamp(year: year, month: 1, day: 1)
            let dstStart = try serverTimestamp(year: year, month: 3, day: try nthSunday(year: year, month: 3, n: 2))
            let dstEnd = try serverTimestamp(year: year, month: 11, day: try nthSunday(year: year, month: 11, n: 1))
            let nextYearStart = try serverTimestamp(year: year + 1, month: 1, day: 1)

            segments.append(try clippedSegment(
                brokerSourceId: brokerSourceId,
                identity: identity,
                validFrom: yearStart,
                validTo: dstStart,
                offset: 7_200,
                requiredFrom: requiredFrom,
                requiredToExclusive: requiredToExclusive
            ))
            segments.append(try clippedSegment(
                brokerSourceId: brokerSourceId,
                identity: identity,
                validFrom: dstStart,
                validTo: dstEnd,
                offset: 10_800,
                requiredFrom: requiredFrom,
                requiredToExclusive: requiredToExclusive
            ))
            segments.append(try clippedSegment(
                brokerSourceId: brokerSourceId,
                identity: identity,
                validFrom: dstEnd,
                validTo: nextYearStart,
                offset: 7_200,
                requiredFrom: requiredFrom,
                requiredToExclusive: requiredToExclusive
            ))
        }

        return mergeAdjacent(
            segments
                .filter { $0.validFrom.rawValue < $0.validTo.rawValue }
                .sorted { $0.validFrom < $1.validFrom }
        )
    }

    public static func policyOffset(
        for identity: BrokerServerIdentity,
        at serverTime: MT5ServerSecond,
        brokerSourceId: BrokerSourceId
    ) throws -> OffsetSeconds? {
        guard isICMarketsMT5(identity) else { return nil }
        let windowSeconds: Int64 = 366 * 86_400
        let start = serverTime.rawValue.subtractingReportingOverflow(windowSeconds)
        let end = serverTime.rawValue.addingReportingOverflow(windowSeconds)
        guard !start.overflow, !end.overflow else {
            throw IngestError.invalidChunk("broker policy live timestamp window overflow")
        }
        let windowStart = MT5ServerSecond(rawValue: start.partialValue)
        let windowEnd = MT5ServerSecond(rawValue: end.partialValue)
        return try historicalSegments(
            for: identity,
            brokerSourceId: brokerSourceId,
            covering: windowStart,
            to: windowEnd
        )
        .first { $0.contains(serverTime) }?
        .offset
    }

    public static func policyName(for identity: BrokerServerIdentity) -> String? {
        isICMarketsMT5(identity) ? "icmarkets_us_dst_new_york_close" : nil
    }

    private static func isICMarketsMT5(_ identity: BrokerServerIdentity) -> Bool {
        let server = identity.server.lowercased()
        return server.contains("icmarkets") && server.contains("mt5")
    }

    private static func clippedSegment(
        brokerSourceId: BrokerSourceId,
        identity: BrokerServerIdentity,
        validFrom: MT5ServerSecond,
        validTo: MT5ServerSecond,
        offset: Int64,
        requiredFrom: MT5ServerSecond,
        requiredToExclusive: MT5ServerSecond
    ) throws -> BrokerOffsetSegment {
        BrokerOffsetSegment(
            brokerSourceId: brokerSourceId,
            terminalIdentity: identity,
            validFrom: MT5ServerSecond(rawValue: max(validFrom.rawValue, requiredFrom.rawValue)),
            validTo: MT5ServerSecond(rawValue: min(validTo.rawValue, requiredToExclusive.rawValue)),
            offset: OffsetSeconds(rawValue: offset),
            source: .brokerPolicy,
            confidence: .verified
        )
    }

    private static func mergeAdjacent(_ segments: [BrokerOffsetSegment]) -> [BrokerOffsetSegment] {
        var merged: [BrokerOffsetSegment] = []
        for segment in segments {
            guard let last = merged.last,
                  last.validTo == segment.validFrom,
                  last.offset == segment.offset,
                  last.source == segment.source,
                  last.confidence == segment.confidence else {
                merged.append(segment)
                continue
            }
            merged[merged.count - 1] = BrokerOffsetSegment(
                brokerSourceId: last.brokerSourceId,
                terminalIdentity: last.terminalIdentity,
                validFrom: last.validFrom,
                validTo: segment.validTo,
                offset: last.offset,
                source: last.source,
                confidence: last.confidence
            )
        }
        return merged
    }

    private static func serverCalendarYearRange(from start: MT5ServerSecond, to endExclusive: MT5ServerSecond) throws -> ClosedRange<Int> {
        let startYear = try serverYear(for: start.rawValue) - 1
        let endYear = try serverYear(for: endExclusive.rawValue) + 1
        return startYear...endYear
    }

    private static func serverYear(for rawServerSecond: Int64) throws -> Int {
        let date = Date(timeIntervalSince1970: TimeInterval(rawServerSecond))
        return try serverCalendar().component(.year, from: date)
    }

    private static func nthSunday(year: Int, month: Int, n: Int) throws -> Int {
        let calendar = try serverCalendar()
        let timeZone = try utcTimeZone()
        guard let firstOfMonth = DateComponents(calendar: calendar, timeZone: timeZone, year: year, month: month, day: 1).date else {
            throw IngestError.invalidChunk("could not build broker policy month start for \(year)-\(month)")
        }
        let weekday = calendar.component(.weekday, from: firstOfMonth)
        let daysUntilFirstSunday = (8 - weekday) % 7
        return 1 + daysUntilFirstSunday + (n - 1) * 7
    }

    private static func serverTimestamp(year: Int, month: Int, day: Int) throws -> MT5ServerSecond {
        let calendar = try serverCalendar()
        let timeZone = try utcTimeZone()
        guard let date = DateComponents(
            calendar: calendar,
            timeZone: timeZone,
            year: year,
            month: month,
            day: day,
            hour: 0,
            minute: 0,
            second: 0
        ).date else {
            throw IngestError.invalidChunk("could not build broker policy timestamp for \(year)-\(month)-\(day)")
        }
        return MT5ServerSecond(rawValue: Int64(date.timeIntervalSince1970))
    }

    /// MT5 server timestamps are broker-local epoch seconds. For policy generation
    /// we intentionally use a fixed UTC calendar as a neutral server-date calendar;
    /// the Mac local timezone must never influence canonical UTC conversion.
    private static func utcTimeZone() throws -> TimeZone {
        guard let timeZone = TimeZone(secondsFromGMT: 0) ?? TimeZone(identifier: "UTC") else {
            throw IngestError.invalidChunk("could not create fixed UTC timezone for broker offset policy")
        }
        return timeZone
    }

    private static func serverCalendar() throws -> Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = try utcTimeZone()
        return calendar
    }
}
