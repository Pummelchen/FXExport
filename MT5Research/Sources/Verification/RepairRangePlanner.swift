import Domain
import Foundation
import TimeMapping

public enum RepairRangePlannerError: Error, CustomStringConvertible, Sendable {
    case invalidUtcRange(UtcSecond, UtcSecond)
    case utcRangeNotMinuteAligned(UtcSecond, UtcSecond)
    case missingVerifiedOffsetCoverage(startUtc: UtcSecond, nextCoveredUtc: UtcSecond?)
    case ambiguousUtcCoverage(startUtc: UtcSecond)
    case conversionOverflow

    public var description: String {
        switch self {
        case .invalidUtcRange(let from, let to):
            return "Invalid repair UTC range \(from.rawValue)..<\(to.rawValue)."
        case .utcRangeNotMinuteAligned(let from, let to):
            return "Repair UTC range must be minute-aligned: \(from.rawValue)..<\(to.rawValue)."
        case .missingVerifiedOffsetCoverage(let startUtc, let nextCoveredUtc):
            if let nextCoveredUtc {
                return "Verified broker offset coverage is missing from UTC \(startUtc.rawValue) until \(nextCoveredUtc.rawValue)."
            }
            return "Verified broker offset coverage is missing from UTC \(startUtc.rawValue) onward."
        case .ambiguousUtcCoverage(let startUtc):
            return "Verified broker offset segments overlap in UTC near \(startUtc.rawValue); repair is refused until the offset authority is unambiguous."
        case .conversionOverflow:
            return "UTC/server-time conversion overflow while planning repair range."
        }
    }
}

public struct RepairRangePlanner: Sendable {
    public init() {}

    public func mt5Ranges(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        utcStart: UtcSecond,
        utcEndExclusive: UtcSecond,
        offsetMap: BrokerOffsetMap
    ) throws -> [VerificationRange] {
        guard utcStart.rawValue < utcEndExclusive.rawValue else {
            throw RepairRangePlannerError.invalidUtcRange(utcStart, utcEndExclusive)
        }
        guard utcStart.isMinuteAligned, utcEndExclusive.isMinuteAligned else {
            throw RepairRangePlannerError.utcRangeNotMinuteAligned(utcStart, utcEndExclusive)
        }

        let windows = try offsetMap.segments.map { segment in
            let utcFrom = try subtract(segment.validFrom.rawValue, segment.offset.rawValue)
            let utcTo = try subtract(segment.validTo.rawValue, segment.offset.rawValue)
            return SegmentWindow(segment: segment, utcStart: utcFrom, utcEndExclusive: utcTo)
        }
        .filter { window in
            window.utcStart < utcEndExclusive.rawValue && window.utcEndExclusive > utcStart.rawValue
        }
        .sorted {
            if $0.utcStart == $1.utcStart {
                return $0.utcEndExclusive < $1.utcEndExclusive
            }
            return $0.utcStart < $1.utcStart
        }

        var cursor = utcStart.rawValue
        var ranges: [VerificationRange] = []
        for window in windows {
            if window.utcEndExclusive <= cursor {
                continue
            }
            if window.utcStart > cursor {
                throw RepairRangePlannerError.missingVerifiedOffsetCoverage(
                    startUtc: UtcSecond(rawValue: cursor),
                    nextCoveredUtc: UtcSecond(rawValue: window.utcStart)
                )
            }
            if !ranges.isEmpty && window.utcStart < cursor {
                throw RepairRangePlannerError.ambiguousUtcCoverage(startUtc: UtcSecond(rawValue: cursor))
            }

            let intersectStartUtc = cursor
            let intersectEndUtc = min(window.utcEndExclusive, utcEndExclusive.rawValue)
            guard intersectStartUtc < intersectEndUtc else { continue }

            let mt5Start = try add(intersectStartUtc, window.segment.offset.rawValue)
            let mt5End = try add(intersectEndUtc, window.segment.offset.rawValue)
            ranges.append(VerificationRange(
                brokerSourceId: brokerSourceId,
                logicalSymbol: logicalSymbol,
                mt5Start: MT5ServerSecond(rawValue: mt5Start),
                mt5EndExclusive: MT5ServerSecond(rawValue: mt5End)
            ))
            cursor = intersectEndUtc
            if cursor == utcEndExclusive.rawValue {
                break
            }
        }

        guard cursor == utcEndExclusive.rawValue else {
            throw RepairRangePlannerError.missingVerifiedOffsetCoverage(
                startUtc: UtcSecond(rawValue: cursor),
                nextCoveredUtc: nil
            )
        }
        return ranges
    }

    private struct SegmentWindow {
        let segment: BrokerOffsetSegment
        let utcStart: Int64
        let utcEndExclusive: Int64
    }

    private func add(_ lhs: Int64, _ rhs: Int64) throws -> Int64 {
        let result = lhs.addingReportingOverflow(rhs)
        guard !result.overflow else { throw RepairRangePlannerError.conversionOverflow }
        return result.partialValue
    }

    private func subtract(_ lhs: Int64, _ rhs: Int64) throws -> Int64 {
        let result = lhs.subtractingReportingOverflow(rhs)
        guard !result.overflow else { throw RepairRangePlannerError.conversionOverflow }
        return result.partialValue
    }
}
