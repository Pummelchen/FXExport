import Domain
import Foundation
import MT5Bridge
import TimeMapping

public enum CoverageRangeError: Error, CustomStringConvertible, Sendable {
    case invalidMT5Range(MT5ServerSecond, MT5ServerSecond)
    case conversionOverflow
    case invalidUTCRange(UtcSecond, UtcSecond)

    public var description: String {
        switch self {
        case .invalidMT5Range(let start, let end):
            return "Invalid MT5 coverage range \(start.rawValue)..<\(end.rawValue)."
        case .conversionOverflow:
            return "Coverage UTC boundary conversion overflowed."
        case .invalidUTCRange(let start, let end):
            return "Converted coverage UTC range is invalid \(start.rawValue)..<\(end.rawValue)."
        }
    }
}

public struct CoverageRangeBuilder: Sendable {
    private let offsetMap: BrokerOffsetMap
    private let timeConverter: TimeConverter

    public init(offsetMap: BrokerOffsetMap) {
        self.offsetMap = offsetMap
        self.timeConverter = TimeConverter(offsetMap: offsetMap)
    }

    public func makeRecords(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        mt5Start: MT5ServerSecond,
        mt5EndExclusive: MT5ServerSecond,
        sourceBars: [MT5RateDTO],
        canonicalBars: [ValidatedBar],
        sourceHash: String,
        verificationMethod: String,
        batchId: BatchId,
        verifiedAtUtc: UtcSecond
    ) throws -> [VerifiedCoverageRecord] {
        guard mt5Start.rawValue < mt5EndExclusive.rawValue,
              mt5Start.isMinuteAligned,
              mt5EndExclusive.isMinuteAligned else {
            throw CoverageRangeError.invalidMT5Range(mt5Start, mt5EndExclusive)
        }

        var cursor = mt5Start
        var records: [VerifiedCoverageRecord] = []
        for segment in offsetMap.segments {
            guard segment.validTo.rawValue > cursor.rawValue else { continue }
            guard segment.validFrom.rawValue < mt5EndExclusive.rawValue else { break }
            guard segment.validFrom.rawValue <= cursor.rawValue else {
                throw TimeMappingError.noOffsetForMT5ServerTime(cursor)
            }

            let subrangeEnd = MT5ServerSecond(rawValue: min(segment.validTo.rawValue, mt5EndExclusive.rawValue))
            let sourceCount = sourceBars.filter { rate in
                rate.mt5ServerTime >= cursor.rawValue && rate.mt5ServerTime < subrangeEnd.rawValue
            }.count
            let canonicalCount = canonicalBars.filter { bar in
                bar.mt5ServerTime.rawValue >= cursor.rawValue && bar.mt5ServerTime.rawValue < subrangeEnd.rawValue
            }.count
            records.append(try makeRecord(
                brokerSourceId: brokerSourceId,
                logicalSymbol: logicalSymbol,
                mt5Symbol: mt5Symbol,
                mt5Start: cursor,
                mt5EndExclusive: subrangeEnd,
                sourceBarCount: sourceCount,
                canonicalRowCount: canonicalCount,
                sourceHash: sourceHash,
                verificationMethod: verificationMethod,
                batchId: batchId,
                verifiedAtUtc: verifiedAtUtc
            ))
            cursor = subrangeEnd
            if cursor.rawValue >= mt5EndExclusive.rawValue {
                break
            }
        }

        guard cursor.rawValue == mt5EndExclusive.rawValue, !records.isEmpty else {
            throw TimeMappingError.noOffsetForMT5ServerTime(cursor)
        }
        return records
    }

    public func makeRecord(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        mt5Start: MT5ServerSecond,
        mt5EndExclusive: MT5ServerSecond,
        sourceBarCount: Int,
        canonicalRowCount: Int,
        sourceHash: String,
        verificationMethod: String,
        batchId: BatchId,
        verifiedAtUtc: UtcSecond
    ) throws -> VerifiedCoverageRecord {
        guard mt5Start.rawValue < mt5EndExclusive.rawValue,
              mt5Start.isMinuteAligned,
              mt5EndExclusive.isMinuteAligned else {
            throw CoverageRangeError.invalidMT5Range(mt5Start, mt5EndExclusive)
        }
        let lastCoveredMT5 = mt5EndExclusive.rawValue.subtractingReportingOverflow(Timeframe.m1.seconds)
        guard !lastCoveredMT5.overflow else {
            throw CoverageRangeError.conversionOverflow
        }
        let utcStart = try timeConverter.convert(mt5ServerTime: mt5Start).utcTime
        let lastUtc = try timeConverter.convert(mt5ServerTime: MT5ServerSecond(rawValue: lastCoveredMT5.partialValue)).utcTime
        let utcEndRaw = lastUtc.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !utcEndRaw.overflow else {
            throw CoverageRangeError.conversionOverflow
        }
        let utcEnd = UtcSecond(rawValue: utcEndRaw.partialValue)
        guard utcStart.rawValue < utcEnd.rawValue else {
            throw CoverageRangeError.invalidUTCRange(utcStart, utcEnd)
        }
        return VerifiedCoverageRecord(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            mt5Symbol: mt5Symbol,
            timeframe: .m1,
            mt5Start: mt5Start,
            mt5EndExclusive: mt5EndExclusive,
            utcStart: utcStart,
            utcEndExclusive: utcEnd,
            sourceBarCount: sourceBarCount,
            canonicalRowCount: canonicalRowCount,
            sourceHash: sourceHash,
            verificationMethod: verificationMethod,
            batchId: batchId,
            verifiedAtUtc: verifiedAtUtc
        )
    }
}
