import Domain
import Foundation

public struct TimeConversionResult: Hashable, Sendable {
    public let utcTime: UtcSecond
    public let offset: OffsetSeconds
    public let source: OffsetSource
    public let confidence: OffsetConfidence

    public init(utcTime: UtcSecond, offset: OffsetSeconds, source: OffsetSource, confidence: OffsetConfidence) {
        self.utcTime = utcTime
        self.offset = offset
        self.source = source
        self.confidence = confidence
    }
}

public struct TimeConverter: Sendable {
    private let offsetMap: BrokerOffsetMap

    public init(offsetMap: BrokerOffsetMap) {
        self.offsetMap = offsetMap
    }

    public func convert(mt5ServerTime: MT5ServerSecond) throws -> TimeConversionResult {
        let segment = try offsetMap.segment(containing: mt5ServerTime)
        let overflowResult = mt5ServerTime.rawValue.subtractingReportingOverflow(segment.offset.rawValue)
        guard !overflowResult.overflow else {
            throw TimeMappingError.utcConversionOverflow(mt5ServerTime, segment.offset)
        }
        let utc = UtcSecond(rawValue: overflowResult.partialValue)
        guard utc.isMinuteAligned else { throw TimeMappingError.utcNotMinuteAligned(utc) }
        return TimeConversionResult(utcTime: utc, offset: segment.offset, source: segment.source, confidence: segment.confidence)
    }
}
