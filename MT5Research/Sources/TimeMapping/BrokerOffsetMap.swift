import Config
import Domain
import Foundation

public struct BrokerOffsetSegment: Hashable, Sendable {
    public let validFrom: MT5ServerSecond
    public let validTo: MT5ServerSecond
    public let offset: OffsetSeconds
    public let source: OffsetSource
    public let confidence: OffsetConfidence

    public init(config: BrokerTimeOffsetConfig) {
        self.validFrom = config.validFromMT5ServerTs
        self.validTo = config.validToMT5ServerTs
        self.offset = config.offsetSeconds
        self.source = config.source
        self.confidence = config.confidence
    }

    public func contains(_ second: MT5ServerSecond) -> Bool {
        second.rawValue >= validFrom.rawValue && second.rawValue < validTo.rawValue
    }
}

public enum TimeMappingError: Error, Equatable, CustomStringConvertible, Sendable {
    case noOffsetForMT5ServerTime(MT5ServerSecond)
    case ambiguousOffsetForMT5ServerTime(MT5ServerSecond)
    case unresolvedOffset(MT5ServerSecond)
    case utcConversionOverflow(MT5ServerSecond, OffsetSeconds)
    case utcNotMinuteAligned(UtcSecond)

    public var description: String {
        switch self {
        case .noOffsetForMT5ServerTime(let time):
            return "No broker UTC offset segment covers MT5 server timestamp \(time.rawValue)."
        case .ambiguousOffsetForMT5ServerTime(let time):
            return "More than one broker UTC offset segment covers MT5 server timestamp \(time.rawValue)."
        case .unresolvedOffset(let time):
            return "Broker UTC offset for MT5 server timestamp \(time.rawValue) is unresolved."
        case .utcConversionOverflow(let time, let offset):
            return "UTC conversion overflow for MT5 server timestamp \(time.rawValue) and offset \(offset.rawValue)."
        case .utcNotMinuteAligned(let utc):
            return "Converted UTC timestamp \(utc.rawValue) is not minute-aligned."
        }
    }
}

public struct BrokerOffsetMap: Sendable {
    public let brokerSourceId: BrokerSourceId
    public let segments: [BrokerOffsetSegment]

    public init(config: BrokerTimeConfig) {
        self.brokerSourceId = config.brokerSourceId
        self.segments = config.offsetSegments.map(BrokerOffsetSegment.init(config:)).sorted {
            $0.validFrom < $1.validFrom
        }
    }

    public init(brokerSourceId: BrokerSourceId, segments: [BrokerOffsetSegment]) {
        self.brokerSourceId = brokerSourceId
        self.segments = segments.sorted { $0.validFrom < $1.validFrom }
    }

    public func segment(containing mt5ServerTime: MT5ServerSecond) throws -> BrokerOffsetSegment {
        let matches = segments.filter { $0.contains(mt5ServerTime) }
        guard !matches.isEmpty else { throw TimeMappingError.noOffsetForMT5ServerTime(mt5ServerTime) }
        guard matches.count == 1 else { throw TimeMappingError.ambiguousOffsetForMT5ServerTime(mt5ServerTime) }
        let segment = matches[0]
        guard segment.confidence != .unresolved else { throw TimeMappingError.unresolvedOffset(mt5ServerTime) }
        return segment
    }
}
