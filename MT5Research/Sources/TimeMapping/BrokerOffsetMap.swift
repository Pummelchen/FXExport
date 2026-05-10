import Domain
import Foundation

public struct BrokerOffsetSegment: Hashable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let terminalIdentity: BrokerServerIdentity
    public let validFrom: MT5ServerSecond
    public let validTo: MT5ServerSecond
    public let offset: OffsetSeconds
    public let source: OffsetSource
    public let confidence: OffsetConfidence

    public init(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        validFrom: MT5ServerSecond,
        validTo: MT5ServerSecond,
        offset: OffsetSeconds,
        source: OffsetSource,
        confidence: OffsetConfidence
    ) {
        self.brokerSourceId = brokerSourceId
        self.terminalIdentity = terminalIdentity
        self.validFrom = validFrom
        self.validTo = validTo
        self.offset = offset
        self.source = source
        self.confidence = confidence
    }

    public func contains(_ second: MT5ServerSecond) -> Bool {
        second.rawValue >= validFrom.rawValue && second.rawValue < validTo.rawValue
    }
}

public enum TimeMappingError: Error, Equatable, CustomStringConvertible, Sendable {
    case noOffsetForMT5ServerTime(MT5ServerSecond)
    case ambiguousOffsetForMT5ServerTime(MT5ServerSecond)
    case emptyVerifiedOffsetAuthority(BrokerSourceId, BrokerServerIdentity)
    case invalidOffsetSegmentDuration(MT5ServerSecond, MT5ServerSecond)
    case offsetSegmentBrokerMismatch(expected: BrokerSourceId, actual: BrokerSourceId)
    case offsetSegmentIdentityMismatch(expected: BrokerServerIdentity, actual: BrokerServerIdentity)
    case unverifiedOffsetInCanonicalAuthority(MT5ServerSecond, OffsetConfidence)
    case offsetSegmentNotMinuteAligned(MT5ServerSecond, MT5ServerSecond)
    case offsetSecondsNotMinuteAligned(OffsetSeconds)
    case overlappingOffsetSegments(previous: MT5ServerSecond, current: MT5ServerSecond)
    case unresolvedOffset(MT5ServerSecond)
    case utcConversionOverflow(MT5ServerSecond, OffsetSeconds)
    case utcNotMinuteAligned(UtcSecond)

    public var description: String {
        switch self {
        case .noOffsetForMT5ServerTime(let time):
            return "No broker UTC offset segment covers MT5 server timestamp \(time.rawValue)."
        case .ambiguousOffsetForMT5ServerTime(let time):
            return "More than one broker UTC offset segment covers MT5 server timestamp \(time.rawValue)."
        case .emptyVerifiedOffsetAuthority(let brokerSourceId, let identity):
            return "No verified broker UTC offset authority is loaded for broker_source_id \(brokerSourceId.rawValue), MT5 identity \(identity)."
        case .invalidOffsetSegmentDuration(let from, let to):
            return "Broker UTC offset segment has invalid duration: \(from.rawValue)..<\(to.rawValue)."
        case .offsetSegmentBrokerMismatch(let expected, let actual):
            return "Broker UTC offset segment broker mismatch. Expected \(expected.rawValue), got \(actual.rawValue)."
        case .offsetSegmentIdentityMismatch(let expected, let actual):
            return "Broker UTC offset segment identity mismatch. Expected \(expected), got \(actual)."
        case .unverifiedOffsetInCanonicalAuthority(let from, let confidence):
            return "Broker UTC offset segment starting \(from.rawValue) has \(confidence.rawValue) confidence. Canonical authority accepts verified offsets only."
        case .offsetSegmentNotMinuteAligned(let from, let to):
            return "Broker UTC offset segment boundaries must be minute-aligned: \(from.rawValue)..<\(to.rawValue)."
        case .offsetSecondsNotMinuteAligned(let offset):
            return "Broker UTC offset \(offset.rawValue) seconds is not minute-aligned."
        case .overlappingOffsetSegments(let previous, let current):
            return "Broker UTC offset segments overlap at \(previous.rawValue) and \(current.rawValue)."
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
    public let terminalIdentity: BrokerServerIdentity
    public let segments: [BrokerOffsetSegment]

    public init(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        segments: [BrokerOffsetSegment],
        requireVerified: Bool = true
    ) throws {
        self.brokerSourceId = brokerSourceId
        self.terminalIdentity = terminalIdentity
        self.segments = try Self.validate(
            brokerSourceId: brokerSourceId,
            terminalIdentity: terminalIdentity,
            segments: segments.sorted { $0.validFrom < $1.validFrom },
            requireVerified: requireVerified
        )
    }

    public func segment(containing mt5ServerTime: MT5ServerSecond) throws -> BrokerOffsetSegment {
        let matches = segments.filter { $0.contains(mt5ServerTime) }
        guard !matches.isEmpty else { throw TimeMappingError.noOffsetForMT5ServerTime(mt5ServerTime) }
        guard matches.count == 1 else { throw TimeMappingError.ambiguousOffsetForMT5ServerTime(mt5ServerTime) }
        let segment = matches[0]
        guard segment.confidence != .unresolved else { throw TimeMappingError.unresolvedOffset(mt5ServerTime) }
        return segment
    }

    private static func validate(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        segments: [BrokerOffsetSegment],
        requireVerified: Bool
    ) throws -> [BrokerOffsetSegment] {
        guard !segments.isEmpty else {
            throw TimeMappingError.emptyVerifiedOffsetAuthority(brokerSourceId, terminalIdentity)
        }
        var previous: BrokerOffsetSegment?
        for segment in segments {
            guard segment.brokerSourceId == brokerSourceId else {
                throw TimeMappingError.offsetSegmentBrokerMismatch(expected: brokerSourceId, actual: segment.brokerSourceId)
            }
            guard segment.terminalIdentity == terminalIdentity else {
                throw TimeMappingError.offsetSegmentIdentityMismatch(expected: terminalIdentity, actual: segment.terminalIdentity)
            }
            guard segment.validFrom.rawValue < segment.validTo.rawValue else {
                throw TimeMappingError.invalidOffsetSegmentDuration(segment.validFrom, segment.validTo)
            }
            guard segment.validFrom.isMinuteAligned && segment.validTo.isMinuteAligned else {
                throw TimeMappingError.offsetSegmentNotMinuteAligned(segment.validFrom, segment.validTo)
            }
            guard segment.offset.rawValue % 60 == 0 else {
                throw TimeMappingError.offsetSecondsNotMinuteAligned(segment.offset)
            }
            if requireVerified {
                guard segment.confidence == .verified else {
                    throw TimeMappingError.unverifiedOffsetInCanonicalAuthority(segment.validFrom, segment.confidence)
                }
            }
            if let previous {
                guard previous.validTo.rawValue <= segment.validFrom.rawValue else {
                    throw TimeMappingError.overlappingOffsetSegments(previous: previous.validFrom, current: segment.validFrom)
                }
            }
            previous = segment
        }
        return segments
    }
}
