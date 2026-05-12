import Foundation

public protocol EpochSecond: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible where RawValue == Int64 {}

extension EpochSecond {
    public var description: String { String(rawValue) }

    public static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.rawValue < rhs.rawValue
    }

    public var isMinuteAligned: Bool {
        rawValue % 60 == 0
    }
}

public struct MT5ServerSecond: EpochSecond {
    public let rawValue: Int64
    public init(rawValue: Int64) { self.rawValue = rawValue }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.rawValue = try container.decode(Int64.self)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
}

public struct UtcSecond: EpochSecond {
    public let rawValue: Int64
    public init(rawValue: Int64) { self.rawValue = rawValue }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.rawValue = try container.decode(Int64.self)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
}

public struct OffsetSeconds: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let rawValue: Int64

    public init(rawValue: Int64) {
        self.rawValue = rawValue
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.rawValue = try container.decode(Int64.self)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    public var description: String { String(rawValue) }

    public static func < (lhs: OffsetSeconds, rhs: OffsetSeconds) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

public struct TimeRange<T: EpochSecond>: Codable, Hashable, Sendable {
    public let startInclusive: T
    public let endExclusive: T

    public init(startInclusive: T, endExclusive: T) throws {
        guard startInclusive.rawValue < endExclusive.rawValue else {
            throw DomainError.invalidTimeRange
        }
        self.startInclusive = startInclusive
        self.endExclusive = endExclusive
    }

    public func contains(_ second: T) -> Bool {
        second.rawValue >= startInclusive.rawValue && second.rawValue < endExclusive.rawValue
    }
}

public enum OffsetSource: String, Codable, Hashable, Sendable {
    case configured
    case inferred
    case manual
    case mt5LiveSnapshot = "mt5_live_snapshot"
    case unknown
}

public enum OffsetConfidence: String, Codable, Hashable, Sendable {
    case verified
    case inferred
    case unresolved
}
