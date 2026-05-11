import Foundation

public struct MT5Symbol: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let rawValue: String

    public init(_ rawValue: String) throws {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { throw DomainError.emptyMT5Symbol }
        self.rawValue = trimmed
    }

    public init?(rawValue: String) {
        do {
            try self.init(rawValue)
        } catch {
            return nil
        }
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        try self.init(try container.decode(String.self))
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    public var description: String { rawValue }

    public static func < (lhs: MT5Symbol, rhs: MT5Symbol) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}
