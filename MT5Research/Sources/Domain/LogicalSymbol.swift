import Foundation

public struct LogicalSymbol: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let rawValue: String

    public init(_ rawValue: String) throws {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { throw DomainError.emptyLogicalSymbol }
        guard trimmed == trimmed.uppercased() else { throw DomainError.invalidLogicalSymbol(rawValue) }
        guard trimmed.allSatisfy({ $0.isUppercase || $0.isNumber }) else {
            throw DomainError.invalidLogicalSymbol(rawValue)
        }
        self.rawValue = trimmed
    }

    public init(rawValue: String) {
        self.rawValue = rawValue
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

    public static func < (lhs: LogicalSymbol, rhs: LogicalSymbol) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

public enum DomainError: Error, Equatable, CustomStringConvertible, Sendable {
    case emptyLogicalSymbol
    case invalidLogicalSymbol(String)
    case emptyMT5Symbol
    case emptyBrokerSourceId
    case emptyBrokerCompany
    case emptyBrokerServer
    case invalidBrokerAccountLogin(Int64)
    case invalidDigits(Int)
    case invalidPrice(String)
    case priceScaleOverflow(String)
    case invalidMinuteAlignment(Int64)
    case invalidTimeRange

    public var description: String {
        switch self {
        case .emptyLogicalSymbol:
            return "Logical symbol is empty."
        case .invalidLogicalSymbol(let value):
            return "Logical symbol '\(value)' must be uppercase letters/numbers only."
        case .emptyMT5Symbol:
            return "MT5 symbol is empty."
        case .emptyBrokerSourceId:
            return "Broker source id is empty."
        case .emptyBrokerCompany:
            return "Broker company is empty."
        case .emptyBrokerServer:
            return "Broker server is empty."
        case .invalidBrokerAccountLogin(let value):
            return "Broker account login \(value) is invalid."
        case .invalidDigits(let value):
            return "Digits value \(value) is outside the supported range 0...10."
        case .invalidPrice(let value):
            return "Price '\(value)' is not a valid positive decimal price."
        case .priceScaleOverflow(let value):
            return "Price '\(value)' cannot be represented as a scaled Int64."
        case .invalidMinuteAlignment(let value):
            return "Timestamp \(value) is not aligned to a whole M1 bar."
        case .invalidTimeRange:
            return "Time range is invalid."
        }
    }
}
