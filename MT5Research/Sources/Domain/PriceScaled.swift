import Foundation

public struct Digits: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let rawValue: Int

    public init(_ rawValue: Int) throws {
        guard (0...10).contains(rawValue) else { throw DomainError.invalidDigits(rawValue) }
        self.rawValue = rawValue
    }

    public init(rawValue: Int) {
        self.rawValue = rawValue
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        try self.init(try container.decode(Int.self))
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    public var description: String { String(rawValue) }

    public static func < (lhs: Digits, rhs: Digits) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

public struct PriceScaled: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let rawValue: Int64
    public let digits: Digits

    public init(rawValue: Int64, digits: Digits) {
        self.rawValue = rawValue
        self.digits = digits
    }

    public init(rawValue: Int64) {
        self.rawValue = rawValue
        self.digits = Digits(rawValue: 0)
    }

    public static func fromDecimalString(_ input: String, digits: Digits) throws -> PriceScaled {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, !trimmed.hasPrefix("-") else { throw DomainError.invalidPrice(input) }

        let pieces = trimmed.split(separator: ".", omittingEmptySubsequences: false)
        guard pieces.count == 1 || pieces.count == 2 else { throw DomainError.invalidPrice(input) }
        let whole = String(pieces[0])
        let fraction = pieces.count == 2 ? String(pieces[1]) : ""
        guard !whole.isEmpty, whole.allSatisfy(\.isNumber), fraction.allSatisfy(\.isNumber) else {
            throw DomainError.invalidPrice(input)
        }

        let scale = pow10(digits.rawValue)
        guard let wholeValue = Int64(whole), wholeValue <= Int64.max / scale else {
            throw DomainError.priceScaleOverflow(input)
        }

        let paddedFraction: String
        if fraction.count >= digits.rawValue {
            let endIndex = fraction.index(fraction.startIndex, offsetBy: digits.rawValue)
            paddedFraction = String(fraction[..<endIndex])
        } else {
            paddedFraction = fraction + String(repeating: "0", count: digits.rawValue - fraction.count)
        }

        guard let fractionValue = Int64(paddedFraction.isEmpty ? "0" : paddedFraction) else {
            throw DomainError.invalidPrice(input)
        }
        let scaled = wholeValue * scale + fractionValue
        guard scaled > 0 else { throw DomainError.invalidPrice(input) }
        return PriceScaled(rawValue: scaled, digits: digits)
    }

    public var description: String {
        guard digits.rawValue > 0 else { return String(rawValue) }
        let scale = Self.pow10(digits.rawValue)
        let whole = rawValue / scale
        let fraction = rawValue % scale
        let fractionText = String(fraction).leftPadded(to: digits.rawValue, with: "0")
        return "\(whole).\(fractionText)"
    }

    public static func < (lhs: PriceScaled, rhs: PriceScaled) -> Bool {
        if lhs.digits == rhs.digits {
            return lhs.rawValue < rhs.rawValue
        }
        return (lhs.digits.rawValue, lhs.rawValue) < (rhs.digits.rawValue, rhs.rawValue)
    }

    private static func pow10(_ exponent: Int) -> Int64 {
        var value: Int64 = 1
        for _ in 0..<exponent {
            value *= 10
        }
        return value
    }
}

private extension String {
    func leftPadded(to length: Int, with character: Character) -> String {
        if count >= length { return self }
        return String(repeating: String(character), count: length - count) + self
    }
}
