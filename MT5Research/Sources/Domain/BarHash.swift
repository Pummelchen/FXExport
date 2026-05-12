import Foundation

public struct BarHash: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let rawValue: UInt64

    public init(rawValue: UInt64) {
        self.rawValue = rawValue
    }

    public var description: String {
        String(format: "%016llx", rawValue)
    }

    public static func < (lhs: BarHash, rhs: BarHash) -> Bool {
        lhs.rawValue < rhs.rawValue
    }

    public static func compute(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        timeframe: Timeframe,
        utcTime: UtcSecond,
        mt5ServerTime: MT5ServerSecond,
        open: PriceScaled,
        high: PriceScaled,
        low: PriceScaled,
        close: PriceScaled,
        digits: Digits
    ) -> BarHash {
        var hasher = FNV1a64()
        hasher.append(brokerSourceId.rawValue)
        hasher.append(logicalSymbol.rawValue)
        hasher.append(mt5Symbol.rawValue)
        hasher.append(timeframe.rawValue)
        hasher.append(utcTime.rawValue)
        hasher.append(mt5ServerTime.rawValue)
        hasher.append(open.rawValue)
        hasher.append(high.rawValue)
        hasher.append(low.rawValue)
        hasher.append(close.rawValue)
        hasher.append(Int64(digits.rawValue))
        return BarHash(rawValue: hasher.value)
    }

}

public struct FNV1a64: Sendable {
    private static let offsetBasis: UInt64 = 0xcbf29ce484222325
    private static let prime: UInt64 = 0x100000001b3
    public private(set) var value: UInt64 = Self.offsetBasis

    public init() {}

    public mutating func append(_ text: String) {
        for byte in text.utf8 {
            append(byte)
        }
        append(UInt8(0xff))
    }

    public mutating func append(_ int: Int64) {
        var bigEndian = int.bigEndian
        withUnsafeBytes(of: &bigEndian) { bytes in
            for byte in bytes {
                append(byte)
            }
        }
    }

    public mutating func append(_ data: Data) {
        for byte in data {
            append(byte)
        }
    }

    public mutating func append(_ byte: UInt8) {
        value ^= UInt64(byte)
        value &*= Self.prime
    }
}
