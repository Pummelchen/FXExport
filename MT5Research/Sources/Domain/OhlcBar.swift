import Foundation

public struct BatchId: RawRepresentable, Codable, Hashable, Sendable, CustomStringConvertible {
    public let rawValue: String
    public init(rawValue: String) { self.rawValue = rawValue }
    public var description: String { rawValue }

    public static func deterministic(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        start: MT5ServerSecond,
        end: MT5ServerSecond
    ) -> BatchId {
        BatchId(rawValue: "\(brokerSourceId.rawValue):\(logicalSymbol.rawValue):\(start.rawValue):\(end.rawValue)")
    }
}

public struct BarIndex: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let rawValue: Int64
    public init(rawValue: Int64) { self.rawValue = rawValue }
    public var description: String { String(rawValue) }
    public static func < (lhs: BarIndex, rhs: BarIndex) -> Bool { lhs.rawValue < rhs.rawValue }
}

public enum SourceStatus: String, Codable, Hashable, Sendable {
    case mt5ClosedBar
    case unresolvedUtcOffset
    case invalid
}

public struct ClosedM1Bar: Codable, Hashable, Sendable {
    public let logicalSymbol: LogicalSymbol
    public let mt5Symbol: MT5Symbol
    public let timeframe: Timeframe
    public let mt5ServerTime: MT5ServerSecond
    public let open: PriceScaled
    public let high: PriceScaled
    public let low: PriceScaled
    public let close: PriceScaled
    public let digits: Digits

    public init(
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        timeframe: Timeframe,
        mt5ServerTime: MT5ServerSecond,
        open: PriceScaled,
        high: PriceScaled,
        low: PriceScaled,
        close: PriceScaled,
        digits: Digits
    ) {
        self.logicalSymbol = logicalSymbol
        self.mt5Symbol = mt5Symbol
        self.timeframe = timeframe
        self.mt5ServerTime = mt5ServerTime
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.digits = digits
    }
}

public struct ValidatedBar: Codable, Hashable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let mt5Symbol: MT5Symbol
    public let timeframe: Timeframe
    public let mt5ServerTime: MT5ServerSecond
    public let utcTime: UtcSecond
    public let serverUtcOffset: OffsetSeconds
    public let offsetSource: OffsetSource
    public let offsetConfidence: OffsetConfidence
    public let open: PriceScaled
    public let high: PriceScaled
    public let low: PriceScaled
    public let close: PriceScaled
    public let digits: Digits
    public let batchId: BatchId
    public let barHash: BarHash
    public let sourceStatus: SourceStatus
    public let ingestedAtUtc: UtcSecond

    public init(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        timeframe: Timeframe,
        mt5ServerTime: MT5ServerSecond,
        utcTime: UtcSecond,
        serverUtcOffset: OffsetSeconds,
        offsetSource: OffsetSource,
        offsetConfidence: OffsetConfidence,
        open: PriceScaled,
        high: PriceScaled,
        low: PriceScaled,
        close: PriceScaled,
        digits: Digits,
        batchId: BatchId,
        sourceStatus: SourceStatus,
        ingestedAtUtc: UtcSecond
    ) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.mt5Symbol = mt5Symbol
        self.timeframe = timeframe
        self.mt5ServerTime = mt5ServerTime
        self.utcTime = utcTime
        self.serverUtcOffset = serverUtcOffset
        self.offsetSource = offsetSource
        self.offsetConfidence = offsetConfidence
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.digits = digits
        self.batchId = batchId
        self.sourceStatus = sourceStatus
        self.ingestedAtUtc = ingestedAtUtc
        self.barHash = BarHash.compute(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            utcTime: utcTime,
            mt5ServerTime: mt5ServerTime,
            open: open,
            high: high,
            low: low,
            close: close
        )
    }
}

public struct CanonicalBar: Codable, Hashable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let utcTime: UtcSecond
    public let open: PriceScaled
    public let high: PriceScaled
    public let low: PriceScaled
    public let close: PriceScaled
    public let barHash: BarHash

    public init(from validatedBar: ValidatedBar) {
        self.brokerSourceId = validatedBar.brokerSourceId
        self.logicalSymbol = validatedBar.logicalSymbol
        self.utcTime = validatedBar.utcTime
        self.open = validatedBar.open
        self.high = validatedBar.high
        self.low = validatedBar.low
        self.close = validatedBar.close
        self.barHash = validatedBar.barHash
    }
}
