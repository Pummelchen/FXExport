import Domain
import Foundation
import TimeMapping

public enum ValidationError: Error, Equatable, CustomStringConvertible, Sendable {
    case wrongTimeframe(Timeframe)
    case unexpectedLogicalSymbol(LogicalSymbol)
    case unexpectedMT5Symbol(MT5Symbol)
    case inconsistentDigits(expected: Digits, actual: Digits)
    case timestampNotMinuteAligned(MT5ServerSecond)
    case openBarOrFutureBar(MT5ServerSecond, latestClosed: MT5ServerSecond)
    case nonPositivePrice(MT5ServerSecond)
    case invalidOhlcInvariant(MT5ServerSecond)
    case unsortedBatch(previous: MT5ServerSecond, current: MT5ServerSecond)
    case duplicateTimestamp(MT5ServerSecond)
    case timeMapping(String)

    public var description: String {
        switch self {
        case .wrongTimeframe(let timeframe):
            return "Expected M1 timeframe, got \(timeframe.rawValue)."
        case .unexpectedLogicalSymbol(let symbol):
            return "Unexpected logical symbol \(symbol.rawValue)."
        case .unexpectedMT5Symbol(let symbol):
            return "Unexpected MT5 symbol \(symbol.rawValue)."
        case .inconsistentDigits(let expected, let actual):
            return "Expected \(expected.rawValue) digits, got \(actual.rawValue)."
        case .timestampNotMinuteAligned(let time):
            return "MT5 server timestamp \(time.rawValue) is not minute-aligned."
        case .openBarOrFutureBar(let time, let latestClosed):
            return "Bar \(time.rawValue) is newer than latest closed M1 bar \(latestClosed.rawValue)."
        case .nonPositivePrice(let time):
            return "Bar \(time.rawValue) contains a non-positive price."
        case .invalidOhlcInvariant(let time):
            return "Bar \(time.rawValue) violates OHLC high/low invariants."
        case .unsortedBatch(let previous, let current):
            return "Batch is not strictly sorted: \(previous.rawValue) then \(current.rawValue)."
        case .duplicateTimestamp(let time):
            return "Batch contains duplicate MT5 server timestamp \(time.rawValue)."
        case .timeMapping(let reason):
            return "Time mapping failed: \(reason)"
        }
    }
}

public struct OhlcValidationContext: Sendable {
    public let brokerSourceId: BrokerSourceId
    public let expectedLogicalSymbol: LogicalSymbol
    public let expectedMT5Symbol: MT5Symbol
    public let expectedDigits: Digits
    public let latestClosedMT5ServerTime: MT5ServerSecond
    public let batchId: BatchId
    public let ingestedAtUtc: UtcSecond

    public init(
        brokerSourceId: BrokerSourceId,
        expectedLogicalSymbol: LogicalSymbol,
        expectedMT5Symbol: MT5Symbol,
        expectedDigits: Digits,
        latestClosedMT5ServerTime: MT5ServerSecond,
        batchId: BatchId,
        ingestedAtUtc: UtcSecond
    ) {
        self.brokerSourceId = brokerSourceId
        self.expectedLogicalSymbol = expectedLogicalSymbol
        self.expectedMT5Symbol = expectedMT5Symbol
        self.expectedDigits = expectedDigits
        self.latestClosedMT5ServerTime = latestClosedMT5ServerTime
        self.batchId = batchId
        self.ingestedAtUtc = ingestedAtUtc
    }
}

public struct OhlcValidator: Sendable {
    private let timeConverter: TimeConverter

    public init(timeConverter: TimeConverter) {
        self.timeConverter = timeConverter
    }

    public func validateBatch(_ bars: [ClosedM1Bar], context: OhlcValidationContext) throws -> [ValidatedBar] {
        var seen = Set<MT5ServerSecond>()
        var previous: MT5ServerSecond?
        var validated: [ValidatedBar] = []
        validated.reserveCapacity(bars.count)

        for bar in bars {
            if let previous, bar.mt5ServerTime.rawValue <= previous.rawValue {
                throw ValidationError.unsortedBatch(previous: previous, current: bar.mt5ServerTime)
            }
            previous = bar.mt5ServerTime

            guard seen.insert(bar.mt5ServerTime).inserted else {
                throw ValidationError.duplicateTimestamp(bar.mt5ServerTime)
            }

            validated.append(try validate(bar, context: context))
        }

        return validated
    }

    public func validate(_ bar: ClosedM1Bar, context: OhlcValidationContext) throws -> ValidatedBar {
        guard bar.timeframe == .m1 else { throw ValidationError.wrongTimeframe(bar.timeframe) }
        guard bar.logicalSymbol == context.expectedLogicalSymbol else {
            throw ValidationError.unexpectedLogicalSymbol(bar.logicalSymbol)
        }
        guard bar.mt5Symbol == context.expectedMT5Symbol else {
            throw ValidationError.unexpectedMT5Symbol(bar.mt5Symbol)
        }
        guard bar.digits == context.expectedDigits else {
            throw ValidationError.inconsistentDigits(expected: context.expectedDigits, actual: bar.digits)
        }
        guard bar.mt5ServerTime.isMinuteAligned else {
            throw ValidationError.timestampNotMinuteAligned(bar.mt5ServerTime)
        }
        guard bar.mt5ServerTime.rawValue <= context.latestClosedMT5ServerTime.rawValue else {
            throw ValidationError.openBarOrFutureBar(bar.mt5ServerTime, latestClosed: context.latestClosedMT5ServerTime)
        }
        guard [bar.open, bar.high, bar.low, bar.close].allSatisfy({ $0.rawValue > 0 }) else {
            throw ValidationError.nonPositivePrice(bar.mt5ServerTime)
        }
        guard bar.high.rawValue >= bar.open.rawValue,
              bar.high.rawValue >= bar.close.rawValue,
              bar.high.rawValue >= bar.low.rawValue,
              bar.low.rawValue <= bar.open.rawValue,
              bar.low.rawValue <= bar.close.rawValue else {
            throw ValidationError.invalidOhlcInvariant(bar.mt5ServerTime)
        }

        let converted: TimeConversionResult
        do {
            converted = try timeConverter.convert(mt5ServerTime: bar.mt5ServerTime)
        } catch {
            throw ValidationError.timeMapping(String(describing: error))
        }

        return ValidatedBar(
            brokerSourceId: context.brokerSourceId,
            logicalSymbol: bar.logicalSymbol,
            mt5Symbol: bar.mt5Symbol,
            timeframe: bar.timeframe,
            mt5ServerTime: bar.mt5ServerTime,
            utcTime: converted.utcTime,
            serverUtcOffset: converted.offset,
            offsetSource: converted.source,
            offsetConfidence: converted.confidence,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            digits: bar.digits,
            batchId: context.batchId,
            sourceStatus: .mt5ClosedBar,
            ingestedAtUtc: context.ingestedAtUtc
        )
    }
}
