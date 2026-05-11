import Domain
import Foundation

public struct VerificationBar: Equatable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let mt5Symbol: MT5Symbol
    public let mt5ServerTime: MT5ServerSecond
    public let utcTime: UtcSecond
    public let open: PriceScaled
    public let high: PriceScaled
    public let low: PriceScaled
    public let close: PriceScaled
    public let digits: Digits
    public let offsetConfidence: OffsetConfidence
    public let barHash: BarHash

    public init(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        mt5ServerTime: MT5ServerSecond,
        utcTime: UtcSecond,
        open: PriceScaled,
        high: PriceScaled,
        low: PriceScaled,
        close: PriceScaled,
        digits: Digits,
        offsetConfidence: OffsetConfidence = .verified,
        barHash: BarHash
    ) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.mt5Symbol = mt5Symbol
        self.mt5ServerTime = mt5ServerTime
        self.utcTime = utcTime
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.digits = digits
        self.offsetConfidence = offsetConfidence
        self.barHash = barHash
    }

    public init(validatedBar: ValidatedBar) {
        self.init(
            brokerSourceId: validatedBar.brokerSourceId,
            logicalSymbol: validatedBar.logicalSymbol,
            mt5Symbol: validatedBar.mt5Symbol,
            mt5ServerTime: validatedBar.mt5ServerTime,
            utcTime: validatedBar.utcTime,
            open: validatedBar.open,
            high: validatedBar.high,
            low: validatedBar.low,
            close: validatedBar.close,
            digits: validatedBar.digits,
            offsetConfidence: validatedBar.offsetConfidence,
            barHash: validatedBar.barHash
        )
    }
}
