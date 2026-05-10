import Domain
import Foundation

public struct VerificationRange: Hashable, Sendable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let mt5Start: MT5ServerSecond
    public let mt5EndExclusive: MT5ServerSecond

    public init(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Start: MT5ServerSecond,
        mt5EndExclusive: MT5ServerSecond
    ) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.mt5Start = mt5Start
        self.mt5EndExclusive = mt5EndExclusive
    }
}

public struct RandomRangeSelector: Sendable {
    public init() {}

    public func selectMonth(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        oldest: MT5ServerSecond,
        latestClosed: MT5ServerSecond,
        random: inout any RandomNumberGenerator
    ) throws -> VerificationRange {
        let monthSeconds: Int64 = 31 * 24 * 60 * 60
        guard latestClosed.rawValue - oldest.rawValue > monthSeconds else {
            return VerificationRange(
                brokerSourceId: brokerSourceId,
                logicalSymbol: logicalSymbol,
                mt5Start: oldest,
                mt5EndExclusive: MT5ServerSecond(rawValue: latestClosed.rawValue + Timeframe.m1.seconds)
            )
        }
        let maxStart = latestClosed.rawValue - monthSeconds
        let start = Int64.random(in: oldest.rawValue...maxStart, using: &random)
        let alignedStart = start - (start % Timeframe.m1.seconds)
        return VerificationRange(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            mt5Start: MT5ServerSecond(rawValue: alignedStart),
            mt5EndExclusive: MT5ServerSecond(rawValue: alignedStart + monthSeconds)
        )
    }
}
