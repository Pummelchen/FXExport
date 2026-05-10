import Foundation

public struct BacktestMetrics: Equatable, Sendable {
    public var totalTrades: Int
    public var netPnL: Int64
    public var maxDrawdown: Int64
    public var winRate: Double
    public var profitFactor: Double
    public var averageTrade: Double
    public var exposureTimeSeconds: Int64

    public init(
        totalTrades: Int = 0,
        netPnL: Int64 = 0,
        maxDrawdown: Int64 = 0,
        winRate: Double = 0,
        profitFactor: Double = 0,
        averageTrade: Double = 0,
        exposureTimeSeconds: Int64 = 0
    ) {
        self.totalTrades = totalTrades
        self.netPnL = netPnL
        self.maxDrawdown = maxDrawdown
        self.winRate = winRate
        self.profitFactor = profitFactor
        self.averageTrade = averageTrade
        self.exposureTimeSeconds = exposureTimeSeconds
    }
}
