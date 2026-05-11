import Foundation

public enum BacktestError: Error, Equatable, CustomStringConvertible, Sendable {
    case invalidSeries(String)
    case strategyNotImplemented

    public var description: String {
        switch self {
        case .invalidSeries(let reason):
            return "Invalid backtest series: \(reason)"
        case .strategyNotImplemented:
            return "No strategy implementation has been supplied."
        }
    }
}

public protocol BacktestStrategy: Sendable {
    associatedtype State: StrategyState
    associatedtype Parameters: StrategyParameters

    static func initialState(parameters: Parameters) -> State
    static func onBar(index: Int, series: ColumnarOhlcSeries, state: inout State, parameters: Parameters, execution: ExecutionModel)
}

public struct BacktestEngine: Sendable {
    public init() {}

    public func run<S: BacktestStrategy>(
        series: ColumnarOhlcSeries,
        strategy: S.Type,
        parameters: S.Parameters,
        execution: ExecutionModel
    ) throws -> BacktestMetrics {
        try validateSeries(series)
        var state = strategy.initialState(parameters: parameters)
        if series.count > 0 {
            for index in 0..<series.count {
                strategy.onBar(index: index, series: series, state: &state, parameters: parameters, execution: execution)
            }
        }
        return BacktestMetrics()
    }

    public func validateSeries(_ series: ColumnarOhlcSeries) throws {
        guard series.count == series.open.count,
              series.count == series.high.count,
              series.count == series.low.count,
              series.count == series.close.count else {
            throw BacktestError.invalidSeries("Column counts changed after construction")
        }
        for timestamp in series.utcTimestamps {
            guard timestamp % 60 == 0 else {
                throw BacktestError.invalidSeries("UTC timestamps must be minute-aligned")
            }
        }
        for index in 1..<series.utcTimestamps.count {
            guard series.utcTimestamps[index] > series.utcTimestamps[index - 1] else {
                throw BacktestError.invalidSeries("UTC timestamps must be strictly increasing")
            }
        }
        for index in 0..<series.count {
            let open = series.open[index]
            let high = series.high[index]
            let low = series.low[index]
            let close = series.close[index]
            guard open > 0, high > 0, low > 0, close > 0 else {
                throw BacktestError.invalidSeries("OHLC prices must be positive")
            }
            guard high >= open, high >= close, high >= low, low <= open, low <= close else {
                throw BacktestError.invalidSeries("OHLC high/low invariant failed at index \(index)")
            }
        }
    }
}
