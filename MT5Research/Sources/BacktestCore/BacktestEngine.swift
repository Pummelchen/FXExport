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
        for index in 1..<series.timestamps.count {
            guard series.timestamps[index] > series.timestamps[index - 1] else {
                throw BacktestError.invalidSeries("Timestamps must be strictly increasing")
            }
        }
    }
}
