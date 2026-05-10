import Foundation

public struct Optimizer: Sendable {
    public init() {}

    public func runCPUReference<S: BacktestStrategy>(
        series: ColumnarOhlcSeries,
        strategy: S.Type,
        parameterSets: [S.Parameters],
        execution: ExecutionModel
    ) throws -> [BacktestMetrics] {
        let engine = BacktestEngine()
        var results: [BacktestMetrics] = []
        results.reserveCapacity(parameterSets.count)
        for parameters in parameterSets {
            results.append(try engine.run(series: series, strategy: strategy, parameters: parameters, execution: execution))
        }
        return results
    }
}
