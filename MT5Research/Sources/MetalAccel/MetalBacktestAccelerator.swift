import BacktestCore
import Foundation

#if canImport(Metal)
import Metal
#endif

public struct MetalBacktestAccelerator: Sendable {
    public let availability: MetalAvailability

    public init(availability: MetalAvailability = MetalAvailability()) {
        self.availability = availability
    }

    public func runParameterSweepOrFallback<S: BacktestStrategy>(
        series: ColumnarOhlcSeries,
        strategy: S.Type,
        parameterSets: [S.Parameters],
        execution: ExecutionModel
    ) throws -> [BacktestMetrics] {
        guard availability.isAvailable else {
            return try Optimizer().runCPUReference(series: series, strategy: strategy, parameterSets: parameterSets, execution: execution)
        }

        // TODO: compile Metal/parameter_sweep.metal into a compute pipeline for embarrassingly parallel parameter sweeps.
        // The CPU reference remains authoritative; every GPU result must be checked against selected CPU runs.
        return try Optimizer().runCPUReference(series: series, strategy: strategy, parameterSets: parameterSets, execution: execution)
    }
}
