import BacktestCore
import Domain
import XCTest

final class BacktestTests: XCTestCase {
    func testCPUBacktestDeterminism() throws {
        let metadata = BarSeriesMetadata(
            brokerSourceId: try BrokerSourceId("demo"),
            logicalSymbol: try LogicalSymbol("EURUSD"),
            digits: try Digits(5)
        )
        let series = try ColumnarOhlcSeries(
            metadata: metadata,
            utcTimestamps: [60, 120, 180],
            open: [100, 101, 102],
            high: [101, 102, 103],
            low: [99, 100, 101],
            close: [100, 102, 101]
        )
        let engine = BacktestEngine()
        let first = try engine.run(series: series, strategy: NoopStrategy.self, parameters: EmptyStrategyParameters(), execution: ExecutionModel(spreadScaled: 0, commissionScaled: 0))
        let second = try engine.run(series: series, strategy: NoopStrategy.self, parameters: EmptyStrategyParameters(), execution: ExecutionModel(spreadScaled: 0, commissionScaled: 0))
        XCTAssertEqual(first, second)
    }

    func testCPUBacktestRejectsNonMinuteAlignedUTC() throws {
        let metadata = BarSeriesMetadata(
            brokerSourceId: try BrokerSourceId("demo"),
            logicalSymbol: try LogicalSymbol("EURUSD"),
            digits: try Digits(5)
        )
        let series = try ColumnarOhlcSeries(
            metadata: metadata,
            utcTimestamps: [60, 121],
            open: [100, 101],
            high: [101, 102],
            low: [99, 100],
            close: [100, 102]
        )
        XCTAssertThrowsError(try BacktestEngine().validateSeries(series))
    }

    func testCPUBacktestRejectsInvalidOHLCInvariant() throws {
        let metadata = BarSeriesMetadata(
            brokerSourceId: try BrokerSourceId("demo"),
            logicalSymbol: try LogicalSymbol("EURUSD"),
            digits: try Digits(5)
        )
        let series = try ColumnarOhlcSeries(
            metadata: metadata,
            utcTimestamps: [60],
            open: [100],
            high: [99],
            low: [98],
            close: [100]
        )
        XCTAssertThrowsError(try BacktestEngine().validateSeries(series))
    }
}

private enum NoopStrategy: BacktestStrategy {
    static func initialState(parameters: EmptyStrategyParameters) -> EmptyStrategyState {
        EmptyStrategyState()
    }

    static func onBar(index: Int, series: ColumnarOhlcSeries, state: inout EmptyStrategyState, parameters: EmptyStrategyParameters, execution: ExecutionModel) {}
}
