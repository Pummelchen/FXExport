import Domain
import Foundation

public struct ColumnarOhlcSeries: BarSeries, Sendable {
    public let metadata: BarSeriesMetadata
    /// Canonical UTC epoch seconds. MT5 server timestamps must never be loaded into this column.
    public let utcTimestamps: [Int64]
    public let open: [Int64]
    public let high: [Int64]
    public let low: [Int64]
    public let close: [Int64]

    public var count: Int { utcTimestamps.count }

    public init(metadata: BarSeriesMetadata, utcTimestamps: [Int64], open: [Int64], high: [Int64], low: [Int64], close: [Int64]) throws {
        let count = utcTimestamps.count
        guard open.count == count, high.count == count, low.count == count, close.count == count else {
            throw BacktestError.invalidSeries("Column arrays must have equal length")
        }
        self.metadata = metadata
        self.utcTimestamps = utcTimestamps
        self.open = open
        self.high = high
        self.low = low
        self.close = close
    }
}
