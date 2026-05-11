import Domain
import Foundation

public struct OhlcSeriesValidator: Sendable {
    public init() {}

    public func validate(_ series: ColumnarOhlcSeries) throws {
        guard series.metadata.timeframe == .m1 else {
            throw HistoryDataError.invalidSeries("Only M1 OHLC series are supported.")
        }
        guard series.count == series.open.count,
              series.count == series.high.count,
              series.count == series.low.count,
              series.count == series.close.count else {
            throw HistoryDataError.invalidSeries("Column counts changed after construction.")
        }
        if let firstUtc = series.metadata.firstUtc,
           let actualFirst = series.utcTimestamps.first,
           firstUtc.rawValue != actualFirst {
            throw HistoryDataError.invalidSeries("Metadata first UTC does not match the first timestamp column.")
        }
        if let lastUtc = series.metadata.lastUtc,
           let actualLast = series.utcTimestamps.last,
           lastUtc.rawValue != actualLast {
            throw HistoryDataError.invalidSeries("Metadata last UTC does not match the last timestamp column.")
        }
        for timestamp in series.utcTimestamps {
            guard timestamp % Timeframe.m1.seconds == 0 else {
                throw HistoryDataError.invalidSeries("UTC timestamps must be minute-aligned.")
            }
        }
        if series.utcTimestamps.count > 1 {
            for index in 1..<series.utcTimestamps.count {
                guard series.utcTimestamps[index] > series.utcTimestamps[index - 1] else {
                    throw HistoryDataError.invalidSeries("UTC timestamps must be strictly increasing.")
                }
            }
        }
        for index in 0..<series.count {
            let open = series.open[index]
            let high = series.high[index]
            let low = series.low[index]
            let close = series.close[index]
            guard open > 0, high > 0, low > 0, close > 0 else {
                throw HistoryDataError.invalidSeries("OHLC prices must be positive.")
            }
            guard high >= open, high >= close, high >= low, low <= open, low <= close else {
                throw HistoryDataError.invalidSeries("OHLC high/low invariant failed at index \(index).")
            }
        }
    }
}
