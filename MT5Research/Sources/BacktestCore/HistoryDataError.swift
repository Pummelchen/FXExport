import Domain
import Foundation

public enum HistoryDataError: Error, Equatable, CustomStringConvertible, Sendable {
    case invalidRequest(String)
    case invalidSeries(String)
    case invalidCanonicalRow(String)
    case emptyResult(LogicalSymbol, UtcSecond, UtcSecond)
    case missingVerifiedCoverage(LogicalSymbol, UtcSecond, UtcSecond)
    case rowLimitExceeded(limit: Int)
    case unsupportedInternalCompute(String)

    public var description: String {
        switch self {
        case .invalidRequest(let reason):
            return "Invalid history data request: \(reason)"
        case .invalidSeries(let reason):
            return "Invalid OHLC series: \(reason)"
        case .invalidCanonicalRow(let reason):
            return "Invalid canonical OHLC row: \(reason)"
        case .emptyResult(let symbol, let from, let to):
            return "No canonical M1 bars were found for \(symbol.rawValue) in UTC range \(from.rawValue)..<\(to.rawValue)."
        case .missingVerifiedCoverage(let symbol, let from, let to):
            return "\(symbol.rawValue) UTC range \(from.rawValue)..<\(to.rawValue) is not fully covered by verified MT5 source coverage. Resume verification/import before using this range."
        case .rowLimitExceeded(let limit):
            return "History data request returned more than \(limit) rows. Split the UTC range into smaller chunks."
        case .unsupportedInternalCompute(let command):
            return "\(command) is not implemented in FXExport. FXExport only provides verified historical data to external Swift CPU/Metal backtest applications."
        }
    }
}
