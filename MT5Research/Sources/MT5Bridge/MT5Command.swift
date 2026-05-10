import Foundation

public enum MT5Command: String, Codable, CaseIterable, Sendable {
    case hello = "HELLO"
    case ping = "PING"
    case getTerminalInfo = "GET_TERMINAL_INFO"
    case getSymbolInfo = "GET_SYMBOL_INFO"
    case prepareSymbol = "PREPARE_SYMBOL"
    case getHistoryStatus = "GET_HISTORY_STATUS"
    case getOldestM1BarTime = "GET_OLDEST_M1_BAR_TIME"
    case getLatestClosedM1Bar = "GET_LATEST_CLOSED_M1_BAR"
    case getRatesRange = "GET_RATES_RANGE"
    case getRatesFromPosition = "GET_RATES_FROM_POSITION"
    case getServerTimeSnapshot = "GET_SERVER_TIME_SNAPSHOT"
}

public struct EmptyPayload: Codable, Sendable {
    public init() {}
}

public struct SymbolPayload: Codable, Sendable {
    public let mt5Symbol: String

    enum CodingKeys: String, CodingKey {
        case mt5Symbol = "mt5_symbol"
    }

    public init(mt5Symbol: String) {
        self.mt5Symbol = mt5Symbol
    }
}

public struct RatesRangePayload: Codable, Sendable {
    public let mt5Symbol: String
    public let fromMT5ServerTs: Int64
    public let toMT5ServerTsExclusive: Int64
    public let maxBars: Int

    enum CodingKeys: String, CodingKey {
        case mt5Symbol = "mt5_symbol"
        case fromMT5ServerTs = "from_mt5_server_ts"
        case toMT5ServerTsExclusive = "to_mt5_server_ts_exclusive"
        case maxBars = "max_bars"
    }

    public init(mt5Symbol: String, fromMT5ServerTs: Int64, toMT5ServerTsExclusive: Int64, maxBars: Int) {
        self.mt5Symbol = mt5Symbol
        self.fromMT5ServerTs = fromMT5ServerTs
        self.toMT5ServerTsExclusive = toMT5ServerTsExclusive
        self.maxBars = maxBars
    }
}
