import Domain
import Foundation

public struct HelloResponseDTO: Codable, Sendable {
    public let bridgeName: String
    public let bridgeVersion: String
    public let schemaVersion: Int

    enum CodingKeys: String, CodingKey {
        case bridgeName = "bridge_name"
        case bridgeVersion = "bridge_version"
        case schemaVersion = "schema_version"
    }
}

public struct TerminalInfoDTO: Codable, Sendable {
    public let terminalName: String
    public let company: String
    public let server: String
    public let accountLogin: Int64
    public let accountCurrency: String?
    public let accountLeverage: Int?
    public let accountMarginMode: Int?

    enum CodingKeys: String, CodingKey {
        case terminalName = "terminal_name"
        case company
        case server
        case accountLogin = "account_login"
        case accountCurrency = "account_currency"
        case accountLeverage = "account_leverage"
        case accountMarginMode = "account_margin_mode"
    }

    public func brokerServerIdentity() throws -> BrokerServerIdentity {
        try BrokerServerIdentity(company: company, server: server, accountLogin: accountLogin)
    }
}

public struct SymbolInfoDTO: Codable, Sendable {
    public let mt5Symbol: String
    public let selected: Bool
    public let digits: Int
    public let bid: Double?
    public let ask: Double?
    public let point: Double?
    public let spread: Int?
    public let spreadFloat: Bool?
    public let contractSize: Double?
    public let volumeMin: Double?
    public let volumeStep: Double?
    public let volumeMax: Double?
    public let swapLong: Double?
    public let swapShort: Double?
    public let swapMode: Int?
    public let marginInitial: Double?
    public let marginMaintenance: Double?
    public let marginBuy: Double?
    public let marginSell: Double?
    public let marginCalcLots: Double?
    public let tradeCalcMode: Int?
    public let tradeMode: Int?
    public let tickSize: Double?
    public let tickValue: Double?
    public let tickValueProfit: Double?
    public let tickValueLoss: Double?

    enum CodingKeys: String, CodingKey {
        case mt5Symbol = "mt5_symbol"
        case selected
        case digits
        case bid
        case ask
        case point
        case spread
        case spreadFloat = "spread_float"
        case contractSize = "contract_size"
        case volumeMin = "volume_min"
        case volumeStep = "volume_step"
        case volumeMax = "volume_max"
        case swapLong = "swap_long"
        case swapShort = "swap_short"
        case swapMode = "swap_mode"
        case marginInitial = "margin_initial"
        case marginMaintenance = "margin_maintenance"
        case marginBuy = "margin_buy"
        case marginSell = "margin_sell"
        case marginCalcLots = "margin_calc_lots"
        case tradeCalcMode = "trade_calc_mode"
        case tradeMode = "trade_mode"
        case tickSize = "tick_size"
        case tickValue = "tick_value"
        case tickValueProfit = "tick_value_profit"
        case tickValueLoss = "tick_value_loss"
    }
}

public struct HistoryStatusDTO: Codable, Sendable {
    public let mt5Symbol: String
    public let synchronized: Bool
    public let bars: Int

    enum CodingKeys: String, CodingKey {
        case mt5Symbol = "mt5_symbol"
        case synchronized
        case bars
    }
}

public struct ServerTimeSnapshotDTO: Codable, Sendable {
    public let timeTradeServer: Int64
    public let timeGMT: Int64
    public let timeLocal: Int64

    enum CodingKeys: String, CodingKey {
        case timeTradeServer = "time_trade_server"
        case timeGMT = "time_gmt"
        case timeLocal = "time_local"
    }

    public init(timeTradeServer: Int64, timeGMT: Int64, timeLocal: Int64) {
        self.timeTradeServer = timeTradeServer
        self.timeGMT = timeGMT
        self.timeLocal = timeLocal
    }
}

public struct MT5RateDTO: Codable, Sendable {
    public let mt5ServerTime: Int64
    public let open: String
    public let high: String
    public let low: String
    public let close: String

    enum CodingKeys: String, CodingKey {
        case mt5ServerTime = "mt5_server_time"
        case open
        case high
        case low
        case close
    }

    public func toClosedM1Bar(logicalSymbol: LogicalSymbol, mt5Symbol: MT5Symbol, digits: Digits) throws -> ClosedM1Bar {
        ClosedM1Bar(
            logicalSymbol: logicalSymbol,
            mt5Symbol: mt5Symbol,
            timeframe: .m1,
            mt5ServerTime: MT5ServerSecond(rawValue: mt5ServerTime),
            open: try PriceScaled.fromDecimalString(open, digits: digits),
            high: try PriceScaled.fromDecimalString(high, digits: digits),
            low: try PriceScaled.fromDecimalString(low, digits: digits),
            close: try PriceScaled.fromDecimalString(close, digits: digits),
            digits: digits
        )
    }
}

public struct RatesResponseDTO: Codable, Sendable {
    public let mt5Symbol: String
    public let timeframe: String
    public let requestedFromMT5ServerTs: Int64?
    public let requestedToMT5ServerTsExclusive: Int64?
    public let effectiveToMT5ServerTsExclusive: Int64?
    public let latestClosedMT5ServerTs: Int64?
    public let seriesSynchronized: Bool?
    public let copiedCount: Int?
    public let emittedCount: Int?
    public let firstMT5ServerTs: Int64?
    public let lastMT5ServerTs: Int64?
    public let rates: [MT5RateDTO]

    enum CodingKeys: String, CodingKey {
        case mt5Symbol = "mt5_symbol"
        case timeframe
        case requestedFromMT5ServerTs = "requested_from_mt5_server_ts"
        case requestedToMT5ServerTsExclusive = "requested_to_mt5_server_ts_exclusive"
        case effectiveToMT5ServerTsExclusive = "effective_to_mt5_server_ts_exclusive"
        case latestClosedMT5ServerTs = "latest_closed_mt5_server_ts"
        case seriesSynchronized = "series_synchronized"
        case copiedCount = "copied_count"
        case emittedCount = "emitted_count"
        case firstMT5ServerTs = "first_mt5_server_ts"
        case lastMT5ServerTs = "last_mt5_server_ts"
        case rates
    }
}

public struct SingleTimeResponseDTO: Codable, Sendable {
    public let mt5Symbol: String
    public let mt5ServerTime: Int64

    enum CodingKeys: String, CodingKey {
        case mt5Symbol = "mt5_symbol"
        case mt5ServerTime = "mt5_server_time"
    }
}
