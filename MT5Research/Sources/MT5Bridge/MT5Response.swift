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

    enum CodingKeys: String, CodingKey {
        case terminalName = "terminal_name"
        case company
        case server
        case accountLogin = "account_login"
    }

    public func brokerServerIdentity() throws -> BrokerServerIdentity {
        try BrokerServerIdentity(company: company, server: server, accountLogin: accountLogin)
    }
}

public struct SymbolInfoDTO: Codable, Sendable {
    public let mt5Symbol: String
    public let selected: Bool
    public let digits: Int

    enum CodingKeys: String, CodingKey {
        case mt5Symbol = "mt5_symbol"
        case selected
        case digits
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
    public let rates: [MT5RateDTO]

    enum CodingKeys: String, CodingKey {
        case mt5Symbol = "mt5_symbol"
        case timeframe
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
