import Foundation

public enum FXBacktestAPIV1 {
    public static let version = "fxexport.fxbacktest.history.v1"
    public static let statusPath = "/v1/status"
    public static let m1HistoryPath = "/v1/history/m1"
    public static let executionSpecPath = "/v1/execution/spec"
    public static let maximumRowsLimit = 5_000_000
}

public struct FXBacktestAPIStatusResponse: Codable, Equatable, Sendable {
    public let apiVersion: String
    public let service: String
    public let status: String

    enum CodingKeys: String, CodingKey {
        case apiVersion = "api_version"
        case service
        case status
    }

    public init(apiVersion: String = FXBacktestAPIV1.version, service: String = "FXExport", status: String = "ok") {
        self.apiVersion = apiVersion
        self.service = service
        self.status = status
    }
}

public struct FXBacktestM1HistoryRequest: Codable, Equatable, Sendable {
    public let apiVersion: String
    public let brokerSourceId: String
    public let logicalSymbol: String
    public let utcStartInclusive: Int64
    public let utcEndExclusive: Int64
    public let expectedMT5Symbol: String?
    public let expectedDigits: Int?
    public let maximumRows: Int?

    enum CodingKeys: String, CodingKey {
        case apiVersion = "api_version"
        case brokerSourceId = "broker_source_id"
        case logicalSymbol = "logical_symbol"
        case utcStartInclusive = "utc_start_inclusive"
        case utcEndExclusive = "utc_end_exclusive"
        case expectedMT5Symbol = "expected_mt5_symbol"
        case expectedDigits = "expected_digits"
        case maximumRows = "maximum_rows"
    }

    public init(
        apiVersion: String = FXBacktestAPIV1.version,
        brokerSourceId: String,
        logicalSymbol: String,
        utcStartInclusive: Int64,
        utcEndExclusive: Int64,
        expectedMT5Symbol: String? = nil,
        expectedDigits: Int? = nil,
        maximumRows: Int? = nil
    ) {
        self.apiVersion = apiVersion
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.utcStartInclusive = utcStartInclusive
        self.utcEndExclusive = utcEndExclusive
        self.expectedMT5Symbol = expectedMT5Symbol
        self.expectedDigits = expectedDigits
        self.maximumRows = maximumRows
    }

    public func validate() throws {
        guard apiVersion == FXBacktestAPIV1.version else {
            throw FXBacktestAPIValidationError.unsupportedVersion(apiVersion)
        }
        guard !brokerSourceId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("broker_source_id must not be empty")
        }
        guard !logicalSymbol.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("logical_symbol must not be empty")
        }
        guard utcStartInclusive < utcEndExclusive else {
            throw FXBacktestAPIValidationError.invalidField("utc_start_inclusive must be before utc_end_exclusive")
        }
        guard utcStartInclusive % 60 == 0, utcEndExclusive % 60 == 0 else {
            throw FXBacktestAPIValidationError.invalidField("UTC range boundaries must be minute-aligned")
        }
        if let expectedMT5Symbol {
            guard !expectedMT5Symbol.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                throw FXBacktestAPIValidationError.invalidField("expected_mt5_symbol must not be empty when supplied")
            }
        }
        if let expectedDigits {
            guard (0...10).contains(expectedDigits) else {
                throw FXBacktestAPIValidationError.invalidField("expected_digits must be between 0 and 10")
            }
        }
        if let maximumRows {
            guard maximumRows > 0 else {
                throw FXBacktestAPIValidationError.invalidField("maximum_rows must be positive when supplied")
            }
            guard maximumRows <= FXBacktestAPIV1.maximumRowsLimit else {
                throw FXBacktestAPIValidationError.invalidField("maximum_rows must not exceed \(FXBacktestAPIV1.maximumRowsLimit)")
            }
        }
    }
}

public struct FXBacktestM1HistoryMetadata: Codable, Equatable, Sendable {
    public let brokerSourceId: String
    public let logicalSymbol: String
    public let mt5Symbol: String
    public let timeframe: String
    public let digits: Int
    public let requestedUtcStart: Int64
    public let requestedUtcEndExclusive: Int64
    public let firstUtc: Int64?
    public let lastUtc: Int64?
    public let rowCount: Int

    enum CodingKeys: String, CodingKey {
        case brokerSourceId = "broker_source_id"
        case logicalSymbol = "logical_symbol"
        case mt5Symbol = "mt5_symbol"
        case timeframe
        case digits
        case requestedUtcStart = "requested_utc_start"
        case requestedUtcEndExclusive = "requested_utc_end_exclusive"
        case firstUtc = "first_utc"
        case lastUtc = "last_utc"
        case rowCount = "row_count"
    }

    public init(
        brokerSourceId: String,
        logicalSymbol: String,
        mt5Symbol: String,
        timeframe: String = "M1",
        digits: Int,
        requestedUtcStart: Int64,
        requestedUtcEndExclusive: Int64,
        firstUtc: Int64?,
        lastUtc: Int64?,
        rowCount: Int
    ) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.mt5Symbol = mt5Symbol
        self.timeframe = timeframe
        self.digits = digits
        self.requestedUtcStart = requestedUtcStart
        self.requestedUtcEndExclusive = requestedUtcEndExclusive
        self.firstUtc = firstUtc
        self.lastUtc = lastUtc
        self.rowCount = rowCount
    }
}

public struct FXBacktestM1HistoryResponse: Codable, Equatable, Sendable {
    public let apiVersion: String
    public let metadata: FXBacktestM1HistoryMetadata
    public let utcTimestamps: [Int64]
    public let open: [Int64]
    public let high: [Int64]
    public let low: [Int64]
    public let close: [Int64]

    enum CodingKeys: String, CodingKey {
        case apiVersion = "api_version"
        case metadata
        case utcTimestamps = "utc_timestamps"
        case open
        case high
        case low
        case close
    }

    public init(
        apiVersion: String = FXBacktestAPIV1.version,
        metadata: FXBacktestM1HistoryMetadata,
        utcTimestamps: [Int64],
        open: [Int64],
        high: [Int64],
        low: [Int64],
        close: [Int64]
    ) {
        self.apiVersion = apiVersion
        self.metadata = metadata
        self.utcTimestamps = utcTimestamps
        self.open = open
        self.high = high
        self.low = low
        self.close = close
    }

    public func validate() throws {
        guard apiVersion == FXBacktestAPIV1.version else {
            throw FXBacktestAPIValidationError.unsupportedVersion(apiVersion)
        }
        let count = utcTimestamps.count
        guard !metadata.brokerSourceId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("metadata.broker_source_id must not be empty")
        }
        guard !metadata.logicalSymbol.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("metadata.logical_symbol must not be empty")
        }
        guard !metadata.mt5Symbol.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("metadata.mt5_symbol must not be empty")
        }
        guard metadata.timeframe == "M1" else {
            throw FXBacktestAPIValidationError.invalidField("metadata.timeframe must be M1")
        }
        guard (0...10).contains(metadata.digits) else {
            throw FXBacktestAPIValidationError.invalidField("metadata.digits must be between 0 and 10")
        }
        guard metadata.requestedUtcStart < metadata.requestedUtcEndExclusive else {
            throw FXBacktestAPIValidationError.invalidField("metadata requested UTC range is invalid")
        }
        guard metadata.requestedUtcStart % 60 == 0, metadata.requestedUtcEndExclusive % 60 == 0 else {
            throw FXBacktestAPIValidationError.invalidField("metadata requested UTC range must be minute-aligned")
        }
        guard metadata.rowCount == count else {
            throw FXBacktestAPIValidationError.invalidField("metadata.row_count does not match utc_timestamps count")
        }
        guard open.count == count, high.count == count, low.count == count, close.count == count else {
            throw FXBacktestAPIValidationError.invalidField("OHLC column counts do not match")
        }
        if count == 0 {
            guard metadata.firstUtc == nil, metadata.lastUtc == nil else {
                throw FXBacktestAPIValidationError.invalidField("metadata first_utc/last_utc must be null when row_count is zero")
            }
            return
        }
        guard metadata.firstUtc == utcTimestamps.first, metadata.lastUtc == utcTimestamps.last else {
            throw FXBacktestAPIValidationError.invalidField("metadata first_utc/last_utc do not match timestamp columns")
        }
        for index in 0..<count {
            if index > 0, utcTimestamps[index] <= utcTimestamps[index - 1] {
                throw FXBacktestAPIValidationError.invalidField("utc_timestamps must be strictly increasing")
            }
            guard utcTimestamps[index] % 60 == 0 else {
                throw FXBacktestAPIValidationError.invalidField("utc_timestamps must be minute-aligned")
            }
            guard utcTimestamps[index] >= metadata.requestedUtcStart,
                  utcTimestamps[index] < metadata.requestedUtcEndExclusive else {
                throw FXBacktestAPIValidationError.invalidField("utc_timestamps must stay inside the requested UTC range")
            }
            guard open[index] > 0, high[index] > 0, low[index] > 0, close[index] > 0 else {
                throw FXBacktestAPIValidationError.invalidField("OHLC values must be positive")
            }
            guard high[index] >= open[index],
                  high[index] >= close[index],
                  high[index] >= low[index],
                  low[index] <= open[index],
                  low[index] <= close[index] else {
                throw FXBacktestAPIValidationError.invalidField("OHLC invariant failed at index \(index)")
            }
        }
    }
}

public struct FXBacktestExecutionSpecRequest: Codable, Equatable, Sendable {
    public let apiVersion: String
    public let brokerSourceId: String
    public let symbols: [FXBacktestExecutionSymbolRequest]

    enum CodingKeys: String, CodingKey {
        case apiVersion = "api_version"
        case brokerSourceId = "broker_source_id"
        case symbols
    }

    public init(
        apiVersion: String = FXBacktestAPIV1.version,
        brokerSourceId: String,
        symbols: [FXBacktestExecutionSymbolRequest]
    ) {
        self.apiVersion = apiVersion
        self.brokerSourceId = brokerSourceId
        self.symbols = symbols
    }

    public func validate() throws {
        guard apiVersion == FXBacktestAPIV1.version else {
            throw FXBacktestAPIValidationError.unsupportedVersion(apiVersion)
        }
        guard !brokerSourceId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("broker_source_id must not be empty")
        }
        guard !symbols.isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("symbols must not be empty")
        }
        guard symbols.count <= 256 else {
            throw FXBacktestAPIValidationError.invalidField("symbols must not contain more than 256 items")
        }
        let logicalSymbols = symbols.map { $0.logicalSymbol.uppercased() }
        guard Set(logicalSymbols).count == logicalSymbols.count else {
            throw FXBacktestAPIValidationError.invalidField("symbols logical_symbol values must be unique")
        }
        for symbol in symbols {
            try symbol.validate()
        }
    }
}

public struct FXBacktestExecutionSymbolRequest: Codable, Equatable, Sendable {
    public let logicalSymbol: String
    public let expectedMT5Symbol: String?
    public let expectedDigits: Int?

    enum CodingKeys: String, CodingKey {
        case logicalSymbol = "logical_symbol"
        case expectedMT5Symbol = "expected_mt5_symbol"
        case expectedDigits = "expected_digits"
    }

    public init(logicalSymbol: String, expectedMT5Symbol: String? = nil, expectedDigits: Int? = nil) {
        self.logicalSymbol = logicalSymbol
        self.expectedMT5Symbol = expectedMT5Symbol
        self.expectedDigits = expectedDigits
    }

    public func validate() throws {
        guard !logicalSymbol.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("logical_symbol must not be empty")
        }
        if let expectedMT5Symbol {
            guard !expectedMT5Symbol.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                throw FXBacktestAPIValidationError.invalidField("expected_mt5_symbol must not be empty when supplied")
            }
        }
        if let expectedDigits {
            guard (0...10).contains(expectedDigits) else {
                throw FXBacktestAPIValidationError.invalidField("expected_digits must be between 0 and 10")
            }
        }
    }
}

public struct FXBacktestExecutionSpecResponse: Codable, Equatable, Sendable {
    public let apiVersion: String
    public let brokerSourceId: String
    public let capturedAtUtc: Int64
    public let accountCurrency: String
    public let accountLeverage: Double
    public let accountMode: String
    public let mt5AccountMarginMode: Int?
    public let symbols: [FXBacktestExecutionSymbolSpec]

    enum CodingKeys: String, CodingKey {
        case apiVersion = "api_version"
        case brokerSourceId = "broker_source_id"
        case capturedAtUtc = "captured_at_utc"
        case accountCurrency = "account_currency"
        case accountLeverage = "account_leverage"
        case accountMode = "account_mode"
        case mt5AccountMarginMode = "mt5_account_margin_mode"
        case symbols
    }

    public init(
        apiVersion: String = FXBacktestAPIV1.version,
        brokerSourceId: String,
        capturedAtUtc: Int64,
        accountCurrency: String,
        accountLeverage: Double,
        accountMode: String = "hedging",
        mt5AccountMarginMode: Int? = nil,
        symbols: [FXBacktestExecutionSymbolSpec]
    ) {
        self.apiVersion = apiVersion
        self.brokerSourceId = brokerSourceId
        self.capturedAtUtc = capturedAtUtc
        self.accountCurrency = accountCurrency
        self.accountLeverage = accountLeverage
        self.accountMode = accountMode
        self.mt5AccountMarginMode = mt5AccountMarginMode
        self.symbols = symbols
    }

    public func validate() throws {
        guard apiVersion == FXBacktestAPIV1.version else {
            throw FXBacktestAPIValidationError.unsupportedVersion(apiVersion)
        }
        guard !brokerSourceId.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("broker_source_id must not be empty")
        }
        guard capturedAtUtc > 0 else {
            throw FXBacktestAPIValidationError.invalidField("captured_at_utc must be positive")
        }
        guard !accountCurrency.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("account_currency must not be empty")
        }
        guard accountLeverage.isFinite, accountLeverage > 0 else {
            throw FXBacktestAPIValidationError.invalidField("account_leverage must be > 0")
        }
        guard accountMode == "hedging" else {
            throw FXBacktestAPIValidationError.invalidField("account_mode must be hedging")
        }
        guard !symbols.isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("symbols must not be empty")
        }
        let logicalSymbols = symbols.map { $0.logicalSymbol.uppercased() }
        guard Set(logicalSymbols).count == logicalSymbols.count else {
            throw FXBacktestAPIValidationError.invalidField("symbols logical_symbol values must be unique")
        }
        for symbol in symbols {
            try symbol.validate()
        }
    }
}

public struct FXBacktestExecutionSymbolSpec: Codable, Equatable, Sendable {
    public let logicalSymbol: String
    public let mt5Symbol: String
    public let selected: Bool
    public let digits: Int
    public let bid: Double
    public let ask: Double
    public let point: Double
    public let spreadPoints: Int
    public let spreadFloat: Bool
    public let contractSize: Double
    public let volumeMin: Double
    public let volumeStep: Double
    public let volumeMax: Double
    public let swapLongPerLot: Double
    public let swapShortPerLot: Double
    public let swapMode: Int
    public let marginInitialPerLot: Double?
    public let marginMaintenancePerLot: Double?
    public let marginBuyPerLot: Double?
    public let marginSellPerLot: Double?
    public let marginCalcLots: Double
    public let tradeCalcMode: Int
    public let tradeMode: Int
    public let tickSize: Double
    public let tickValue: Double
    public let tickValueProfit: Double?
    public let tickValueLoss: Double?
    public let commissionPerLotPerSide: Double?
    public let commissionSource: String
    public let slippagePoints: Int
    public let slippageSource: String

    enum CodingKeys: String, CodingKey {
        case logicalSymbol = "logical_symbol"
        case mt5Symbol = "mt5_symbol"
        case selected
        case digits
        case bid
        case ask
        case point
        case spreadPoints = "spread_points"
        case spreadFloat = "spread_float"
        case contractSize = "contract_size"
        case volumeMin = "volume_min"
        case volumeStep = "volume_step"
        case volumeMax = "volume_max"
        case swapLongPerLot = "swap_long_per_lot"
        case swapShortPerLot = "swap_short_per_lot"
        case swapMode = "swap_mode"
        case marginInitialPerLot = "margin_initial_per_lot"
        case marginMaintenancePerLot = "margin_maintenance_per_lot"
        case marginBuyPerLot = "margin_buy_per_lot"
        case marginSellPerLot = "margin_sell_per_lot"
        case marginCalcLots = "margin_calc_lots"
        case tradeCalcMode = "trade_calc_mode"
        case tradeMode = "trade_mode"
        case tickSize = "tick_size"
        case tickValue = "tick_value"
        case tickValueProfit = "tick_value_profit"
        case tickValueLoss = "tick_value_loss"
        case commissionPerLotPerSide = "commission_per_lot_per_side"
        case commissionSource = "commission_source"
        case slippagePoints = "slippage_points"
        case slippageSource = "slippage_source"
    }

    public init(
        logicalSymbol: String,
        mt5Symbol: String,
        selected: Bool,
        digits: Int,
        bid: Double,
        ask: Double,
        point: Double,
        spreadPoints: Int,
        spreadFloat: Bool,
        contractSize: Double,
        volumeMin: Double,
        volumeStep: Double,
        volumeMax: Double,
        swapLongPerLot: Double,
        swapShortPerLot: Double,
        swapMode: Int,
        marginInitialPerLot: Double?,
        marginMaintenancePerLot: Double?,
        marginBuyPerLot: Double?,
        marginSellPerLot: Double?,
        marginCalcLots: Double,
        tradeCalcMode: Int,
        tradeMode: Int,
        tickSize: Double,
        tickValue: Double,
        tickValueProfit: Double?,
        tickValueLoss: Double?,
        commissionPerLotPerSide: Double?,
        commissionSource: String,
        slippagePoints: Int,
        slippageSource: String
    ) {
        self.logicalSymbol = logicalSymbol
        self.mt5Symbol = mt5Symbol
        self.selected = selected
        self.digits = digits
        self.bid = bid
        self.ask = ask
        self.point = point
        self.spreadPoints = spreadPoints
        self.spreadFloat = spreadFloat
        self.contractSize = contractSize
        self.volumeMin = volumeMin
        self.volumeStep = volumeStep
        self.volumeMax = volumeMax
        self.swapLongPerLot = swapLongPerLot
        self.swapShortPerLot = swapShortPerLot
        self.swapMode = swapMode
        self.marginInitialPerLot = marginInitialPerLot
        self.marginMaintenancePerLot = marginMaintenancePerLot
        self.marginBuyPerLot = marginBuyPerLot
        self.marginSellPerLot = marginSellPerLot
        self.marginCalcLots = marginCalcLots
        self.tradeCalcMode = tradeCalcMode
        self.tradeMode = tradeMode
        self.tickSize = tickSize
        self.tickValue = tickValue
        self.tickValueProfit = tickValueProfit
        self.tickValueLoss = tickValueLoss
        self.commissionPerLotPerSide = commissionPerLotPerSide
        self.commissionSource = commissionSource
        self.slippagePoints = slippagePoints
        self.slippageSource = slippageSource
    }

    public func validate() throws {
        guard !logicalSymbol.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("logical_symbol must not be empty")
        }
        guard !mt5Symbol.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("mt5_symbol must not be empty")
        }
        guard selected else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) is not selected in MT5")
        }
        guard (0...10).contains(digits) else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) digits must be between 0 and 10")
        }
        guard bid.isFinite, ask.isFinite, bid > 0, ask > 0, ask >= bid else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) bid/ask values are invalid")
        }
        guard point.isFinite, point > 0 else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) point must be > 0")
        }
        guard spreadPoints >= 0, contractSize.isFinite, contractSize > 0 else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) spread/contract values are invalid")
        }
        guard volumeMin.isFinite, volumeStep.isFinite, volumeMax.isFinite,
              volumeMin > 0, volumeStep > 0, volumeMax >= volumeMin else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) volume constraints are invalid")
        }
        guard swapLongPerLot.isFinite, swapShortPerLot.isFinite else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) swap values are invalid")
        }
        let optionalValues = [
            marginInitialPerLot,
            marginMaintenancePerLot,
            marginBuyPerLot,
            marginSellPerLot,
            tickValueProfit,
            tickValueLoss,
            commissionPerLotPerSide
        ]
        guard optionalValues.allSatisfy({ value in
            guard let value else { return true }
            return value.isFinite
        }) else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) optional execution values must be finite when present")
        }
        guard marginCalcLots.isFinite, marginCalcLots > 0 else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) margin_calc_lots must be > 0")
        }
        guard tickSize.isFinite, tickSize >= 0, tickValue.isFinite else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) tick values are invalid")
        }
        guard !commissionSource.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              slippagePoints >= 0,
              !slippageSource.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw FXBacktestAPIValidationError.invalidField("\(logicalSymbol) execution source fields are invalid")
        }
    }
}

public struct FXBacktestAPIErrorResponse: Codable, Equatable, Sendable {
    public let apiVersion: String
    public let error: FXBacktestAPIErrorBody

    enum CodingKeys: String, CodingKey {
        case apiVersion = "api_version"
        case error
    }

    public init(apiVersion: String = FXBacktestAPIV1.version, code: String, message: String) {
        self.apiVersion = apiVersion
        self.error = FXBacktestAPIErrorBody(code: code, message: message)
    }
}

public struct FXBacktestAPIErrorBody: Codable, Equatable, Sendable {
    public let code: String
    public let message: String
}

public enum FXBacktestAPIValidationError: Error, Equatable, CustomStringConvertible, Sendable {
    case unsupportedVersion(String)
    case invalidField(String)

    public var description: String {
        switch self {
        case .unsupportedVersion(let version):
            return "Unsupported FXBacktest API version '\(version)'; expected '\(FXBacktestAPIV1.version)'."
        case .invalidField(let reason):
            return "Invalid FXBacktest API field: \(reason)."
        }
    }
}
