import Foundation

public enum FXBacktestAPIV1 {
    public static let version = "fxexport.fxbacktest.history.v1"
    public static let statusPath = "/v1/status"
    public static let m1HistoryPath = "/v1/history/m1"
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
