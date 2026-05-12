import ClickHouse
import Domain
import Foundation

public struct HistoricalOhlcRequest: Sendable, Equatable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let utcStartInclusive: UtcSecond
    public let utcEndExclusive: UtcSecond
    public let expectedMT5Symbol: MT5Symbol?
    public let expectedDigits: Digits?
    public let maximumRows: Int?
    public let allowEmpty: Bool

    public init(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        utcStartInclusive: UtcSecond,
        utcEndExclusive: UtcSecond,
        expectedMT5Symbol: MT5Symbol? = nil,
        expectedDigits: Digits? = nil,
        maximumRows: Int? = nil,
        allowEmpty: Bool = false
    ) throws {
        guard utcStartInclusive.rawValue < utcEndExclusive.rawValue else {
            throw HistoryDataError.invalidRequest("UTC start must be before UTC end.")
        }
        guard utcStartInclusive.isMinuteAligned, utcEndExclusive.isMinuteAligned else {
            throw HistoryDataError.invalidRequest("UTC range boundaries must be minute-aligned.")
        }
        if let maximumRows, maximumRows <= 0 {
            throw HistoryDataError.invalidRequest("maximumRows must be positive when supplied.")
        }
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.utcStartInclusive = utcStartInclusive
        self.utcEndExclusive = utcEndExclusive
        self.expectedMT5Symbol = expectedMT5Symbol
        self.expectedDigits = expectedDigits
        self.maximumRows = maximumRows
        self.allowEmpty = allowEmpty
    }
}

public protocol HistoricalOhlcDataProviding: Sendable {
    func loadM1Ohlc(_ request: HistoricalOhlcRequest) async throws -> ColumnarOhlcSeries
}

public struct ClickHouseHistoricalOhlcDataProvider: HistoricalOhlcDataProviding {
    private let client: ClickHouseClientProtocol
    private let database: String
    private let defaultMaximumRows: Int

    public init(
        client: ClickHouseClientProtocol,
        database: String,
        defaultMaximumRows: Int = 5_000_000
    ) {
        self.client = client
        self.database = database
        self.defaultMaximumRows = defaultMaximumRows
    }

    public func loadM1Ohlc(_ request: HistoricalOhlcRequest) async throws -> ColumnarOhlcSeries {
        let limit = request.maximumRows ?? defaultMaximumRows
        guard limit > 0 else {
            throw HistoryDataError.invalidRequest("maximumRows must be positive.")
        }
        guard limit < Int.max else {
            throw HistoryDataError.invalidRequest("maximumRows is too large.")
        }
        try await validateVerifiedCoverage(request)
        let body = try await client.execute(.select(try sql(for: request, limit: limit + 1)))
        let rows = try parseRows(body, request: request)
        guard rows.count <= limit else {
            throw HistoryDataError.rowLimitExceeded(limit: limit)
        }
        guard !rows.isEmpty else {
            if request.allowEmpty, let expectedDigits = request.expectedDigits {
                return try ColumnarOhlcSeries(
                    metadata: BarSeriesMetadata(
                        brokerSourceId: request.brokerSourceId,
                        logicalSymbol: request.logicalSymbol,
                        digits: expectedDigits,
                        requestedUtcStart: request.utcStartInclusive,
                        requestedUtcEndExclusive: request.utcEndExclusive
                    ),
                    utcTimestamps: [],
                    open: [],
                    high: [],
                    low: [],
                    close: []
                )
            }
            throw HistoryDataError.emptyResult(request.logicalSymbol, request.utcStartInclusive, request.utcEndExclusive)
        }

        let digits = try validatedDigits(rows, expected: request.expectedDigits)
        let timestamps = rows.map(\.utcTime.rawValue)
        let metadata = BarSeriesMetadata(
            brokerSourceId: request.brokerSourceId,
            logicalSymbol: request.logicalSymbol,
            digits: digits,
            requestedUtcStart: request.utcStartInclusive,
            requestedUtcEndExclusive: request.utcEndExclusive,
            firstUtc: rows.first?.utcTime,
            lastUtc: rows.last?.utcTime
        )
        return try ColumnarOhlcSeries(
            metadata: metadata,
            utcTimestamps: timestamps,
            open: rows.map(\.open),
            high: rows.map(\.high),
            low: rows.map(\.low),
            close: rows.map(\.close)
        )
    }

    private func sql(for request: HistoricalOhlcRequest, limit: Int) throws -> String {
        let databaseName = try Self.sqlIdentifier(database)
        return """
        SELECT mt5_symbol, ts_utc, mt5_server_ts_raw, open_scaled, high_scaled, low_scaled, close_scaled,
               digits, timeframe, offset_confidence, source_status, bar_hash
        FROM \(databaseName).ohlc_m1_canonical
        WHERE broker_source_id = '\(Self.sqlLiteral(request.brokerSourceId.rawValue))'
          AND logical_symbol = '\(Self.sqlLiteral(request.logicalSymbol.rawValue))'
          AND ts_utc >= \(request.utcStartInclusive.rawValue)
          AND ts_utc < \(request.utcEndExclusive.rawValue)
        ORDER BY ts_utc ASC, ingested_at_utc ASC
        LIMIT \(limit)
        FORMAT TabSeparated
        """
    }

    private func validateVerifiedCoverage(_ request: HistoricalOhlcRequest) async throws {
        let databaseName = try Self.sqlIdentifier(database)
        let body = try await client.execute(.select("""
        SELECT utc_range_start, utc_range_end_exclusive
        FROM \(databaseName).ohlc_m1_verified_coverage
        WHERE broker_source_id = '\(Self.sqlLiteral(request.brokerSourceId.rawValue))'
          AND logical_symbol = '\(Self.sqlLiteral(request.logicalSymbol.rawValue))'
          AND timeframe = 'M1'
          AND utc_range_end_exclusive > \(request.utcStartInclusive.rawValue)
          AND utc_range_start < \(request.utcEndExclusive.rawValue)
        ORDER BY utc_range_start ASC, utc_range_end_exclusive ASC
        FORMAT TabSeparated
        """))
        let intervals = try parseCoverageIntervals(body)
        var cursor = request.utcStartInclusive.rawValue
        for interval in intervals where interval.end > cursor {
            guard interval.start <= cursor else {
                throw HistoryDataError.missingVerifiedCoverage(
                    request.logicalSymbol,
                    UtcSecond(rawValue: cursor),
                    UtcSecond(rawValue: min(interval.start, request.utcEndExclusive.rawValue))
                )
            }
            cursor = max(cursor, interval.end)
            if cursor >= request.utcEndExclusive.rawValue {
                return
            }
        }
        throw HistoryDataError.missingVerifiedCoverage(
            request.logicalSymbol,
            UtcSecond(rawValue: cursor),
            request.utcEndExclusive
        )
    }

    private func parseCoverageIntervals(_ body: String) throws -> [CoverageInterval] {
        try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { line in
                let fields = line.split(separator: "\t", omittingEmptySubsequences: false)
                guard fields.count == 2,
                      let start = Int64(fields[0]),
                      let end = Int64(fields[1]),
                      start < end else {
                    throw HistoryDataError.invalidCanonicalRow("invalid verified coverage row '\(line)'")
                }
                return CoverageInterval(start: start, end: end)
            }
    }

    private func parseRows(_ body: String, request: HistoricalOhlcRequest) throws -> [CanonicalOhlcRow] {
        try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try parseRow(String($0), request: request) }
    }

    private func parseRow(_ row: String, request: HistoricalOhlcRequest) throws -> CanonicalOhlcRow {
        let fields = row.split(separator: "\t", omittingEmptySubsequences: false).map { Self.unescapeTabSeparated(String($0)) }
        guard fields.count == 12,
              let mt5Symbol = MT5Symbol(rawValue: fields[0]),
              let tsUtc = Int64(fields[1]),
              let mt5ServerTime = Int64(fields[2]),
              let open = Int64(fields[3]),
              let high = Int64(fields[4]),
              let low = Int64(fields[5]),
              let close = Int64(fields[6]),
              let digitsRaw = Int(fields[7]) else {
            throw HistoryDataError.invalidCanonicalRow(row)
        }
        guard let timeframe = Timeframe(rawValue: fields[8]) else {
            throw HistoryDataError.invalidCanonicalRow("unknown timeframe '\(fields[8])'")
        }
        guard let confidence = OffsetConfidence(rawValue: fields[9]) else {
            throw HistoryDataError.invalidCanonicalRow("unknown offset confidence '\(fields[9])'")
        }
        guard let sourceStatus = SourceStatus(rawValue: fields[10]) else {
            throw HistoryDataError.invalidCanonicalRow("unknown source status '\(fields[10])'")
        }
        guard let storedHashValue = UInt64(fields[11], radix: 16) else {
            throw HistoryDataError.invalidCanonicalRow("invalid bar hash '\(fields[11])'")
        }
        guard timeframe == .m1 else {
            throw HistoryDataError.invalidCanonicalRow("canonical data API only accepts M1 rows")
        }
        let serverTime = MT5ServerSecond(rawValue: mt5ServerTime)
        guard serverTime.isMinuteAligned else {
            throw HistoryDataError.invalidCanonicalRow("MT5 server timestamp \(mt5ServerTime) is not minute-aligned")
        }
        let utcTime = UtcSecond(rawValue: tsUtc)
        guard utcTime.isMinuteAligned else {
            throw HistoryDataError.invalidCanonicalRow("UTC timestamp \(tsUtc) is not minute-aligned")
        }
        guard confidence == .verified else {
            throw HistoryDataError.invalidCanonicalRow("canonical row has \(confidence.rawValue) UTC offset confidence")
        }
        guard sourceStatus == .mt5ClosedBar else {
            throw HistoryDataError.invalidCanonicalRow("canonical row source_status is \(sourceStatus.rawValue), expected \(SourceStatus.mt5ClosedBar.rawValue)")
        }
        if let expectedMT5Symbol = request.expectedMT5Symbol, mt5Symbol != expectedMT5Symbol {
            throw HistoryDataError.invalidCanonicalRow("MT5 symbol mismatch: expected \(expectedMT5Symbol.rawValue), got \(mt5Symbol.rawValue)")
        }
        let digits = try Digits(digitsRaw)
        let computedHash = BarHash.compute(
            brokerSourceId: request.brokerSourceId,
            logicalSymbol: request.logicalSymbol,
            mt5Symbol: mt5Symbol,
            timeframe: timeframe,
            utcTime: utcTime,
            mt5ServerTime: serverTime,
            open: PriceScaled(rawValue: open, digits: digits),
            high: PriceScaled(rawValue: high, digits: digits),
            low: PriceScaled(rawValue: low, digits: digits),
            close: PriceScaled(rawValue: close, digits: digits),
            digits: digits
        )
        guard storedHashValue == computedHash.rawValue else {
            throw HistoryDataError.invalidCanonicalRow("bar hash mismatch at UTC \(tsUtc)")
        }
        return CanonicalOhlcRow(
            utcTime: utcTime,
            mt5ServerTime: serverTime,
            open: open,
            high: high,
            low: low,
            close: close,
            digits: digits
        )
    }

    private func validatedDigits(_ rows: [CanonicalOhlcRow], expected: Digits?) throws -> Digits {
        guard let first = rows.first else {
            if let expected { return expected }
            throw HistoryDataError.invalidCanonicalRow("cannot infer digits from an empty result")
        }
        for row in rows where row.digits != first.digits {
            throw HistoryDataError.invalidCanonicalRow("mixed digits in canonical result")
        }
        if let expected, expected != first.digits {
            throw HistoryDataError.invalidCanonicalRow("digits mismatch: expected \(expected.rawValue), got \(first.digits.rawValue)")
        }
        return first.digits
    }

    private static func sqlLiteral(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
    }

    private static func sqlIdentifier(_ value: String) throws -> String {
        guard let first = value.first,
              first == "_" || first.isLetter,
              value.allSatisfy({ $0.isLetter || $0.isNumber || $0 == "_" }) else {
            throw HistoryDataError.invalidRequest("ClickHouse database name must contain only letters, numbers, or underscore, and must not start with a digit.")
        }
        return value
    }

    private static func unescapeTabSeparated(_ value: String) -> String {
        var result = ""
        var escaping = false
        for character in value {
            if escaping {
                switch character {
                case "t": result.append("\t")
                case "n": result.append("\n")
                case "r": result.append("\r")
                case "\\": result.append("\\")
                default: result.append(character)
                }
                escaping = false
            } else if character == "\\" {
                escaping = true
            } else {
                result.append(character)
            }
        }
        if escaping {
            result.append("\\")
        }
        return result
    }
}

private struct CanonicalOhlcRow: Sendable {
    let utcTime: UtcSecond
    let mt5ServerTime: MT5ServerSecond
    let open: Int64
    let high: Int64
    let low: Int64
    let close: Int64
    let digits: Digits
}

private struct CoverageInterval: Sendable {
    let start: Int64
    let end: Int64
}
