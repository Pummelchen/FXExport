@testable import ClickHouse
import Domain
import XCTest

final class ClickHouseTests: XCTestCase {
    func testClickHouseExceptionParsing() {
        let parser = ClickHouseErrorParser()
        XCTAssertNotNil(parser.parseException(in: "Code: 62. DB::Exception: Syntax error"))
        XCTAssertNil(parser.parseException(in: "1\n"))
    }

    func testClickHouseBasicAuthorizationHeader() {
        let header = ClickHouseHTTPClient.basicAuthorization(username: "default", password: "secret")
        XCTAssertEqual(header, "Basic ZGVmYXVsdDpzZWNyZXQ=")
        XCTAssertNil(ClickHouseHTTPClient.basicAuthorization(username: nil, password: "secret"))
        XCTAssertNil(ClickHouseHTTPClient.basicAuthorization(username: "", password: "secret"))
    }

    func testCanonicalRangeDeleteIsBrokerScopedAndSynchronous() throws {
        let bars = [try makeBar(mt5: 120, utc: 60), try makeBar(mt5: 180, utc: 120)]
        let query = try ClickHouseInsertBuilder(database: "db").canonicalRangeDelete(bars)
        XCTAssertTrue(query.sql.contains("broker_source_id = 'demo'"))
        XCTAssertTrue(query.sql.contains("logical_symbol = 'EURUSD'"))
        XCTAssertTrue(query.sql.contains("mt5_server_ts_raw >= 120"))
        XCTAssertTrue(query.sql.contains("mt5_server_ts_raw <= 180"))
        XCTAssertTrue(query.sql.contains("ts_utc >= 60"))
        XCTAssertTrue(query.sql.contains("ts_utc <= 120"))
        XCTAssertTrue(query.sql.contains("mutations_sync = 1"))
        XCTAssertTrue(query.isIdempotent)
    }

    func testCanonicalRangeDeleteRejectsMixedSymbols() throws {
        let eurusd = try makeBar(mt5: 120, utc: 60)
        let usdjpy = try makeBar(mt5: 180, utc: 120, logicalSymbol: "USDJPY")
        XCTAssertThrowsError(try ClickHouseInsertBuilder(database: "db").canonicalRangeDelete([eurusd, usdjpy]))
    }

    func testCanonicalRangeRejectsMixedMT5SymbolForSameLogicalSymbol() throws {
        let first = try makeBar(mt5: 120, utc: 60, logicalSymbol: "EURUSD", mt5Symbol: "EURUSD")
        let second = try makeBar(mt5: 180, utc: 120, logicalSymbol: "EURUSD", mt5Symbol: "EURUSD.a")
        XCTAssertThrowsError(try ClickHouseInsertBuilder(database: "db").canonicalRangeDelete([first, second]))
    }

    func testCanonicalRangeRejectsMixedDigits() throws {
        let first = try makeBar(mt5: 120, utc: 60, digits: 5)
        let second = try makeBar(mt5: 180, utc: 120, digits: 3)
        XCTAssertThrowsError(try ClickHouseInsertBuilder(database: "db").canonicalBarsInsert([first, second]))
    }

    func testCanonicalRangeDeleteRejectsDuplicateTimestamps() throws {
        let first = try makeBar(mt5: 120, utc: 60)
        let duplicate = try makeBar(mt5: 120, utc: 60)
        XCTAssertThrowsError(try ClickHouseInsertBuilder(database: "db").canonicalRangeDelete([first, duplicate]))
    }

    func testCanonicalRangeDeleteRejectsUnverifiedOffsets() throws {
        let bar = try makeBar(mt5: 120, utc: 60, offsetConfidence: .inferred)
        XCTAssertThrowsError(try ClickHouseInsertBuilder(database: "db").canonicalRangeDelete([bar]))
        XCTAssertThrowsError(try ClickHouseInsertBuilder(database: "db").canonicalBarsInsert([bar]))
    }

    func testCanonicalRangeIntegrityCheckIsBrokerScoped() throws {
        let bars = [try makeBar(mt5: 120, utc: 60), try makeBar(mt5: 180, utc: 120)]
        let query = try ClickHouseInsertBuilder(database: "db").canonicalRangeIntegrityCheck(bars)
        XCTAssertTrue(query.sql.contains("broker_source_id = 'demo'"))
        XCTAssertTrue(query.sql.contains("logical_symbol = 'EURUSD'"))
        XCTAssertTrue(query.sql.contains("ts_utc >= 60"))
        XCTAssertTrue(query.sql.contains("ts_utc <= 120"))
        XCTAssertTrue(query.sql.contains("uniqExact(mt5_server_ts_raw)"))
        XCTAssertTrue(query.sql.contains("uniqExact(ts_utc)"))
        XCTAssertTrue(query.sql.contains("uniqExact(mt5_symbol)"))
        XCTAssertTrue(query.sql.contains("countIf(offset_confidence != 'verified')"))
    }

    func testCanonicalRangeReadbackRowsAreBrokerScopedAndOrdered() throws {
        let bars = [try makeBar(mt5: 120, utc: 60), try makeBar(mt5: 180, utc: 120)]
        let query = try ClickHouseInsertBuilder(database: "db").canonicalRangeReadbackRows(bars)
        XCTAssertTrue(query.sql.contains("SELECT mt5_symbol, timeframe, mt5_server_ts_raw, ts_utc"))
        XCTAssertTrue(query.sql.contains("server_utc_offset_seconds, offset_source, offset_confidence"))
        XCTAssertTrue(query.sql.contains("open_scaled, high_scaled, low_scaled, close_scaled, digits, bar_hash"))
        XCTAssertTrue(query.sql.contains("broker_source_id = 'demo'"))
        XCTAssertTrue(query.sql.contains("logical_symbol = 'EURUSD'"))
        XCTAssertTrue(query.sql.contains("ts_utc >= 60"))
        XCTAssertTrue(query.sql.contains("ts_utc <= 120"))
        XCTAssertTrue(query.sql.contains("ORDER BY mt5_server_ts_raw ASC, ts_utc ASC"))
    }

    func testCanonicalConflictCandidateQueryIsBrokerScoped() throws {
        let bars = [try makeBar(mt5: 120, utc: 60), try makeBar(mt5: 180, utc: 120)]
        let query = try ClickHouseInsertBuilder(database: "db").canonicalConflictCandidates(bars)
        XCTAssertTrue(query.sql.contains("SELECT ts_utc, bar_hash, open_scaled, high_scaled, low_scaled, close_scaled"))
        XCTAssertTrue(query.sql.contains("FROM db.ohlc_m1_canonical"))
        XCTAssertTrue(query.sql.contains("broker_source_id = 'demo'"))
        XCTAssertTrue(query.sql.contains("logical_symbol = 'EURUSD'"))
        XCTAssertTrue(query.sql.contains("ts_utc >= 60"))
        XCTAssertTrue(query.sql.contains("ts_utc <= 120"))
    }

    private func makeBar(
        mt5: Int64,
        utc: Int64,
        logicalSymbol: String = "EURUSD",
        mt5Symbol mt5SymbolValue: String? = nil,
        digits digitsValue: Int = 5,
        offsetConfidence: OffsetConfidence = .verified
    ) throws -> ValidatedBar {
        let broker = try BrokerSourceId("demo")
        let logical = try LogicalSymbol(logicalSymbol)
        let mt5Symbol = try MT5Symbol(mt5SymbolValue ?? logicalSymbol)
        let digits = try Digits(digitsValue)
        let priceText = digitsValue == 3 ? "1.100" : "1.10000"
        let open = try PriceScaled.fromDecimalString(priceText, digits: digits)
        return ValidatedBar(
            brokerSourceId: broker,
            logicalSymbol: logical,
            mt5Symbol: mt5Symbol,
            timeframe: .m1,
            mt5ServerTime: MT5ServerSecond(rawValue: mt5),
            utcTime: UtcSecond(rawValue: utc),
            serverUtcOffset: OffsetSeconds(rawValue: 60),
            offsetSource: .configured,
            offsetConfidence: offsetConfidence,
            open: open,
            high: open,
            low: open,
            close: open,
            digits: digits,
            batchId: BatchId(rawValue: "batch"),
            sourceStatus: .mt5ClosedBar,
            ingestedAtUtc: UtcSecond(rawValue: 1)
        )
    }
}
