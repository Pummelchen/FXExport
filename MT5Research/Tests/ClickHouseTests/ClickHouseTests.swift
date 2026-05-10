import ClickHouse
import Domain
import XCTest

final class ClickHouseTests: XCTestCase {
    func testClickHouseExceptionParsing() {
        let parser = ClickHouseErrorParser()
        XCTAssertNotNil(parser.parseException(in: "Code: 62. DB::Exception: Syntax error"))
        XCTAssertNil(parser.parseException(in: "1\n"))
    }

    func testCanonicalRangeDeleteIsBrokerScopedAndSynchronous() throws {
        let bars = [try makeBar(mt5: 120, utc: 60), try makeBar(mt5: 180, utc: 120)]
        let query = try ClickHouseInsertBuilder(database: "db").canonicalRangeDelete(bars)
        XCTAssertTrue(query.sql.contains("broker_source_id = 'demo'"))
        XCTAssertTrue(query.sql.contains("logical_symbol = 'EURUSD'"))
        XCTAssertTrue(query.sql.contains("mt5_server_ts_raw >= 120"))
        XCTAssertTrue(query.sql.contains("mt5_server_ts_raw <= 180"))
        XCTAssertTrue(query.sql.contains("mutations_sync = 1"))
        XCTAssertTrue(query.isIdempotent)
    }

    func testCanonicalRangeDeleteRejectsMixedSymbols() throws {
        let eurusd = try makeBar(mt5: 120, utc: 60)
        let usdjpy = try makeBar(mt5: 180, utc: 120, logicalSymbol: "USDJPY")
        XCTAssertThrowsError(try ClickHouseInsertBuilder(database: "db").canonicalRangeDelete([eurusd, usdjpy]))
    }

    func testCanonicalRangeDeleteRejectsDuplicateTimestamps() throws {
        let first = try makeBar(mt5: 120, utc: 60)
        let duplicate = try makeBar(mt5: 120, utc: 60)
        XCTAssertThrowsError(try ClickHouseInsertBuilder(database: "db").canonicalRangeDelete([first, duplicate]))
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
    }

    private func makeBar(mt5: Int64, utc: Int64, logicalSymbol: String = "EURUSD") throws -> ValidatedBar {
        let broker = try BrokerSourceId("demo")
        let logical = try LogicalSymbol(logicalSymbol)
        let mt5Symbol = try MT5Symbol(logicalSymbol)
        let digits = try Digits(5)
        let open = try PriceScaled.fromDecimalString("1.10000", digits: digits)
        return ValidatedBar(
            brokerSourceId: broker,
            logicalSymbol: logical,
            mt5Symbol: mt5Symbol,
            timeframe: .m1,
            mt5ServerTime: MT5ServerSecond(rawValue: mt5),
            utcTime: UtcSecond(rawValue: utc),
            serverUtcOffset: OffsetSeconds(rawValue: 60),
            offsetSource: .configured,
            offsetConfidence: .verified,
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
