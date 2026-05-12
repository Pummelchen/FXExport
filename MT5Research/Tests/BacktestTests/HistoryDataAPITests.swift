import BacktestCore
import ClickHouse
import Domain
import XCTest

final class HistoryDataAPITests: XCTestCase {
    func testColumnarSeriesRejectsNonMinuteAlignedUTC() throws {
        let metadata = try metadata()
        XCTAssertThrowsError(try ColumnarOhlcSeries(
            metadata: metadata,
            utcTimestamps: [60, 121],
            open: [100, 101],
            high: [101, 102],
            low: [99, 100],
            close: [100, 102]
        ))
    }

    func testColumnarSeriesRejectsInvalidOHLCInvariant() throws {
        let metadata = try metadata()
        XCTAssertThrowsError(try ColumnarOhlcSeries(
            metadata: metadata,
            utcTimestamps: [60],
            open: [100],
            high: [99],
            low: [98],
            close: [100]
        ))
    }

    func testClickHouseProviderBuildsValidatedColumnarSeries() async throws {
        let client = MockHistoryClickHouse(body: try [
            historyRow(utc: 1_577_836_800, mt5: 1_577_844_000, open: 108_342, high: 108_360, low: 108_300, close: 108_350),
            historyRow(utc: 1_577_836_860, mt5: 1_577_844_060, open: 108_350, high: 108_370, low: 108_320, close: 108_355)
        ].joined(separator: "\n"))
        let provider = ClickHouseHistoricalOhlcDataProvider(client: client, database: "fx")
        let request = try request()

        let series = try await provider.loadM1Ohlc(request)

        XCTAssertEqual(series.count, 2)
        XCTAssertEqual(series.utcTimestamps, [1_577_836_800, 1_577_836_860])
        XCTAssertEqual(series.open, [108_342, 108_350])
        XCTAssertEqual(series.metadata.logicalSymbol, try LogicalSymbol("EURUSD"))
        XCTAssertEqual(series.metadata.digits, try Digits(5))
        XCTAssertEqual(series.metadata.firstUtc, UtcSecond(rawValue: 1_577_836_800))
        XCTAssertEqual(series.metadata.lastUtc, UtcSecond(rawValue: 1_577_836_860))
        let sql = await client.lastSQL()
        XCTAssertTrue(sql.contains("FROM fx.ohlc_m1_canonical"))
        XCTAssertTrue(sql.contains("logical_symbol = 'EURUSD'"))
        XCTAssertTrue(sql.contains("LIMIT 5000001"))
    }

    func testClickHouseProviderRejectsNonVerifiedCanonicalRows() async throws {
        let client = MockHistoryClickHouse(body: try historyRow(
            utc: 1_577_836_800,
            mt5: 1_577_844_000,
            open: 108_342,
            high: 108_360,
            low: 108_300,
            close: 108_350,
            confidence: "inferred"
        ))
        let provider = ClickHouseHistoricalOhlcDataProvider(client: client, database: "fx")

        await XCTAssertThrowsErrorAsync(try await provider.loadM1Ohlc(try self.request())) { error in
            XCTAssertTrue(String(describing: error).contains("inferred UTC offset confidence"))
        }
    }

    func testClickHouseProviderRejectsDuplicateUtcRows() async throws {
        let row = try historyRow(utc: 1_577_836_800, mt5: 1_577_844_000, open: 108_342, high: 108_360, low: 108_300, close: 108_350)
        let client = MockHistoryClickHouse(body: [row, row].joined(separator: "\n"))
        let provider = ClickHouseHistoricalOhlcDataProvider(client: client, database: "fx")

        await XCTAssertThrowsErrorAsync(try await provider.loadM1Ohlc(try self.request())) { error in
            XCTAssertTrue(String(describing: error).contains("strictly increasing"))
        }
    }

    func testClickHouseProviderRejectsStoredHashMismatch() async throws {
        let validRow = try historyRow(utc: 1_577_836_800, mt5: 1_577_844_000, open: 108_342, high: 108_360, low: 108_300, close: 108_350)
        let fields = validRow.split(separator: "\t", omittingEmptySubsequences: false).map(String.init)
        let corruptRow = (Array(fields.dropLast()) + ["0000000000000000"]).joined(separator: "\t")
        let client = MockHistoryClickHouse(body: corruptRow)
        let provider = ClickHouseHistoricalOhlcDataProvider(client: client, database: "fx")

        await XCTAssertThrowsErrorAsync(try await provider.loadM1Ohlc(try self.request())) { error in
            XCTAssertTrue(String(describing: error).contains("bar hash mismatch"))
        }
    }

    func testClickHouseProviderRejectsUnexpectedMT5SymbolWhenExpected() async throws {
        let client = MockHistoryClickHouse(body: try historyRow(
            utc: 1_577_836_800,
            mt5: 1_577_844_000,
            open: 108_342,
            high: 108_360,
            low: 108_300,
            close: 108_350,
            mt5SymbolText: "EURUSD.a"
        ))
        let provider = ClickHouseHistoricalOhlcDataProvider(client: client, database: "fx")
        let expectedMT5Symbol = try MT5Symbol("EURUSD")

        await XCTAssertThrowsErrorAsync(try await provider.loadM1Ohlc(try self.request(expectedMT5Symbol: expectedMT5Symbol))) { error in
            XCTAssertTrue(String(describing: error).contains("MT5 symbol mismatch"))
        }
    }

    func testClickHouseProviderEnforcesRowLimit() async throws {
        let client = MockHistoryClickHouse(body: try [
            historyRow(utc: 1_577_836_800, mt5: 1_577_844_000, open: 108_342, high: 108_360, low: 108_300, close: 108_350),
            historyRow(utc: 1_577_836_860, mt5: 1_577_844_060, open: 108_350, high: 108_370, low: 108_320, close: 108_355)
        ].joined(separator: "\n"))
        let provider = ClickHouseHistoricalOhlcDataProvider(client: client, database: "fx")
        let limitedRequest = try HistoricalOhlcRequest(
            brokerSourceId: try BrokerSourceId("demo"),
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStartInclusive: UtcSecond(rawValue: 1_577_836_800),
            utcEndExclusive: UtcSecond(rawValue: 1_577_836_920),
            expectedDigits: try Digits(5),
            maximumRows: 1
        )

        await XCTAssertThrowsErrorAsync(try await provider.loadM1Ohlc(limitedRequest)) { error in
            XCTAssertEqual(error as? HistoryDataError, .rowLimitExceeded(limit: 1))
        }
    }

    private func metadata() throws -> BarSeriesMetadata {
        BarSeriesMetadata(
            brokerSourceId: try BrokerSourceId("demo"),
            logicalSymbol: try LogicalSymbol("EURUSD"),
            digits: try Digits(5)
        )
    }

    private func request(expectedMT5Symbol: MT5Symbol? = nil) throws -> HistoricalOhlcRequest {
        try HistoricalOhlcRequest(
            brokerSourceId: try BrokerSourceId("demo"),
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStartInclusive: UtcSecond(rawValue: 1_577_836_800),
            utcEndExclusive: UtcSecond(rawValue: 1_577_836_920),
            expectedMT5Symbol: expectedMT5Symbol,
            expectedDigits: try Digits(5)
        )
    }
}

private func historyRow(
    utc: Int64,
    mt5: Int64,
    open: Int64,
    high: Int64,
    low: Int64,
    close: Int64,
    confidence: String = "verified",
    sourceStatus: String = "mt5ClosedBar",
    mt5SymbolText: String = "EURUSD"
) throws -> String {
    let broker = try BrokerSourceId("demo")
    let logicalSymbol = try LogicalSymbol("EURUSD")
    let mt5Symbol = try MT5Symbol(mt5SymbolText)
    let digits = try Digits(5)
    let hash = BarHash.compute(
        brokerSourceId: broker,
        logicalSymbol: logicalSymbol,
        mt5Symbol: mt5Symbol,
        timeframe: .m1,
        utcTime: UtcSecond(rawValue: utc),
        mt5ServerTime: MT5ServerSecond(rawValue: mt5),
        open: PriceScaled(rawValue: open, digits: digits),
        high: PriceScaled(rawValue: high, digits: digits),
        low: PriceScaled(rawValue: low, digits: digits),
        close: PriceScaled(rawValue: close, digits: digits),
        digits: digits
    )
    return [
        mt5Symbol.rawValue,
        String(utc),
        String(mt5),
        String(open),
        String(high),
        String(low),
        String(close),
        String(digits.rawValue),
        Timeframe.m1.rawValue,
        confidence,
        sourceStatus,
        hash.description
    ].joined(separator: "\t")
}

private actor MockHistoryClickHouse: ClickHouseClientProtocol {
    private let body: String
    private var sql: String = ""

    init(body: String) {
        self.body = body
    }

    func execute(_ query: ClickHouseQuery) async throws -> String {
        sql = query.sql
        return body
    }

    func lastSQL() -> String {
        sql
    }
}

private func XCTAssertThrowsErrorAsync<T>(
    _ expression: @autoclosure @escaping () async throws -> T,
    _ validation: (Error) -> Void = { _ in },
    file: StaticString = #filePath,
    line: UInt = #line
) async {
    do {
        _ = try await expression()
        XCTFail("Expected error", file: file, line: line)
    } catch {
        validation(error)
    }
}
