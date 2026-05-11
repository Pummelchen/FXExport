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
        let client = MockHistoryClickHouse(body: """
        1577836800\t1577844000\t108342\t108360\t108300\t108350\t5\tM1\tverified\tmt5ClosedBar\tabc
        1577836860\t1577844060\t108350\t108370\t108320\t108355\t5\tM1\tverified\tmt5ClosedBar\tdef
        """)
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
        let client = MockHistoryClickHouse(body: """
        1577836800\t1577844000\t108342\t108360\t108300\t108350\t5\tM1\tinferred\tmt5ClosedBar\tabc
        """)
        let provider = ClickHouseHistoricalOhlcDataProvider(client: client, database: "fx")

        await XCTAssertThrowsErrorAsync(try await provider.loadM1Ohlc(try self.request())) { error in
            XCTAssertTrue(String(describing: error).contains("inferred UTC offset confidence"))
        }
    }

    func testClickHouseProviderRejectsDuplicateUtcRows() async throws {
        let client = MockHistoryClickHouse(body: """
        1577836800\t1577844000\t108342\t108360\t108300\t108350\t5\tM1\tverified\tmt5ClosedBar\tabc
        1577836800\t1577844000\t108342\t108360\t108300\t108350\t5\tM1\tverified\tmt5ClosedBar\tdef
        """)
        let provider = ClickHouseHistoricalOhlcDataProvider(client: client, database: "fx")

        await XCTAssertThrowsErrorAsync(try await provider.loadM1Ohlc(try self.request())) { error in
            XCTAssertTrue(String(describing: error).contains("strictly increasing"))
        }
    }

    func testClickHouseProviderEnforcesRowLimit() async throws {
        let client = MockHistoryClickHouse(body: """
        1577836800\t1577844000\t108342\t108360\t108300\t108350\t5\tM1\tverified\tmt5ClosedBar\tabc
        1577836860\t1577844060\t108350\t108370\t108320\t108355\t5\tM1\tverified\tmt5ClosedBar\tdef
        """)
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

    private func request() throws -> HistoricalOhlcRequest {
        try HistoricalOhlcRequest(
            brokerSourceId: try BrokerSourceId("demo"),
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStartInclusive: UtcSecond(rawValue: 1_577_836_800),
            utcEndExclusive: UtcSecond(rawValue: 1_577_836_920),
            expectedDigits: try Digits(5)
        )
    }
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
