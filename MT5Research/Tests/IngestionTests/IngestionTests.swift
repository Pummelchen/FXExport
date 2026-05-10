import ClickHouse
import Domain
import Ingestion
import XCTest

final class IngestionTests: XCTestCase {
    func testBatchBuilderRange() {
        let builder = BatchBuilder(chunkSize: 2)
        let range = builder.nextRange(start: MT5ServerSecond(rawValue: 120), endInclusive: MT5ServerSecond(rawValue: 600))
        XCTAssertEqual(range.from.rawValue, 120)
        XCTAssertEqual(range.toExclusive.rawValue, 240)
    }

    func testCheckpointStoreRoundTrip() async throws {
        let store = InMemoryCheckpointStore()
        let state = IngestState(
            brokerSourceId: try BrokerSourceId("demo"),
            logicalSymbol: try LogicalSymbol("EURUSD"),
            mt5Symbol: try MT5Symbol("EURUSD"),
            oldestMT5ServerTime: MT5ServerSecond(rawValue: 60),
            latestIngestedClosedMT5ServerTime: MT5ServerSecond(rawValue: 120),
            latestIngestedClosedUtcTime: UtcSecond(rawValue: 0),
            status: .backfilling,
            lastBatchId: BatchId(rawValue: "b"),
            updatedAtUtc: UtcSecond(rawValue: 1)
        )
        try await store.save(state)
        let loaded = try await store.latestState(brokerSourceId: state.brokerSourceId, logicalSymbol: state.logicalSymbol)
        XCTAssertEqual(loaded, state)
    }

    func testBrokerOffsetStoreLoadsVerifiedIdentityBoundRows() async throws {
        let client = MockClickHouseClient(body: """
        demo\tBroker Ltd\tBroker-Server\t12345\t0\t3600\t7200\tmanual\tverified\t1700000000

        """)
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker Ltd", server: "Broker-Server", accountLogin: 12345)
        let map = try await ClickHouseBrokerOffsetStore(client: client, database: "db")
            .loadVerifiedOffsetMap(brokerSourceId: broker, terminalIdentity: identity)

        XCTAssertEqual(map.brokerSourceId, broker)
        XCTAssertEqual(map.terminalIdentity, identity)
        XCTAssertEqual(map.segments.count, 1)
        XCTAssertEqual(map.segments[0].offset, OffsetSeconds(rawValue: 7200))
        let lastQuery = await client.lastQuery()
        XCTAssertTrue(lastQuery.contains("confidence = 'verified'"))
        XCTAssertTrue(lastQuery.contains("is_active = 1"))
        XCTAssertTrue(lastQuery.contains("mt5_company = 'Broker Ltd'"))
        XCTAssertTrue(lastQuery.contains("mt5_account_login = 12345"))
    }

    func testBrokerOffsetStoreRejectsInferredRowsEvenIfReturnedByDatabase() async throws {
        let client = MockClickHouseClient(body: """
        demo\tBroker Ltd\tBroker-Server\t12345\t0\t3600\t7200\tinferred\tinferred\t1700000000

        """)
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker Ltd", server: "Broker-Server", accountLogin: 12345)
        await XCTAssertThrowsErrorAsync(try await ClickHouseBrokerOffsetStore(client: client, database: "db")
            .loadVerifiedOffsetMap(brokerSourceId: broker, terminalIdentity: identity))
    }
}

private actor MockClickHouseClient: ClickHouseClientProtocol {
    private let body: String
    private var query: String = ""

    init(body: String) {
        self.body = body
    }

    func execute(_ query: ClickHouseQuery) async throws -> String {
        self.query = query.sql
        return body
    }

    func lastQuery() -> String {
        query
    }
}

private func XCTAssertThrowsErrorAsync<T>(
    _ expression: @autoclosure () async throws -> T,
    file: StaticString = #filePath,
    line: UInt = #line
) async {
    do {
        _ = try await expression()
        XCTFail("Expected async expression to throw", file: file, line: line)
    } catch {}
}
