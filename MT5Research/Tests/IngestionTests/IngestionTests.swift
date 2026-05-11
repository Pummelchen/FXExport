import ClickHouse
import Domain
import Ingestion
import MT5Bridge
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

    func testClickHouseCheckpointReadUsesHighestIngestedServerTime() async throws {
        let client = MockClickHouseClient(body: """
        demo\tEURUSD\tEURUSD\t60\t180\t120\tbackfilling\tb\t1

        """)
        let store = ClickHouseCheckpointStore(
            client: client,
            insertBuilder: ClickHouseInsertBuilder(database: "db"),
            database: "db"
        )
        _ = try await store.latestState(brokerSourceId: try BrokerSourceId("demo"), logicalSymbol: try LogicalSymbol("EURUSD"))
        let query = await client.lastQuery()
        XCTAssertFalse(query.contains("FINAL"))
        XCTAssertTrue(query.contains("ORDER BY latest_ingested_closed_mt5_server_ts_raw DESC, updated_at_utc DESC"))
    }

    func testBackfillResumeReprocessesWhenMT5HistoryExpandsOlder() throws {
        let state = try ingestState(oldest: 1_000, latest: 2_000)
        let decision = try BackfillResumePolicy.decide(
            logicalSymbol: state.logicalSymbol,
            mt5Symbol: state.mt5Symbol,
            oldest: MT5ServerSecond(rawValue: 500),
            latestClosed: MT5ServerSecond(rawValue: 3_000),
            existingState: state
        )
        XCTAssertEqual(decision.cursor, MT5ServerSecond(rawValue: 500))
        XCTAssertEqual(decision.action, .reprocessExpandedOlderHistory)
    }

    func testBackfillResumeRejectsCheckpointAheadOfMT5() throws {
        let state = try ingestState(oldest: 1_000, latest: 4_000)
        XCTAssertThrowsError(try BackfillResumePolicy.decide(
            logicalSymbol: state.logicalSymbol,
            mt5Symbol: state.mt5Symbol,
            oldest: MT5ServerSecond(rawValue: 1_000),
            latestClosed: MT5ServerSecond(rawValue: 3_000),
            existingState: state
        )) { error in
            guard case IngestError.checkpointAheadOfMT5 = error else {
                XCTFail("Expected checkpointAheadOfMT5, got \(error)")
                return
            }
        }
    }

    func testRuntimeOffsetVerifierRoundsSnapshotOffset() throws {
        let snapshot = ServerTimeSnapshotDTO(timeTradeServer: 1_700_010_803, timeGMT: 1_700_000_000, timeLocal: 1)
        let observed = try BrokerOffsetRuntimeVerifier.observedOffset(from: snapshot)
        XCTAssertEqual(observed, OffsetSeconds(rawValue: 10_800))
    }

    func testRuntimeOffsetVerifierRejectsImpossibleSnapshotOffset() {
        let snapshot = ServerTimeSnapshotDTO(timeTradeServer: 1_700_200_000, timeGMT: 1_700_000_000, timeLocal: 1)
        XCTAssertThrowsError(try BrokerOffsetRuntimeVerifier.observedOffset(from: snapshot))
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

private func ingestState(oldest: Int64, latest: Int64) throws -> IngestState {
    IngestState(
        brokerSourceId: try BrokerSourceId("demo"),
        logicalSymbol: try LogicalSymbol("EURUSD"),
        mt5Symbol: try MT5Symbol("EURUSD"),
        oldestMT5ServerTime: MT5ServerSecond(rawValue: oldest),
        latestIngestedClosedMT5ServerTime: MT5ServerSecond(rawValue: latest),
        latestIngestedClosedUtcTime: UtcSecond(rawValue: latest - 60),
        status: .backfilling,
        lastBatchId: BatchId(rawValue: "b"),
        updatedAtUtc: UtcSecond(rawValue: 1)
    )
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
