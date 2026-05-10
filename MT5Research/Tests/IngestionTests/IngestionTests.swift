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
}
