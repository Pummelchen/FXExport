import ClickHouse
import Domain
import Foundation
import Ingestion
import MT5Bridge
import TimeMapping
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

    func testCanonicalInsertVerifierComparesReadbackHashes() async throws {
        let bar = try validatedBar(mt5: 120, utc: 60)
        let client = SequenceClickHouseClient(bodies: [
            "1\t1\t1\t1\t1\t1\t0\n",
            "EURUSD\tM1\t120\t60\t60\tmanual\tverified\t110000\t110000\t110000\t110000\t5\t\(bar.barHash.description)\n"
        ])

        let result = try await CanonicalInsertVerifier(
            clickHouse: client,
            insertBuilder: ClickHouseInsertBuilder(database: "db")
        ).verify([bar])
        XCTAssertEqual(result.rowCount, 1)
        XCTAssertEqual(result.expectedCanonicalSHA256, result.canonicalReadbackSHA256)
    }

    func testCanonicalInsertVerifierRejectsHashMismatch() async throws {
        let bar = try validatedBar(mt5: 120, utc: 60)
        let client = SequenceClickHouseClient(bodies: [
            "1\t1\t1\t1\t1\t1\t0\n",
            "EURUSD\tM1\t120\t60\t60\tmanual\tverified\t110000\t110000\t110000\t110000\t5\tbad-hash\n"
        ])

        await XCTAssertThrowsErrorAsync(try await CanonicalInsertVerifier(
            clickHouse: client,
            insertBuilder: ClickHouseInsertBuilder(database: "db")
        ).verify([bar]))
    }

    func testCanonicalInsertVerifierRejectsMetadataMismatch() async throws {
        let bar = try validatedBar(mt5: 120, utc: 60)
        let client = SequenceClickHouseClient(bodies: [
            "1\t1\t1\t1\t1\t1\t0\n",
            "EURUSD.a\tM1\t120\t60\t60\tmanual\tverified\t110000\t110000\t110000\t110000\t5\t\(bar.barHash.description)\n"
        ])

        await XCTAssertThrowsErrorAsync(try await CanonicalInsertVerifier(
            clickHouse: client,
            insertBuilder: ClickHouseInsertBuilder(database: "db")
        ).verify([bar]))
    }

    func testCanonicalInsertVerifierRejectsStaleRowsInsideEmptyMT5Range() async throws {
        let bar = try validatedBar(mt5: 120, utc: 60)
        let client = SequenceClickHouseClient(bodies: [
            "EURUSD\tM1\t120\t60\t60\tmanual\tverified\t110000\t110000\t110000\t110000\t5\t\(bar.barHash.description)\n"
        ])

        await XCTAssertThrowsErrorAsync(try await CanonicalInsertVerifier(
            clickHouse: client,
            insertBuilder: ClickHouseInsertBuilder(database: "db")
        ).verifyEmptyMT5Range(
            brokerSourceId: bar.brokerSourceId,
            logicalSymbol: bar.logicalSymbol,
            mt5Symbol: bar.mt5Symbol,
            mt5Start: MT5ServerSecond(rawValue: 120),
            mt5EndExclusive: MT5ServerSecond(rawValue: 180)
        )) { error in
            XCTAssertTrue(String(describing: error).contains("not empty in canonical storage"))
        }
    }

    func testCanonicalConflictRecorderWritesConflictBeforeReplace() async throws {
        let bar = try validatedBar(mt5: 120, utc: 60)
        let client = SequenceClickHouseClient(bodies: [
            "60\toldhash\t109000\t110000\t108000\t109500\n",
            ""
        ])
        try await CanonicalConflictRecorder(
            clickHouse: client,
            insertBuilder: ClickHouseInsertBuilder(database: "db")
        ).recordConflictsBeforeCanonicalReplace([bar], detectedAtUtc: UtcSecond(rawValue: 999))

        let queries = await client.allQueries()
        XCTAssertEqual(queries.count, 2)
        XCTAssertTrue(queries[0].contains("FROM db.ohlc_m1_canonical"))
        XCTAssertTrue(queries[1].contains("INSERT INTO db.ohlc_m1_conflicts"))
        XCTAssertTrue(queries[1].contains("oldhash"))
        XCTAssertTrue(queries[1].contains(bar.barHash.description))
        XCTAssertTrue(queries[1].contains("\t999\tbatch"))
    }

    func testMT5SourceRangeVerifierAcceptsStableManifestedRange() async throws {
        let response = try ratesResponse(
            from: 120,
            toExclusive: 240,
            emitted: [
                (120, "1.10000"),
                (180, "1.10010")
            ]
        )
        let verifier = MT5SourceRangeVerifier(confirmationDelayNanoseconds: 0, retryDelayNanoseconds: 0)
        var responses = [response, response]

        let stable = try await verifier.fetchStableRange(
            mt5Symbol: try MT5Symbol("EURUSD"),
            from: MT5ServerSecond(rawValue: 120),
            toExclusive: MT5ServerSecond(rawValue: 240),
            maxBars: 2
        ) {
            responses.removeFirst()
        }

        XCTAssertEqual(stable.manifest.emittedCount, 2)
        XCTAssertEqual(stable.response.rates.count, 2)
        XCTAssertTrue(stable.sourceHash.hasPrefix("fnv64:"))
        XCTAssertEqual(stable.sourceSHA256.rawValue.count, 64)
    }

    func testMT5SourceRangeVerifierRejectsUnstableConfirmationRead() async throws {
        let first = try ratesResponse(
            from: 120,
            toExclusive: 240,
            emitted: [(120, "1.10000")]
        )
        let second = try ratesResponse(
            from: 120,
            toExclusive: 240,
            emitted: [(120, "1.10001")]
        )
        let verifier = MT5SourceRangeVerifier(maxAttempts: 1, confirmationDelayNanoseconds: 0, retryDelayNanoseconds: 0)
        var responses = [first, second]

        await XCTAssertThrowsErrorAsync(try await verifier.fetchStableRange(
            mt5Symbol: try MT5Symbol("EURUSD"),
            from: MT5ServerSecond(rawValue: 120),
            toExclusive: MT5ServerSecond(rawValue: 240),
            maxBars: 2,
            request: {
            responses.removeFirst()
            }
        )) { error in
            XCTAssertTrue(String(describing: error).contains("not stable"))
        }
    }

    func testMT5SourceRangeVerifierRejectsMissingManifest() throws {
        let data = """
        {"mt5_symbol":"EURUSD","timeframe":"M1","rates":[]}
        """.data(using: .utf8)!
        let response = try JSONDecoder().decode(RatesResponseDTO.self, from: data)

        XCTAssertThrowsError(try MT5SourceRangeVerifier().validate(
            response,
            expectedMT5Symbol: try MT5Symbol("EURUSD"),
            from: MT5ServerSecond(rawValue: 120),
            toExclusive: MT5ServerSecond(rawValue: 240),
            maxBars: 2
        ))
    }

    func testCoverageBuilderSplitsAtBrokerOffsetBoundary() throws {
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker Ltd", server: "Broker-Server", accountLogin: 1)
        let map = try BrokerOffsetMap(
            brokerSourceId: broker,
            terminalIdentity: identity,
            segments: [
                BrokerOffsetSegment(
                    brokerSourceId: broker,
                    terminalIdentity: identity,
                    validFrom: MT5ServerSecond(rawValue: 0),
                    validTo: MT5ServerSecond(rawValue: 180),
                    offset: OffsetSeconds(rawValue: 60),
                    source: .manual,
                    confidence: .verified
                ),
                BrokerOffsetSegment(
                    brokerSourceId: broker,
                    terminalIdentity: identity,
                    validFrom: MT5ServerSecond(rawValue: 180),
                    validTo: MT5ServerSecond(rawValue: 360),
                    offset: OffsetSeconds(rawValue: 120),
                    source: .manual,
                    confidence: .verified
                )
            ]
        )
        let rates = try ratesResponse(
            from: 120,
            toExclusive: 300,
            emitted: [(120, "1.10000"), (180, "1.10010"), (240, "1.10020")]
        ).rates
        let records = try CoverageRangeBuilder(offsetMap: map).makeRecords(
            brokerSourceId: broker,
            logicalSymbol: try LogicalSymbol("EURUSD"),
            mt5Symbol: try MT5Symbol("EURUSD"),
            mt5Start: MT5ServerSecond(rawValue: 120),
            mt5EndExclusive: MT5ServerSecond(rawValue: 300),
            sourceBars: rates,
            canonicalBars: [],
            sourceHash: "fnv64:test",
            mt5SourceSHA256: ChunkHashing.emptySHA256(namespace: "test_mt5"),
            canonicalReadbackSHA256: ChunkHashing.emptySHA256(namespace: "test_canonical"),
            offsetAuthoritySHA256: map.authoritySHA256(),
            verificationMethod: "test",
            batchId: BatchId(rawValue: "batch"),
            verifiedAtUtc: UtcSecond(rawValue: 1)
        )

        XCTAssertEqual(records.count, 2)
        XCTAssertEqual(records[0].mt5Start.rawValue, 120)
        XCTAssertEqual(records[0].mt5EndExclusive.rawValue, 180)
        XCTAssertEqual(records[0].utcStart.rawValue, 60)
        XCTAssertEqual(records[0].utcEndExclusive.rawValue, 120)
        XCTAssertEqual(records[1].mt5Start.rawValue, 180)
        XCTAssertEqual(records[1].mt5EndExclusive.rawValue, 300)
        XCTAssertEqual(records[1].utcStart.rawValue, 60)
        XCTAssertEqual(records[1].utcEndExclusive.rawValue, 180)
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

private actor SequenceClickHouseClient: ClickHouseClientProtocol {
    private var bodies: [String]
    private var queries: [String] = []

    init(bodies: [String]) {
        self.bodies = bodies
    }

    func execute(_ query: ClickHouseQuery) async throws -> String {
        queries.append(query.sql)
        if bodies.isEmpty {
            return ""
        }
        return bodies.removeFirst()
    }

    func allQueries() -> [String] {
        queries
    }
}

private func validatedBar(mt5: Int64, utc: Int64) throws -> ValidatedBar {
    let broker = try BrokerSourceId("demo")
    let logical = try LogicalSymbol("EURUSD")
    let mt5Symbol = try MT5Symbol("EURUSD")
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
        offsetSource: .manual,
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

private func ratesResponse(from: Int64, toExclusive: Int64, emitted: [(Int64, String)]) throws -> RatesResponseDTO {
    let rates = emitted.map { timestamp, price in
        """
        {"mt5_server_time":\(timestamp),"open":"\(price)","high":"\(price)","low":"\(price)","close":"\(price)"}
        """
    }.joined(separator: ",")
    let first = emitted.first?.0 ?? 0
    let last = emitted.last?.0 ?? 0
    let json = """
    {
      "mt5_symbol":"EURUSD",
      "timeframe":"M1",
      "requested_from_mt5_server_ts":\(from),
      "requested_to_mt5_server_ts_exclusive":\(toExclusive),
      "effective_to_mt5_server_ts_exclusive":\(toExclusive),
      "latest_closed_mt5_server_ts":\(toExclusive - 60),
      "series_synchronized":true,
      "copied_count":\(emitted.count),
      "emitted_count":\(emitted.count),
      "first_mt5_server_ts":\(first),
      "last_mt5_server_ts":\(last),
      "rates":[\(rates)]
    }
    """
    let data = try XCTUnwrap(json.data(using: .utf8))
    return try JSONDecoder().decode(RatesResponseDTO.self, from: data)
}

private func XCTAssertThrowsErrorAsync<T>(
    _ expression: @autoclosure () async throws -> T,
    _ validation: (Error) -> Void = { _ in },
    file: StaticString = #filePath,
    line: UInt = #line
) async {
    do {
        _ = try await expression()
        XCTFail("Expected async expression to throw", file: file, line: line)
    } catch {
        validation(error)
    }
}
