import AppCore
import ClickHouse
import Config
import Domain
import TimeMapping
import Verification
import XCTest

final class VerificationTests: XCTestCase {
    func testRepairDecisionLogic() {
        let policy = RepairPolicy()
        XCTAssertEqual(
            policy.decide(
                verification: VerificationResult(isClean: true, mismatches: []),
                mt5Available: true,
                utcMappingAmbiguous: false
            ),
            .noRepairNeeded
        )
        XCTAssertEqual(
            policy.decide(
                verification: VerificationResult(isClean: false, mismatches: [.rowCount(mt5: 1, database: 0)]),
                mt5Available: false,
                utcMappingAmbiguous: false
            ),
            .refuse(reason: "MT5 source data is unavailable")
        )
    }

    func testVerificationComparatorUsesStoredDatabaseHash() throws {
        let broker = try BrokerSourceId("demo")
        let symbol = try LogicalSymbol("EURUSD")
        let mt5Symbol = try MT5Symbol("EURUSD")
        let digits = try Digits(5)
        let mt5 = VerificationBar(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            mt5Symbol: mt5Symbol,
            mt5ServerTime: MT5ServerSecond(rawValue: 7_260),
            utcTime: UtcSecond(rawValue: 60),
            open: PriceScaled(rawValue: 100_000, digits: digits),
            high: PriceScaled(rawValue: 100_010, digits: digits),
            low: PriceScaled(rawValue: 99_990, digits: digits),
            close: PriceScaled(rawValue: 100_001, digits: digits),
            digits: digits,
            barHash: BarHash(rawValue: 1)
        )
        let database = VerificationBar(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            mt5Symbol: mt5Symbol,
            mt5ServerTime: mt5.mt5ServerTime,
            utcTime: mt5.utcTime,
            open: mt5.open,
            high: mt5.high,
            low: mt5.low,
            close: mt5.close,
            digits: digits,
            barHash: BarHash(rawValue: 2)
        )

        let result = VerificationComparator().compare(mt5SourceBars: [mt5], databaseBars: [database])

        XCTAssertFalse(result.isClean)
        XCTAssertEqual(result.mismatches, [.hash(index: 0, mt5: BarHash(rawValue: 1), database: BarHash(rawValue: 2))])
    }

    func testVerificationComparatorRejectsNonVerifiedCanonicalOffset() throws {
        let broker = try BrokerSourceId("demo")
        let symbol = try LogicalSymbol("EURUSD")
        let mt5Symbol = try MT5Symbol("EURUSD")
        let digits = try Digits(5)
        let mt5 = VerificationBar(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            mt5Symbol: mt5Symbol,
            mt5ServerTime: MT5ServerSecond(rawValue: 7_260),
            utcTime: UtcSecond(rawValue: 60),
            open: PriceScaled(rawValue: 100_000, digits: digits),
            high: PriceScaled(rawValue: 100_010, digits: digits),
            low: PriceScaled(rawValue: 99_990, digits: digits),
            close: PriceScaled(rawValue: 100_001, digits: digits),
            digits: digits,
            barHash: BarHash(rawValue: 1)
        )
        let database = VerificationBar(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            mt5Symbol: mt5Symbol,
            mt5ServerTime: mt5.mt5ServerTime,
            utcTime: mt5.utcTime,
            open: mt5.open,
            high: mt5.high,
            low: mt5.low,
            close: mt5.close,
            digits: digits,
            offsetConfidence: .inferred,
            barHash: mt5.barHash
        )

        let result = VerificationComparator().compare(mt5SourceBars: [mt5], databaseBars: [database])

        XCTAssertFalse(result.isClean)
        XCTAssertEqual(result.mismatches, [.offsetConfidence(index: 0, database: .inferred)])
    }

    func testRepairRangePlannerMapsUtcRangeThroughVerifiedOffsetSegments() throws {
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker", server: "Server", accountLogin: 1)
        let map = try BrokerOffsetMap(
            brokerSourceId: broker,
            terminalIdentity: identity,
            segments: [
                BrokerOffsetSegment(
                    brokerSourceId: broker,
                    terminalIdentity: identity,
                    validFrom: MT5ServerSecond(rawValue: 7_200),
                    validTo: MT5ServerSecond(rawValue: 10_800),
                    offset: OffsetSeconds(rawValue: 7_200),
                    source: .manual,
                    confidence: .verified
                )
            ]
        )

        let ranges = try RepairRangePlanner().mt5Ranges(
            brokerSourceId: broker,
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStart: UtcSecond(rawValue: 0),
            utcEndExclusive: UtcSecond(rawValue: 3_600),
            offsetMap: map
        )

        XCTAssertEqual(ranges.count, 1)
        XCTAssertEqual(ranges[0].mt5Start, MT5ServerSecond(rawValue: 7_200))
        XCTAssertEqual(ranges[0].mt5EndExclusive, MT5ServerSecond(rawValue: 10_800))
    }

    func testRepairRangePlannerAcceptsRangeInsideExistingSegment() throws {
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker", server: "Server", accountLogin: 1)
        let map = try BrokerOffsetMap(
            brokerSourceId: broker,
            terminalIdentity: identity,
            segments: [
                BrokerOffsetSegment(
                    brokerSourceId: broker,
                    terminalIdentity: identity,
                    validFrom: MT5ServerSecond(rawValue: 7_200),
                    validTo: MT5ServerSecond(rawValue: 10_800),
                    offset: OffsetSeconds(rawValue: 7_200),
                    source: .manual,
                    confidence: .verified
                )
            ]
        )

        let ranges = try RepairRangePlanner().mt5Ranges(
            brokerSourceId: broker,
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStart: UtcSecond(rawValue: 60),
            utcEndExclusive: UtcSecond(rawValue: 120),
            offsetMap: map
        )

        XCTAssertEqual(ranges.count, 1)
        XCTAssertEqual(ranges[0].mt5Start, MT5ServerSecond(rawValue: 7_260))
        XCTAssertEqual(ranges[0].mt5EndExclusive, MT5ServerSecond(rawValue: 7_320))
    }

    func testRepairRangePlannerRejectsUncoveredUtcRange() throws {
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker", server: "Server", accountLogin: 1)
        let map = try BrokerOffsetMap(
            brokerSourceId: broker,
            terminalIdentity: identity,
            segments: [
                BrokerOffsetSegment(
                    brokerSourceId: broker,
                    terminalIdentity: identity,
                    validFrom: MT5ServerSecond(rawValue: 7_200),
                    validTo: MT5ServerSecond(rawValue: 10_800),
                    offset: OffsetSeconds(rawValue: 7_200),
                    source: .manual,
                    confidence: .verified
                )
            ]
        )

        XCTAssertThrowsError(try RepairRangePlanner().mt5Ranges(
            brokerSourceId: broker,
            logicalSymbol: try LogicalSymbol("EURUSD"),
            utcStart: UtcSecond(rawValue: 0),
            utcEndExclusive: UtcSecond(rawValue: 7_200),
            offsetMap: map
        )) { error in
            guard case RepairRangePlannerError.missingVerifiedOffsetCoverage = error else {
                XCTFail("Expected missingVerifiedOffsetCoverage, got \(error)")
                return
            }
        }
    }

    func testStartupDatabaseChecksAreScopedToConfiguredBroker() async throws {
        let config = try makeVerificationConfig()
        let clickHouse = StartupVerificationClickHouse()
        let agent = VerificationAgent(
            config: config,
            bridge: nil,
            clickHouse: clickHouse,
            logger: Logger(level: .quiet)
        )

        try await agent.startupChecks(randomRanges: 0)

        let queries = await clickHouse.queries
        XCTAssertEqual(queries.count, 3)
        for query in queries {
            XCTAssertTrue(query.sql.contains("broker_source_id = 'demo'"), query.sql)
        }
    }
}

private actor StartupVerificationClickHouse: ClickHouseClientProtocol {
    private(set) var queries: [ClickHouseQuery] = []

    func execute(_ query: ClickHouseQuery) async throws -> String {
        queries.append(query)
        return query.sql.contains("SELECT broker_source_id") ? "" : "0\n"
    }
}

private func makeVerificationConfig() throws -> ConfigBundle {
    ConfigBundle(
        app: AppConfigFile(
            chunkSize: 50_000,
            liveScanIntervalSeconds: 10,
            logLevel: .normal,
            strictSymbolFailures: false,
            verifierRandomRanges: 0
        ),
        clickHouse: ClickHouseConfig(
            url: try XCTUnwrap(URL(string: "http://localhost:8123")),
            database: "db",
            username: nil,
            password: nil,
            requestTimeoutSeconds: 10,
            retryCount: 0
        ),
        mt5Bridge: MT5BridgeConfig(
            mode: .listen,
            host: "127.0.0.1",
            port: 5055,
            connectTimeoutSeconds: 10,
            requestTimeoutSeconds: 10
        ),
        brokerTime: BrokerTimeConfig(
            brokerSourceId: try BrokerSourceId("demo"),
            offsetSegments: []
        ),
        symbols: SymbolConfig(symbols: [
            SymbolMapping(logicalSymbol: try LogicalSymbol("EURUSD"), mt5Symbol: try MT5Symbol("EURUSD"), digits: try Digits(5))
        ])
    )
}
