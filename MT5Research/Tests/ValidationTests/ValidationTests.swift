import Config
import Domain
import TimeMapping
import Validation
import XCTest

final class ValidationTests: XCTestCase {
    func testOHLCValidation() throws {
        let fixture = try ValidationFixture()
        let bar = try fixture.bar(open: "1.10000", high: "1.10100", low: "1.09900", close: "1.10050")
        let validated = try fixture.validator.validateBatch([bar], context: fixture.context)
        XCTAssertEqual(validated.count, 1)
        XCTAssertEqual(validated[0].utcTime.rawValue, bar.mt5ServerTime.rawValue - 7200)
    }

    func testOHLCInvariantFailure() throws {
        let fixture = try ValidationFixture()
        let bar = try fixture.bar(open: "1.10000", high: "1.09900", low: "1.09800", close: "1.10050")
        XCTAssertThrowsError(try fixture.validator.validateBatch([bar], context: fixture.context))
    }

    func testLatestClosedBarExclusion() throws {
        let fixture = try ValidationFixture(latestClosed: MT5ServerSecond(rawValue: 1_700_007_240))
        let openBar = try fixture.bar(time: MT5ServerSecond(rawValue: 1_700_007_300))
        XCTAssertThrowsError(try fixture.validator.validateBatch([openBar], context: fixture.context))
    }

    func testDuplicateDetection() throws {
        let fixture = try ValidationFixture()
        let bar = try fixture.bar()
        XCTAssertThrowsError(try fixture.validator.validateBatch([bar, bar], context: fixture.context))
    }

    func testInferredOffsetIsRejectedForCanonicalValidation() throws {
        let fixture = try ValidationFixture(offsetConfidence: .inferred)
        let bar = try fixture.bar()
        XCTAssertThrowsError(try fixture.validator.validateBatch([bar], context: fixture.context)) { error in
            guard case ValidationError.unverifiedUTCOffset = error else {
                XCTFail("Expected unverified UTC offset error, got \(error)")
                return
            }
        }
    }

    func testUTCSequenceMustStayStrictlyIncreasingAcrossOffsetSegments() throws {
        let fixture = try ValidationFixture(offsetSegments: [
            BrokerTimeOffsetConfig(
                validFromMT5ServerTs: MT5ServerSecond(rawValue: 0),
                validToMT5ServerTs: MT5ServerSecond(rawValue: 1_700_007_300),
                offsetSeconds: OffsetSeconds(rawValue: 0),
                source: .configured,
                confidence: .verified
            ),
            BrokerTimeOffsetConfig(
                validFromMT5ServerTs: MT5ServerSecond(rawValue: 1_700_007_300),
                validToMT5ServerTs: MT5ServerSecond(rawValue: 2_000_000_000),
                offsetSeconds: OffsetSeconds(rawValue: 3600),
                source: .configured,
                confidence: .verified
            )
        ])
        let first = try fixture.bar(time: MT5ServerSecond(rawValue: 1_700_007_240))
        let second = try fixture.bar(time: MT5ServerSecond(rawValue: 1_700_007_300))
        XCTAssertThrowsError(try fixture.validator.validateBatch([first, second], context: fixture.context)) { error in
            guard case ValidationError.unsortedUTC = error else {
                XCTFail("Expected unsorted UTC error, got \(error)")
                return
            }
        }
    }
}

private struct ValidationFixture {
    let broker: BrokerSourceId
    let logical: LogicalSymbol
    let mt5: MT5Symbol
    let digits: Digits
    let validator: OhlcValidator
    let context: OhlcValidationContext

    init(
        latestClosed: MT5ServerSecond = MT5ServerSecond(rawValue: 1_700_007_300),
        offsetConfidence: OffsetConfidence = .verified,
        offsetSegments: [BrokerTimeOffsetConfig]? = nil
    ) throws {
        let broker = try BrokerSourceId("demo")
        let logical = try LogicalSymbol("EURUSD")
        let mt5 = try MT5Symbol("EURUSD")
        let digits = try Digits(5)
        self.broker = broker
        self.logical = logical
        self.mt5 = mt5
        self.digits = digits
        let config = BrokerTimeConfig(
            brokerSourceId: broker,
            offsetSegments: offsetSegments ?? [
                BrokerTimeOffsetConfig(
                    validFromMT5ServerTs: MT5ServerSecond(rawValue: 0),
                    validToMT5ServerTs: MT5ServerSecond(rawValue: 2_000_000_000),
                    offsetSeconds: OffsetSeconds(rawValue: 7200),
                    source: .configured,
                    confidence: offsetConfidence
                )
            ]
        )
        self.validator = OhlcValidator(timeConverter: TimeConverter(offsetMap: BrokerOffsetMap(config: config)))
        self.context = OhlcValidationContext(
            brokerSourceId: broker,
            expectedLogicalSymbol: logical,
            expectedMT5Symbol: mt5,
            expectedDigits: digits,
            latestClosedMT5ServerTime: latestClosed,
            batchId: BatchId(rawValue: "batch"),
            ingestedAtUtc: UtcSecond(rawValue: 1_700_000_000)
        )
    }

    func bar(
        time: MT5ServerSecond = MT5ServerSecond(rawValue: 1_700_007_240),
        open: String = "1.10000",
        high: String = "1.10100",
        low: String = "1.09900",
        close: String = "1.10050"
    ) throws -> ClosedM1Bar {
        ClosedM1Bar(
            logicalSymbol: logical,
            mt5Symbol: mt5,
            timeframe: .m1,
            mt5ServerTime: time,
            open: try PriceScaled.fromDecimalString(open, digits: digits),
            high: try PriceScaled.fromDecimalString(high, digits: digits),
            low: try PriceScaled.fromDecimalString(low, digits: digits),
            close: try PriceScaled.fromDecimalString(close, digits: digits),
            digits: digits
        )
    }
}
