import Domain
import XCTest

final class DomainTests: XCTestCase {
    func testPriceScaledConversion() throws {
        let digits = try Digits(5)
        let price = try PriceScaled.fromDecimalString("1.08342", digits: digits)
        XCTAssertEqual(price.rawValue, 108342)
        XCTAssertEqual(price.description, "1.08342")
    }

    func testJPYPriceScaledConversion() throws {
        let digits = try Digits(3)
        let price = try PriceScaled.fromDecimalString("154.721", digits: digits)
        XCTAssertEqual(price.rawValue, 154721)
    }

    func testPriceScaledRejectsExcessPrecision() throws {
        XCTAssertThrowsError(try PriceScaled.fromDecimalString("1.083429", digits: try Digits(5)))
    }

    func testPriceScaledRejectsInt64OverflowAfterFractionIsAdded() throws {
        XCTAssertThrowsError(try PriceScaled.fromDecimalString("922337203685477580.8", digits: try Digits(1))) { error in
            XCTAssertEqual(error as? DomainError, .priceScaleOverflow("922337203685477580.8"))
        }
    }

    func testDigitsRejectsOutOfRangeValues() {
        XCTAssertThrowsError(try Digits(-1))
        XCTAssertThrowsError(try Digits(11))
    }

    func testTimestampTypeSeparation() {
        let mt5 = MT5ServerSecond(rawValue: 1_700_000_000)
        let utc = UtcSecond(rawValue: 1_700_000_000)
        XCTAssertEqual(mt5.rawValue, utc.rawValue)
    }

    func testRawRepresentableSymbolInitializersDoNotBypassValidation() {
        XCTAssertNil(LogicalSymbol(rawValue: "eurusd"))
        XCTAssertNil(MT5Symbol(rawValue: ""))
        XCTAssertNil(BrokerSourceId(rawValue: ""))
    }

    func testBrokerServerIdentityRejectsEmptyIdentityParts() throws {
        XCTAssertThrowsError(try BrokerServerIdentity(company: "", server: "Broker-Demo", accountLogin: 1))
        XCTAssertThrowsError(try BrokerServerIdentity(company: "Broker", server: "", accountLogin: 1))
        XCTAssertThrowsError(try BrokerServerIdentity(company: "Broker", server: "Broker-Demo", accountLogin: 0))
    }

    func testBarHashDeterminism() throws {
        let broker = try BrokerSourceId("demo")
        let symbol = try LogicalSymbol("EURUSD")
        let mt5Symbol = try MT5Symbol("EURUSD")
        let digits = try Digits(5)
        let open = try PriceScaled.fromDecimalString("1.10000", digits: digits)
        let hash1 = BarHash.compute(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            mt5Symbol: mt5Symbol,
            timeframe: .m1,
            utcTime: UtcSecond(rawValue: 1_700_000_000),
            mt5ServerTime: MT5ServerSecond(rawValue: 1_700_007_200),
            open: open,
            high: open,
            low: open,
            close: open,
            digits: digits
        )
        let hash2 = BarHash.compute(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            mt5Symbol: mt5Symbol,
            timeframe: .m1,
            utcTime: UtcSecond(rawValue: 1_700_000_000),
            mt5ServerTime: MT5ServerSecond(rawValue: 1_700_007_200),
            open: open,
            high: open,
            low: open,
            close: open,
            digits: digits
        )
        XCTAssertEqual(hash1, hash2)

        let suffixHash = BarHash.compute(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            mt5Symbol: try MT5Symbol("EURUSD.a"),
            timeframe: .m1,
            utcTime: UtcSecond(rawValue: 1_700_000_000),
            mt5ServerTime: MT5ServerSecond(rawValue: 1_700_007_200),
            open: open,
            high: open,
            low: open,
            close: open,
            digits: digits
        )
        XCTAssertNotEqual(hash1, suffixHash)
    }
}
