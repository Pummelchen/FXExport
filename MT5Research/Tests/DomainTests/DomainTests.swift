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
        let digits = try Digits(5)
        let open = try PriceScaled.fromDecimalString("1.10000", digits: digits)
        let hash1 = BarHash.compute(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            utcTime: UtcSecond(rawValue: 1_700_000_000),
            mt5ServerTime: MT5ServerSecond(rawValue: 1_700_007_200),
            open: open,
            high: open,
            low: open,
            close: open
        )
        let hash2 = BarHash.compute(
            brokerSourceId: broker,
            logicalSymbol: symbol,
            utcTime: UtcSecond(rawValue: 1_700_000_000),
            mt5ServerTime: MT5ServerSecond(rawValue: 1_700_007_200),
            open: open,
            high: open,
            low: open,
            close: open
        )
        XCTAssertEqual(hash1, hash2)
    }
}
