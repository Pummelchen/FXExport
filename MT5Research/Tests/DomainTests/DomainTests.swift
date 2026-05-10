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

    func testTimestampTypeSeparation() {
        let mt5 = MT5ServerSecond(rawValue: 1_700_000_000)
        let utc = UtcSecond(rawValue: 1_700_000_000)
        XCTAssertEqual(mt5.rawValue, utc.rawValue)
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
