import Config
import Domain
import TimeMapping
import XCTest

final class TimeMappingTests: XCTestCase {
    func testUTCOffsetConversion() throws {
        let config = BrokerTimeConfig(
            brokerSourceId: try BrokerSourceId("demo"),
            offsetSegments: [
                BrokerTimeOffsetConfig(
                    validFromMT5ServerTs: MT5ServerSecond(rawValue: 0),
                    validToMT5ServerTs: MT5ServerSecond(rawValue: 2_000_000_000),
                    offsetSeconds: OffsetSeconds(rawValue: 7200),
                    source: .configured,
                    confidence: .verified
                )
            ]
        )
        let converter = TimeConverter(offsetMap: BrokerOffsetMap(config: config))
        let result = try converter.convert(mt5ServerTime: MT5ServerSecond(rawValue: 1_700_007_240))
        XCTAssertEqual(result.utcTime, UtcSecond(rawValue: 1_700_000_040))
    }

    func testMissingOffsetThrows() throws {
        let config = BrokerTimeConfig(brokerSourceId: try BrokerSourceId("demo"), offsetSegments: [])
        let converter = TimeConverter(offsetMap: BrokerOffsetMap(config: config))
        XCTAssertThrowsError(try converter.convert(mt5ServerTime: MT5ServerSecond(rawValue: 1_700_000_000)))
    }
}
