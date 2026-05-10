import Domain
import TimeMapping
import XCTest

final class TimeMappingTests: XCTestCase {
    func testUTCOffsetConversion() throws {
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker", server: "Broker-Demo", accountLogin: 1)
        let converter = try TimeConverter(offsetMap: BrokerOffsetMap(
            brokerSourceId: broker,
            terminalIdentity: identity,
            segments: [
                BrokerOffsetSegment(
                    brokerSourceId: broker,
                    terminalIdentity: identity,
                    validFrom: MT5ServerSecond(rawValue: 0),
                    validTo: MT5ServerSecond(rawValue: 2_000_000_040),
                    offset: OffsetSeconds(rawValue: 7200),
                    source: .configured,
                    confidence: .verified
                )
            ]
        ))
        let result = try converter.convert(mt5ServerTime: MT5ServerSecond(rawValue: 1_700_007_240))
        XCTAssertEqual(result.utcTime, UtcSecond(rawValue: 1_700_000_040))
    }

    func testEmptyVerifiedOffsetAuthorityThrows() throws {
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker", server: "Broker-Demo", accountLogin: 1)
        XCTAssertThrowsError(try BrokerOffsetMap(brokerSourceId: broker, terminalIdentity: identity, segments: []))
    }

    func testCanonicalAuthorityRejectsInferredOffsets() throws {
        let broker = try BrokerSourceId("demo")
        let identity = try BrokerServerIdentity(company: "Broker", server: "Broker-Demo", accountLogin: 1)
        let inferred = BrokerOffsetSegment(
            brokerSourceId: broker,
            terminalIdentity: identity,
            validFrom: MT5ServerSecond(rawValue: 0),
            validTo: MT5ServerSecond(rawValue: 2_000_000_040),
            offset: OffsetSeconds(rawValue: 7200),
            source: .inferred,
            confidence: .inferred
        )
        XCTAssertThrowsError(try BrokerOffsetMap(
            brokerSourceId: broker,
            terminalIdentity: identity,
            segments: [inferred],
            requireVerified: true
        ))
    }
}
