import MT5Bridge
import XCTest

final class ProtocolTests: XCTestCase {
    func testFrameParserHandlesPartialReads() throws {
        let codec = FramedProtocolCodec()
        let frame = try codec.encode(command: .ping, requestId: "abc", timestampSentUtc: .init(rawValue: 1), payload: EmptyPayload())
        var parser = FrameParser()
        let first = try parser.append(frame.prefix(3))
        XCTAssertTrue(first.isEmpty)
        let second = try parser.append(frame.dropFirst(3))
        XCTAssertEqual(second.count, 1)
        let message = try codec.decode(second[0], payloadType: EmptyPayload.self)
        XCTAssertEqual(message.requestId, "abc")
    }

    func testChecksumValidationRejectsTampering() throws {
        let codec = FramedProtocolCodec()
        let frame = try codec.encode(command: .ping, requestId: "abc", timestampSentUtc: .init(rawValue: 1), payload: EmptyPayload())
        var parser = FrameParser()
        let bodies = try parser.append(frame)
        var body = bodies[0]
        let text = try XCTUnwrap(String(data: body, encoding: .utf8))
        let tampered = text.replacingOccurrences(of: "\"payload\":{}", with: "\"payload\":{\"x\":1}")
        body = try XCTUnwrap(tampered.data(using: .utf8))
        XCTAssertThrowsError(try codec.decode(body, payloadType: EmptyPayload.self))
    }

    func testFrameParserRejectsZeroLengthFrame() throws {
        var parser = FrameParser()
        XCTAssertThrowsError(try parser.append(Data([0, 0, 0, 0]))) { error in
            XCTAssertEqual(error as? ProtocolError, .malformedLengthPrefix)
        }
    }

    func testProtocolAcceptsInt64TimestampField() throws {
        let codec = FramedProtocolCodec()
        let frame = try codec.encode(command: .ping, requestId: "abc", timestampSentUtc: .init(rawValue: 3_000_000_000), payload: EmptyPayload())
        var parser = FrameParser()
        let bodies = try parser.append(frame)
        let message = try codec.decode(bodies[0], payloadType: EmptyPayload.self)
        XCTAssertEqual(message.timestampSentUtc.rawValue, 3_000_000_000)
    }

    func testProtocolRejectsBooleanNumericField() throws {
        let codec = FramedProtocolCodec()
        let frame = try codec.encode(command: .ping, requestId: "abc", timestampSentUtc: .init(rawValue: 1), payload: EmptyPayload())
        var parser = FrameParser()
        let bodies = try parser.append(frame)
        let text = try XCTUnwrap(String(data: bodies[0], encoding: .utf8))
        let tampered = text.replacingOccurrences(of: "\"timestamp_sent_utc\":1", with: "\"timestamp_sent_utc\":true")
        let body = try XCTUnwrap(tampered.data(using: .utf8))
        XCTAssertThrowsError(try codec.decode(body, payloadType: EmptyPayload.self)) { error in
            XCTAssertEqual(error as? ProtocolError, .invalidField("timestamp_sent_utc"))
        }
    }

    func testRatesFromPositionPayloadIsFramedWithStartPosition() throws {
        let codec = FramedProtocolCodec()
        let payload = RatesFromPositionPayload(mt5Symbol: "EURUSD", startPosition: 1, count: 2)
        let frame = try codec.encode(command: .getRatesFromPosition, requestId: "rates-pos", timestampSentUtc: .init(rawValue: 1), payload: payload)
        var parser = FrameParser()
        let bodies = try parser.append(frame)

        let message = try codec.decode(bodies[0], payloadType: RatesFromPositionPayload.self)

        XCTAssertEqual(message.command, .getRatesFromPosition)
        XCTAssertEqual(message.payload.mt5Symbol, "EURUSD")
        XCTAssertEqual(message.payload.startPosition, 1)
        XCTAssertEqual(message.payload.count, 2)
    }
}
