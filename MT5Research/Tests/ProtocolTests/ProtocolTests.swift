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
}
