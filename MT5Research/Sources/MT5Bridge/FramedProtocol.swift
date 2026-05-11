import Domain
import Foundation

public enum ProtocolError: Error, Equatable, CustomStringConvertible, Sendable {
    case frameTooLarge(Int)
    case incompleteFrame
    case malformedLengthPrefix
    case invalidUTF8
    case malformedJSON(String)
    case missingField(String)
    case invalidField(String)
    case unsupportedSchemaVersion(Int)
    case unknownCommand(String)
    case payloadNotObject
    case payloadLengthMismatch(expected: Int, actual: Int)
    case payloadChecksumMismatch(expected: String, actual: String)
    case bridgeError(code: String, message: String)

    public var description: String {
        switch self {
        case .frameTooLarge(let size):
            return "Protocol frame too large: \(size) bytes."
        case .incompleteFrame:
            return "Protocol frame is incomplete."
        case .malformedLengthPrefix:
            return "Protocol frame has an invalid length prefix."
        case .invalidUTF8:
            return "Protocol frame is not valid UTF-8."
        case .malformedJSON(let reason):
            return "Protocol frame JSON is malformed: \(reason)"
        case .missingField(let field):
            return "Protocol frame is missing field '\(field)'."
        case .invalidField(let field):
            return "Protocol frame field '\(field)' is invalid."
        case .unsupportedSchemaVersion(let version):
            return "Unsupported protocol schema version \(version)."
        case .unknownCommand(let command):
            return "Unknown protocol command '\(command)'."
        case .payloadNotObject:
            return "Protocol payload must be a JSON object."
        case .payloadLengthMismatch(let expected, let actual):
            return "Payload length mismatch. Expected \(expected), got \(actual)."
        case .payloadChecksumMismatch(let expected, let actual):
            return "Payload checksum mismatch. Expected \(expected), got \(actual)."
        case .bridgeError(let code, let message):
            return "MT5 bridge returned \(code): \(message)"
        }
    }
}

public struct ProtocolMessage<Payload: Decodable & Sendable>: Sendable {
    public let schemaVersion: Int
    public let requestId: String
    public let command: MT5Command
    public let timestampSentUtc: UtcSecond
    public let payload: Payload

    public init(schemaVersion: Int, requestId: String, command: MT5Command, timestampSentUtc: UtcSecond, payload: Payload) {
        self.schemaVersion = schemaVersion
        self.requestId = requestId
        self.command = command
        self.timestampSentUtc = timestampSentUtc
        self.payload = payload
    }
}

public struct FramedProtocolCodec: Sendable {
    public static let schemaVersion = 1
    public let maxFrameBytes: Int

    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    public init(maxFrameBytes: Int = 16 * 1024 * 1024) {
        self.maxFrameBytes = maxFrameBytes
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        self.encoder = encoder
        self.decoder = JSONDecoder()
    }

    public func encode<Payload: Encodable>(
        command: MT5Command,
        requestId: String,
        timestampSentUtc: UtcSecond,
        payload: Payload,
        errorCode: String? = nil,
        errorMessage: String? = nil
    ) throws -> Data {
        let payloadData = try encoder.encode(payload)
        guard let payloadJSON = String(data: payloadData, encoding: .utf8) else { throw ProtocolError.invalidUTF8 }
        let checksum = Self.payloadChecksum(payloadData)
        let errorCodeJSON = errorCode.map { "\"\(Self.escapeJSONString($0))\"" } ?? "null"
        let errorMessageJSON = errorMessage.map { "\"\(Self.escapeJSONString($0))\"" } ?? "null"
        let envelope = """
        {"schema_version":\(Self.schemaVersion),"request_id":"\(Self.escapeJSONString(requestId))","command":"\(command.rawValue)","timestamp_sent_utc":\(timestampSentUtc.rawValue),"payload_length":\(payloadData.count),"payload_checksum":"\(checksum)","payload":\(payloadJSON),"error_code":\(errorCodeJSON),"error_message":\(errorMessageJSON)}
        """
        guard let envelopeData = envelope.data(using: .utf8) else { throw ProtocolError.invalidUTF8 }
        guard envelopeData.count <= maxFrameBytes else { throw ProtocolError.frameTooLarge(envelopeData.count) }
        return Self.withLengthPrefix(envelopeData)
    }

    public func decode<Payload: Decodable & Sendable>(_ frameBody: Data, payloadType: Payload.Type) throws -> ProtocolMessage<Payload> {
        guard frameBody.count <= maxFrameBytes else { throw ProtocolError.frameTooLarge(frameBody.count) }
        guard let jsonText = String(data: frameBody, encoding: .utf8) else { throw ProtocolError.invalidUTF8 }
        let object: [String: Any]
        do {
            guard let decoded = try JSONSerialization.jsonObject(with: frameBody) as? [String: Any] else {
                throw ProtocolError.malformedJSON("Top-level JSON is not an object")
            }
            object = decoded
        } catch let error as ProtocolError {
            throw error
        } catch {
            throw ProtocolError.malformedJSON(error.localizedDescription)
        }

        let schemaVersion = try Self.requiredInt("schema_version", in: object)
        guard schemaVersion == Self.schemaVersion else { throw ProtocolError.unsupportedSchemaVersion(schemaVersion) }
        let requestId = try Self.requiredString("request_id", in: object)
        guard !requestId.isEmpty else { throw ProtocolError.invalidField("request_id") }
        let commandText = try Self.requiredString("command", in: object)
        guard let command = MT5Command(rawValue: commandText) else { throw ProtocolError.unknownCommand(commandText) }
        let timestampSentUtc = UtcSecond(rawValue: try Self.requiredInt64("timestamp_sent_utc", in: object))
        let expectedPayloadLength = try Self.requiredInt("payload_length", in: object)
        guard expectedPayloadLength >= 0, expectedPayloadLength <= maxFrameBytes else {
            throw ProtocolError.invalidField("payload_length")
        }
        let expectedPayloadChecksum = try Self.requiredString("payload_checksum", in: object)
        let errorCode = try Self.optionalString("error_code", in: object)
        let errorMessage = try Self.optionalString("error_message", in: object)

        let payloadData = try Self.extractPayloadBytes(from: jsonText)
        guard payloadData.count == expectedPayloadLength else {
            throw ProtocolError.payloadLengthMismatch(expected: expectedPayloadLength, actual: payloadData.count)
        }
        let actualChecksum = Self.payloadChecksum(payloadData)
        guard actualChecksum == expectedPayloadChecksum else {
            throw ProtocolError.payloadChecksumMismatch(expected: expectedPayloadChecksum, actual: actualChecksum)
        }
        if let errorCode, errorCode != "OK" {
            throw ProtocolError.bridgeError(code: errorCode, message: errorMessage ?? "No MT5 bridge error message supplied")
        }

        do {
            let payload = try decoder.decode(Payload.self, from: payloadData)
            return ProtocolMessage(schemaVersion: schemaVersion, requestId: requestId, command: command, timestampSentUtc: timestampSentUtc, payload: payload)
        } catch {
            throw ProtocolError.malformedJSON("Payload decode failed: \(error.localizedDescription)")
        }
    }

    public static func withLengthPrefix(_ body: Data) -> Data {
        var length = UInt32(body.count).bigEndian
        var frame = Data(bytes: &length, count: MemoryLayout<UInt32>.size)
        frame.append(body)
        return frame
    }

    public static func bodyLength(from prefix: Data) throws -> Int {
        guard prefix.count == 4 else { throw ProtocolError.malformedLengthPrefix }
        let bytes = Array(prefix)
        let value = (UInt32(bytes[0]) << 24) | (UInt32(bytes[1]) << 16) | (UInt32(bytes[2]) << 8) | UInt32(bytes[3])
        return Int(value)
    }

    public static func payloadChecksum(_ data: Data) -> String {
        var hash = FNV1a64()
        hash.append(data)
        return "fnv64:" + String(format: "%016llx", hash.value)
    }

    static func extractPayloadBytes(from jsonText: String) throws -> Data {
        guard let keyRange = jsonText.range(of: "\"payload\"") else { throw ProtocolError.missingField("payload") }
        guard let colonRange = jsonText[keyRange.upperBound...].range(of: ":") else {
            throw ProtocolError.invalidField("payload")
        }
        var index = colonRange.upperBound
        while index < jsonText.endIndex, jsonText[index].isWhitespace {
            index = jsonText.index(after: index)
        }
        guard index < jsonText.endIndex, jsonText[index] == "{" else {
            throw ProtocolError.payloadNotObject
        }

        var depth = 0
        var inString = false
        var escaping = false
        var end = index

        while end < jsonText.endIndex {
            let char = jsonText[end]
            if inString {
                if escaping {
                    escaping = false
                } else if char == "\\" {
                    escaping = true
                } else if char == "\"" {
                    inString = false
                }
            } else {
                if char == "\"" {
                    inString = true
                } else if char == "{" {
                    depth += 1
                } else if char == "}" {
                    depth -= 1
                    if depth == 0 {
                        let payloadEnd = jsonText.index(after: end)
                        let payloadText = String(jsonText[index..<payloadEnd])
                        guard let data = payloadText.data(using: .utf8) else { throw ProtocolError.invalidUTF8 }
                        return data
                    }
                }
            }
            end = jsonText.index(after: end)
        }
        throw ProtocolError.incompleteFrame
    }

    private static func requiredString(_ key: String, in object: [String: Any]) throws -> String {
        guard let value = object[key] else { throw ProtocolError.missingField(key) }
        guard let string = value as? String else { throw ProtocolError.invalidField(key) }
        return string
    }

    private static func optionalString(_ key: String, in object: [String: Any]) throws -> String? {
        guard let value = object[key], !(value is NSNull) else { return nil }
        guard let string = value as? String else { throw ProtocolError.invalidField(key) }
        return string
    }

    private static func requiredInt(_ key: String, in object: [String: Any]) throws -> Int {
        let value = try requiredInt64(key, in: object)
        guard value >= Int64(Int.min), value <= Int64(Int.max) else {
            throw ProtocolError.invalidField(key)
        }
        return Int(value)
    }

    private static func requiredInt64(_ key: String, in object: [String: Any]) throws -> Int64 {
        guard let value = object[key] else { throw ProtocolError.missingField(key) }
        if let number = value as? NSNumber, CFGetTypeID(number) == CFBooleanGetTypeID() {
            throw ProtocolError.invalidField(key)
        }
        if let int = value as? Int { return Int64(int) }
        if let int64 = value as? Int64 { return int64 }
        if let number = value as? NSNumber {
            let doubleValue = number.doubleValue
            guard doubleValue.isFinite,
                  doubleValue.rounded(.towardZero) == doubleValue,
                  doubleValue >= Double(Int64.min),
                  doubleValue <= Double(Int64.max) else {
                throw ProtocolError.invalidField(key)
            }
            let int64Value = number.int64Value
            guard Double(int64Value) == doubleValue else {
                throw ProtocolError.invalidField(key)
            }
            return int64Value
        }
        throw ProtocolError.invalidField(key)
    }

    private static func escapeJSONString(_ text: String) -> String {
        var result = ""
        for scalar in text.unicodeScalars {
            switch scalar {
            case "\"": result += "\\\""
            case "\\": result += "\\\\"
            case "\n": result += "\\n"
            case "\r": result += "\\r"
            case "\t": result += "\\t"
            default: result.unicodeScalars.append(scalar)
            }
        }
        return result
    }
}

public struct FrameParser: Sendable {
    private var buffer = Data()
    public let maxFrameBytes: Int

    public init(maxFrameBytes: Int = 16 * 1024 * 1024) {
        self.maxFrameBytes = maxFrameBytes
    }

    public mutating func append(_ data: Data) throws -> [Data] {
        buffer.append(data)
        var frames: [Data] = []

        while buffer.count >= 4 {
            let prefix = buffer.prefix(4)
            let length = try FramedProtocolCodec.bodyLength(from: Data(prefix))
            guard length <= maxFrameBytes else { throw ProtocolError.frameTooLarge(length) }
            guard buffer.count >= 4 + length else { break }
            let body = buffer.dropFirst(4).prefix(length)
            frames.append(Data(body))
            buffer.removeFirst(4 + length)
        }

        return frames
    }
}
