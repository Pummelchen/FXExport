import CryptoKit
import Foundation

public enum ChunkHashSchemaVersion {
    public static let sha256V1 = "sha256.chunk.v1"
}

public struct SHA256DigestHex: RawRepresentable, Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let rawValue: String

    fileprivate init(uncheckedRawValue: String) {
        self.rawValue = uncheckedRawValue
    }

    public init?(rawValue: String) {
        let canonical = rawValue.lowercased()
        guard canonical.utf8.count == 64,
              canonical.utf8.allSatisfy({ byte in
                  (48...57).contains(byte) || (97...102).contains(byte)
              }) else {
            return nil
        }
        self.rawValue = canonical
    }

    public var description: String { rawValue }

    public static func < (lhs: SHA256DigestHex, rhs: SHA256DigestHex) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

public struct SHA256ChunkHasher {
    private var hasher = SHA256()

    public init(namespace: String) {
        appendField("hash_schema_version", ChunkHashSchemaVersion.sha256V1)
        appendField("namespace", namespace)
    }

    public mutating func appendField(_ name: String, _ value: String) {
        appendByte(0x53)
        appendString(name)
        appendString(value)
    }

    public mutating func appendField(_ name: String, _ value: Int64) {
        appendByte(0x49)
        appendString(name)
        appendInt64(value)
    }

    public mutating func appendField(_ name: String, _ value: Int) {
        appendField(name, Int64(value))
    }

    public mutating func appendField(_ name: String, _ value: Bool) {
        appendByte(0x42)
        appendString(name)
        appendByte(value ? 1 : 0)
    }

    public mutating func finalize() -> SHA256DigestHex {
        let digest = hasher.finalize()
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return SHA256DigestHex(uncheckedRawValue: hex)
    }

    private mutating func appendString(_ value: String) {
        let data = Data(value.utf8)
        appendUInt64(UInt64(data.count))
        hasher.update(data: data)
    }

    private mutating func appendInt64(_ value: Int64) {
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) { buffer in
            hasher.update(data: Data(buffer))
        }
    }

    private mutating func appendUInt64(_ value: UInt64) {
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) { buffer in
            hasher.update(data: Data(buffer))
        }
    }

    private mutating func appendByte(_ value: UInt8) {
        hasher.update(data: Data([value]))
    }
}
