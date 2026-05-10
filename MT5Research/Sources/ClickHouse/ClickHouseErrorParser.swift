import Foundation

public enum ClickHouseError: Error, Equatable, CustomStringConvertible, Sendable {
    case invalidURL(String)
    case httpStatus(Int, String)
    case exception(String)
    case transport(String)
    case decoding(String)
    case nonIdempotentRetryRefused

    public var description: String {
        switch self {
        case .invalidURL(let reason):
            return "Invalid ClickHouse URL: \(reason)"
        case .httpStatus(let status, let body):
            return "ClickHouse HTTP status \(status): \(body)"
        case .exception(let body):
            return "ClickHouse exception: \(body)"
        case .transport(let reason):
            return "ClickHouse transport error: \(reason)"
        case .decoding(let reason):
            return "ClickHouse response decode error: \(reason)"
        case .nonIdempotentRetryRefused:
            return "Refusing to retry non-idempotent ClickHouse operation."
        }
    }
}

public struct ClickHouseErrorParser: Sendable {
    public init() {}

    public func parseException(in body: String) -> ClickHouseError? {
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        if trimmed.hasPrefix("Code:") || trimmed.contains("\nCode:") || trimmed.contains("DB::Exception") {
            return .exception(trimmed)
        }
        return nil
    }
}
