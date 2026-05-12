import AppCore
import Config
import Foundation

public protocol ClickHouseClientProtocol: Sendable {
    func execute(_ query: ClickHouseQuery) async throws -> String
}

public struct ClickHouseHTTPClient: ClickHouseClientProtocol, Sendable {
    private let config: ClickHouseConfig
    private let logger: Logger
    private let parser = ClickHouseErrorParser()

    public init(config: ClickHouseConfig, logger: Logger) {
        self.config = config
        self.logger = logger
    }

    public func execute(_ query: ClickHouseQuery) async throws -> String {
        let attempts = query.isIdempotent ? max(1, config.retryCount + 1) : 1
        var lastError: Error?
        for attempt in 1...attempts {
            do {
                return try await executeOnce(query, attempt: attempt)
            } catch {
                lastError = error
                guard attempt < attempts else { break }
                logger.warn("ClickHouse request failed, retrying attempt \(attempt + 1)/\(attempts): \(error)")
                try await Task.sleep(nanoseconds: UInt64(250_000_000 * attempt))
            }
        }
        throw lastError ?? ClickHouseError.transport("Unknown ClickHouse failure")
    }

    private func executeOnce(_ query: ClickHouseQuery, attempt: Int) async throws -> String {
        let url = try Self.requestURL(
            config: config,
            query: query,
            queryID: Self.queryId(prefix: config.queryIdPrefix, attempt: attempt)
        )

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.timeoutInterval = config.requestTimeoutSeconds
        request.httpBody = query.sql.data(using: .utf8)
        request.setValue("text/plain; charset=utf-8", forHTTPHeaderField: "Content-Type")
        request.setValue("FXExport ClickHouseHTTPClient", forHTTPHeaderField: "User-Agent")
        if let authorization = Self.basicAuthorization(username: config.username, password: config.resolvedPassword) {
            request.setValue(authorization, forHTTPHeaderField: "Authorization")
        }

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await URLSession.shared.data(for: request)
        } catch {
            throw ClickHouseError.transport(error.localizedDescription)
        }

        let body = String(data: data, encoding: .utf8) ?? ""
        guard let httpResponse = response as? HTTPURLResponse else {
            throw ClickHouseError.transport("Response was not HTTP")
        }
        guard (200..<300).contains(httpResponse.statusCode) else {
            throw ClickHouseError.httpStatus(httpResponse.statusCode, body)
        }
        if let exception = parser.parseException(in: body) {
            throw exception
        }
        return body
    }

    static func queryId(prefix: String, attempt: Int, uuid: UUID = UUID()) -> String {
        let sanitizedBytes = prefix.utf8.filter { byte in
            (byte >= 48 && byte <= 57) ||
            (byte >= 65 && byte <= 90) ||
            (byte >= 97 && byte <= 122) ||
            byte == 45 ||
            byte == 95
        }.prefix(64)
        let sanitizedPrefix = String(decoding: sanitizedBytes, as: UTF8.self)
        let safePrefix = sanitizedPrefix.isEmpty ? "fxexport" : sanitizedPrefix
        return "\(safePrefix)-\(uuid.uuidString.lowercased())-a\(attempt)"
    }

    static func requestURL(config: ClickHouseConfig, query: ClickHouseQuery, queryID: String) throws -> URL {
        var components = URLComponents(url: config.url, resolvingAgainstBaseURL: false)
        var queryItems: [URLQueryItem] = []
        let database = query.databaseOverride ?? config.database
        if !database.isEmpty {
            queryItems.append(URLQueryItem(name: "database", value: database))
        }
        if config.waitEndOfQuery {
            queryItems.append(URLQueryItem(name: "wait_end_of_query", value: "1"))
        }
        queryItems.append(URLQueryItem(name: "send_progress_in_http_headers", value: "0"))
        queryItems.append(URLQueryItem(name: "query_id", value: queryID))
        components?.queryItems = queryItems
        guard let url = components?.url else { throw ClickHouseError.invalidURL(config.url.absoluteString) }
        return url
    }

    static func basicAuthorization(username: String?, password: String?) -> String? {
        guard let username, !username.isEmpty else { return nil }
        let password = password ?? ""
        let token = "\(username):\(password)"
        guard let tokenData = token.data(using: .utf8) else { return nil }
        return "Basic \(tokenData.base64EncodedString())"
    }
}
