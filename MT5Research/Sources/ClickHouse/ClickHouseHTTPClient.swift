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
                return try await executeOnce(query)
            } catch {
                lastError = error
                guard attempt < attempts else { break }
                logger.warn("ClickHouse request failed, retrying attempt \(attempt + 1)/\(attempts): \(error)")
                try await Task.sleep(nanoseconds: UInt64(250_000_000 * attempt))
            }
        }
        throw lastError ?? ClickHouseError.transport("Unknown ClickHouse failure")
    }

    private func executeOnce(_ query: ClickHouseQuery) async throws -> String {
        var components = URLComponents(url: config.url, resolvingAgainstBaseURL: false)
        var queryItems: [URLQueryItem] = []
        let database = query.databaseOverride ?? config.database
        if !database.isEmpty {
            queryItems.append(URLQueryItem(name: "database", value: database))
        }
        components?.queryItems = queryItems
        guard let url = components?.url else { throw ClickHouseError.invalidURL(config.url.absoluteString) }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.timeoutInterval = config.requestTimeoutSeconds
        request.httpBody = query.sql.data(using: .utf8)
        if let authorization = Self.basicAuthorization(username: config.username, password: config.password) {
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

    static func basicAuthorization(username: String?, password: String?) -> String? {
        guard let username, !username.isEmpty else { return nil }
        let password = password ?? ""
        let token = "\(username):\(password)"
        guard let tokenData = token.data(using: .utf8) else { return nil }
        return "Basic \(tokenData.base64EncodedString())"
    }
}
