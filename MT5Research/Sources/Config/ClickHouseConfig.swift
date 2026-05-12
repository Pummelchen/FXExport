import Domain
import Foundation

public struct ClickHouseConfig: Codable, Sendable {
    public let url: URL
    public let database: String
    public let username: String?
    public let password: String?
    public let passwordEnvironmentVariable: String?
    public let allowPlaintextRemotePassword: Bool
    public let requestTimeoutSeconds: Double
    public let retryCount: Int
    public let allowInsecureRemoteHTTP: Bool
    public let waitEndOfQuery: Bool
    public let queryIdPrefix: String

    public init(
        url: URL,
        database: String,
        username: String?,
        password: String?,
        requestTimeoutSeconds: Double,
        retryCount: Int
    ) {
        self.init(
            url: url,
            database: database,
            username: username,
            password: password,
            passwordEnvironmentVariable: nil,
            allowPlaintextRemotePassword: false,
            requestTimeoutSeconds: requestTimeoutSeconds,
            retryCount: retryCount,
            allowInsecureRemoteHTTP: false,
            waitEndOfQuery: true,
            queryIdPrefix: "fxexport"
        )
    }

    public init(
        url: URL,
        database: String,
        username: String?,
        password: String?,
        passwordEnvironmentVariable: String? = nil,
        allowPlaintextRemotePassword: Bool = false,
        requestTimeoutSeconds: Double,
        retryCount: Int,
        allowInsecureRemoteHTTP: Bool,
        waitEndOfQuery: Bool,
        queryIdPrefix: String
    ) {
        self.url = url
        self.database = database
        self.username = username
        self.password = password
        self.passwordEnvironmentVariable = passwordEnvironmentVariable
        self.allowPlaintextRemotePassword = allowPlaintextRemotePassword
        self.requestTimeoutSeconds = requestTimeoutSeconds
        self.retryCount = retryCount
        self.allowInsecureRemoteHTTP = allowInsecureRemoteHTTP
        self.waitEndOfQuery = waitEndOfQuery
        self.queryIdPrefix = queryIdPrefix
    }

    enum CodingKeys: String, CodingKey {
        case url
        case database
        case username
        case password
        case passwordEnvironmentVariable
        case allowPlaintextRemotePassword
        case requestTimeoutSeconds
        case retryCount
        case allowInsecureRemoteHTTP
        case waitEndOfQuery
        case queryIdPrefix
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        try self.init(
            url: container.decode(URL.self, forKey: .url),
            database: container.decode(String.self, forKey: .database),
            username: container.decodeIfPresent(String.self, forKey: .username),
            password: container.decodeIfPresent(String.self, forKey: .password),
            passwordEnvironmentVariable: container.decodeIfPresent(String.self, forKey: .passwordEnvironmentVariable),
            allowPlaintextRemotePassword: container.decodeIfPresent(Bool.self, forKey: .allowPlaintextRemotePassword) ?? false,
            requestTimeoutSeconds: container.decode(Double.self, forKey: .requestTimeoutSeconds),
            retryCount: container.decode(Int.self, forKey: .retryCount),
            allowInsecureRemoteHTTP: container.decodeIfPresent(Bool.self, forKey: .allowInsecureRemoteHTTP) ?? false,
            waitEndOfQuery: container.decodeIfPresent(Bool.self, forKey: .waitEndOfQuery) ?? true,
            queryIdPrefix: container.decodeIfPresent(String.self, forKey: .queryIdPrefix) ?? "fxexport"
        )
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(url, forKey: .url)
        try container.encode(database, forKey: .database)
        try container.encodeIfPresent(username, forKey: .username)
        try container.encodeIfPresent(password, forKey: .password)
        try container.encodeIfPresent(passwordEnvironmentVariable, forKey: .passwordEnvironmentVariable)
        try container.encode(allowPlaintextRemotePassword, forKey: .allowPlaintextRemotePassword)
        try container.encode(requestTimeoutSeconds, forKey: .requestTimeoutSeconds)
        try container.encode(retryCount, forKey: .retryCount)
        try container.encode(allowInsecureRemoteHTTP, forKey: .allowInsecureRemoteHTTP)
        try container.encode(waitEndOfQuery, forKey: .waitEndOfQuery)
        try container.encode(queryIdPrefix, forKey: .queryIdPrefix)
    }

    public var isLocalEndpoint: Bool {
        guard let host = url.host(percentEncoded: false)?.lowercased() else { return false }
        return host == "localhost" || host == "127.0.0.1" || host == "::1"
    }

    public var usesInsecureRemoteHTTP: Bool {
        url.scheme == "http" && !isLocalEndpoint
    }

    public var resolvedPassword: String? {
        if let passwordEnvironmentVariable,
           !passwordEnvironmentVariable.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
           let value = ProcessInfo.processInfo.environment[passwordEnvironmentVariable] {
            return value
        }
        return password
    }
}

public struct MT5BridgeConfig: Codable, Sendable {
    public enum Mode: String, Codable, Sendable {
        case listen
        case connect
    }

    public let mode: Mode
    public let host: String
    public let port: UInt16
    public let connectTimeoutSeconds: Double
    public let requestTimeoutSeconds: Double

    public init(mode: Mode, host: String, port: UInt16, connectTimeoutSeconds: Double, requestTimeoutSeconds: Double) {
        self.mode = mode
        self.host = host
        self.port = port
        self.connectTimeoutSeconds = connectTimeoutSeconds
        self.requestTimeoutSeconds = requestTimeoutSeconds
    }
}
