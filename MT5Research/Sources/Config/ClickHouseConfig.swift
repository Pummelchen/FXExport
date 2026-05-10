import Domain
import Foundation

public struct ClickHouseConfig: Codable, Sendable {
    public let url: URL
    public let database: String
    public let username: String?
    public let password: String?
    public let requestTimeoutSeconds: Double
    public let retryCount: Int

    public init(
        url: URL,
        database: String,
        username: String?,
        password: String?,
        requestTimeoutSeconds: Double,
        retryCount: Int
    ) {
        self.url = url
        self.database = database
        self.username = username
        self.password = password
        self.requestTimeoutSeconds = requestTimeoutSeconds
        self.retryCount = retryCount
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
