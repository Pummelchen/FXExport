import Foundation

public struct BrokerServerIdentity: Codable, Hashable, Sendable, Comparable, CustomStringConvertible {
    public let company: String
    public let server: String
    public let accountLogin: Int64

    public init(company: String, server: String, accountLogin: Int64) throws {
        let trimmedCompany = company.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedServer = server.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedCompany.isEmpty else { throw DomainError.emptyBrokerCompany }
        guard !trimmedServer.isEmpty else { throw DomainError.emptyBrokerServer }
        guard accountLogin > 0 else { throw DomainError.invalidBrokerAccountLogin(accountLogin) }
        self.company = trimmedCompany
        self.server = trimmedServer
        self.accountLogin = accountLogin
    }

    public var description: String {
        "\(company) / \(server) / \(accountLogin)"
    }

    public static func < (lhs: BrokerServerIdentity, rhs: BrokerServerIdentity) -> Bool {
        (lhs.company, lhs.server, lhs.accountLogin) < (rhs.company, rhs.server, rhs.accountLogin)
    }
}
