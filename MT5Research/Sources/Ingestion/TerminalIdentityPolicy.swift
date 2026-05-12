import AppCore
import Config
import Domain
import Foundation
import MT5Bridge

public enum TerminalIdentityPolicyError: Error, CustomStringConvertible, Sendable {
    case mismatch(String)

    public var description: String {
        switch self {
        case .mismatch(let reason):
            return "MT5 terminal identity mismatch: \(reason)"
        }
    }
}

public struct TerminalIdentityPolicy: Sendable {
    public init() {}

    public func resolve(
        actual: TerminalInfoDTO,
        brokerSourceId: BrokerSourceId,
        expected: ExpectedTerminalIdentity?,
        logger: Logger
    ) throws -> BrokerServerIdentity {
        let identity = try actual.brokerServerIdentity()
        guard let expected, !expected.isEmpty else {
            logger.ok("MT5 terminal identity auto-detected for broker_source_id \(brokerSourceId.rawValue): \(identity)")
            return identity
        }
        if let company = expected.company, company != actual.company {
            throw TerminalIdentityPolicyError.mismatch("expected company '\(company)', got '\(actual.company)'")
        }
        if let server = expected.server, server != actual.server {
            throw TerminalIdentityPolicyError.mismatch("expected server '\(server)', got '\(actual.server)'")
        }
        if let accountLogin = expected.accountLogin, accountLogin != actual.accountLogin {
            throw TerminalIdentityPolicyError.mismatch("expected account \(accountLogin), got \(actual.accountLogin)")
        }
        logger.ok("MT5 terminal identity verified for broker_source_id \(brokerSourceId.rawValue): \(identity)")
        return identity
    }
}
