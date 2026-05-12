import ClickHouse
import Domain
import Foundation
import MT5Bridge

public struct BrokerSourceResolution: Sendable, Equatable {
    public let brokerSourceId: BrokerSourceId
    public let terminalIdentity: BrokerServerIdentity
    public let wasCreated: Bool
}

public enum BrokerSourceRegistryError: Error, CustomStringConvertible, Sendable {
    case invalidRow(String)
    case ambiguousBrokerSource(BrokerServerIdentity, [BrokerSourceId])

    public var description: String {
        switch self {
        case .invalidRow(let row):
            return "Invalid broker_sources row: \(row)"
        case .ambiguousBrokerSource(let identity, let ids):
            let idList = ids.map(\.rawValue).joined(separator: ", ")
            return "MT5 identity \(identity) maps to multiple active broker source ids: \(idList). Keep exactly one active row in broker_sources."
        }
    }
}

public struct BrokerSourceRegistry: Sendable {
    private let client: ClickHouseClientProtocol
    private let database: String

    public init(client: ClickHouseClientProtocol, database: String) {
        self.client = client
        self.database = database
    }

    public func resolve(terminalInfo: TerminalInfoDTO, now: UtcSecond = UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))) async throws -> BrokerSourceResolution {
        let identity = try terminalInfo.brokerServerIdentity()
        let existing = try await activeBrokerSources(for: identity)
        if existing.count == 1, let brokerSourceId = existing.first {
            try await record(
                brokerSourceId: brokerSourceId,
                identity: identity,
                status: "seen",
                now: now
            )
            return BrokerSourceResolution(brokerSourceId: brokerSourceId, terminalIdentity: identity, wasCreated: false)
        }
        if existing.count > 1 {
            throw BrokerSourceRegistryError.ambiguousBrokerSource(identity, existing.sorted())
        }

        let derived = try Self.deriveBrokerSourceId(from: identity)
        try await record(
            brokerSourceId: derived,
            identity: identity,
            status: "auto_discovered",
            now: now
        )
        return BrokerSourceResolution(brokerSourceId: derived, terminalIdentity: identity, wasCreated: true)
    }

    public static func deriveBrokerSourceId(from identity: BrokerServerIdentity) throws -> BrokerSourceId {
        let companyTokens = slugTokens(from: identity.company)
        let serverTokens = slugTokens(from: identity.server)
        // The canonical OHLC key uses broker_source_id, so the derived id must
        // not collide when the same MT5 server/account label appears under a
        // different broker company.
        return try BrokerSourceId((companyTokens + serverTokens + ["account", String(identity.accountLogin)]).joined(separator: "-"))
    }

    private func activeBrokerSources(for identity: BrokerServerIdentity) async throws -> [BrokerSourceId] {
        let body = try await client.execute(.select("""
        SELECT DISTINCT broker_source_id
        FROM \(database).broker_sources
        WHERE mt5_company = '\(Self.sqlLiteral(identity.company))'
          AND mt5_server = '\(Self.sqlLiteral(identity.server))'
          AND mt5_account_login = \(identity.accountLogin)
          AND is_active = 1
        ORDER BY broker_source_id ASC
        FORMAT TabSeparated
        """))
        return try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try BrokerSourceId(String($0)) }
    }

    public func knownActiveBrokerSources(for identity: BrokerServerIdentity) async throws -> [BrokerSourceId] {
        try await activeBrokerSources(for: identity)
    }

    private func record(
        brokerSourceId: BrokerSourceId,
        identity: BrokerServerIdentity,
        status: String,
        now: UtcSecond
    ) async throws {
        let values = [
            Self.tsv(brokerSourceId.rawValue),
            Self.tsv(identity.company),
            Self.tsv(identity.server),
            String(identity.accountLogin),
            Self.tsv("automatic_mt5_identity"),
            Self.tsv(status),
            "1",
            String(now.rawValue),
            String(now.rawValue)
        ].joined(separator: "\t")
        _ = try await client.execute(.mutation("""
        INSERT INTO \(database).broker_sources
        (broker_source_id, mt5_company, mt5_server, mt5_account_login,
         discovery_source, status, is_active, first_seen_utc, last_seen_utc)
        FORMAT TabSeparated
        \(values)
        """, idempotent: true))
    }

    private static func slugTokens(from value: String) -> [String] {
        var tokens: [String] = []
        var current = ""

        for scalar in value.unicodeScalars {
            let character = Character(scalar)
            guard CharacterSet.alphanumerics.contains(scalar) else {
                appendToken(current, into: &tokens)
                current = ""
                continue
            }
            current.append(character)
        }
        appendToken(current, into: &tokens)

        var splitTokens: [String] = []
        for token in tokens {
            splitTokens.append(contentsOf: splitTrailingAcronym(token))
        }
        let normalized = splitTokens
            .map { $0.lowercased() }
            .filter { !$0.isEmpty }
        return normalized.isEmpty ? ["broker"] : normalized
    }

    private static func splitTrailingAcronym(_ token: String) -> [String] {
        guard token.count > 3 else { return [token] }
        let scalars = Array(token.unicodeScalars)
        var suffixStart = scalars.count
        while suffixStart > 0, CharacterSet.uppercaseLetters.contains(scalars[suffixStart - 1]) {
            suffixStart -= 1
        }
        guard suffixStart > 0,
              suffixStart < scalars.count,
              scalars.count - suffixStart >= 2,
              scalars[..<suffixStart].contains(where: { CharacterSet.lowercaseLetters.contains($0) }) else {
            return [token]
        }
        return [
            String(String.UnicodeScalarView(scalars[..<suffixStart])),
            String(String.UnicodeScalarView(scalars[suffixStart...]))
        ]
    }

    private static func appendToken(_ token: String, into tokens: inout [String]) {
        guard !token.isEmpty else { return }
        tokens.append(token)
    }

    private static func sqlLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "\\", with: "\\\\").replacingOccurrences(of: "'", with: "\\'")
    }

    private static func tsv(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\t", with: "\\t")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }
}
