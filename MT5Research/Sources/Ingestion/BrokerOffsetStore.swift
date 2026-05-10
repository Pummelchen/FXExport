import ClickHouse
import Domain
import Foundation
import TimeMapping

public protocol BrokerOffsetStore: Sendable {
    func loadVerifiedOffsetMap(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity
    ) async throws -> BrokerOffsetMap
}

public enum BrokerOffsetStoreError: Error, CustomStringConvertible, Sendable {
    case noVerifiedOffsets(BrokerSourceId, BrokerServerIdentity)
    case invalidRow(String)
    case invalidConfidence(String)
    case invalidSource(String)

    public var description: String {
        switch self {
        case .noVerifiedOffsets(let brokerSourceId, let identity):
            return "No active verified broker UTC offset rows found in ClickHouse for broker_source_id \(brokerSourceId.rawValue), MT5 identity \(identity)."
        case .invalidRow(let row):
            return "Invalid broker_time_offsets row: \(row)"
        case .invalidConfidence(let value):
            return "Invalid broker offset confidence '\(value)' in ClickHouse."
        case .invalidSource(let value):
            return "Invalid broker offset source '\(value)' in ClickHouse."
        }
    }
}

public struct ClickHouseBrokerOffsetStore: BrokerOffsetStore {
    private let client: ClickHouseClientProtocol
    private let database: String

    public init(client: ClickHouseClientProtocol, database: String) {
        self.client = client
        self.database = database
    }

    public func loadVerifiedOffsetMap(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity
    ) async throws -> BrokerOffsetMap {
        let sql = """
        SELECT broker_source_id, mt5_company, mt5_server, mt5_account_login,
               valid_from_mt5_server_ts, valid_to_mt5_server_ts, offset_seconds,
               source, confidence, created_at_utc
        FROM \(database).broker_time_offsets
        WHERE broker_source_id = '\(Self.sqlLiteral(brokerSourceId.rawValue))'
          AND mt5_company = '\(Self.sqlLiteral(terminalIdentity.company))'
          AND mt5_server = '\(Self.sqlLiteral(terminalIdentity.server))'
          AND mt5_account_login = \(terminalIdentity.accountLogin)
          AND confidence = 'verified'
          AND is_active = 1
        ORDER BY valid_from_mt5_server_ts ASC, created_at_utc ASC
        FORMAT TabSeparated
        """
        let body = try await client.execute(.select(sql))
        let rows = body.split(separator: "\n", omittingEmptySubsequences: true).map(String.init)
        guard !rows.isEmpty else {
            throw BrokerOffsetStoreError.noVerifiedOffsets(brokerSourceId, terminalIdentity)
        }

        let segments = try rows.map { row in
            try parseRow(row, expectedBrokerSourceId: brokerSourceId, expectedIdentity: terminalIdentity)
        }
        return try BrokerOffsetMap(
            brokerSourceId: brokerSourceId,
            terminalIdentity: terminalIdentity,
            segments: segments,
            requireVerified: true
        )
    }

    private func parseRow(
        _ row: String,
        expectedBrokerSourceId: BrokerSourceId,
        expectedIdentity: BrokerServerIdentity
    ) throws -> BrokerOffsetSegment {
        let fields = row.split(separator: "\t", omittingEmptySubsequences: false).map { Self.unescapeTabSeparated(String($0)) }
        guard fields.count == 10,
              let accountLogin = Int64(fields[3]),
              let validFrom = Int64(fields[4]),
              let validTo = Int64(fields[5]),
              let offset = Int64(fields[6]),
              let createdAt = Int64(fields[9]) else {
            throw BrokerOffsetStoreError.invalidRow(row)
        }
        guard createdAt > 0 else { throw BrokerOffsetStoreError.invalidRow(row) }
        let brokerSourceId = try BrokerSourceId(fields[0])
        let identity = try BrokerServerIdentity(company: fields[1], server: fields[2], accountLogin: accountLogin)
        guard let source = OffsetSource(rawValue: fields[7]) else {
            throw BrokerOffsetStoreError.invalidSource(fields[7])
        }
        guard let confidence = OffsetConfidence(rawValue: fields[8]) else {
            throw BrokerOffsetStoreError.invalidConfidence(fields[8])
        }
        guard brokerSourceId == expectedBrokerSourceId, identity == expectedIdentity else {
            throw BrokerOffsetStoreError.invalidRow(row)
        }
        return BrokerOffsetSegment(
            brokerSourceId: brokerSourceId,
            terminalIdentity: identity,
            validFrom: MT5ServerSecond(rawValue: validFrom),
            validTo: MT5ServerSecond(rawValue: validTo),
            offset: OffsetSeconds(rawValue: offset),
            source: source,
            confidence: confidence
        )
    }

    private static func sqlLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "\\", with: "\\\\").replacingOccurrences(of: "'", with: "\\'")
    }

    private static func unescapeTabSeparated(_ value: String) -> String {
        var result = ""
        var escaping = false
        for character in value {
            if escaping {
                switch character {
                case "t": result.append("\t")
                case "n": result.append("\n")
                case "r": result.append("\r")
                case "\\": result.append("\\")
                default: result.append(character)
                }
                escaping = false
            } else if character == "\\" {
                escaping = true
            } else {
                result.append(character)
            }
        }
        if escaping {
            result.append("\\")
        }
        return result
    }
}
