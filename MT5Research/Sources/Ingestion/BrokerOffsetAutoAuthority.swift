import AppCore
import ClickHouse
import Domain
import Foundation
import MT5Bridge

public enum BrokerOffsetAutoAuthorityError: Error, CustomStringConvertible, Sendable {
    case invalidExistingRow(String)
    case multipleActiveSegments(BrokerSourceId, BrokerServerIdentity, MT5ServerSecond)

    public var description: String {
        switch self {
        case .invalidExistingRow(let row):
            return "Invalid broker_time_offsets row while checking automatic live offset authority: \(row)"
        case .multipleActiveSegments(let brokerSourceId, let identity, let serverTime):
            return "Multiple active verified broker UTC offset segments cover live server timestamp \(serverTime.rawValue) for \(brokerSourceId.rawValue), \(identity)."
        }
    }
}

public struct BrokerOffsetAutoAuthority: Sendable {
    private let clickHouse: ClickHouseClientProtocol
    private let database: String
    private let logger: Logger?

    public init(clickHouse: ClickHouseClientProtocol, database: String, logger: Logger? = nil) {
        self.clickHouse = clickHouse
        self.database = database
        self.logger = logger
    }

    /// Records a verified current-day broker offset segment only when no active verified
    /// segment covers the EA-observed live MT5 server time. This deliberately does not
    /// invent historical DST/server segments; historical canonical ingestion still requires
    /// audited coverage already present in ClickHouse.
    public func ensureLiveSegmentIfMissing(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        snapshot: ServerTimeSnapshotDTO,
        now: UtcSecond = UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))
    ) async throws {
        let observed = try BrokerOffsetRuntimeVerifier.observedOffset(from: snapshot)
        let serverTime = MT5ServerSecond(rawValue: snapshot.timeTradeServer)
        let accepted = BrokerOffsetPolicy.acceptedLiveOffsets(for: terminalIdentity)
        if !accepted.isEmpty && !accepted.contains(observed) {
            throw BrokerOffsetRuntimeError.observedOffsetNotAccepted(observed, accepted: accepted)
        }
        let existing = try await activeVerifiedSegments(
            brokerSourceId: brokerSourceId,
            terminalIdentity: terminalIdentity,
            containing: serverTime
        )
        if existing.count > 1 {
            throw BrokerOffsetAutoAuthorityError.multipleActiveSegments(brokerSourceId, terminalIdentity, serverTime)
        }
        if let existingSegment = existing.first {
            guard existingSegment.offset == observed else {
                throw BrokerOffsetRuntimeError.liveOffsetMismatch(
                    observed: observed,
                    configured: existingSegment.offset,
                    serverTime: serverTime
                )
            }
            return
        }

        let dayStart = Self.serverDayStart(containing: serverTime)
        let dayEnd = MT5ServerSecond(rawValue: dayStart.rawValue + 86_400)
        try await insertLiveSegment(
            brokerSourceId: brokerSourceId,
            terminalIdentity: terminalIdentity,
            validFrom: dayStart,
            validTo: dayEnd,
            offset: observed,
            snapshot: snapshot,
            now: now
        )
        logger?.ok("Automatic broker UTC authority recorded: \(brokerSourceId.rawValue) \(terminalIdentity) offset \(observed.rawValue) seconds for live server day \(dayStart.rawValue)..<\(dayEnd.rawValue)")
    }

    private func activeVerifiedSegments(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        containing serverTime: MT5ServerSecond
    ) async throws -> [ExistingOffsetSegment] {
        let body = try await clickHouse.execute(.select("""
        SELECT valid_from_mt5_server_ts, valid_to_mt5_server_ts, offset_seconds
        FROM \(database).broker_time_offsets
        WHERE broker_source_id = '\(Self.sqlLiteral(brokerSourceId.rawValue))'
          AND mt5_company = '\(Self.sqlLiteral(terminalIdentity.company))'
          AND mt5_server = '\(Self.sqlLiteral(terminalIdentity.server))'
          AND mt5_account_login = \(terminalIdentity.accountLogin)
          AND confidence = 'verified'
          AND is_active = 1
          AND valid_from_mt5_server_ts <= \(serverTime.rawValue)
          AND valid_to_mt5_server_ts > \(serverTime.rawValue)
        ORDER BY valid_from_mt5_server_ts ASC, created_at_utc ASC
        LIMIT 2
        FORMAT TabSeparated
        """))
        return try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try ExistingOffsetSegment.parse(String($0)) }
    }

    private func insertLiveSegment(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        validFrom: MT5ServerSecond,
        validTo: MT5ServerSecond,
        offset: OffsetSeconds,
        snapshot: ServerTimeSnapshotDTO,
        now: UtcSecond
    ) async throws {
        let evidence = """
        source=EA_GET_SERVER_TIME_SNAPSHOT;time_trade_server=\(snapshot.timeTradeServer);time_gmt=\(snapshot.timeGMT);time_local=\(snapshot.timeLocal);observed_offset_seconds=\(offset.rawValue)
        """
        let row = [
            Self.tsv(brokerSourceId.rawValue),
            Self.tsv(terminalIdentity.company),
            Self.tsv(terminalIdentity.server),
            String(terminalIdentity.accountLogin),
            String(validFrom.rawValue),
            String(validTo.rawValue),
            String(offset.rawValue),
            Self.tsv(OffsetSource.mt5LiveSnapshot.rawValue),
            Self.tsv(OffsetConfidence.verified.rawValue),
            Self.tsv(evidence),
            "1",
            String(now.rawValue)
        ].joined(separator: "\t")
        _ = try await clickHouse.execute(.mutation("""
        INSERT INTO \(database).broker_time_offsets
        (broker_source_id, mt5_company, mt5_server, mt5_account_login,
         valid_from_mt5_server_ts, valid_to_mt5_server_ts, offset_seconds,
         source, confidence, verification_evidence, is_active, created_at_utc)
        FORMAT TabSeparated
        \(row)
        """, idempotent: true))
    }

    private static func serverDayStart(containing serverTime: MT5ServerSecond) -> MT5ServerSecond {
        let day: Int64 = 86_400
        let raw = serverTime.rawValue
        let start: Int64
        if raw >= 0 {
            start = (raw / day) * day
        } else {
            start = ((raw - day + 1) / day) * day
        }
        return MT5ServerSecond(rawValue: start)
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

private struct ExistingOffsetSegment: Sendable {
    let validFrom: MT5ServerSecond
    let validTo: MT5ServerSecond
    let offset: OffsetSeconds

    static func parse(_ row: String) throws -> ExistingOffsetSegment {
        let fields = row.split(separator: "\t", omittingEmptySubsequences: false)
        guard fields.count == 3,
              let validFrom = Int64(fields[0]),
              let validTo = Int64(fields[1]),
              let offset = Int64(fields[2]) else {
            throw BrokerOffsetAutoAuthorityError.invalidExistingRow(row)
        }
        return ExistingOffsetSegment(
            validFrom: MT5ServerSecond(rawValue: validFrom),
            validTo: MT5ServerSecond(rawValue: validTo),
            offset: OffsetSeconds(rawValue: offset)
        )
    }
}
