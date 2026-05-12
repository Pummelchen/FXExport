import AppCore
import ClickHouse
import Domain
import Foundation
import MT5Bridge
import TimeMapping

public enum BrokerOffsetHistoricalPolicyAuthorityError: Error, CustomStringConvertible, Sendable {
    case invalidExistingRow(String)
    case liveSnapshotOutsideKnownPolicy(BrokerSourceId, BrokerServerIdentity, MT5ServerSecond)
    case liveSnapshotPolicyMismatch(observed: OffsetSeconds, policy: OffsetSeconds, serverTime: MT5ServerSecond)
    case existingVerifiedSegmentContradictsPolicy(
        brokerSourceId: BrokerSourceId,
        identity: BrokerServerIdentity,
        validFrom: MT5ServerSecond,
        validTo: MT5ServerSecond,
        existing: OffsetSeconds,
        policy: OffsetSeconds
    )

    public var description: String {
        switch self {
        case .invalidExistingRow(let row):
            return "Invalid broker_time_offsets row while applying automatic historical broker policy: \(row)"
        case .liveSnapshotOutsideKnownPolicy(let brokerSourceId, let identity, let serverTime):
            return "Automatic broker policy has no segment for live server timestamp \(serverTime.rawValue) for \(brokerSourceId.rawValue), \(identity)."
        case .liveSnapshotPolicyMismatch(let observed, let policy, let serverTime):
            return "EA-observed live broker UTC offset \(observed.rawValue) does not match automatic broker policy offset \(policy.rawValue) at server timestamp \(serverTime.rawValue)."
        case .existingVerifiedSegmentContradictsPolicy(let brokerSourceId, let identity, let validFrom, let validTo, let existing, let policy):
            return "Existing verified broker UTC offset segment \(validFrom.rawValue)..<\(validTo.rawValue) for \(brokerSourceId.rawValue), \(identity) has offset \(existing.rawValue), but automatic broker policy requires \(policy.rawValue)."
        }
    }
}

public struct BrokerOffsetHistoricalPolicyAuthority: Sendable {
    private let clickHouse: ClickHouseClientProtocol
    private let database: String
    private let logger: Logger?

    public init(clickHouse: ClickHouseClientProtocol, database: String, logger: Logger? = nil) {
        self.clickHouse = clickHouse
        self.database = database
        self.logger = logger
    }

    /// Adds verified, identity-bound historical broker offset rows only for brokers
    /// with code-owned policy and only after the EA live snapshot agrees with that
    /// policy. Unknown brokers remain fail-closed and require audited DB authority.
    @discardableResult
    public func ensureHistoricalCoverageIfKnown(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        requiredFrom: MT5ServerSecond,
        requiredToExclusive: MT5ServerSecond,
        liveSnapshot: ServerTimeSnapshotDTO,
        now: UtcSecond = UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))
    ) async throws -> Int {
        guard requiredFrom.rawValue < requiredToExclusive.rawValue else { return 0 }
        let policySegments = try BrokerOffsetPolicy.historicalSegments(
            for: terminalIdentity,
            brokerSourceId: brokerSourceId,
            covering: requiredFrom,
            to: requiredToExclusive
        )
        guard !policySegments.isEmpty else { return 0 }
        try verifyLiveSnapshotMatchesPolicy(
            brokerSourceId: brokerSourceId,
            terminalIdentity: terminalIdentity,
            snapshot: liveSnapshot
        )

        let existing = try await activeVerifiedSegments(
            brokerSourceId: brokerSourceId,
            terminalIdentity: terminalIdentity,
            overlappingFrom: requiredFrom,
            to: requiredToExclusive
        )
        var inserted = 0
        for segment in policySegments {
            try Self.validateExistingSegmentsAgreeWithPolicy(segment, existing: existing)
            let gaps = Self.subtractExisting(from: segment, existing: existing)
            for gap in gaps {
                try await insertPolicySegment(
                    brokerSourceId: brokerSourceId,
                    terminalIdentity: terminalIdentity,
                    validFrom: gap.validFrom,
                    validTo: gap.validTo,
                    offset: segment.offset,
                    policyName: BrokerOffsetPolicy.policyName(for: terminalIdentity) ?? "unknown",
                    liveSnapshot: liveSnapshot,
                    now: now
                )
                inserted += 1
            }
        }
        if inserted > 0 {
            logger?.ok("Automatic historical broker UTC authority recorded \(inserted) verified segment(s) for \(brokerSourceId.rawValue), \(terminalIdentity)")
        }
        return inserted
    }

    private func verifyLiveSnapshotMatchesPolicy(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        snapshot: ServerTimeSnapshotDTO
    ) throws {
        let observed = try BrokerOffsetRuntimeVerifier.observedOffset(from: snapshot)
        let serverTime = MT5ServerSecond(rawValue: snapshot.timeTradeServer)
        guard let policy = try BrokerOffsetPolicy.policyOffset(
            for: terminalIdentity,
            at: serverTime,
            brokerSourceId: brokerSourceId
        ) else {
            throw BrokerOffsetHistoricalPolicyAuthorityError.liveSnapshotOutsideKnownPolicy(
                brokerSourceId,
                terminalIdentity,
                serverTime
            )
        }
        guard observed == policy else {
            throw BrokerOffsetHistoricalPolicyAuthorityError.liveSnapshotPolicyMismatch(
                observed: observed,
                policy: policy,
                serverTime: serverTime
            )
        }
    }

    private func activeVerifiedSegments(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        overlappingFrom validFrom: MT5ServerSecond,
        to validTo: MT5ServerSecond
    ) async throws -> [HistoricalExistingOffsetSegment] {
        let body = try await clickHouse.execute(.select("""
        SELECT valid_from_mt5_server_ts, valid_to_mt5_server_ts, offset_seconds
        FROM \(database).broker_time_offsets
        WHERE broker_source_id = '\(Self.sqlLiteral(brokerSourceId.rawValue))'
          AND mt5_company = '\(Self.sqlLiteral(terminalIdentity.company))'
          AND mt5_server = '\(Self.sqlLiteral(terminalIdentity.server))'
          AND mt5_account_login = \(terminalIdentity.accountLogin)
          AND confidence = 'verified'
          AND is_active = 1
          AND valid_to_mt5_server_ts > \(validFrom.rawValue)
          AND valid_from_mt5_server_ts < \(validTo.rawValue)
        ORDER BY valid_from_mt5_server_ts ASC, valid_to_mt5_server_ts ASC
        FORMAT TabSeparated
        """))
        return try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try HistoricalExistingOffsetSegment.parse(String($0)) }
    }

    private func insertPolicySegment(
        brokerSourceId: BrokerSourceId,
        terminalIdentity: BrokerServerIdentity,
        validFrom: MT5ServerSecond,
        validTo: MT5ServerSecond,
        offset: OffsetSeconds,
        policyName: String,
        liveSnapshot: ServerTimeSnapshotDTO,
        now: UtcSecond
    ) async throws {
        let observedOffset = try BrokerOffsetRuntimeVerifier.observedOffset(from: liveSnapshot)
        let evidence = """
        source=automatic_broker_policy;policy=\(policyName);time_trade_server=\(liveSnapshot.timeTradeServer);time_gmt=\(liveSnapshot.timeGMT);observed_offset_seconds=\(observedOffset.rawValue)
        """
        let row = [
            Self.tsv(brokerSourceId.rawValue),
            Self.tsv(terminalIdentity.company),
            Self.tsv(terminalIdentity.server),
            String(terminalIdentity.accountLogin),
            String(validFrom.rawValue),
            String(validTo.rawValue),
            String(offset.rawValue),
            Self.tsv(OffsetSource.brokerPolicy.rawValue),
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

    private static func validateExistingSegmentsAgreeWithPolicy(
        _ policySegment: BrokerOffsetSegment,
        existing: [HistoricalExistingOffsetSegment]
    ) throws {
        for existingSegment in existing where existingSegment.overlaps(policySegment) {
            guard existingSegment.offset == policySegment.offset else {
                throw BrokerOffsetHistoricalPolicyAuthorityError.existingVerifiedSegmentContradictsPolicy(
                    brokerSourceId: policySegment.brokerSourceId,
                    identity: policySegment.terminalIdentity,
                    validFrom: existingSegment.validFrom,
                    validTo: existingSegment.validTo,
                    existing: existingSegment.offset,
                    policy: policySegment.offset
                )
            }
        }
    }

    private static func subtractExisting(
        from policySegment: BrokerOffsetSegment,
        existing: [HistoricalExistingOffsetSegment]
    ) -> [(validFrom: MT5ServerSecond, validTo: MT5ServerSecond)] {
        var gaps = [(validFrom: policySegment.validFrom, validTo: policySegment.validTo)]
        for existingSegment in existing {
            gaps = gaps.flatMap { gap in
                subtract(existingSegment, from: gap)
            }
        }
        return gaps.filter { $0.validFrom.rawValue < $0.validTo.rawValue }
    }

    private static func subtract(
        _ existing: HistoricalExistingOffsetSegment,
        from gap: (validFrom: MT5ServerSecond, validTo: MT5ServerSecond)
    ) -> [(validFrom: MT5ServerSecond, validTo: MT5ServerSecond)] {
        if existing.validTo.rawValue <= gap.validFrom.rawValue || existing.validFrom.rawValue >= gap.validTo.rawValue {
            return [gap]
        }
        var result: [(validFrom: MT5ServerSecond, validTo: MT5ServerSecond)] = []
        if existing.validFrom.rawValue > gap.validFrom.rawValue {
            result.append((gap.validFrom, MT5ServerSecond(rawValue: min(existing.validFrom.rawValue, gap.validTo.rawValue))))
        }
        if existing.validTo.rawValue < gap.validTo.rawValue {
            result.append((MT5ServerSecond(rawValue: max(existing.validTo.rawValue, gap.validFrom.rawValue)), gap.validTo))
        }
        return result
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

private struct HistoricalExistingOffsetSegment: Sendable {
    let validFrom: MT5ServerSecond
    let validTo: MT5ServerSecond
    let offset: OffsetSeconds

    func overlaps(_ segment: BrokerOffsetSegment) -> Bool {
        validTo.rawValue > segment.validFrom.rawValue && validFrom.rawValue < segment.validTo.rawValue
    }

    static func parse(_ row: String) throws -> HistoricalExistingOffsetSegment {
        let fields = row.split(separator: "\t", omittingEmptySubsequences: false)
        guard fields.count == 3,
              let validFrom = Int64(fields[0]),
              let validTo = Int64(fields[1]),
              let offset = Int64(fields[2]),
              validFrom < validTo else {
            throw BrokerOffsetHistoricalPolicyAuthorityError.invalidExistingRow(row)
        }
        return HistoricalExistingOffsetSegment(
            validFrom: MT5ServerSecond(rawValue: validFrom),
            validTo: MT5ServerSecond(rawValue: validTo),
            offset: OffsetSeconds(rawValue: offset)
        )
    }
}
