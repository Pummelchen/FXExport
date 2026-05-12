import ClickHouse
import Domain
import Foundation

public struct CanonicalConflictRecorder: Sendable {
    private let clickHouse: ClickHouseClientProtocol
    private let insertBuilder: ClickHouseInsertBuilder

    public init(clickHouse: ClickHouseClientProtocol, insertBuilder: ClickHouseInsertBuilder) {
        self.clickHouse = clickHouse
        self.insertBuilder = insertBuilder
    }

    public func recordConflictsBeforeCanonicalReplace(_ bars: [ValidatedBar], detectedAtUtc: UtcSecond) async throws {
        guard let first = bars.first else { return }
        let body = try await clickHouse.execute(try insertBuilder.canonicalConflictCandidates(bars))
        let candidates = try Self.parseCandidates(body, digits: first.digits)
        guard !candidates.isEmpty else { return }

        var incomingByUtc: [UtcSecond: ValidatedBar] = [:]
        incomingByUtc.reserveCapacity(bars.count)
        for bar in bars {
            incomingByUtc[bar.utcTime] = bar
        }
        let conflicts = candidates.compactMap { candidate -> CanonicalConflictRow? in
            guard let incoming = incomingByUtc[candidate.utcTime],
                  candidate.barHash != incoming.barHash.description else {
                return nil
            }
            return CanonicalConflictRow(
                brokerSourceId: incoming.brokerSourceId,
                logicalSymbol: incoming.logicalSymbol,
                mt5Symbol: incoming.mt5Symbol,
                utcTime: incoming.utcTime,
                existingBarHash: candidate.barHash,
                incomingBarHash: incoming.barHash.description,
                existingOpen: candidate.open,
                existingHigh: candidate.high,
                existingLow: candidate.low,
                existingClose: candidate.close,
                incomingOpen: incoming.open,
                incomingHigh: incoming.high,
                incomingLow: incoming.low,
                incomingClose: incoming.close,
                detectedAtUtc: detectedAtUtc,
                batchId: incoming.batchId
            )
        }
        guard !conflicts.isEmpty else { return }
        _ = try await clickHouse.execute(insertBuilder.conflictRowsInsert(conflicts))
    }

    static func parseCandidates(_ body: String, digits: Digits) throws -> [CanonicalConflictCandidate] {
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return [] }
        return try trimmed.split(separator: "\n", omittingEmptySubsequences: true).map { row in
            let fields = row.split(separator: "\t", omittingEmptySubsequences: false).map(String.init)
            guard fields.count == 6,
                  let utc = Int64(fields[0]),
                  let open = Int64(fields[2]),
                  let high = Int64(fields[3]),
                  let low = Int64(fields[4]),
                  let close = Int64(fields[5]) else {
                throw IngestError.canonicalInsertVerificationFailed("invalid canonical conflict candidate row '\(row)'")
            }
            return CanonicalConflictCandidate(
                utcTime: UtcSecond(rawValue: utc),
                barHash: fields[1],
                open: PriceScaled(rawValue: open, digits: digits),
                high: PriceScaled(rawValue: high, digits: digits),
                low: PriceScaled(rawValue: low, digits: digits),
                close: PriceScaled(rawValue: close, digits: digits)
            )
        }
    }
}

public struct CanonicalConflictCandidate: Sendable, Hashable {
    public let utcTime: UtcSecond
    public let barHash: String
    public let open: PriceScaled
    public let high: PriceScaled
    public let low: PriceScaled
    public let close: PriceScaled
}
