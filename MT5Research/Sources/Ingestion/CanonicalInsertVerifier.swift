import ClickHouse
import Domain
import Foundation

public struct CanonicalInsertVerifier: Sendable {
    private let clickHouse: ClickHouseClientProtocol
    private let insertBuilder: ClickHouseInsertBuilder

    public init(clickHouse: ClickHouseClientProtocol, insertBuilder: ClickHouseInsertBuilder) {
        self.clickHouse = clickHouse
        self.insertBuilder = insertBuilder
    }

    public func verify(_ bars: [ValidatedBar]) async throws {
        guard !bars.isEmpty else { return }
        let body = try await clickHouse.execute(try insertBuilder.canonicalRangeIntegrityCheck(bars))
        let readback = try parseReadback(body)
        let expected = bars.count
        guard readback.rowCount == expected else {
            throw IngestError.canonicalInsertVerificationFailed("expected \(expected) canonical rows, read back \(readback.rowCount)")
        }
        guard readback.uniqueMT5ServerTimes == expected else {
            throw IngestError.canonicalInsertVerificationFailed("expected \(expected) unique MT5 server timestamps, read back \(readback.uniqueMT5ServerTimes)")
        }
        guard readback.uniqueUTCTimes == expected else {
            throw IngestError.canonicalInsertVerificationFailed("expected \(expected) unique UTC timestamps, read back \(readback.uniqueUTCTimes)")
        }

        let rowsBody = try await clickHouse.execute(try insertBuilder.canonicalRangeReadbackRows(bars))
        let rows = try parseRows(rowsBody)
        guard rows.count == bars.count else {
            throw IngestError.canonicalInsertVerificationFailed("expected \(bars.count) readback rows, got \(rows.count)")
        }
        for (index, pair) in zip(rows, bars).enumerated() {
            let (row, bar) = pair
            guard row.mt5ServerTime == bar.mt5ServerTime,
                  row.utcTime == bar.utcTime,
                  row.barHash == bar.barHash.description else {
                throw IngestError.canonicalInsertVerificationFailed(
                    "row \(index) mismatch after insert: expected mt5=\(bar.mt5ServerTime.rawValue), utc=\(bar.utcTime.rawValue), hash=\(bar.barHash.description); got mt5=\(row.mt5ServerTime.rawValue), utc=\(row.utcTime.rawValue), hash=\(row.barHash)"
                )
            }
        }
    }

    private func parseReadback(_ body: String) throws -> CanonicalRangeReadback {
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        let fields = trimmed.split(separator: "\t", omittingEmptySubsequences: false).map(String.init)
        guard fields.count == 3,
              let rowCount = Int(fields[0]),
              let uniqueMT5ServerTimes = Int(fields[1]),
              let uniqueUTCTimes = Int(fields[2]) else {
            throw IngestError.canonicalInsertVerificationFailed("invalid ClickHouse readback row '\(trimmed)'")
        }
        return CanonicalRangeReadback(
            rowCount: rowCount,
            uniqueMT5ServerTimes: uniqueMT5ServerTimes,
            uniqueUTCTimes: uniqueUTCTimes
        )
    }

    private func parseRows(_ body: String) throws -> [CanonicalRangeRow] {
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return [] }
        return try trimmed.split(separator: "\n", omittingEmptySubsequences: true).map { row in
            let fields = row.split(separator: "\t", omittingEmptySubsequences: false).map(String.init)
            guard fields.count == 3,
                  let mt5 = Int64(fields[0]),
                  let utc = Int64(fields[1]) else {
                throw IngestError.canonicalInsertVerificationFailed("invalid ClickHouse canonical readback row '\(row)'")
            }
            return CanonicalRangeRow(
                mt5ServerTime: MT5ServerSecond(rawValue: mt5),
                utcTime: UtcSecond(rawValue: utc),
                barHash: fields[2]
            )
        }
    }
}

private struct CanonicalRangeReadback: Sendable {
    let rowCount: Int
    let uniqueMT5ServerTimes: Int
    let uniqueUTCTimes: Int
}

private struct CanonicalRangeRow: Sendable {
    let mt5ServerTime: MT5ServerSecond
    let utcTime: UtcSecond
    let barHash: String
}
