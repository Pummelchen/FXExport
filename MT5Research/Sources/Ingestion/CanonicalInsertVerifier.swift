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
}

private struct CanonicalRangeReadback: Sendable {
    let rowCount: Int
    let uniqueMT5ServerTimes: Int
    let uniqueUTCTimes: Int
}
