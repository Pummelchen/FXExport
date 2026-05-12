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
        guard readback.uniqueMT5Symbols == 1 else {
            throw IngestError.canonicalInsertVerificationFailed("expected one MT5 symbol in canonical readback, got \(readback.uniqueMT5Symbols)")
        }
        guard readback.uniqueTimeframes == 1 else {
            throw IngestError.canonicalInsertVerificationFailed("expected one timeframe in canonical readback, got \(readback.uniqueTimeframes)")
        }
        guard readback.uniqueDigits == 1 else {
            throw IngestError.canonicalInsertVerificationFailed("expected one digits value in canonical readback, got \(readback.uniqueDigits)")
        }
        guard readback.nonVerifiedOffsetRows == 0 else {
            throw IngestError.canonicalInsertVerificationFailed("canonical readback contains \(readback.nonVerifiedOffsetRows) non-verified UTC offset row(s)")
        }

        let rowsBody = try await clickHouse.execute(try insertBuilder.canonicalRangeReadbackRows(bars))
        let rows = try parseRows(rowsBody)
        guard rows.count == bars.count else {
            throw IngestError.canonicalInsertVerificationFailed("expected \(bars.count) readback rows, got \(rows.count)")
        }
        for (index, pair) in zip(rows, bars).enumerated() {
            let (row, bar) = pair
            guard row.mt5Symbol == bar.mt5Symbol,
                  row.timeframe == bar.timeframe,
                  row.mt5ServerTime == bar.mt5ServerTime,
                  row.utcTime == bar.utcTime,
                  row.offsetConfidence == bar.offsetConfidence,
                  row.open == bar.open.rawValue,
                  row.high == bar.high.rawValue,
                  row.low == bar.low.rawValue,
                  row.close == bar.close.rawValue,
                  row.digits == bar.digits,
                  row.barHash == bar.barHash else {
                throw IngestError.canonicalInsertVerificationFailed(
                    "row \(index) mismatch after insert: expected symbol=\(bar.mt5Symbol.rawValue), mt5=\(bar.mt5ServerTime.rawValue), utc=\(bar.utcTime.rawValue), digits=\(bar.digits.rawValue), hash=\(bar.barHash.description); got symbol=\(row.mt5Symbol.rawValue), mt5=\(row.mt5ServerTime.rawValue), utc=\(row.utcTime.rawValue), digits=\(row.digits.rawValue), hash=\(row.barHash.description)"
                )
            }
        }
    }

    private func parseReadback(_ body: String) throws -> CanonicalRangeReadback {
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        let fields = trimmed.split(separator: "\t", omittingEmptySubsequences: false).map(String.init)
        guard fields.count == 7,
              let rowCount = Int(fields[0]),
              let uniqueMT5ServerTimes = Int(fields[1]),
              let uniqueUTCTimes = Int(fields[2]),
              let uniqueMT5Symbols = Int(fields[3]),
              let uniqueTimeframes = Int(fields[4]),
              let uniqueDigits = Int(fields[5]),
              let nonVerifiedOffsetRows = Int(fields[6]) else {
            throw IngestError.canonicalInsertVerificationFailed("invalid ClickHouse readback row '\(trimmed)'")
        }
        return CanonicalRangeReadback(
            rowCount: rowCount,
            uniqueMT5ServerTimes: uniqueMT5ServerTimes,
            uniqueUTCTimes: uniqueUTCTimes,
            uniqueMT5Symbols: uniqueMT5Symbols,
            uniqueTimeframes: uniqueTimeframes,
            uniqueDigits: uniqueDigits,
            nonVerifiedOffsetRows: nonVerifiedOffsetRows
        )
    }

    private func parseRows(_ body: String) throws -> [CanonicalRangeRow] {
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return [] }
        return try trimmed.split(separator: "\n", omittingEmptySubsequences: true).map { row in
            let fields = row.split(separator: "\t", omittingEmptySubsequences: false).map { Self.unescapeTabSeparated(String($0)) }
            guard fields.count == 11,
                  let mt5Symbol = MT5Symbol(rawValue: fields[0]),
                  let timeframe = Timeframe(rawValue: fields[1]),
                  let mt5 = Int64(fields[2]),
                  let utc = Int64(fields[3]),
                  let offsetConfidence = OffsetConfidence(rawValue: fields[4]),
                  let open = Int64(fields[5]),
                  let high = Int64(fields[6]),
                  let low = Int64(fields[7]),
                  let close = Int64(fields[8]),
                  let digitsValue = Int(fields[9]),
                  let hashValue = UInt64(fields[10], radix: 16) else {
                throw IngestError.canonicalInsertVerificationFailed("invalid ClickHouse canonical readback row '\(row)'")
            }
            let digits = try Digits(digitsValue)
            return CanonicalRangeRow(
                mt5Symbol: mt5Symbol,
                timeframe: timeframe,
                mt5ServerTime: MT5ServerSecond(rawValue: mt5),
                utcTime: UtcSecond(rawValue: utc),
                offsetConfidence: offsetConfidence,
                open: open,
                high: high,
                low: low,
                close: close,
                digits: digits,
                barHash: BarHash(rawValue: hashValue)
            )
        }
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

private struct CanonicalRangeReadback: Sendable {
    let rowCount: Int
    let uniqueMT5ServerTimes: Int
    let uniqueUTCTimes: Int
    let uniqueMT5Symbols: Int
    let uniqueTimeframes: Int
    let uniqueDigits: Int
    let nonVerifiedOffsetRows: Int
}

private struct CanonicalRangeRow: Sendable {
    let mt5Symbol: MT5Symbol
    let timeframe: Timeframe
    let mt5ServerTime: MT5ServerSecond
    let utcTime: UtcSecond
    let offsetConfidence: OffsetConfidence
    let open: Int64
    let high: Int64
    let low: Int64
    let close: Int64
    let digits: Digits
    let barHash: BarHash
}
