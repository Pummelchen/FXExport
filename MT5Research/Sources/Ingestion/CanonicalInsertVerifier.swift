import ClickHouse
import Domain
import Foundation

public struct CanonicalInsertVerificationResult: Sendable, Equatable {
    public let rowCount: Int
    public let expectedCanonicalSHA256: SHA256DigestHex
    public let canonicalReadbackSHA256: SHA256DigestHex

    public init(rowCount: Int, expectedCanonicalSHA256: SHA256DigestHex, canonicalReadbackSHA256: SHA256DigestHex) {
        self.rowCount = rowCount
        self.expectedCanonicalSHA256 = expectedCanonicalSHA256
        self.canonicalReadbackSHA256 = canonicalReadbackSHA256
    }
}

public struct CanonicalInsertVerifier: Sendable {
    private let clickHouse: ClickHouseClientProtocol
    private let insertBuilder: ClickHouseInsertBuilder

    public init(clickHouse: ClickHouseClientProtocol, insertBuilder: ClickHouseInsertBuilder) {
        self.clickHouse = clickHouse
        self.insertBuilder = insertBuilder
    }

    @discardableResult
    public func verify(
        _ bars: [ValidatedBar],
        mt5Start: MT5ServerSecond? = nil,
        mt5EndExclusive: MT5ServerSecond? = nil
    ) async throws -> CanonicalInsertVerificationResult {
        guard !bars.isEmpty else {
            let digest = ChunkHashing.emptySHA256(namespace: "canonical_insert_verifier_empty")
            return CanonicalInsertVerificationResult(rowCount: 0, expectedCanonicalSHA256: digest, canonicalReadbackSHA256: digest)
        }
        let body = try await clickHouse.execute(try insertBuilder.canonicalRangeIntegrityCheck(
            bars,
            mt5Start: mt5Start,
            mt5EndExclusive: mt5EndExclusive
        ))
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

        let rowsBody = try await clickHouse.execute(try insertBuilder.canonicalRangeReadbackRows(
            bars,
            mt5Start: mt5Start,
            mt5EndExclusive: mt5EndExclusive
        ))
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
                  row.serverUtcOffset == bar.serverUtcOffset,
                  row.offsetSource == bar.offsetSource,
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
        guard let first = bars.first, let last = bars.last else {
            throw IngestError.canonicalInsertVerificationFailed("canonical readback hash requested for empty range")
        }
        let hashStart = mt5Start ?? first.mt5ServerTime
        let hashEndExclusive: MT5ServerSecond
        if let mt5EndExclusive {
            hashEndExclusive = mt5EndExclusive
        } else {
            hashEndExclusive = try addOneMinute(to: last.mt5ServerTime)
        }
        guard hashStart.rawValue <= first.mt5ServerTime.rawValue,
              hashEndExclusive.rawValue > last.mt5ServerTime.rawValue else {
            throw IngestError.canonicalInsertVerificationFailed(
                "canonical hash envelope \(hashStart.rawValue)..<\(hashEndExclusive.rawValue) does not cover readback bars"
            )
        }
        let expectedHash = ChunkHashing.canonicalSHA256(
            brokerSourceId: first.brokerSourceId,
            logicalSymbol: first.logicalSymbol,
            mt5Symbol: first.mt5Symbol,
            timeframe: first.timeframe,
            mt5Start: hashStart,
            mt5EndExclusive: hashEndExclusive,
            bars: bars
        )
        let readbackHash = ChunkHashing.canonicalSHA256(
            brokerSourceId: first.brokerSourceId,
            logicalSymbol: first.logicalSymbol,
            mt5Symbol: first.mt5Symbol,
            timeframe: first.timeframe,
            mt5Start: hashStart,
            mt5EndExclusive: hashEndExclusive,
            rows: rows.map(\.canonicalHashRow)
        )
        guard expectedHash == readbackHash else {
            throw IngestError.canonicalInsertVerificationFailed(
                "SHA-256 canonical readback mismatch: expected \(expectedHash.rawValue), got \(readbackHash.rawValue)"
            )
        }
        return CanonicalInsertVerificationResult(
            rowCount: rows.count,
            expectedCanonicalSHA256: expectedHash,
            canonicalReadbackSHA256: readbackHash
        )
    }

    @discardableResult
    public func verifyEmptyMT5Range(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        mt5Start: MT5ServerSecond,
        mt5EndExclusive: MT5ServerSecond
    ) async throws -> CanonicalInsertVerificationResult {
        let rowsBody = try await clickHouse.execute(insertBuilder.canonicalMT5RangeReadbackRows(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            mt5Start: mt5Start,
            mt5EndExclusive: mt5EndExclusive
        ))
        let rows = try parseRows(rowsBody)
        guard rows.isEmpty else {
            throw IngestError.canonicalInsertVerificationFailed(
                "MT5 source gap \(mt5Start.rawValue)..<\(mt5EndExclusive.rawValue) is not empty in canonical storage; read back \(rows.count) stale row(s)"
            )
        }
        let digest = ChunkHashing.canonicalSHA256(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            mt5Symbol: mt5Symbol,
            timeframe: .m1,
            mt5Start: mt5Start,
            mt5EndExclusive: mt5EndExclusive,
            rows: []
        )
        return CanonicalInsertVerificationResult(
            rowCount: 0,
            expectedCanonicalSHA256: digest,
            canonicalReadbackSHA256: digest
        )
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
            guard fields.count == 13,
                  let mt5Symbol = MT5Symbol(rawValue: fields[0]),
                  let timeframe = Timeframe(rawValue: fields[1]),
                  let mt5 = Int64(fields[2]),
                  let utc = Int64(fields[3]),
                  let offset = Int64(fields[4]),
                  let offsetSource = OffsetSource(rawValue: fields[5]),
                  let offsetConfidence = OffsetConfidence(rawValue: fields[6]),
                  let open = Int64(fields[7]),
                  let high = Int64(fields[8]),
                  let low = Int64(fields[9]),
                  let close = Int64(fields[10]),
                  let digitsValue = Int(fields[11]),
                  let hashValue = UInt64(fields[12], radix: 16) else {
                throw IngestError.canonicalInsertVerificationFailed("invalid ClickHouse canonical readback row '\(row)'")
            }
            let digits = try Digits(digitsValue)
            return CanonicalRangeRow(
                mt5Symbol: mt5Symbol,
                timeframe: timeframe,
                mt5ServerTime: MT5ServerSecond(rawValue: mt5),
                utcTime: UtcSecond(rawValue: utc),
                serverUtcOffset: OffsetSeconds(rawValue: offset),
                offsetSource: offsetSource,
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

    private func addOneMinute(to value: MT5ServerSecond) throws -> MT5ServerSecond {
        let result = value.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw IngestError.canonicalInsertVerificationFailed("MT5 server timestamp overflow while hashing canonical readback")
        }
        return MT5ServerSecond(rawValue: result.partialValue)
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
    let serverUtcOffset: OffsetSeconds
    let offsetSource: OffsetSource
    let offsetConfidence: OffsetConfidence
    let open: Int64
    let high: Int64
    let low: Int64
    let close: Int64
    let digits: Digits
    let barHash: BarHash

    var canonicalHashRow: CanonicalChunkHashRow {
        CanonicalChunkHashRow(
            mt5ServerTime: mt5ServerTime,
            utcTime: utcTime,
            serverUtcOffset: serverUtcOffset,
            offsetSource: offsetSource,
            offsetConfidence: offsetConfidence,
            openScaled: open,
            highScaled: high,
            lowScaled: low,
            closeScaled: close,
            digits: digits,
            barHash: barHash
        )
    }
}
