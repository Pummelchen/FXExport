import ClickHouse
import Domain
import Foundation
import Ingestion

public struct DataCertificate: Sendable, Equatable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let utcStart: UtcSecond
    public let utcEndExclusive: UtcSecond
    public let certificateSHA256: SHA256DigestHex
    public let coverageRowCount: UInt32
    public let coverageSourceBarCount: UInt64
    public let coverageCanonicalRowCount: UInt64
    public let firstCoveredUtc: UtcSecond
    public let lastCoveredUtc: UtcSecond
}

public enum DataCertificateError: Error, CustomStringConvertible, Sendable {
    case missingVerifiedCoverage(LogicalSymbol, UtcSecond, UtcSecond)
    case invalidCoverageRow(String)
    case incompleteVerifiedCoverage(LogicalSymbol, UtcSecond, UtcSecond)
    case inconsistentCoverageCounts(LogicalSymbol, String)
    case invalidRange(UtcSecond, UtcSecond)

    public var description: String {
        switch self {
        case .missingVerifiedCoverage(let symbol, let start, let end):
            return "\(symbol.rawValue) has no verified coverage to certify for UTC range \(start.rawValue)..<\(end.rawValue)."
        case .invalidCoverageRow(let row):
            return "Invalid verified coverage row while building data certificate: \(row)"
        case .incompleteVerifiedCoverage(let symbol, let start, let end):
            return "\(symbol.rawValue) verified coverage is incomplete and cannot be certified for UTC range \(start.rawValue)..<\(end.rawValue)."
        case .inconsistentCoverageCounts(let symbol, let row):
            return "\(symbol.rawValue) verified coverage row has inconsistent MT5/canonical counts and cannot be certified: \(row)"
        case .invalidRange(let start, let end):
            return "Cannot certify invalid UTC range \(start.rawValue)..<\(end.rawValue)."
        }
    }
}

public struct DataCertificateStore: Sendable {
    private let clickHouse: ClickHouseClientProtocol
    private let database: String

    public init(clickHouse: ClickHouseClientProtocol, database: String) {
        self.clickHouse = clickHouse
        self.database = database
    }

    @discardableResult
    public func certify(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        utcStart: UtcSecond,
        utcEndExclusive: UtcSecond,
        createdAtUtc: UtcSecond = utcNow()
    ) async throws -> DataCertificate {
        guard utcStart.rawValue < utcEndExclusive.rawValue,
              utcStart.isMinuteAligned,
              utcEndExclusive.isMinuteAligned else {
            throw DataCertificateError.invalidRange(utcStart, utcEndExclusive)
        }
        let rows = try await loadCoverageRows(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            utcStart: utcStart,
            utcEndExclusive: utcEndExclusive
        )
        guard !rows.isEmpty else {
            throw DataCertificateError.missingVerifiedCoverage(logicalSymbol, utcStart, utcEndExclusive)
        }
        let selectedRows = try selectCompleteCoverageRows(
            rows: rows,
            logicalSymbol: logicalSymbol,
            utcStart: utcStart,
            utcEndExclusive: utcEndExclusive
        )
        let certificate = buildCertificate(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            utcStart: utcStart,
            utcEndExclusive: utcEndExclusive,
            rows: selectedRows
        )
        try await insert(certificate, rows: selectedRows, createdAtUtc: createdAtUtc)
        return certificate
    }

    private func loadCoverageRows(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        utcStart: UtcSecond,
        utcEndExclusive: UtcSecond
    ) async throws -> [CoverageCertificateRow] {
        let body = try await clickHouse.execute(.select("""
        SELECT mt5_symbol, timeframe, mt5_range_start, mt5_range_end_exclusive,
               utc_range_start, utc_range_end_exclusive,
               source_bar_count, canonical_row_count,
               mt5_source_sha256, canonical_readback_sha256, offset_authority_sha256,
               batch_id, verified_at_utc
        FROM \(database).ohlc_m1_verified_coverage
        WHERE broker_source_id = '\(SQLText.literal(brokerSourceId.rawValue))'
          AND logical_symbol = '\(SQLText.literal(logicalSymbol.rawValue))'
          AND timeframe = 'M1'
          AND hash_schema_version = '\(SQLText.literal(ChunkHashSchemaVersion.sha256V1))'
          AND length(mt5_source_sha256) = 64
          AND length(canonical_readback_sha256) = 64
          AND length(offset_authority_sha256) = 64
          AND utc_range_end_exclusive > \(utcStart.rawValue)
          AND utc_range_start < \(utcEndExclusive.rawValue)
        ORDER BY utc_range_start ASC, utc_range_end_exclusive ASC, verified_at_utc ASC
        FORMAT TabSeparated
        """))
        return try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try CoverageCertificateRow.parse(String($0)) }
    }

    private func selectCompleteCoverageRows(
        rows: [CoverageCertificateRow],
        logicalSymbol: LogicalSymbol,
        utcStart: UtcSecond,
        utcEndExclusive: UtcSecond
    ) throws -> [CoverageCertificateRow] {
        for row in rows {
            try validateCoverageRow(row, logicalSymbol: logicalSymbol)
        }
        let sorted = rows.sorted { lhs, rhs in
            if lhs.utcStart != rhs.utcStart { return lhs.utcStart < rhs.utcStart }
            if lhs.utcEndExclusive != rhs.utcEndExclusive { return lhs.utcEndExclusive > rhs.utcEndExclusive }
            return lhs.verifiedAtUtc > rhs.verifiedAtUtc
        }
        var cursor = utcStart.rawValue
        var selected: [CoverageCertificateRow] = []
        while cursor < utcEndExclusive.rawValue {
            let candidates = sorted.filter { $0.utcStart <= cursor && $0.utcEndExclusive > cursor }
            guard let best = candidates.max(by: { lhs, rhs in
                if lhs.utcEndExclusive != rhs.utcEndExclusive { return lhs.utcEndExclusive < rhs.utcEndExclusive }
                return lhs.verifiedAtUtc < rhs.verifiedAtUtc
            }) else {
                throw DataCertificateError.incompleteVerifiedCoverage(
                    logicalSymbol,
                    UtcSecond(rawValue: cursor),
                    utcEndExclusive
                )
            }
            selected.append(best)
            cursor = max(cursor, best.utcEndExclusive)
        }
        return selected
    }

    private func validateCoverageRow(_ row: CoverageCertificateRow, logicalSymbol: LogicalSymbol) throws {
        // A verified MT5 source gap is valid evidence when both source and
        // canonical counts are zero. Reject only mismatched counts; the range
        // still has cryptographic source/readback/offset hashes.
        guard row.sourceBarCount == row.canonicalRowCount else {
            throw DataCertificateError.inconsistentCoverageCounts(logicalSymbol, row.rawRow)
        }
        guard row.timeframe == Timeframe.m1.rawValue,
              row.utcStart < row.utcEndExclusive,
              row.utcStart % 60 == 0,
              row.utcEndExclusive % 60 == 0 else {
            throw DataCertificateError.invalidCoverageRow(row.rawRow)
        }
    }

    private func buildCertificate(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        utcStart: UtcSecond,
        utcEndExclusive: UtcSecond,
        rows: [CoverageCertificateRow]
    ) -> DataCertificate {
        let sourceAggregate = ChunkHashing.combinedSHA256(namespace: "certificate_mt5_source_hashes", values: rows.map(\.mt5SourceSHA256))
        let canonicalAggregate = ChunkHashing.combinedSHA256(namespace: "certificate_canonical_readback_hashes", values: rows.map(\.canonicalReadbackSHA256))
        let offsetAggregate = ChunkHashing.combinedSHA256(namespace: "certificate_offset_authority_hashes", values: rows.map(\.offsetAuthoritySHA256))

        var hasher = SHA256ChunkHasher(namespace: "fxexport_m1_data_certificate")
        hasher.appendField("broker_source_id", brokerSourceId.rawValue)
        hasher.appendField("logical_symbol", logicalSymbol.rawValue)
        hasher.appendField("timeframe", Timeframe.m1.rawValue)
        hasher.appendField("utc_range_start", utcStart.rawValue)
        hasher.appendField("utc_range_end_exclusive", utcEndExclusive.rawValue)
        hasher.appendField("coverage_row_count", rows.count)
        hasher.appendField("mt5_source_sha256_aggregate", sourceAggregate.rawValue)
        hasher.appendField("canonical_readback_sha256_aggregate", canonicalAggregate.rawValue)
        hasher.appendField("offset_authority_sha256_aggregate", offsetAggregate.rawValue)
        for (index, row) in rows.enumerated() {
            hasher.appendField("coverage_index", index)
            hasher.appendField("mt5_symbol", row.mt5Symbol)
            hasher.appendField("mt5_range_start", row.mt5Start)
            hasher.appendField("mt5_range_end_exclusive", row.mt5EndExclusive)
            hasher.appendField("utc_range_start", row.utcStart)
            hasher.appendField("utc_range_end_exclusive", row.utcEndExclusive)
            hasher.appendField("source_bar_count", Int64(row.sourceBarCount))
            hasher.appendField("canonical_row_count", Int64(row.canonicalRowCount))
            hasher.appendField("batch_id", row.batchId)
            hasher.appendField("verified_at_utc", row.verifiedAtUtc)
        }

        return DataCertificate(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            utcStart: utcStart,
            utcEndExclusive: utcEndExclusive,
            certificateSHA256: hasher.finalize(),
            coverageRowCount: UInt32(rows.count),
            coverageSourceBarCount: rows.reduce(UInt64(0)) { $0 + UInt64($1.sourceBarCount) },
            coverageCanonicalRowCount: rows.reduce(UInt64(0)) { $0 + UInt64($1.canonicalRowCount) },
            firstCoveredUtc: utcStart,
            lastCoveredUtc: UtcSecond(rawValue: utcEndExclusive.rawValue - 60)
        )
    }

    private func insert(_ certificate: DataCertificate, rows: [CoverageCertificateRow], createdAtUtc: UtcSecond) async throws {
        let sourceAggregate = ChunkHashing.combinedSHA256(namespace: "certificate_mt5_source_hashes", values: rows.map(\.mt5SourceSHA256))
        let canonicalAggregate = ChunkHashing.combinedSHA256(namespace: "certificate_canonical_readback_hashes", values: rows.map(\.canonicalReadbackSHA256))
        let offsetAggregate = ChunkHashing.combinedSHA256(namespace: "certificate_offset_authority_hashes", values: rows.map(\.offsetAuthoritySHA256))
        let fields = [
            tsv(certificate.brokerSourceId.rawValue),
            tsv(certificate.logicalSymbol.rawValue),
            tsv(Timeframe.m1.rawValue),
            String(certificate.utcStart.rawValue),
            String(certificate.utcEndExclusive.rawValue),
            tsv(certificate.certificateSHA256.rawValue),
            tsv(ChunkHashSchemaVersion.sha256V1),
            String(certificate.coverageRowCount),
            String(certificate.coverageSourceBarCount),
            String(certificate.coverageCanonicalRowCount),
            String(certificate.firstCoveredUtc.rawValue),
            String(certificate.lastCoveredUtc.rawValue),
            tsv(sourceAggregate.rawValue),
            tsv(canonicalAggregate.rawValue),
            tsv(offsetAggregate.rawValue),
            tsv("valid"),
            String(createdAtUtc.rawValue)
        ].joined(separator: "\t")
        _ = try await clickHouse.execute(.mutation("""
        INSERT INTO \(database).data_certificates
        (broker_source_id, logical_symbol, timeframe,
         utc_range_start, utc_range_end_exclusive, certificate_sha256,
         hash_schema_version, coverage_row_count, coverage_source_bar_count,
         coverage_canonical_row_count, first_covered_utc, last_covered_utc,
         mt5_source_sha256_aggregate, canonical_readback_sha256_aggregate,
         offset_authority_sha256_aggregate, certificate_status, created_at_utc)
        FORMAT TabSeparated
        \(fields)
        """, idempotent: true))
    }

    private func tsv(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\t", with: "\\t")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }
}

private struct CoverageCertificateRow: Sendable {
    let rawRow: String
    let mt5Symbol: String
    let timeframe: String
    let mt5Start: Int64
    let mt5EndExclusive: Int64
    let utcStart: Int64
    let utcEndExclusive: Int64
    let sourceBarCount: UInt32
    let canonicalRowCount: UInt32
    let mt5SourceSHA256: SHA256DigestHex
    let canonicalReadbackSHA256: SHA256DigestHex
    let offsetAuthoritySHA256: SHA256DigestHex
    let batchId: String
    let verifiedAtUtc: Int64

    static func parse(_ row: String) throws -> CoverageCertificateRow {
        let fields = row.split(separator: "\t", omittingEmptySubsequences: false).map(String.init)
        guard fields.count == 13,
              let mt5Start = Int64(fields[2]),
              let mt5End = Int64(fields[3]),
              let utcStart = Int64(fields[4]),
              let utcEnd = Int64(fields[5]),
              let sourceBarCount = UInt32(fields[6]),
              let canonicalRowCount = UInt32(fields[7]),
              let mt5SourceSHA256 = SHA256DigestHex(rawValue: fields[8]),
              let canonicalReadbackSHA256 = SHA256DigestHex(rawValue: fields[9]),
              let offsetAuthoritySHA256 = SHA256DigestHex(rawValue: fields[10]),
              let verifiedAtUtc = Int64(fields[12]) else {
            throw DataCertificateError.invalidCoverageRow(row)
        }
        return CoverageCertificateRow(
            rawRow: row,
            mt5Symbol: fields[0],
            timeframe: fields[1],
            mt5Start: mt5Start,
            mt5EndExclusive: mt5End,
            utcStart: utcStart,
            utcEndExclusive: utcEnd,
            sourceBarCount: sourceBarCount,
            canonicalRowCount: canonicalRowCount,
            mt5SourceSHA256: mt5SourceSHA256,
            canonicalReadbackSHA256: canonicalReadbackSHA256,
            offsetAuthoritySHA256: offsetAuthoritySHA256,
            batchId: fields[11],
            verifiedAtUtc: verifiedAtUtc
        )
    }
}
