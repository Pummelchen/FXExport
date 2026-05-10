import Domain
import Foundation

public enum ClickHouseInsertError: Error, CustomStringConvertible, Sendable {
    case mixedCanonicalRange
    case unsortedCanonicalRange
    case unsortedCanonicalUTCRange
    case unverifiedCanonicalBar(MT5ServerSecond, OffsetConfidence)

    public var description: String {
        switch self {
        case .mixedCanonicalRange:
            return "Canonical delete range contains multiple broker/source identities."
        case .unsortedCanonicalRange:
            return "Canonical delete range is not sorted by MT5 server timestamp."
        case .unsortedCanonicalUTCRange:
            return "Canonical range is not sorted by UTC timestamp."
        case .unverifiedCanonicalBar(let time, let confidence):
            return "Canonical bar \(time.rawValue) has \(confidence.rawValue) UTC offset confidence."
        }
    }
}

public struct ClickHouseInsertBuilder: Sendable {
    private let database: String

    public init(database: String) {
        self.database = database
    }

    public func rawBarsInsert(_ bars: [ValidatedBar]) -> ClickHouseQuery {
        var sql = """
        INSERT INTO \(database).mt5_ohlc_m1_raw (
            broker_source_id, logical_symbol, mt5_symbol, timeframe, mt5_server_ts_raw, ts_utc,
            server_utc_offset_seconds, offset_source, offset_confidence,
            open_scaled, high_scaled, low_scaled, close_scaled, digits,
            batch_id, bar_hash, source_status, ingested_at_utc
        ) FORMAT TabSeparated
        """
        sql += "\n"
        sql += bars.map(rawRow).joined(separator: "\n")
        return ClickHouseQuery.mutation(sql, idempotent: false)
    }

    public func canonicalBarsInsert(_ bars: [ValidatedBar]) throws -> ClickHouseQuery {
        _ = try canonicalRangeIdentity(bars)
        var sql = """
        INSERT INTO \(database).ohlc_m1_canonical (
            broker_source_id, logical_symbol, mt5_symbol, timeframe, mt5_server_ts_raw, ts_utc,
            server_utc_offset_seconds, offset_source, offset_confidence,
            open_scaled, high_scaled, low_scaled, close_scaled, digits,
            batch_id, bar_hash, source_status, ingested_at_utc
        ) FORMAT TabSeparated
        """
        sql += "\n"
        sql += bars.map(rawRow).joined(separator: "\n")
        return ClickHouseQuery.mutation(sql, idempotent: false)
    }

    public func canonicalRangeDelete(_ bars: [ValidatedBar]) throws -> ClickHouseQuery {
        let range = try canonicalRangeIdentity(bars)
        guard let first = range.first, let last = range.last else {
            return ClickHouseQuery.mutation("SELECT 1", idempotent: true)
        }
        let sql = """
        ALTER TABLE \(database).ohlc_m1_canonical DELETE
        WHERE broker_source_id = '\(sqlLiteral(first.brokerSourceId.rawValue))'
          AND logical_symbol = '\(sqlLiteral(first.logicalSymbol.rawValue))'
          AND mt5_server_ts_raw >= \(first.mt5ServerTime.rawValue)
          AND mt5_server_ts_raw <= \(last.mt5ServerTime.rawValue)
        SETTINGS mutations_sync = 1
        """
        return ClickHouseQuery.mutation(sql, idempotent: true)
    }

    public func canonicalRangeIntegrityCheck(_ bars: [ValidatedBar]) throws -> ClickHouseQuery {
        let range = try canonicalRangeIdentity(bars)
        guard let first = range.first, let last = range.last else {
            return .select("SELECT 0, 0, 0 FORMAT TabSeparated")
        }
        let sql = """
        SELECT count(), uniqExact(mt5_server_ts_raw), uniqExact(ts_utc)
        FROM \(database).ohlc_m1_canonical
        WHERE broker_source_id = '\(sqlLiteral(first.brokerSourceId.rawValue))'
          AND logical_symbol = '\(sqlLiteral(first.logicalSymbol.rawValue))'
          AND ts_utc >= \(first.utcTime.rawValue)
          AND ts_utc <= \(last.utcTime.rawValue)
        FORMAT TabSeparated
        """
        return .select(sql)
    }

    public func ingestStateUpsert(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        oldestMT5ServerTime: MT5ServerSecond,
        latestMT5ServerTime: MT5ServerSecond,
        latestUtcTime: UtcSecond,
        status: String,
        batchId: BatchId,
        updatedAtUtc: UtcSecond
    ) -> ClickHouseQuery {
        let row = [
            tsv(brokerSourceId.rawValue),
            tsv(logicalSymbol.rawValue),
            tsv(mt5Symbol.rawValue),
            String(oldestMT5ServerTime.rawValue),
            String(latestMT5ServerTime.rawValue),
            String(latestUtcTime.rawValue),
            tsv(status),
            tsv(batchId.rawValue),
            String(updatedAtUtc.rawValue)
        ].joined(separator: "\t")
        let sql = """
        INSERT INTO \(database).ingest_state (
            broker_source_id, logical_symbol, mt5_symbol, oldest_mt5_server_ts_raw,
            latest_ingested_closed_mt5_server_ts_raw, latest_ingested_closed_ts_utc,
            status, last_batch_id, updated_at_utc
        ) FORMAT TabSeparated
        \(row)
        """
        return ClickHouseQuery.mutation(sql, idempotent: true)
    }

    private func rawRow(_ bar: ValidatedBar) -> String {
        [
            tsv(bar.brokerSourceId.rawValue),
            tsv(bar.logicalSymbol.rawValue),
            tsv(bar.mt5Symbol.rawValue),
            tsv(bar.timeframe.rawValue),
            String(bar.mt5ServerTime.rawValue),
            String(bar.utcTime.rawValue),
            String(bar.serverUtcOffset.rawValue),
            tsv(bar.offsetSource.rawValue),
            tsv(bar.offsetConfidence.rawValue),
            String(bar.open.rawValue),
            String(bar.high.rawValue),
            String(bar.low.rawValue),
            String(bar.close.rawValue),
            String(bar.digits.rawValue),
            tsv(bar.batchId.rawValue),
            tsv(bar.barHash.description),
            tsv(bar.sourceStatus.rawValue),
            String(bar.ingestedAtUtc.rawValue)
        ].joined(separator: "\t")
    }

    private func canonicalRangeIdentity(_ bars: [ValidatedBar]) throws -> (first: ValidatedBar?, last: ValidatedBar?) {
        guard let first = bars.first, let last = bars.last else {
            return (nil, nil)
        }
        var previous = first.mt5ServerTime
        var previousUTC = first.utcTime
        for bar in bars.dropFirst() {
            guard bar.brokerSourceId == first.brokerSourceId,
                  bar.logicalSymbol == first.logicalSymbol else {
                throw ClickHouseInsertError.mixedCanonicalRange
            }
            guard bar.offsetConfidence == .verified else {
                throw ClickHouseInsertError.unverifiedCanonicalBar(bar.mt5ServerTime, bar.offsetConfidence)
            }
            guard bar.mt5ServerTime.rawValue > previous.rawValue else {
                throw ClickHouseInsertError.unsortedCanonicalRange
            }
            guard bar.utcTime.rawValue > previousUTC.rawValue else {
                throw ClickHouseInsertError.unsortedCanonicalUTCRange
            }
            previous = bar.mt5ServerTime
            previousUTC = bar.utcTime
        }
        guard first.offsetConfidence == .verified else {
            throw ClickHouseInsertError.unverifiedCanonicalBar(first.mt5ServerTime, first.offsetConfidence)
        }
        return (first, last)
    }

    private func tsv(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\t", with: "\\t")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }

    private func sqlLiteral(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
    }
}
