import Domain
import Foundation

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

    public func canonicalBarsInsert(_ bars: [ValidatedBar]) -> ClickHouseQuery {
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

    private func tsv(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\t", with: "\\t")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }
}
