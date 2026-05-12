import Domain
import Foundation
import MT5Bridge

public struct CanonicalChunkHashRow: Sendable, Hashable {
    public let mt5ServerTime: MT5ServerSecond
    public let utcTime: UtcSecond
    public let serverUtcOffset: OffsetSeconds
    public let offsetSource: OffsetSource
    public let offsetConfidence: OffsetConfidence
    public let openScaled: Int64
    public let highScaled: Int64
    public let lowScaled: Int64
    public let closeScaled: Int64
    public let digits: Digits
    public let barHash: BarHash

    public init(
        mt5ServerTime: MT5ServerSecond,
        utcTime: UtcSecond,
        serverUtcOffset: OffsetSeconds,
        offsetSource: OffsetSource,
        offsetConfidence: OffsetConfidence,
        openScaled: Int64,
        highScaled: Int64,
        lowScaled: Int64,
        closeScaled: Int64,
        digits: Digits,
        barHash: BarHash
    ) {
        self.mt5ServerTime = mt5ServerTime
        self.utcTime = utcTime
        self.serverUtcOffset = serverUtcOffset
        self.offsetSource = offsetSource
        self.offsetConfidence = offsetConfidence
        self.openScaled = openScaled
        self.highScaled = highScaled
        self.lowScaled = lowScaled
        self.closeScaled = closeScaled
        self.digits = digits
        self.barHash = barHash
    }

    public init(validatedBar bar: ValidatedBar) {
        self.init(
            mt5ServerTime: bar.mt5ServerTime,
            utcTime: bar.utcTime,
            serverUtcOffset: bar.serverUtcOffset,
            offsetSource: bar.offsetSource,
            offsetConfidence: bar.offsetConfidence,
            openScaled: bar.open.rawValue,
            highScaled: bar.high.rawValue,
            lowScaled: bar.low.rawValue,
            closeScaled: bar.close.rawValue,
            digits: bar.digits,
            barHash: bar.barHash
        )
    }
}

public enum ChunkHashing {
    public static let schemaVersion = ChunkHashSchemaVersion.sha256V1

    public static func mt5SourceSHA256(response: RatesResponseDTO, manifest: MT5RangeManifest) -> SHA256DigestHex {
        var hasher = SHA256ChunkHasher(namespace: "mt5_source_m1_chunk")
        hasher.appendField("mt5_symbol", response.mt5Symbol)
        hasher.appendField("timeframe", response.timeframe)
        hasher.appendField("requested_from_mt5_server_ts", manifest.requestedFrom.rawValue)
        hasher.appendField("requested_to_mt5_server_ts_exclusive", manifest.requestedToExclusive.rawValue)
        hasher.appendField("effective_to_mt5_server_ts_exclusive", manifest.effectiveToExclusive.rawValue)
        hasher.appendField("latest_closed_mt5_server_ts", manifest.latestClosed.rawValue)
        hasher.appendField("series_synchronized", manifest.seriesSynchronized)
        hasher.appendField("copied_count", manifest.copiedCount)
        hasher.appendField("emitted_count", manifest.emittedCount)
        hasher.appendField("first_mt5_server_ts", manifest.firstMT5ServerTime?.rawValue ?? -1)
        hasher.appendField("last_mt5_server_ts", manifest.lastMT5ServerTime?.rawValue ?? -1)
        hasher.appendField("rate_count", response.rates.count)

        for (index, rate) in response.rates.enumerated() {
            hasher.appendField("rate_index", index)
            hasher.appendField("mt5_server_time", rate.mt5ServerTime)
            hasher.appendField("open", rate.open)
            hasher.appendField("high", rate.high)
            hasher.appendField("low", rate.low)
            hasher.appendField("close", rate.close)
        }

        return hasher.finalize()
    }

    public static func canonicalSHA256(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        timeframe: Timeframe,
        mt5Start: MT5ServerSecond,
        mt5EndExclusive: MT5ServerSecond,
        rows: [CanonicalChunkHashRow]
    ) -> SHA256DigestHex {
        var hasher = SHA256ChunkHasher(namespace: "canonical_m1_chunk")
        hasher.appendField("broker_source_id", brokerSourceId.rawValue)
        hasher.appendField("logical_symbol", logicalSymbol.rawValue)
        hasher.appendField("mt5_symbol", mt5Symbol.rawValue)
        hasher.appendField("timeframe", timeframe.rawValue)
        hasher.appendField("mt5_range_start", mt5Start.rawValue)
        hasher.appendField("mt5_range_end_exclusive", mt5EndExclusive.rawValue)
        hasher.appendField("row_count", rows.count)

        for (index, row) in rows.enumerated() {
            hasher.appendField("row_index", index)
            hasher.appendField("mt5_server_ts_raw", row.mt5ServerTime.rawValue)
            hasher.appendField("ts_utc", row.utcTime.rawValue)
            hasher.appendField("server_utc_offset_seconds", row.serverUtcOffset.rawValue)
            hasher.appendField("offset_source", row.offsetSource.rawValue)
            hasher.appendField("offset_confidence", row.offsetConfidence.rawValue)
            hasher.appendField("open_scaled", row.openScaled)
            hasher.appendField("high_scaled", row.highScaled)
            hasher.appendField("low_scaled", row.lowScaled)
            hasher.appendField("close_scaled", row.closeScaled)
            hasher.appendField("digits", row.digits.rawValue)
            hasher.appendField("bar_hash", row.barHash.description)
        }

        return hasher.finalize()
    }

    public static func canonicalSHA256(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        timeframe: Timeframe,
        mt5Start: MT5ServerSecond,
        mt5EndExclusive: MT5ServerSecond,
        bars: [ValidatedBar]
    ) -> SHA256DigestHex {
        canonicalSHA256(
            brokerSourceId: brokerSourceId,
            logicalSymbol: logicalSymbol,
            mt5Symbol: mt5Symbol,
            timeframe: timeframe,
            mt5Start: mt5Start,
            mt5EndExclusive: mt5EndExclusive,
            rows: bars.map(CanonicalChunkHashRow.init(validatedBar:))
        )
    }

    public static func combinedSHA256(namespace: String, values: [SHA256DigestHex]) -> SHA256DigestHex {
        var hasher = SHA256ChunkHasher(namespace: namespace)
        hasher.appendField("digest_count", values.count)
        for (index, value) in values.enumerated() {
            hasher.appendField("digest_index", index)
            hasher.appendField("digest", value.rawValue)
        }
        return hasher.finalize()
    }

    public static func emptySHA256(namespace: String) -> SHA256DigestHex {
        var hasher = SHA256ChunkHasher(namespace: namespace)
        hasher.appendField("empty", true)
        return hasher.finalize()
    }
}
