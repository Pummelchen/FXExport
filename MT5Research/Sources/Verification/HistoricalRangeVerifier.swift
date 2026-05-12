import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import Ingestion
import MT5Bridge
import TimeMapping
import Validation

public struct HistoricalVerificationOutcome: Sendable {
    public let range: VerificationRange
    public let mt5Bars: [ValidatedBar]
    public let databaseBars: [VerificationBar]
    public let result: VerificationResult
    public let sourceComplete: Bool
    public let verifiedCoverage: [VerifiedCoverageRecord]

    public init(
        range: VerificationRange,
        mt5Bars: [ValidatedBar],
        databaseBars: [VerificationBar],
        result: VerificationResult,
        sourceComplete: Bool,
        verifiedCoverage: [VerifiedCoverageRecord]
    ) {
        self.range = range
        self.mt5Bars = mt5Bars
        self.databaseBars = databaseBars
        self.result = result
        self.sourceComplete = sourceComplete
        self.verifiedCoverage = verifiedCoverage
    }
}

private struct MT5VerificationFetch: Sendable {
    let bars: [ValidatedBar]
    let coverage: [VerifiedCoverageRecord]
}

public enum HistoricalRangeVerifierError: Error, CustomStringConvertible, Sendable {
    case missingSymbolMapping(LogicalSymbol)
    case invalidMT5Response(String)
    case invalidRange(VerificationRange)

    public var description: String {
        switch self {
        case .missingSymbolMapping(let symbol):
            return "No configured MT5 mapping found for \(symbol.rawValue)."
        case .invalidMT5Response(let reason):
            return "Invalid MT5 response during verification: \(reason)"
        case .invalidRange(let range):
            return "Invalid verification range \(range.logicalSymbol.rawValue) \(range.mt5Start.rawValue)..<\(range.mt5EndExclusive.rawValue)."
        }
    }
}

public struct HistoricalRangeVerifier: Sendable {
    private let config: ConfigBundle
    private let bridge: MT5BridgeClient
    private let clickHouse: ClickHouseClientProtocol
    private let offsetMap: BrokerOffsetMap
    private let logger: Logger

    public init(
        config: ConfigBundle,
        bridge: MT5BridgeClient,
        clickHouse: ClickHouseClientProtocol,
        offsetMap: BrokerOffsetMap,
        logger: Logger
    ) {
        self.config = config
        self.bridge = bridge
        self.clickHouse = clickHouse
        self.offsetMap = offsetMap
        self.logger = logger
    }

    public func verify(range: VerificationRange) async throws -> HistoricalVerificationOutcome {
        guard range.mt5Start.rawValue < range.mt5EndExclusive.rawValue else {
            throw HistoricalRangeVerifierError.invalidRange(range)
        }
        guard let mapping = config.symbols.mapping(for: range.logicalSymbol) else {
            throw HistoricalRangeVerifierError.missingSymbolMapping(range.logicalSymbol)
        }
        let rangeLabel = OperatorStatusText.monthRangeLabel(start: range.mt5Start, endExclusive: range.mt5EndExclusive)
        logger.verify("\(range.logicalSymbol.rawValue) - checking \(rangeLabel) against MT5 source of truth")

        let latestClosedResponse = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
        guard latestClosedResponse.mt5Symbol == mapping.mt5Symbol.rawValue else {
            throw HistoricalRangeVerifierError.invalidMT5Response("expected latest closed for \(mapping.mt5Symbol.rawValue), got \(latestClosedResponse.mt5Symbol)")
        }
        let latestClosed = MT5ServerSecond(rawValue: latestClosedResponse.mt5ServerTime)

        let mt5Fetch = try await fetchMT5Bars(range: range, mapping: mapping, latestClosed: latestClosed)
        logger.verify("\(range.logicalSymbol.rawValue) - loading canonical M1 OHLC for \(rangeLabel) from ClickHouse")
        let databaseBars = try await CanonicalOhlcStore(clickHouse: clickHouse, database: config.clickHouse.database).fetch(range: range)
        let result = VerificationComparator().compare(
            mt5SourceBars: mt5Fetch.bars.map(VerificationBar.init(validatedBar:)),
            databaseBars: databaseBars
        )
        try await writeVerificationResult(range: range, result: result)
        if result.isClean {
            logger.verify("\(range.logicalSymbol.rawValue) - \(rangeLabel) verified against MT5; UTC correct and all canonical data clean")
        } else {
            logger.warn("\(range.logicalSymbol.rawValue) - \(rangeLabel) is not clean; MT5 comparison found \(result.mismatches.count) mismatch(es)")
        }
        return HistoricalVerificationOutcome(
            range: range,
            mt5Bars: mt5Fetch.bars,
            databaseBars: databaseBars,
            result: result,
            sourceComplete: true,
            verifiedCoverage: mt5Fetch.coverage
        )
    }

    private func fetchMT5Bars(
        range: VerificationRange,
        mapping: SymbolMapping,
        latestClosed: MT5ServerSecond
    ) async throws -> MT5VerificationFetch {
        var cursor = range.mt5Start
        var output: [ValidatedBar] = []
        var coverage: [VerifiedCoverageRecord] = []
        let validator = OhlcValidator(timeConverter: TimeConverter(offsetMap: offsetMap))
        let coverageBuilder = CoverageRangeBuilder(offsetMap: offsetMap)
        let sourceVerifier = MT5SourceRangeVerifier()
        while cursor.rawValue < range.mt5EndExclusive.rawValue {
            let chunkEnd = try chunkEndExclusive(cursor: cursor, rangeEndExclusive: range.mt5EndExclusive)
            let chunkLabel = OperatorStatusText.monthRangeLabel(start: cursor, endExclusive: chunkEnd)
            logger.verify("\(range.logicalSymbol.rawValue) - pulling MT5 verification M1 OHLC for \(chunkLabel)")
            let batchId = BatchId.deterministic(
                brokerSourceId: range.brokerSourceId,
                logicalSymbol: range.logicalSymbol,
                start: cursor,
                end: chunkEnd
            )
            let sourceRange = try await sourceVerifier.fetchStableRange(
                mt5Symbol: mapping.mt5Symbol,
                from: cursor,
                toExclusive: chunkEnd,
                maxBars: config.app.chunkSize
            ) {
                try bridge.ratesRange(
                    mt5Symbol: mapping.mt5Symbol,
                    from: cursor,
                    toExclusive: chunkEnd,
                    maxBars: config.app.chunkSize
                )
            }
            let closedBars = try sourceRange.response.rates.map {
                try $0.toClosedM1Bar(logicalSymbol: mapping.logicalSymbol, mt5Symbol: mapping.mt5Symbol, digits: mapping.digits)
            }
            try validateClosedBarsInRange(closedBars, from: cursor, toExclusive: chunkEnd)
            let context = OhlcValidationContext(
                brokerSourceId: range.brokerSourceId,
                expectedLogicalSymbol: mapping.logicalSymbol,
                expectedMT5Symbol: mapping.mt5Symbol,
                expectedDigits: mapping.digits,
                latestClosedMT5ServerTime: latestClosed,
                batchId: batchId,
                ingestedAtUtc: UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))
            )
            let validated = try validator.validateBatch(closedBars, context: context)
            output.append(contentsOf: validated)
            coverage.append(contentsOf: try coverageBuilder.makeRecords(
                brokerSourceId: range.brokerSourceId,
                logicalSymbol: range.logicalSymbol,
                mt5Symbol: mapping.mt5Symbol,
                mt5Start: cursor,
                mt5EndExclusive: chunkEnd,
                sourceBars: sourceRange.response.rates,
                canonicalBars: validated,
                sourceHash: sourceRange.sourceHash,
                verificationMethod: "mt5_verifier_stable_double_read",
                batchId: batchId,
                verifiedAtUtc: context.ingestedAtUtc
            ))
            cursor = chunkEnd
        }
        return MT5VerificationFetch(bars: output, coverage: coverage)
    }

    private func chunkEndExclusive(cursor: MT5ServerSecond, rangeEndExclusive: MT5ServerSecond) throws -> MT5ServerSecond {
        let duration = Int64(config.app.chunkSize).multipliedReportingOverflow(by: Timeframe.m1.seconds)
        guard !duration.overflow else {
            throw HistoricalRangeVerifierError.invalidMT5Response("chunk size overflows M1 duration")
        }
        let candidate = cursor.rawValue.addingReportingOverflow(duration.partialValue)
        guard !candidate.overflow else {
            return rangeEndExclusive
        }
        return MT5ServerSecond(rawValue: min(rangeEndExclusive.rawValue, candidate.partialValue))
    }

    private func validateClosedBarsInRange(_ bars: [ClosedM1Bar], from: MT5ServerSecond, toExclusive: MT5ServerSecond) throws {
        for bar in bars {
            guard bar.mt5ServerTime.rawValue >= from.rawValue,
                  bar.mt5ServerTime.rawValue < toExclusive.rawValue else {
                throw HistoricalRangeVerifierError.invalidMT5Response("bar \(bar.mt5ServerTime.rawValue) is outside requested range \(from.rawValue)..<\(toExclusive.rawValue)")
            }
        }
    }

    private func writeVerificationResult(range: VerificationRange, result: VerificationResult) async throws {
        let details = result.mismatches.prefix(20).map(String.init(describing:)).joined(separator: "; ")
        let row = [
            Self.tsv(range.brokerSourceId.rawValue),
            Self.tsv(range.logicalSymbol.rawValue),
            String(range.mt5Start.rawValue),
            String(range.mt5EndExclusive.rawValue),
            Self.tsv(result.isClean ? "clean" : "mismatch"),
            String(result.mismatches.count),
            Self.tsv(details),
            String(Int64(Date().timeIntervalSince1970))
        ].joined(separator: "\t")
        let sql = """
        INSERT INTO \(config.clickHouse.database).verification_results (
            broker_source_id, logical_symbol, range_start_mt5_server_ts,
            range_end_mt5_server_ts, result, mismatch_count, details, checked_at_utc
        ) FORMAT TabSeparated
        \(row)
        """
        _ = try await clickHouse.execute(.mutation(sql, idempotent: false))
    }

    private static func tsv(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\t", with: "\\t")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }
}
