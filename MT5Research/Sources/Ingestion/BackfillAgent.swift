import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import MT5Bridge
import TimeMapping
import Validation

public enum IngestError: Error, CustomStringConvertible, Sendable {
    case symbolMissing(String)
    case selectedSymbolNotConfigured(String)
    case digitsMismatch(symbol: String, expected: Int, actual: Int)
    case emptyMT5Range(String)
    case invalidChunk(String)
    case invalidBridgeResponse(String)
    case canonicalInsertVerificationFailed(String)
    case terminalIdentityMismatch(String)
    case checkpointSymbolMismatch(logicalSymbol: String, expected: String, actual: String)
    case checkpointAheadOfMT5(logicalSymbol: String, checkpoint: Int64, latestClosed: Int64)

    public var description: String {
        switch self {
        case .symbolMissing(let symbol):
            return "Configured MT5 symbol '\(symbol)' is missing or cannot be selected."
        case .selectedSymbolNotConfigured(let symbol):
            return "Requested symbol '\(symbol)' is not configured in symbols.json."
        case .digitsMismatch(let symbol, let expected, let actual):
            return "\(symbol) digits mismatch. Config expected \(expected), MT5 reported \(actual)."
        case .emptyMT5Range(let symbol):
            return "\(symbol) has no closed M1 history available in MT5."
        case .invalidChunk(let reason):
            return "Invalid ingest chunk: \(reason)"
        case .invalidBridgeResponse(let reason):
            return "Invalid MT5 bridge response: \(reason)"
        case .canonicalInsertVerificationFailed(let reason):
            return "Canonical insert verification failed: \(reason)"
        case .terminalIdentityMismatch(let reason):
            return "MT5 terminal identity mismatch: \(reason)"
        case .checkpointSymbolMismatch(let logicalSymbol, let expected, let actual):
            return "\(logicalSymbol) checkpoint MT5 symbol mismatch. Config expects '\(expected)', checkpoint contains '\(actual)'. Stop and inspect symbol mapping before resuming."
        case .checkpointAheadOfMT5(let logicalSymbol, let checkpoint, let latestClosed):
            return "\(logicalSymbol) checkpoint \(checkpoint) is newer than MT5 latest closed bar \(latestClosed). Stop and inspect broker/source identity before resuming."
        }
    }
}

public struct BackfillAgent: Sendable {
    private let config: ConfigBundle
    private let bridge: MT5BridgeClient
    private let clickHouse: ClickHouseClientProtocol
    private let checkpointStore: CheckpointStore
    private let offsetStore: BrokerOffsetStore
    private let logger: Logger

    public init(
        config: ConfigBundle,
        bridge: MT5BridgeClient,
        clickHouse: ClickHouseClientProtocol,
        checkpointStore: CheckpointStore,
        offsetStore: BrokerOffsetStore,
        logger: Logger
    ) {
        self.config = config
        self.bridge = bridge
        self.clickHouse = clickHouse
        self.checkpointStore = checkpointStore
        self.offsetStore = offsetStore
        self.logger = logger
    }

    public func run(selectedSymbols: [LogicalSymbol]?) async throws {
        let terminalIdentity = try loadTerminalIdentity()
        let selected = selectedSymbols.map(Set.init)
        if let selectedSymbols {
            let configured = Set(config.symbols.symbols.map(\.logicalSymbol))
            if let missing = selectedSymbols.first(where: { !configured.contains($0) }) {
                throw IngestError.selectedSymbolNotConfigured(missing.rawValue)
            }
        }
        let mappings = config.symbols.symbols.filter { mapping in
            selected?.contains(mapping.logicalSymbol) ?? true
        }
        let liveSnapshot = try bridge.serverTimeSnapshot()
        try await BrokerOffsetAutoAuthority(
            clickHouse: clickHouse,
            database: config.clickHouse.database,
            logger: logger
        ).ensureLiveSegmentIfMissing(
            brokerSourceId: config.brokerTime.brokerSourceId,
            terminalIdentity: terminalIdentity,
            snapshot: liveSnapshot
        )
        try await ensureHistoricalOffsetAuthorityIfKnown(
            mappings: mappings,
            terminalIdentity: terminalIdentity,
            liveSnapshot: liveSnapshot
        )
        let offsetMap = try await offsetStore.loadVerifiedOffsetMap(
            brokerSourceId: config.brokerTime.brokerSourceId,
            terminalIdentity: terminalIdentity
        )
        logger.ok("Loaded \(offsetMap.segments.count) verified broker UTC offset segment(s) from ClickHouse for \(terminalIdentity)")
        let offsetAuthoritySHA256 = offsetMap.authoritySHA256()
        try BrokerOffsetRuntimeVerifier().verify(
            snapshot: liveSnapshot,
            offsetMap: offsetMap,
            acceptedLiveOffsetSeconds: config.brokerTime.acceptedLiveOffsetSeconds,
            logger: logger
        )
        let timeConverter = TimeConverter(offsetMap: offsetMap)
        let validator = OhlcValidator(timeConverter: timeConverter)
        let insertBuilder = ClickHouseInsertBuilder(database: config.clickHouse.database)
        let batchBuilder = BatchBuilder(chunkSize: config.app.chunkSize)
        let sourceVerifier = MT5SourceRangeVerifier()
        let coverageBuilder = CoverageRangeBuilder(offsetMap: offsetMap)
        let auditStore = IngestAuditStore(clickHouse: clickHouse, database: config.clickHouse.database)

        for mapping in mappings {
            try Task.checkCancellation()
            do {
                try await backfill(
                    mapping: mapping,
                    validator: validator,
                    insertBuilder: insertBuilder,
                    batchBuilder: batchBuilder,
                    sourceVerifier: sourceVerifier,
                    coverageBuilder: coverageBuilder,
                    auditStore: auditStore,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
            } catch {
                logger.error("\(mapping.logicalSymbol.rawValue): \(error)")
                if config.app.strictSymbolFailures { throw error }
            }
        }
    }

    private func ensureHistoricalOffsetAuthorityIfKnown(
        mappings: [SymbolMapping],
        terminalIdentity: BrokerServerIdentity,
        liveSnapshot: ServerTimeSnapshotDTO
    ) async throws {
        guard BrokerOffsetPolicy.hasAutomaticHistoricalPolicy(for: terminalIdentity) else {
            logger.info("No code-owned historical UTC offset policy for \(terminalIdentity); requiring audited broker_time_offsets rows from ClickHouse")
            return
        }
        var requiredFrom: MT5ServerSecond?
        var requiredTo: MT5ServerSecond?
        for mapping in mappings {
            do {
                let symbolInfo = try bridge.prepareSymbol(mapping.mt5Symbol)
                guard symbolInfo.selected else { throw IngestError.symbolMissing(mapping.mt5Symbol.rawValue) }
                guard symbolInfo.digits == mapping.digits.rawValue else {
                    throw IngestError.digitsMismatch(symbol: mapping.mt5Symbol.rawValue, expected: mapping.digits.rawValue, actual: symbolInfo.digits)
                }
                try await ensureHistorySynchronized(mapping: mapping)
                let oldestResponse = try bridge.oldestM1BarTime(mapping.mt5Symbol)
                try validateSingleTimeResponse(oldestResponse, expectedMT5Symbol: mapping.mt5Symbol)
                let latestClosedResponse = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
                try validateSingleTimeResponse(latestClosedResponse, expectedMT5Symbol: mapping.mt5Symbol)
                let oldest = MT5ServerSecond(rawValue: oldestResponse.mt5ServerTime)
                let latestExclusive = try addOneMinute(to: MT5ServerSecond(rawValue: latestClosedResponse.mt5ServerTime))
                requiredFrom = MT5ServerSecond(rawValue: min(requiredFrom?.rawValue ?? oldest.rawValue, oldest.rawValue))
                requiredTo = MT5ServerSecond(rawValue: max(requiredTo?.rawValue ?? latestExclusive.rawValue, latestExclusive.rawValue))
            } catch {
                logger.error("\(mapping.logicalSymbol.rawValue): historical UTC authority pre-scan failed: \(error)")
                if config.app.strictSymbolFailures { throw error }
            }
        }
        guard let requiredFrom, let requiredTo, requiredFrom.rawValue < requiredTo.rawValue else {
            logger.warn("Automatic historical UTC offset policy could not determine an MT5 history range from the selected symbols")
            return
        }
        let inserted = try await BrokerOffsetHistoricalPolicyAuthority(
            clickHouse: clickHouse,
            database: config.clickHouse.database,
            logger: logger
        ).ensureHistoricalCoverageIfKnown(
            brokerSourceId: config.brokerTime.brokerSourceId,
            terminalIdentity: terminalIdentity,
            requiredFrom: requiredFrom,
            requiredToExclusive: requiredTo,
            liveSnapshot: liveSnapshot
        )
        if inserted == 0 {
            logger.ok("Automatic historical broker UTC authority already covers \(requiredFrom.rawValue)..<\(requiredTo.rawValue) for \(terminalIdentity)")
        }
    }

    private func backfill(
        mapping: SymbolMapping,
        validator: OhlcValidator,
        insertBuilder: ClickHouseInsertBuilder,
        batchBuilder: BatchBuilder,
        sourceVerifier: MT5SourceRangeVerifier,
        coverageBuilder: CoverageRangeBuilder,
        auditStore: IngestAuditStore,
        offsetAuthoritySHA256: SHA256DigestHex
    ) async throws {
        logger.info("\(mapping.logicalSymbol.rawValue) - preparing MT5 symbol \(mapping.mt5Symbol.rawValue) for historical M1 OHLC import")
        let symbolInfo = try bridge.prepareSymbol(mapping.mt5Symbol)
        guard symbolInfo.selected else { throw IngestError.symbolMissing(mapping.mt5Symbol.rawValue) }
        guard symbolInfo.digits == mapping.digits.rawValue else {
            throw IngestError.digitsMismatch(symbol: mapping.mt5Symbol.rawValue, expected: mapping.digits.rawValue, actual: symbolInfo.digits)
        }
        try await ensureHistorySynchronized(mapping: mapping)

        logger.info("\(mapping.logicalSymbol.rawValue) - finding oldest available and latest closed M1 bar in MT5")
        let oldestResponse = try bridge.oldestM1BarTime(mapping.mt5Symbol)
        try validateSingleTimeResponse(oldestResponse, expectedMT5Symbol: mapping.mt5Symbol)
        let latestClosedResponse = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
        try validateSingleTimeResponse(latestClosedResponse, expectedMT5Symbol: mapping.mt5Symbol)
        let oldest = MT5ServerSecond(rawValue: oldestResponse.mt5ServerTime)
        let latestClosed = MT5ServerSecond(rawValue: latestClosedResponse.mt5ServerTime)
        guard oldest.rawValue <= latestClosed.rawValue else {
            throw IngestError.emptyMT5Range(mapping.logicalSymbol.rawValue)
        }
        logger.ok("\(mapping.logicalSymbol.rawValue) - MT5 history range found from \(oldest.rawValue) to \(latestClosed.rawValue) server time")

        let existingState = try await checkpointStore.latestState(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol
        )
        let resumeDecision = try BackfillResumePolicy.decide(
            logicalSymbol: mapping.logicalSymbol,
            mt5Symbol: mapping.mt5Symbol,
            oldest: oldest,
            latestClosed: latestClosed,
            existingState: existingState
        )
        logResumeDecision(resumeDecision, logicalSymbol: mapping.logicalSymbol)
        var cursor = resumeDecision.cursor

        while cursor.rawValue <= latestClosed.rawValue {
            try Task.checkCancellation()
            let range = batchBuilder.nextRange(start: cursor, endInclusive: latestClosed)
            let batchId = BatchId.deterministic(
                brokerSourceId: config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol,
                start: range.from,
                end: range.toExclusive
            )
            let sourceRangeLabel = OperatorStatusText.monthRangeLabel(start: range.from, endExclusive: range.toExclusive)
            logger.info("\(mapping.logicalSymbol.rawValue) - pulling M1 OHLC for \(sourceRangeLabel)")

            try await recordChunkOperation(
                auditStore: auditStore,
                mapping: mapping,
                operationType: .backfill,
                batchId: batchId,
                range: range,
                status: .started,
                stage: "range_selected",
                sourceBarCount: nil,
                canonicalRowCount: nil,
                sourceHash: nil,
                offsetAuthoritySHA256: offsetAuthoritySHA256
            )
            do {
                try Task.checkCancellation()
                let sourceRange = try await sourceVerifier.fetchStableRange(
                    mt5Symbol: mapping.mt5Symbol,
                    from: range.from,
                    toExclusive: range.toExclusive,
                    maxBars: config.app.chunkSize
                ) {
                    try bridge.ratesRange(
                        mt5Symbol: mapping.mt5Symbol,
                        from: range.from,
                        toExclusive: range.toExclusive,
                        maxBars: config.app.chunkSize
                    )
                }
                try Task.checkCancellation()
                try await recordChunkOperation(
                    auditStore: auditStore,
                    mapping: mapping,
                    operationType: .backfill,
                    batchId: batchId,
                    range: range,
                    status: .sourceVerified,
                    stage: "mt5_stable_double_read",
                    sourceBarCount: sourceRange.manifest.emittedCount,
                    canonicalRowCount: nil,
                    sourceHash: sourceRange.sourceHash,
                    mt5SourceSHA256: sourceRange.sourceSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                try validateRatesResponse(sourceRange.response, expectedMT5Symbol: mapping.mt5Symbol)
                let closedBars = try sourceRange.response.rates.map {
                    try $0.toClosedM1Bar(logicalSymbol: mapping.logicalSymbol, mt5Symbol: mapping.mt5Symbol, digits: mapping.digits)
                }
                try validateClosedBarsInRange(closedBars, from: range.from, toExclusive: range.toExclusive)

                let now = UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))
                guard !closedBars.isEmpty else {
                    let canonicalVerification = try await CanonicalInsertVerifier(
                        clickHouse: clickHouse,
                        insertBuilder: insertBuilder
                    ).verifyEmptyMT5Range(
                        brokerSourceId: config.brokerTime.brokerSourceId,
                        logicalSymbol: mapping.logicalSymbol,
                        mt5Symbol: mapping.mt5Symbol,
                        mt5Start: range.from,
                        mt5EndExclusive: range.toExclusive
                    )
                    let coverageRecords = try coverageBuilder.makeRecords(
                        brokerSourceId: config.brokerTime.brokerSourceId,
                        logicalSymbol: mapping.logicalSymbol,
                        mt5Symbol: mapping.mt5Symbol,
                        mt5Start: range.from,
                        mt5EndExclusive: range.toExclusive,
                        sourceBars: sourceRange.response.rates,
                        canonicalBars: [],
                        sourceHash: sourceRange.sourceHash,
                        mt5SourceSHA256: sourceRange.sourceSHA256,
                        canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
                        offsetAuthoritySHA256: offsetAuthoritySHA256,
                        verificationMethod: "mt5_stable_double_read_empty",
                        batchId: batchId,
                        verifiedAtUtc: now
                    )
                    for coverage in coverageRecords {
                        try await auditStore.recordVerifiedCoverage(coverage)
                    }
                    try await recordChunkOperation(
                        auditStore: auditStore,
                        mapping: mapping,
                        operationType: .backfill,
                        batchId: batchId,
                        range: range,
                        status: .emptyCoverageVerified,
                        stage: "empty_source_coverage_written",
                        sourceBarCount: 0,
                        canonicalRowCount: 0,
                        sourceHash: sourceRange.sourceHash,
                        mt5SourceSHA256: sourceRange.sourceSHA256,
                        canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
                        offsetAuthoritySHA256: offsetAuthoritySHA256
                    )
                    logger.ok("\(mapping.logicalSymbol.rawValue) - \(sourceRangeLabel) contains no MT5 bars; source gap verified and checkpoint left unchanged")
                    cursor = range.toExclusive
                    continue
                }

                let context = OhlcValidationContext(
                    brokerSourceId: config.brokerTime.brokerSourceId,
                    expectedLogicalSymbol: mapping.logicalSymbol,
                    expectedMT5Symbol: mapping.mt5Symbol,
                    expectedDigits: mapping.digits,
                    latestClosedMT5ServerTime: latestClosed,
                    batchId: batchId,
                    ingestedAtUtc: now
                )
                logger.info("\(mapping.logicalSymbol.rawValue) - validating \(sourceRangeLabel) for OHLC integrity and verified UTC conversion")
                let validated = try validator.validateBatch(closedBars, context: context)
                let canonicalVerification = try await writeValidatedBars(
                    validated,
                    insertBuilder: insertBuilder,
                    auditStore: auditStore,
                    mapping: mapping,
                    operationType: .backfill,
                    batchId: batchId,
                    range: range,
                    sourceBarCount: sourceRange.manifest.emittedCount,
                    sourceHash: sourceRange.sourceHash,
                    mt5SourceSHA256: sourceRange.sourceSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )

                guard let first = validated.first, let last = validated.last else {
                    throw IngestError.invalidChunk("validated chunk unexpectedly empty")
                }
                let coverageRecords = try coverageBuilder.makeRecords(
                    brokerSourceId: config.brokerTime.brokerSourceId,
                    logicalSymbol: mapping.logicalSymbol,
                    mt5Symbol: mapping.mt5Symbol,
                    mt5Start: range.from,
                    mt5EndExclusive: range.toExclusive,
                    sourceBars: sourceRange.response.rates,
                    canonicalBars: validated,
                    sourceHash: sourceRange.sourceHash,
                    mt5SourceSHA256: sourceRange.sourceSHA256,
                    canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256,
                    verificationMethod: "mt5_stable_double_read_canonical_readback",
                    batchId: batchId,
                    verifiedAtUtc: now
                )
                for coverage in coverageRecords {
                    try await auditStore.recordVerifiedCoverage(coverage)
                }
                let verifiedRangeLabel = OperatorStatusText.monthRangeLabel(
                    start: first.utcTime,
                    endExclusive: try addOneMinute(to: last.utcTime)
                )
                let state = IngestState(
                    brokerSourceId: config.brokerTime.brokerSourceId,
                    logicalSymbol: mapping.logicalSymbol,
                    mt5Symbol: mapping.mt5Symbol,
                    oldestMT5ServerTime: oldest,
                    latestIngestedClosedMT5ServerTime: last.mt5ServerTime,
                    latestIngestedClosedUtcTime: last.utcTime,
                    status: last.mt5ServerTime.rawValue >= latestClosed.rawValue ? .live : .backfilling,
                    lastBatchId: batchId,
                    updatedAtUtc: now
                )
                try await checkpointStore.save(state)
                try await recordChunkOperation(
                    auditStore: auditStore,
                    mapping: mapping,
                    operationType: .backfill,
                    batchId: batchId,
                    range: range,
                    status: .checkpointed,
                    stage: "checkpoint_saved",
                    sourceBarCount: sourceRange.manifest.emittedCount,
                    canonicalRowCount: validated.count,
                    sourceHash: sourceRange.sourceHash,
                    mt5SourceSHA256: sourceRange.sourceSHA256,
                    canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                logger.ok("\(mapping.logicalSymbol.rawValue) - \(verifiedRangeLabel) pulled, verified, UTC correct and canonical data clean (\(validated.count) closed M1 bars)")
                cursor = try addOneMinute(to: last.mt5ServerTime)
            } catch {
                do {
                    try await recordChunkOperation(
                        auditStore: auditStore,
                        mapping: mapping,
                        operationType: .backfill,
                        batchId: batchId,
                        range: range,
                        status: .failed,
                        stage: "chunk_failed",
                        sourceBarCount: nil,
                        canonicalRowCount: nil,
                        sourceHash: nil,
                        offsetAuthoritySHA256: offsetAuthoritySHA256,
                        errorMessage: String(describing: error)
                    )
                } catch {
                    logger.warn("\(mapping.logicalSymbol.rawValue) - failed to record ingest failure for \(sourceRangeLabel): \(error)")
                }
                throw error
            }
        }
    }

    private func writeValidatedBars(
        _ bars: [ValidatedBar],
        insertBuilder: ClickHouseInsertBuilder,
        auditStore: IngestAuditStore,
        mapping: SymbolMapping,
        operationType: IngestOperationType,
        batchId: BatchId,
        range: (from: MT5ServerSecond, toExclusive: MT5ServerSecond),
        sourceBarCount: Int,
        sourceHash: String,
        mt5SourceSHA256: SHA256DigestHex,
        offsetAuthoritySHA256: SHA256DigestHex
    ) async throws -> CanonicalInsertVerificationResult {
        guard let first = bars.first else {
            throw IngestError.invalidChunk("cannot write an empty validated canonical chunk")
        }
        let rawInsert = insertBuilder.rawBarsInsert(bars)
        let canonicalDelete = try insertBuilder.canonicalRangeDelete(
            bars,
            mt5Start: range.from,
            mt5EndExclusive: range.toExclusive
        )
        let canonicalInsert = try insertBuilder.canonicalBarsInsert(bars)
        try await CanonicalConflictRecorder(clickHouse: clickHouse, insertBuilder: insertBuilder)
            .recordConflictsBeforeCanonicalReplace(bars, detectedAtUtc: first.ingestedAtUtc)
        _ = try await clickHouse.execute(rawInsert)
        try await recordChunkOperation(
            auditStore: auditStore,
            mapping: mapping,
            operationType: operationType,
            batchId: batchId,
            range: range,
            status: .rawWritten,
            stage: "raw_audit_written",
            sourceBarCount: sourceBarCount,
            canonicalRowCount: nil,
            sourceHash: sourceHash,
            mt5SourceSHA256: mt5SourceSHA256,
            offsetAuthoritySHA256: offsetAuthoritySHA256
        )
        _ = try await clickHouse.execute(canonicalDelete)
        try await recordChunkOperation(
            auditStore: auditStore,
            mapping: mapping,
            operationType: operationType,
            batchId: batchId,
            range: range,
            status: .canonicalDeleted,
            stage: "canonical_range_deleted",
            sourceBarCount: sourceBarCount,
            canonicalRowCount: nil,
            sourceHash: sourceHash,
            mt5SourceSHA256: mt5SourceSHA256,
            offsetAuthoritySHA256: offsetAuthoritySHA256
        )
        _ = try await clickHouse.execute(canonicalInsert)
        try await recordChunkOperation(
            auditStore: auditStore,
            mapping: mapping,
            operationType: operationType,
            batchId: batchId,
            range: range,
            status: .canonicalWritten,
            stage: "canonical_written",
            sourceBarCount: sourceBarCount,
            canonicalRowCount: bars.count,
            sourceHash: sourceHash,
            mt5SourceSHA256: mt5SourceSHA256,
            offsetAuthoritySHA256: offsetAuthoritySHA256
        )
        let canonicalVerification = try await CanonicalInsertVerifier(clickHouse: clickHouse, insertBuilder: insertBuilder).verify(
            bars,
            mt5Start: range.from,
            mt5EndExclusive: range.toExclusive
        )
        try await recordChunkOperation(
            auditStore: auditStore,
            mapping: mapping,
            operationType: operationType,
            batchId: batchId,
            range: range,
            status: .readbackVerified,
            stage: "canonical_readback_verified",
            sourceBarCount: sourceBarCount,
            canonicalRowCount: bars.count,
            sourceHash: sourceHash,
            mt5SourceSHA256: mt5SourceSHA256,
            canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
            offsetAuthoritySHA256: offsetAuthoritySHA256
        )
        return canonicalVerification
    }

    private func recordChunkOperation(
        auditStore: IngestAuditStore,
        mapping: SymbolMapping,
        operationType: IngestOperationType,
        batchId: BatchId,
        range: (from: MT5ServerSecond, toExclusive: MT5ServerSecond),
        status: IngestOperationStatus,
        stage: String,
        sourceBarCount: Int?,
        canonicalRowCount: Int?,
        sourceHash: String?,
        mt5SourceSHA256: SHA256DigestHex? = nil,
        canonicalReadbackSHA256: SHA256DigestHex? = nil,
        offsetAuthoritySHA256: SHA256DigestHex? = nil,
        errorMessage: String? = nil
    ) async throws {
        let hasSHA256Evidence = mt5SourceSHA256 != nil || canonicalReadbackSHA256 != nil || offsetAuthoritySHA256 != nil
        try await auditStore.recordOperation(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol,
            mt5Symbol: mapping.mt5Symbol,
            operationType: operationType,
            batchId: batchId,
            mt5Start: range.from,
            mt5EndExclusive: range.toExclusive,
            status: status,
            stage: stage,
            sourceBarCount: sourceBarCount,
            canonicalRowCount: canonicalRowCount,
            sourceHash: sourceHash,
            hashSchemaVersion: hasSHA256Evidence ? ChunkHashing.schemaVersion : nil,
            mt5SourceSHA256: mt5SourceSHA256,
            canonicalReadbackSHA256: canonicalReadbackSHA256,
            offsetAuthoritySHA256: offsetAuthoritySHA256,
            errorMessage: errorMessage
        )
    }

    private func validateSingleTimeResponse(_ response: SingleTimeResponseDTO, expectedMT5Symbol: MT5Symbol) throws {
        guard response.mt5Symbol == expectedMT5Symbol.rawValue else {
            throw IngestError.invalidBridgeResponse("expected \(expectedMT5Symbol.rawValue), got \(response.mt5Symbol)")
        }
        guard MT5ServerSecond(rawValue: response.mt5ServerTime).isMinuteAligned else {
            throw IngestError.invalidBridgeResponse("MT5 timestamp \(response.mt5ServerTime) is not minute-aligned")
        }
    }

    private func validateRatesResponse(_ response: RatesResponseDTO, expectedMT5Symbol: MT5Symbol) throws {
        guard response.mt5Symbol == expectedMT5Symbol.rawValue else {
            throw IngestError.invalidBridgeResponse("expected rates for \(expectedMT5Symbol.rawValue), got \(response.mt5Symbol)")
        }
        guard response.timeframe == Timeframe.m1.rawValue else {
            throw IngestError.invalidBridgeResponse("expected M1 rates, got \(response.timeframe)")
        }
    }

    private func validateClosedBarsInRange(_ bars: [ClosedM1Bar], from: MT5ServerSecond, toExclusive: MT5ServerSecond) throws {
        for bar in bars {
            guard bar.mt5ServerTime.rawValue >= from.rawValue,
                  bar.mt5ServerTime.rawValue < toExclusive.rawValue else {
                throw IngestError.invalidBridgeResponse("MT5 bar \(bar.mt5ServerTime.rawValue) is outside requested range \(from.rawValue)..<\(toExclusive.rawValue)")
            }
        }
    }

    private func ensureHistorySynchronized(mapping: SymbolMapping) async throws {
        let attempts = 60
        for attempt in 1...attempts {
            let status = try bridge.historyStatus(mapping.mt5Symbol)
            guard status.mt5Symbol == mapping.mt5Symbol.rawValue else {
                throw IngestError.invalidBridgeResponse("expected history status for \(mapping.mt5Symbol.rawValue), got \(status.mt5Symbol)")
            }
            if status.synchronized && status.bars > 0 {
                logger.ok("\(mapping.logicalSymbol.rawValue) - MT5 M1 history synchronized with \(status.bars) local bars")
                return
            }
            if attempt == 1 || attempt % 5 == 0 || attempt == attempts {
                logger.warn("\(mapping.logicalSymbol.rawValue) - waiting for MT5 M1 history synchronization, attempt \(attempt)/\(attempts)")
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
        throw IngestError.invalidBridgeResponse("\(mapping.logicalSymbol.rawValue) M1 history did not synchronize in MT5 before oldest/latest discovery")
    }

    private func logResumeDecision(_ decision: BackfillResumeDecision, logicalSymbol: LogicalSymbol) {
        switch decision.action {
        case .freshBackfill:
            logger.info("\(logicalSymbol.rawValue) - no checkpoint found; starting from MT5 oldest available bar")
        case .resumeFromCheckpoint:
            logger.info("\(logicalSymbol.rawValue) - resuming from checkpoint at \(decision.cursor.rawValue) server time")
        case .reprocessExpandedOlderHistory:
            logger.warn("\(logicalSymbol.rawValue) - MT5 history now starts earlier than the stored checkpoint oldest; reprocessing from \(decision.cursor.rawValue) to keep canonical history complete")
        case .resumeAfterPrunedHistory:
            logger.warn("\(logicalSymbol.rawValue) - MT5 no longer exposes the checkpoint's next server-time range; resuming at current MT5 oldest \(decision.cursor.rawValue)")
        }
    }

    private func loadTerminalIdentity() throws -> BrokerServerIdentity {
        let actual = try bridge.terminalInfo()
        do {
            return try TerminalIdentityPolicy().resolve(
                actual: actual,
                brokerSourceId: config.brokerTime.brokerSourceId,
                expected: config.brokerTime.expectedTerminalIdentity,
                logger: logger
            )
        } catch let error as TerminalIdentityPolicyError {
            throw IngestError.terminalIdentityMismatch(error.description)
        }
    }

    private func addOneMinute(to value: MT5ServerSecond) throws -> MT5ServerSecond {
        let result = value.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw IngestError.invalidChunk("MT5 server timestamp overflow while advancing one M1 bar")
        }
        return MT5ServerSecond(rawValue: result.partialValue)
    }

    private func addOneMinute(to value: UtcSecond) throws -> UtcSecond {
        let result = value.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw IngestError.invalidChunk("UTC timestamp overflow while advancing one M1 bar")
        }
        return UtcSecond(rawValue: result.partialValue)
    }
}
