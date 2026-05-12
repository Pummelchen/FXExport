import AppCore
import ClickHouse
import Config
import Domain
import Foundation
import MT5Bridge
import TimeMapping
import Validation

public struct LiveUpdateAgent: Sendable {
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

    public func runForever() async throws {
        logger.info("Starting live updater; scan interval \(config.app.liveScanIntervalSeconds)s")
        let terminalIdentity = try loadTerminalIdentity()
        while !Task.isCancelled {
            do {
                try await runOnce(terminalIdentity: terminalIdentity)
            } catch {
                logger.warn("Live update cycle failed safely: \(error)")
            }
            do {
                try await Task.sleep(nanoseconds: UInt64(config.app.liveScanIntervalSeconds) * 1_000_000_000)
            } catch {
                return
            }
        }
    }

    public func runOnce() async throws {
        try await runOnce(terminalIdentity: loadTerminalIdentity())
    }

    private func runOnce(terminalIdentity: BrokerServerIdentity) async throws {
        let offsetMap = try await offsetStore.loadVerifiedOffsetMap(
            brokerSourceId: config.brokerTime.brokerSourceId,
            terminalIdentity: terminalIdentity
        )
        let offsetAuthoritySHA256 = offsetMap.authoritySHA256()
        try BrokerOffsetRuntimeVerifier().verify(
            snapshot: bridge.serverTimeSnapshot(),
            offsetMap: offsetMap,
            acceptedLiveOffsetSeconds: config.brokerTime.acceptedLiveOffsetSeconds,
            logger: logger
        )
        let validator = OhlcValidator(timeConverter: TimeConverter(offsetMap: offsetMap))
        let insertBuilder = ClickHouseInsertBuilder(database: config.clickHouse.database)
        let sourceVerifier = MT5SourceRangeVerifier()
        let coverageBuilder = CoverageRangeBuilder(offsetMap: offsetMap)
        let auditStore = IngestAuditStore(clickHouse: clickHouse, database: config.clickHouse.database)
        var failureCount = 0
        for mapping in config.symbols.symbols {
            do {
                try await update(
                    mapping: mapping,
                    validator: validator,
                    insertBuilder: insertBuilder,
                    sourceVerifier: sourceVerifier,
                    coverageBuilder: coverageBuilder,
                    auditStore: auditStore,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
            } catch let error as MT5BridgeError {
                throw error
            } catch let error as ProtocolError {
                throw error
            } catch let error as ClickHouseError {
                throw error
            } catch {
                failureCount += 1
                logger.warn("\(mapping.logicalSymbol.rawValue): live update skipped safely: \(error)")
            }
        }
        if failureCount == config.symbols.symbols.count {
            throw IngestError.invalidChunk("all configured symbols failed during the live update cycle")
        }
    }

    private func update(
        mapping: SymbolMapping,
        validator: OhlcValidator,
        insertBuilder: ClickHouseInsertBuilder,
        sourceVerifier: MT5SourceRangeVerifier,
        coverageBuilder: CoverageRangeBuilder,
        auditStore: IngestAuditStore,
        offsetAuthoritySHA256: SHA256DigestHex
    ) async throws {
        let latestResponse = try bridge.latestClosedM1Bar(mapping.mt5Symbol)
        guard latestResponse.mt5Symbol == mapping.mt5Symbol.rawValue else {
            throw IngestError.invalidBridgeResponse("expected \(mapping.mt5Symbol.rawValue), got \(latestResponse.mt5Symbol)")
        }
        let latestClosed = MT5ServerSecond(rawValue: latestResponse.mt5ServerTime)
        guard latestClosed.isMinuteAligned else {
            throw IngestError.invalidBridgeResponse("latest closed M1 timestamp \(latestResponse.mt5ServerTime) is not minute-aligned")
        }
        guard let state = try await checkpointStore.latestState(
            brokerSourceId: config.brokerTime.brokerSourceId,
            logicalSymbol: mapping.logicalSymbol
        ) else {
            logger.warn("\(mapping.logicalSymbol.rawValue) - no checkpoint exists; run historical import before live M1 updates")
            return
        }
        guard state.mt5Symbol == mapping.mt5Symbol else {
            throw IngestError.checkpointSymbolMismatch(
                logicalSymbol: mapping.logicalSymbol.rawValue,
                expected: mapping.mt5Symbol.rawValue,
                actual: state.mt5Symbol.rawValue
            )
        }

        guard latestClosed.rawValue >= state.latestIngestedClosedMT5ServerTime.rawValue else {
            throw IngestError.checkpointAheadOfMT5(
                logicalSymbol: mapping.logicalSymbol.rawValue,
                checkpoint: state.latestIngestedClosedMT5ServerTime.rawValue,
                latestClosed: latestClosed.rawValue
            )
        }
        guard latestClosed.rawValue > state.latestIngestedClosedMT5ServerTime.rawValue else {
            logger.debug("\(mapping.logicalSymbol.rawValue) - no new closed M1 bar; checkpoint is current at \(state.latestIngestedClosedMT5ServerTime.rawValue) server time")
            return
        }

        let overallStart = try addOneMinute(to: state.latestIngestedClosedMT5ServerTime)
        let overallEndExclusive = try addOneMinute(to: latestClosed)
        let batchBuilder = BatchBuilder(chunkSize: config.app.chunkSize)
        var cursor = overallStart
        var totalInserted = 0
        while cursor.rawValue < overallEndExclusive.rawValue {
            let chunkRange = batchBuilder.nextRange(start: cursor, endInclusive: latestClosed)
            let rangeLabel = OperatorStatusText.monthRangeLabel(start: chunkRange.from, endExclusive: chunkRange.toExclusive)
            let batchId = BatchId.deterministic(
                brokerSourceId: config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol,
                start: chunkRange.from,
                end: chunkRange.toExclusive
            )
            logger.info("\(mapping.logicalSymbol.rawValue) - pulling closed M1 OHLC for \(rangeLabel)")
            try await recordChunkOperation(
                auditStore: auditStore,
                mapping: mapping,
                batchId: batchId,
                range: chunkRange,
                status: .started,
                stage: "range_selected",
                sourceBarCount: nil,
                canonicalRowCount: nil,
                sourceHash: nil,
                offsetAuthoritySHA256: offsetAuthoritySHA256
            )
            do {
                let sourceRange = try await sourceVerifier.fetchStableRange(
                    mt5Symbol: mapping.mt5Symbol,
                    from: chunkRange.from,
                    toExclusive: chunkRange.toExclusive,
                    maxBars: config.app.chunkSize
                ) {
                    try bridge.ratesRange(
                        mt5Symbol: mapping.mt5Symbol,
                        from: chunkRange.from,
                        toExclusive: chunkRange.toExclusive,
                        maxBars: config.app.chunkSize
                    )
                }
                try await recordChunkOperation(
                    auditStore: auditStore,
                    mapping: mapping,
                    batchId: batchId,
                    range: chunkRange,
                    status: .sourceVerified,
                    stage: "mt5_stable_double_read",
                    sourceBarCount: sourceRange.manifest.emittedCount,
                    canonicalRowCount: nil,
                    sourceHash: sourceRange.sourceHash,
                    mt5SourceSHA256: sourceRange.sourceSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                let closedBars = try sourceRange.response.rates.map {
                    try $0.toClosedM1Bar(logicalSymbol: mapping.logicalSymbol, mt5Symbol: mapping.mt5Symbol, digits: mapping.digits)
                }
                try validateClosedBarsInRange(closedBars, from: chunkRange.from, toExclusive: chunkRange.toExclusive)
                let now = UtcSecond(rawValue: Int64(Date().timeIntervalSince1970))
                guard !closedBars.isEmpty else {
                    guard chunkRange.toExclusive.rawValue < overallEndExclusive.rawValue else {
                        throw IngestError.invalidChunk("\(mapping.logicalSymbol.rawValue) final live range \(rangeLabel) was newer than the checkpoint but MT5 returned zero bars")
                    }
                    let canonicalVerification = try await CanonicalInsertVerifier(
                        clickHouse: clickHouse,
                        insertBuilder: insertBuilder
                    ).verifyEmptyMT5Range(
                        brokerSourceId: config.brokerTime.brokerSourceId,
                        logicalSymbol: mapping.logicalSymbol,
                        mt5Symbol: mapping.mt5Symbol,
                        mt5Start: chunkRange.from,
                        mt5EndExclusive: chunkRange.toExclusive
                    )
                    let coverageRecords = try coverageBuilder.makeRecords(
                        brokerSourceId: config.brokerTime.brokerSourceId,
                        logicalSymbol: mapping.logicalSymbol,
                        mt5Symbol: mapping.mt5Symbol,
                        mt5Start: chunkRange.from,
                        mt5EndExclusive: chunkRange.toExclusive,
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
                        batchId: batchId,
                        range: chunkRange,
                        status: .emptyCoverageVerified,
                        stage: "empty_source_coverage_written",
                        sourceBarCount: 0,
                        canonicalRowCount: 0,
                        sourceHash: sourceRange.sourceHash,
                        mt5SourceSHA256: sourceRange.sourceSHA256,
                        canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
                        offsetAuthoritySHA256: offsetAuthoritySHA256
                    )
                    logger.ok("\(mapping.logicalSymbol.rawValue) - \(rangeLabel) contains no MT5 bars; source gap verified and checkpoint left unchanged")
                    cursor = chunkRange.toExclusive
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
                logger.info("\(mapping.logicalSymbol.rawValue) - validating \(rangeLabel) for OHLC integrity and verified UTC conversion")
                let validated = try validator.validateBatch(closedBars, context: context)
                let canonicalVerification = try await writeValidatedBars(
                    validated,
                    insertBuilder: insertBuilder,
                    auditStore: auditStore,
                    mapping: mapping,
                    batchId: batchId,
                    range: chunkRange,
                    sourceBarCount: sourceRange.manifest.emittedCount,
                    sourceHash: sourceRange.sourceHash,
                    mt5SourceSHA256: sourceRange.sourceSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                guard let first = validated.first, let last = validated.last else {
                    throw IngestError.invalidChunk("validated live update chunk unexpectedly empty")
                }
                let coverageRecords = try coverageBuilder.makeRecords(
                    brokerSourceId: config.brokerTime.brokerSourceId,
                    logicalSymbol: mapping.logicalSymbol,
                    mt5Symbol: mapping.mt5Symbol,
                    mt5Start: chunkRange.from,
                    mt5EndExclusive: chunkRange.toExclusive,
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
                try await checkpointStore.save(IngestState(
                    brokerSourceId: config.brokerTime.brokerSourceId,
                    logicalSymbol: mapping.logicalSymbol,
                    mt5Symbol: mapping.mt5Symbol,
                    oldestMT5ServerTime: state.oldestMT5ServerTime,
                    latestIngestedClosedMT5ServerTime: last.mt5ServerTime,
                    latestIngestedClosedUtcTime: last.utcTime,
                    status: .live,
                    lastBatchId: batchId,
                    updatedAtUtc: now
                ))
                try await recordChunkOperation(
                    auditStore: auditStore,
                    mapping: mapping,
                    batchId: batchId,
                    range: chunkRange,
                    status: .checkpointed,
                    stage: "checkpoint_saved",
                    sourceBarCount: sourceRange.manifest.emittedCount,
                    canonicalRowCount: validated.count,
                    sourceHash: sourceRange.sourceHash,
                    mt5SourceSHA256: sourceRange.sourceSHA256,
                    canonicalReadbackSHA256: canonicalVerification.canonicalReadbackSHA256,
                    offsetAuthoritySHA256: offsetAuthoritySHA256
                )
                let verifiedRangeLabel = OperatorStatusText.monthRangeLabel(
                    start: first.utcTime,
                    endExclusive: try addOneMinute(to: last.utcTime)
                )
                totalInserted += validated.count
                logger.ok("\(mapping.logicalSymbol.rawValue) - \(verifiedRangeLabel) pulled, verified, UTC correct and canonical data clean (\(validated.count) new closed M1 bars)")
                cursor = try addOneMinute(to: last.mt5ServerTime)
            } catch {
                do {
                    try await recordChunkOperation(
                        auditStore: auditStore,
                        mapping: mapping,
                        batchId: batchId,
                        range: chunkRange,
                        status: .failed,
                        stage: "live_chunk_failed",
                        sourceBarCount: nil,
                        canonicalRowCount: nil,
                        sourceHash: nil,
                        offsetAuthoritySHA256: offsetAuthoritySHA256,
                        errorMessage: String(describing: error)
                    )
                } catch {
                    logger.warn("\(mapping.logicalSymbol.rawValue) - failed to record live ingest failure for \(rangeLabel): \(error)")
                }
                throw error
            }
        }
        logger.ok("\(mapping.logicalSymbol.rawValue) - live catch-up complete; \(totalInserted) new closed M1 bars inserted")
    }

    private func writeValidatedBars(
        _ bars: [ValidatedBar],
        insertBuilder: ClickHouseInsertBuilder,
        auditStore: IngestAuditStore,
        mapping: SymbolMapping,
        batchId: BatchId,
        range: (from: MT5ServerSecond, toExclusive: MT5ServerSecond),
        sourceBarCount: Int,
        sourceHash: String,
        mt5SourceSHA256: SHA256DigestHex,
        offsetAuthoritySHA256: SHA256DigestHex
    ) async throws -> CanonicalInsertVerificationResult {
        guard let first = bars.first else {
            throw IngestError.invalidChunk("cannot write an empty validated live update chunk")
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
            operationType: .live,
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

    private func validateClosedBarsInRange(_ bars: [ClosedM1Bar], from: MT5ServerSecond, toExclusive: MT5ServerSecond) throws {
        for bar in bars {
            guard bar.mt5ServerTime.rawValue >= from.rawValue,
                  bar.mt5ServerTime.rawValue < toExclusive.rawValue else {
                throw IngestError.invalidBridgeResponse("MT5 bar \(bar.mt5ServerTime.rawValue) is outside requested range \(from.rawValue)..<\(toExclusive.rawValue)")
            }
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
