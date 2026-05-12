import ClickHouse
import Config
import Domain
import Foundation
import Ingestion

public struct BacktestReadinessRequest: Sendable, Equatable {
    public let brokerSourceId: BrokerSourceId
    public let logicalSymbol: LogicalSymbol
    public let utcStart: UtcSecond
    public let utcEndExclusive: UtcSecond

    public init(
        brokerSourceId: BrokerSourceId,
        logicalSymbol: LogicalSymbol,
        utcStart: UtcSecond,
        utcEndExclusive: UtcSecond
    ) {
        self.brokerSourceId = brokerSourceId
        self.logicalSymbol = logicalSymbol
        self.utcStart = utcStart
        self.utcEndExclusive = utcEndExclusive
    }

    public init(config: HistoryDataConfigFile) {
        self.init(
            brokerSourceId: config.brokerSourceId,
            logicalSymbol: config.logicalSymbol,
            utcStart: config.fromUtc,
            utcEndExclusive: config.toUtc
        )
    }
}

public enum BacktestReadinessError: Error, CustomStringConvertible, Sendable {
    case invalidRange(UtcSecond, UtcSecond)
    case brokerMismatch(expected: BrokerSourceId, actual: BrokerSourceId)
    case unconfiguredSymbol(LogicalSymbol)
    case missingCheckpoint(LogicalSymbol)
    case incompleteIngest(symbol: LogicalSymbol, status: IngestStatus)
    case mt5SymbolMismatch(symbol: LogicalSymbol, expected: MT5Symbol, actual: MT5Symbol)
    case requestedRangeBeyondCheckpoint(symbol: LogicalSymbol, requestedEnd: UtcSecond, checkpoint: UtcSecond)
    case noCanonicalBars(LogicalSymbol, UtcSecond, UtcSecond)
    case missingVerifiedCoverage(symbol: LogicalSymbol, from: UtcSecond, to: UtcSecond)
    case unfinishedIngestOperations(Int64)
    case duplicateCanonicalKeys(Int64)
    case ohlcInvariantFailures(Int64)
    case nonVerifiedCanonicalOffsets(Int64)
    case unresolvedVerificationMismatches(Int64)
    case failedRepairs(Int64)
    case blockingAgentState(String)
    case missingRequiredAgentState(ProductionAgentKind)
    case staleRequiredAgentState(agent: ProductionAgentKind, lastOkAtUtc: UtcSecond, maxAgeSeconds: Int64)
    case invalidScalar(String)

    public var description: String {
        switch self {
        case .invalidRange(let from, let to):
            return "Backtest UTC range is invalid or not minute-aligned: \(from.rawValue)..<\(to.rawValue)."
        case .brokerMismatch(let expected, let actual):
            return "Backtest broker_source_id \(actual.rawValue) does not match loaded config broker_source_id \(expected.rawValue)."
        case .unconfiguredSymbol(let symbol):
            return "\(symbol.rawValue) is not configured in symbols.json."
        case .missingCheckpoint(let symbol):
            return "\(symbol.rawValue) has no ingest checkpoint. Run or resume backfill before backtesting."
        case .incompleteIngest(let symbol, let status):
            return "\(symbol.rawValue) ingest status is \(status.rawValue), not live. Backfill is incomplete or was interrupted."
        case .mt5SymbolMismatch(let symbol, let expected, let actual):
            return "\(symbol.rawValue) checkpoint MT5 symbol is \(actual.rawValue), expected \(expected.rawValue)."
        case .requestedRangeBeyondCheckpoint(let symbol, let requestedEnd, let checkpoint):
            return "\(symbol.rawValue) requested backtest end \(requestedEnd.rawValue) is beyond latest verified checkpoint \(checkpoint.rawValue)."
        case .noCanonicalBars(let symbol, let from, let to):
            return "\(symbol.rawValue) has no canonical bars in requested UTC range \(from.rawValue)..<\(to.rawValue)."
        case .missingVerifiedCoverage(let symbol, let from, let to):
            return "Backtest blocked: \(symbol.rawValue) UTC range \(from.rawValue)..<\(to.rawValue) is not fully covered by verified MT5 source coverage records."
        case .unfinishedIngestOperations(let count):
            return "Backtest blocked: \(count) ingest operation batch(es) are unfinished or failed. Resume the importer/live updater or verifier repair so each batch reaches a terminal verified status."
        case .duplicateCanonicalKeys(let count):
            return "Backtest blocked: \(count) duplicate canonical UTC key group(s) exist."
        case .ohlcInvariantFailures(let count):
            return "Backtest blocked: \(count) canonical OHLC invariant failure row(s) exist."
        case .nonVerifiedCanonicalOffsets(let count):
            return "Backtest blocked: \(count) canonical row(s) have non-verified UTC offsets."
        case .unresolvedVerificationMismatches(let count):
            return "Backtest blocked: \(count) verification range(s) still have latest mismatch results."
        case .failedRepairs(let count):
            return "Backtest blocked: \(count) repair range(s) still have latest failed repair outcomes."
        case .blockingAgentState(let details):
            return "Backtest blocked by production agent state: \(details)"
        case .missingRequiredAgentState(let agent):
            return "Backtest blocked: required safety agent \(agent.rawValue) has not reported a successful run yet."
        case .staleRequiredAgentState(let agent, let lastOkAtUtc, let maxAgeSeconds):
            return "Backtest blocked: required safety agent \(agent.rawValue) last passed at \(lastOkAtUtc.rawValue), older than \(maxAgeSeconds) seconds."
        case .invalidScalar(let body):
            return "Backtest readiness query returned an invalid scalar value: \(body)"
        }
    }
}

public struct BacktestReadinessGate: Sendable {
    private let config: ConfigBundle
    private let clickHouse: ClickHouseClientProtocol

    public init(config: ConfigBundle, clickHouse: ClickHouseClientProtocol) {
        self.config = config
        self.clickHouse = clickHouse
    }

    public func assertReady(_ request: BacktestReadinessRequest) async throws {
        try validateRequest(request)
        try await validateIngestIsComplete(request)
        try await validateNoUnfinishedIngestOperations()
        try await validateVerifiedCoverage(request)
        try await validateCanonicalDatabase()
        try await validateRequestedRangeHasData(request)
        try await validateVerificationAndRepairState()
        try await validateAgentState()
        try await validateRequiredAgentOkState()
    }

    private func validateRequest(_ request: BacktestReadinessRequest) throws {
        guard request.utcStart.rawValue < request.utcEndExclusive.rawValue,
              request.utcStart.isMinuteAligned,
              request.utcEndExclusive.isMinuteAligned else {
            throw BacktestReadinessError.invalidRange(request.utcStart, request.utcEndExclusive)
        }
        guard request.brokerSourceId == config.brokerTime.brokerSourceId else {
            throw BacktestReadinessError.brokerMismatch(expected: config.brokerTime.brokerSourceId, actual: request.brokerSourceId)
        }
        guard config.symbols.mapping(for: request.logicalSymbol) != nil else {
            throw BacktestReadinessError.unconfiguredSymbol(request.logicalSymbol)
        }
    }

    private func validateIngestIsComplete(_ request: BacktestReadinessRequest) async throws {
        let checkpointStore = ClickHouseCheckpointStore(
            client: clickHouse,
            insertBuilder: ClickHouseInsertBuilder(database: config.clickHouse.database),
            database: config.clickHouse.database
        )
        for mapping in config.symbols.symbols {
            guard let state = try await checkpointStore.latestState(
                brokerSourceId: config.brokerTime.brokerSourceId,
                logicalSymbol: mapping.logicalSymbol
            ) else {
                throw BacktestReadinessError.missingCheckpoint(mapping.logicalSymbol)
            }
            guard state.mt5Symbol == mapping.mt5Symbol else {
                throw BacktestReadinessError.mt5SymbolMismatch(
                    symbol: mapping.logicalSymbol,
                    expected: mapping.mt5Symbol,
                    actual: state.mt5Symbol
                )
            }
            guard state.status == .live else {
                throw BacktestReadinessError.incompleteIngest(symbol: mapping.logicalSymbol, status: state.status)
            }
            if mapping.logicalSymbol == request.logicalSymbol {
                let requiredEnd = request.utcEndExclusive.rawValue - 60
                guard state.latestIngestedClosedUtcTime.rawValue >= requiredEnd else {
                    throw BacktestReadinessError.requestedRangeBeyondCheckpoint(
                        symbol: mapping.logicalSymbol,
                        requestedEnd: request.utcEndExclusive,
                        checkpoint: state.latestIngestedClosedUtcTime
                    )
                }
            }
        }
    }

    private func validateNoUnfinishedIngestOperations() async throws {
        let unfinished = try await scalar("""
        SELECT count()
        FROM (
            SELECT operation_type, batch_id, argMax(status, tuple(event_at_utc, status_rank)) AS latest_status
            FROM \(config.clickHouse.database).ingest_operations
            WHERE broker_source_id = '\(SQLText.literal(config.brokerTime.brokerSourceId.rawValue))'
            GROUP BY operation_type, batch_id
        )
        WHERE NOT (
            (operation_type IN ('backfill', 'live') AND latest_status IN ('checkpointed', 'empty_coverage_verified'))
            OR (operation_type = 'repair' AND latest_status = 'repair_verified')
        )
        FORMAT TabSeparated
        """)
        guard unfinished == 0 else { throw BacktestReadinessError.unfinishedIngestOperations(unfinished) }
    }

    private func validateVerifiedCoverage(_ request: BacktestReadinessRequest) async throws {
        let body = try await clickHouse.execute(.select("""
        SELECT utc_range_start, utc_range_end_exclusive
        FROM \(config.clickHouse.database).ohlc_m1_verified_coverage
        WHERE broker_source_id = '\(SQLText.literal(request.brokerSourceId.rawValue))'
          AND logical_symbol = '\(SQLText.literal(request.logicalSymbol.rawValue))'
          AND timeframe = 'M1'
          AND utc_range_end_exclusive > \(request.utcStart.rawValue)
          AND utc_range_start < \(request.utcEndExclusive.rawValue)
        ORDER BY utc_range_start ASC, utc_range_end_exclusive ASC
        FORMAT TabSeparated
        """))
        let intervals = try parseCoverageIntervals(body)
        var cursor = request.utcStart.rawValue
        for interval in intervals where interval.end > cursor {
            guard interval.start <= cursor else {
                throw BacktestReadinessError.missingVerifiedCoverage(
                    symbol: request.logicalSymbol,
                    from: UtcSecond(rawValue: cursor),
                    to: UtcSecond(rawValue: min(interval.start, request.utcEndExclusive.rawValue))
                )
            }
            cursor = max(cursor, interval.end)
            if cursor >= request.utcEndExclusive.rawValue {
                return
            }
        }
        throw BacktestReadinessError.missingVerifiedCoverage(
            symbol: request.logicalSymbol,
            from: UtcSecond(rawValue: cursor),
            to: request.utcEndExclusive
        )
    }

    private func validateCanonicalDatabase() async throws {
        let duplicates = try await scalar("""
        SELECT count()
        FROM (
            SELECT broker_source_id, logical_symbol, ts_utc, count()
            FROM \(config.clickHouse.database).ohlc_m1_canonical
            WHERE broker_source_id = '\(SQLText.literal(config.brokerTime.brokerSourceId.rawValue))'
            GROUP BY broker_source_id, logical_symbol, ts_utc
            HAVING count() > 1
        )
        FORMAT TabSeparated
        """)
        guard duplicates == 0 else { throw BacktestReadinessError.duplicateCanonicalKeys(duplicates) }

        let ohlcFailures = try await scalar("""
        SELECT count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        WHERE broker_source_id = '\(SQLText.literal(config.brokerTime.brokerSourceId.rawValue))'
          AND (open_scaled <= 0 OR high_scaled < open_scaled OR high_scaled < close_scaled OR high_scaled < low_scaled OR low_scaled > open_scaled OR low_scaled > close_scaled)
        FORMAT TabSeparated
        """)
        guard ohlcFailures == 0 else { throw BacktestReadinessError.ohlcInvariantFailures(ohlcFailures) }

        let nonVerifiedOffsets = try await scalar("""
        SELECT count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        WHERE broker_source_id = '\(SQLText.literal(config.brokerTime.brokerSourceId.rawValue))'
          AND offset_confidence != 'verified'
        FORMAT TabSeparated
        """)
        guard nonVerifiedOffsets == 0 else { throw BacktestReadinessError.nonVerifiedCanonicalOffsets(nonVerifiedOffsets) }
    }

    private func validateRequestedRangeHasData(_ request: BacktestReadinessRequest) async throws {
        let rows = try await scalar("""
        SELECT count()
        FROM \(config.clickHouse.database).ohlc_m1_canonical
        WHERE broker_source_id = '\(SQLText.literal(request.brokerSourceId.rawValue))'
          AND logical_symbol = '\(SQLText.literal(request.logicalSymbol.rawValue))'
          AND ts_utc >= \(request.utcStart.rawValue)
          AND ts_utc < \(request.utcEndExclusive.rawValue)
        FORMAT TabSeparated
        """)
        guard rows > 0 else {
            throw BacktestReadinessError.noCanonicalBars(request.logicalSymbol, request.utcStart, request.utcEndExclusive)
        }
    }

    private func validateVerificationAndRepairState() async throws {
        let mismatches = try await scalar("""
        SELECT count()
        FROM (
            SELECT logical_symbol, range_start_mt5_server_ts, range_end_mt5_server_ts,
                   argMax(result, checked_at_utc) AS latest_result
            FROM \(config.clickHouse.database).verification_results
            WHERE broker_source_id = '\(SQLText.literal(config.brokerTime.brokerSourceId.rawValue))'
            GROUP BY logical_symbol, range_start_mt5_server_ts, range_end_mt5_server_ts
        )
        WHERE latest_result != 'clean'
        FORMAT TabSeparated
        """)
        guard mismatches == 0 else { throw BacktestReadinessError.unresolvedVerificationMismatches(mismatches) }

        let failedRepairs = try await scalar("""
        SELECT count()
        FROM (
            SELECT logical_symbol, range_start_mt5_server_ts, range_end_mt5_server_ts,
                   argMax(outcome, created_at_utc) AS latest_outcome
            FROM \(config.clickHouse.database).repair_log
            WHERE broker_source_id = '\(SQLText.literal(config.brokerTime.brokerSourceId.rawValue))'
            GROUP BY logical_symbol, range_start_mt5_server_ts, range_end_mt5_server_ts
        )
        WHERE latest_outcome = 'failed'
        FORMAT TabSeparated
        """)
        guard failedRepairs == 0 else { throw BacktestReadinessError.failedRepairs(failedRepairs) }
    }

    private func validateAgentState() async throws {
        let agentList = AgentExecutionPolicy.backtestBlockingAgentKinds
            .map { "'\(SQLText.literal($0.rawValue))'" }
            .sorted()
            .joined(separator: ",")
        let body = try await clickHouse.execute(.select("""
        SELECT agent_name, status, last_message
        FROM \(config.clickHouse.database).runtime_agent_state FINAL
        WHERE broker_source_id = '\(SQLText.literal(config.brokerTime.brokerSourceId.rawValue))'
          AND agent_name IN (\(agentList))
          AND status IN ('warning', 'failed')
        ORDER BY agent_name ASC
        FORMAT TabSeparated
        """))
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.isEmpty else {
            throw BacktestReadinessError.blockingAgentState(trimmed.replacingOccurrences(of: "\n", with: " | "))
        }
    }

    private func validateRequiredAgentOkState() async throws {
        let requiredAgents = AgentExecutionPolicy.backtestRequiredOkAgentKinds.sorted { $0.priorityRank < $1.priorityRank }
        let agentList = requiredAgents
            .map { "'\(SQLText.literal($0.rawValue))'" }
            .joined(separator: ",")
        let body = try await clickHouse.execute(.select("""
        SELECT agent_name, last_ok_at_utc
        FROM \(config.clickHouse.database).runtime_agent_state FINAL
        WHERE broker_source_id = '\(SQLText.literal(config.brokerTime.brokerSourceId.rawValue))'
          AND agent_name IN (\(agentList))
        FORMAT TabSeparated
        """))
        let rows = try parseAgentLastOkRows(body)
        let now = utcNow()
        for agent in requiredAgents {
            guard let lastOkRaw = rows[agent], lastOkRaw > 0 else {
                throw BacktestReadinessError.missingRequiredAgentState(agent)
            }
            guard lastOkRaw <= now.rawValue + 60 else {
                throw BacktestReadinessError.blockingAgentState(
                    "\(agent.rawValue) last_ok_at_utc is in the future: \(lastOkRaw)"
                )
            }
            let maxAge = AgentFreshnessPolicy(config: config).maxOkAgeSeconds(for: agent)
            if now.rawValue - lastOkRaw > maxAge {
                throw BacktestReadinessError.staleRequiredAgentState(
                    agent: agent,
                    lastOkAtUtc: UtcSecond(rawValue: lastOkRaw),
                    maxAgeSeconds: maxAge
                )
            }
        }
    }

    private func parseAgentLastOkRows(_ body: String) throws -> [ProductionAgentKind: Int64] {
        var rows: [ProductionAgentKind: Int64] = [:]
        for line in body.split(separator: "\n", omittingEmptySubsequences: true) {
            let fields = line.split(separator: "\t", omittingEmptySubsequences: false)
            guard fields.count == 2,
                  let agent = ProductionAgentKind(rawValue: String(fields[0])),
                  let lastOk = Int64(fields[1]) else {
                throw BacktestReadinessError.blockingAgentState("invalid runtime_agent_state row: \(line)")
            }
            rows[agent] = lastOk
        }
        return rows
    }

    private func parseCoverageIntervals(_ body: String) throws -> [UtcCoverageInterval] {
        try body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { line in
                let fields = line.split(separator: "\t", omittingEmptySubsequences: false)
                guard fields.count == 2,
                      let start = Int64(fields[0]),
                      let end = Int64(fields[1]),
                      start < end else {
                    throw BacktestReadinessError.invalidScalar(String(line))
                }
                return UtcCoverageInterval(start: start, end: end)
            }
    }

    private func scalar(_ sql: String) async throws -> Int64 {
        let body = try await clickHouse.execute(.select(sql))
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let value = Int64(trimmed) else {
            throw BacktestReadinessError.invalidScalar(trimmed)
        }
        return value
    }
}

private struct UtcCoverageInterval: Sendable {
    let start: Int64
    let end: Int64
}
