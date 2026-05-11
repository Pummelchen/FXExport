import ClickHouse
import Config
import Domain
import Foundation
import Ingestion
import MT5Bridge
import TimeMapping
import Validation
import Verification

public enum RecoverySeverity: String, Sendable, Equatable {
    case info
    case warning
    case stop
}

public struct OperationalRecoveryAdvice: Sendable, Equatable {
    public let code: String
    public let title: String
    public let severity: RecoverySeverity
    public let summary: String
    public let automaticRecovery: String
    public let dataSafety: String
    public let operatorSteps: [String]
    public let commands: [String]

    public init(
        code: String,
        title: String,
        severity: RecoverySeverity,
        summary: String,
        automaticRecovery: String,
        dataSafety: String,
        operatorSteps: [String],
        commands: [String] = []
    ) {
        self.code = code
        self.title = title
        self.severity = severity
        self.summary = summary
        self.automaticRecovery = automaticRecovery
        self.dataSafety = dataSafety
        self.operatorSteps = operatorSteps
        self.commands = commands
    }

    public var formatted: String {
        var lines = [
            "[\(code)] \(title)",
            "What happened: \(summary)",
            "Automatic recovery: \(automaticRecovery)",
            "Data safety: \(dataSafety)"
        ]
        if !operatorSteps.isEmpty {
            lines.append("Human action:")
            lines.append(contentsOf: operatorSteps.enumerated().map { index, step in "  \(index + 1). \(step)" })
        }
        if !commands.isEmpty {
            lines.append("Useful commands:")
            lines.append(contentsOf: commands.map { "  \($0)" })
        }
        return lines.joined(separator: "\n")
    }
}

public enum OperationalFailureGuide {
    public static func advice(for error: Error) -> OperationalRecoveryAdvice {
        if let error = error as? ClickHouseStartupError {
            return clickHouseStartupAdvice(error)
        }
        if let error = error as? ClickHouseError {
            return clickHouseAdvice(error)
        }
        if let error = error as? MT5BridgeError {
            return mt5BridgeAdvice(error)
        }
        if let error = error as? ProtocolError {
            return protocolAdvice(error)
        }
        if let error = error as? BrokerOffsetStoreError {
            return brokerOffsetStoreAdvice(error)
        }
        if let error = error as? BrokerOffsetRuntimeError {
            return brokerOffsetRuntimeAdvice(error)
        }
        if let error = error as? TimeMappingError {
            return timeMappingAdvice(error)
        }
        if let error = error as? ValidationError {
            return validationAdvice(error)
        }
        if let error = error as? IngestError {
            return ingestAdvice(error)
        }
        if let error = error as? CheckpointError {
            return checkpointAdvice(error)
        }
        if let error = error as? VerificationError {
            return verificationAdvice(error)
        }
        if let error = error as? HistoricalRangeVerifierError {
            return verifierAdvice(error)
        }
        if let error = error as? RepairError {
            return repairAdvice(error)
        }
        if let error = error as? BacktestReadinessError {
            return backtestReadinessAdvice(error)
        }
        if let error = error as? TerminalIdentityPolicyError {
            return terminalIdentityAdvice(error)
        }
        if let error = error as? SupervisorError {
            return supervisorAdvice(error)
        }
        if let error = error as? ConfigError {
            return configAdvice(error)
        }
        if let error = error as? StartCheckError {
            return startCheckAdvice(error)
        }
        if let error = error as? ClickHouseInsertError {
            return clickHouseInsertAdvice(error)
        }
        return unknownAdvice(error)
    }

    public static func catalogText() -> String {
        scenarioCatalog.map(\.formatted).joined(separator: "\n\n")
    }

    public static let scenarioCatalog: [OperationalRecoveryAdvice] = [
        scenario(
            code: "DB-001",
            title: "ClickHouse HTTP endpoint is down",
            summary: "The local HTTP endpoint is not reachable.",
            automaticRecovery: "For localhost/127.0.0.1, the program tries Homebrew and standalone ClickHouse start commands, then waits for HTTP readiness.",
            dataSafety: "No checkpoint is advanced while ClickHouse is unavailable.",
            steps: ["If auto-start fails, inspect the service status and logs before rerunning the same command."],
            commands: ["brew services list | grep clickhouse", "brew services start clickhouse", "clickhouse start", "curl http://localhost:8123"]
        ),
        scenario(
            code: "DB-002",
            title: "ClickHouse authentication or HTTP status failure",
            summary: "ClickHouse responded, but rejected the request or returned a non-2xx status.",
            automaticRecovery: "The program does not guess credentials or modify users.",
            dataSafety: "Writes stop before checkpoints move.",
            steps: ["Fix only local Config/clickhouse.json.", "Do not put passwords into Git, wiki, README, or command snippets."],
            commands: ["FXExport startcheck --config-dir Config --migrations-dir Migrations"]
        ),
        scenario(
            code: "DB-003",
            title: "ClickHouse exception in response body",
            summary: "ClickHouse returned HTTP success but the body contains a DB::Exception.",
            automaticRecovery: "The client detects this and fails the operation instead of treating HTTP 200 as success.",
            dataSafety: "The failed batch is not considered inserted unless readback verification succeeds.",
            steps: ["Run migrations.", "Inspect schema drift or SQL exception text.", "Rerun the interrupted operation after fixing the schema."],
            commands: ["FXExport migrate --config-dir Config --migrations-dir Migrations"]
        ),
        scenario(
            code: "MT5-001",
            title: "MT5 bridge not connected",
            summary: "Swift could not accept/connect the localhost TCP bridge.",
            automaticRecovery: "The supervisor retries connection on later cycles; standalone commands print exact EA/port steps.",
            dataSafety: "No MT5-backed ingestion happens without a valid bridge.",
            steps: ["Start MT5 under Wine.", "Attach HistoryBridgeEA.", "Match SwiftHost/SwiftPort to Config/mt5_bridge.json.", "Enable Algo Trading and localhost sockets."],
            commands: ["FXExport startcheck --config-dir Config --migrations-dir Migrations", "lsof -nP -iTCP:5055"]
        ),
        scenario(
            code: "MT5-002",
            title: "MT5 bridge disconnects during live run",
            summary: "The socket closed or a read/write failed while the live updater was running.",
            automaticRecovery: "Bridge failures are rethrown so supervise/live can reconnect and retry from the last verified checkpoint.",
            dataSafety: "The current batch is abandoned unless canonical readback verification completed.",
            steps: ["If reconnect loops continue, reattach the EA and rerun startcheck."],
            commands: ["FXExport supervise --config-dir Config"]
        ),
        scenario(
            code: "MT5-003",
            title: "Protocol frame or checksum failure",
            summary: "The EA and Swift exchanged malformed, mismatched, or unsupported protocol data.",
            automaticRecovery: "The frame is rejected; no partial payload is accepted.",
            dataSafety: "Malformed MT5 data never reaches canonical storage.",
            steps: ["Recompile the EA from the same repo version.", "Reattach the EA.", "Rerun full startcheck."],
            commands: ["FXExport startcheck --config-dir Config --migrations-dir Migrations --skip-bridge"]
        ),
        scenario(
            code: "MT5-004",
            title: "MT5 history not synchronized",
            summary: "The terminal has not finished downloading local M1 history.",
            automaticRecovery: "Backfill waits up to the configured synchronization window before failing the symbol.",
            dataSafety: "The app refuses to snapshot oldest/latest history while MT5 is incomplete.",
            steps: ["Open the symbol in MT5 Market Watch.", "Let MT5 download history.", "Rerun backfill."],
            commands: ["FXExport symbol-check --config-dir Config", "FXExport backfill --config-dir Config --symbols all"]
        ),
        scenario(
            code: "MT5-005",
            title: "MetaEditor EA compile or toolchain failure",
            summary: "The EA source, MetaEditor executable, Wine path, or compile output is missing or invalid.",
            automaticRecovery: "The startcheck compile stage stops before any MT5-backed ingestion can run.",
            dataSafety: "Swift refuses to trust an unknown or stale EA binary for history export.",
            steps: ["Confirm MT5 and MetaEditor are installed.", "Set MT5RESEARCH_METAEDITOR, MT5RESEARCH_WINE, or MT5RESEARCH_WINEPREFIX only when the default toolchain cannot be found.", "Rerun startcheck after the EA compiles cleanly."],
            commands: ["FXExport startcheck --config-dir Config --migrations-dir Migrations --skip-bridge --compile-timeout-seconds 180"]
        ),
        scenario(
            code: "TIME-001",
            title: "Missing verified broker UTC offsets",
            summary: "Canonical UTC conversion has no active verified segment for the exact MT5 company/server/account.",
            automaticRecovery: "None. This requires audited human confirmation.",
            dataSafety: "Canonical rows are blocked; raw MT5 timestamps remain the source evidence.",
            steps: ["Run full startcheck to print MT5 identity.", "Insert verified broker_time_offsets for every historical server-time segment.", "Rerun startcheck before ingest."],
            commands: ["FXExport startcheck --config-dir Config --migrations-dir Migrations"]
        ),
        scenario(
            code: "TIME-002",
            title: "Live broker offset mismatch",
            summary: "The EA-observed live server offset differs from the audited DB offset or accepted offset list.",
            automaticRecovery: "Ingestion and verification are blocked.",
            dataSafety: "The program does not invent UTC or continue with ambiguous broker time.",
            steps: ["Confirm the connected MT5 server/account.", "Inspect DST/server-time change.", "Add or correct verified offset segment only after proof."],
            commands: ["FXExport bridge-check --config-dir Config"]
        ),
        scenario(
            code: "TIME-003",
            title: "UTC conversion gap or overlap",
            summary: "The broker offset map cannot convert a raw MT5 timestamp cleanly.",
            automaticRecovery: "The affected batch is rejected.",
            dataSafety: "No canonical UTC row is written for unresolved time.",
            steps: ["Repair broker_time_offsets coverage.", "Rerun backfill or repair after startcheck passes."],
            commands: ["FXExport startcheck --config-dir Config --migrations-dir Migrations"]
        ),
        scenario(
            code: "DATA-001",
            title: "OHLC validation failed",
            summary: "A bar is not M1, is open/future, has non-positive prices, bad high/low invariants, duplicate timestamps, or wrong digits.",
            automaticRecovery: "The batch is rejected and checkpoint does not move.",
            dataSafety: "Invalid bars cannot enter canonical storage.",
            steps: ["Check symbol mapping/digits.", "Run symbol-check.", "If MT5 data itself is bad, preserve audit evidence and do not force repair."],
            commands: ["FXExport symbol-check --config-dir Config"]
        ),
        scenario(
            code: "DATA-002",
            title: "Canonical insert readback verification failed",
            summary: "After insert, ClickHouse readback did not match the exact expected timestamp/hash sequence.",
            automaticRecovery: "The checkpoint is not advanced; rerun will reprocess the range.",
            dataSafety: "Backtests remain blocked until verification and checkpoint state are clean.",
            steps: ["Rerun verify.", "Rerun backfill for the affected symbol.", "Inspect ohlc_m1_conflicts before manual intervention."],
            commands: ["FXExport verify --config-dir Config --random-ranges 0"]
        ),
        scenario(
            code: "DATA-003",
            title: "Duplicate canonical UTC key",
            summary: "ClickHouse contains more than one canonical row for the same broker/symbol/UTC identity.",
            automaticRecovery: "Verifier blocks backtests; repair only happens when MT5 comparison is unambiguous.",
            dataSafety: "Raw audit rows are never deleted.",
            steps: ["Run MT5-backed verify.", "Use repair for the exact UTC range if MT5 source is available and offset mapping is verified."],
            commands: ["FXExport verify --config-dir Config --random-ranges 20"]
        ),
        scenario(
            code: "STATE-001",
            title: "Checkpoint missing or interrupted first run",
            summary: "A configured symbol has no checkpoint or status is still backfilling.",
            automaticRecovery: "Backfill resumes from the last verified checkpoint.",
            dataSafety: "Backtests are blocked until all configured symbols are live.",
            steps: ["Rerun backfill; do not edit ingest_state by hand."],
            commands: ["FXExport backfill --config-dir Config --symbols all"]
        ),
        scenario(
            code: "STATE-002",
            title: "Checkpoint ahead of MT5",
            summary: "The saved checkpoint is newer than MT5's latest closed bar.",
            automaticRecovery: "Ingestion stops for that symbol.",
            dataSafety: "This may indicate wrong account/server/source; continuing could corrupt history.",
            steps: ["Confirm broker_source_id and MT5 terminal identity.", "Inspect symbols and offsets before resuming."],
            commands: ["FXExport bridge-check --config-dir Config", "FXExport startcheck --config-dir Config --migrations-dir Migrations"]
        ),
        scenario(
            code: "STATE-003",
            title: "Checkpoint MT5 symbol mismatch",
            summary: "The checkpoint was created for a different MT5 symbol than current config.",
            automaticRecovery: "Ingestion stops.",
            dataSafety: "The app refuses to merge histories from different broker symbols.",
            steps: ["Fix Config/symbols.json or create a new broker_source_id if this is truly a different data source."],
            commands: ["FXExport symbol-check --config-dir Config"]
        ),
        scenario(
            code: "VERIFY-001",
            title: "Random MT5 cross-check mismatch",
            summary: "MT5 source-of-truth data does not match canonical ClickHouse data.",
            automaticRecovery: "Canonical repair is attempted only when MT5 data and UTC mapping are unambiguous.",
            dataSafety: "Backtests are blocked until latest verification results are clean.",
            steps: ["Inspect verification_results and repair_log.", "Run repair only on explicit ranges with MT5 available."],
            commands: ["FXExport verify --config-dir Config --random-ranges 20"]
        ),
        scenario(
            code: "REPAIR-001",
            title: "Repair refused or failed",
            summary: "The requested repair was unsafe, ambiguous, or did not pass post-repair verification.",
            automaticRecovery: "The range remains marked unsafe.",
            dataSafety: "Raw audit data and conflict records are preserved.",
            steps: ["Do not delete raw audit rows.", "Fix UTC/source ambiguity, then rerun verify and repair."],
            commands: ["FXExport repair --config-dir Config --symbol EURUSD --from 2020-01-01 --to 2020-02-01"]
        ),
        scenario(
            code: "BACKTEST-001",
            title: "Backtest data readiness blocked",
            summary: "The readiness gate found incomplete ingest, damaged data, stale safety agents, or unresolved verification/repair state.",
            automaticRecovery: "None inside backtest; it fails closed.",
            dataSafety: "Research never runs on data the agents know is unsafe.",
            steps: ["Run supervise/startcheck/verify until all safety agents are OK and all symbols are live."],
            commands: ["FXExport supervise --config-dir Config --supervisor-cycles 1", "FXExport verify --config-dir Config --random-ranges 20"]
        ),
        scenario(
            code: "LOG-001",
            title: "Persistent logging unavailable",
            summary: "The configured log or alert file cannot be created, written, or rotated.",
            automaticRecovery: "Terminal logging continues; the affected persistent sink is disabled for that process.",
            dataSafety: "Ingestion and checkpoints do not depend on log-file writes, but unattended monitoring is degraded.",
            steps: ["Fix the configured log directory permissions or disk space.", "Rerun startcheck or supervise and confirm the persistent log file enabled message appears."],
            commands: ["mkdir -p Logs", "df -h", "FXExport startcheck --config-dir Config --migrations-dir Migrations --skip-bridge"]
        ),
        scenario(
            code: "CONFIG-001",
            title: "Invalid configuration",
            summary: "A required config file or value is missing or invalid.",
            automaticRecovery: "The program stops before touching MT5 or ClickHouse data.",
            dataSafety: "No source identity or symbol assumptions are guessed.",
            steps: ["Copy fresh samples if needed.", "Fix local Config files only.", "Never commit credentials."],
            commands: ["cp ConfigSamples/*.sample.json Config/"]
        ),
        scenario(
            code: "LOCK-001",
            title: "Another writer is already running",
            summary: "The broker runtime lock is held by backfill, live, repair, or supervise.",
            automaticRecovery: "The second process exits before writing.",
            dataSafety: "Only one canonical writer/checkpoint owner runs per broker source.",
            steps: ["Use the existing process, or stop it cleanly before starting another writer."],
            commands: ["ps aux | grep FXExport"]
        ),
        scenario(
            code: "OS-001",
            title: "Disk full or ClickHouse storage pressure",
            summary: "The local machine or ClickHouse data directory has insufficient writable storage.",
            automaticRecovery: "No destructive cleanup is attempted automatically.",
            dataSafety: "The current batch fails closed and checkpoints do not advance unless readback verification completed.",
            steps: ["Free disk space or move ClickHouse storage safely.", "Restart ClickHouse.", "Rerun startcheck and verify before resuming backtests."],
            commands: ["df -h", "brew services restart clickhouse", "FXExport startcheck --config-dir Config --migrations-dir Migrations"]
        ),
        scenario(
            code: "OS-002",
            title: "Computer sleep, shutdown, or process interruption",
            summary: "The process stopped while backfill, live update, verification, or repair was active.",
            automaticRecovery: "Backfill/live resume from the last verified checkpoint; canonical repair ranges require explicit verification.",
            dataSafety: "Interrupted work is reprocessed; checkpoints are advanced only after validated insert and readback checks.",
            steps: ["Start ClickHouse/MT5 if needed.", "Run startcheck.", "Resume backfill or supervise; do not edit ingest_state manually."],
            commands: ["FXExport startcheck --config-dir Config --migrations-dir Migrations", "FXExport backfill --config-dir Config --symbols all", "FXExport supervise --config-dir Config --with-backfill"]
        )
    ]

    private static func clickHouseStartupAdvice(_ error: ClickHouseStartupError) -> OperationalRecoveryAdvice {
        switch error {
        case .notAutoStartable:
            return withSummary(from: error, base: catalogScenario("DB-001"))
        case .noStartCommand, .startFailed:
            return withSummary(from: error, base: catalogScenario("DB-001"))
        }
    }

    private static func clickHouseAdvice(_ error: ClickHouseError) -> OperationalRecoveryAdvice {
        switch error {
        case .transport:
            return withSummary(from: error, base: catalogScenario("DB-001"))
        case .httpStatus:
            return withSummary(from: error, base: catalogScenario("DB-002"))
        case .exception:
            return withSummary(from: error, base: catalogScenario("DB-003"))
        case .invalidURL, .decoding, .nonIdempotentRetryRefused:
            return withSummary(from: error, base: catalogScenario("DB-002"))
        }
    }

    private static func mt5BridgeAdvice(_ error: MT5BridgeError) -> OperationalRecoveryAdvice {
        switch error {
        case .connectionClosed, .readFailed, .writeFailed:
            return withSummary(from: error, base: catalogScenario("MT5-002"))
        default:
            return withSummary(from: error, base: catalogScenario("MT5-001"))
        }
    }

    private static func protocolAdvice(_ error: ProtocolError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("MT5-003"))
    }

    private static func brokerOffsetStoreAdvice(_ error: BrokerOffsetStoreError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("TIME-001"))
    }

    private static func brokerOffsetRuntimeAdvice(_ error: BrokerOffsetRuntimeError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("TIME-002"))
    }

    private static func timeMappingAdvice(_ error: TimeMappingError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("TIME-003"))
    }

    private static func validationAdvice(_ error: ValidationError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("DATA-001"))
    }

    private static func ingestAdvice(_ error: IngestError) -> OperationalRecoveryAdvice {
        switch error {
        case .canonicalInsertVerificationFailed:
            return withSummary(from: error, base: catalogScenario("DATA-002"))
        case .checkpointAheadOfMT5:
            return withSummary(from: error, base: catalogScenario("STATE-002"))
        case .checkpointSymbolMismatch:
            return withSummary(from: error, base: catalogScenario("STATE-003"))
        case .invalidBridgeResponse:
            return withSummary(from: error, base: catalogScenario("MT5-003"))
        default:
            return withSummary(from: error, base: catalogScenario("DATA-001"))
        }
    }

    private static func checkpointAdvice(_ error: CheckpointError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("STATE-001"))
    }

    private static func verificationAdvice(_ error: VerificationError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("VERIFY-001"))
    }

    private static func verifierAdvice(_ error: HistoricalRangeVerifierError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("VERIFY-001"))
    }

    private static func repairAdvice(_ error: RepairError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("REPAIR-001"))
    }

    private static func backtestReadinessAdvice(_ error: BacktestReadinessError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("BACKTEST-001"))
    }

    private static func terminalIdentityAdvice(_ error: TerminalIdentityPolicyError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("TIME-002"))
    }

    private static func supervisorAdvice(_ error: SupervisorError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("LOCK-001"))
    }

    private static func configAdvice(_ error: ConfigError) -> OperationalRecoveryAdvice {
        withSummary(from: error, base: catalogScenario("CONFIG-001"))
    }

    private static func startCheckAdvice(_ error: StartCheckError) -> OperationalRecoveryAdvice {
        switch error {
        case .missingTables:
            return withSummary(from: error, base: catalogScenario("DB-003"))
        case .offsetCoverageGaps:
            return withSummary(from: error, base: catalogScenario("TIME-003"))
        case .invalidBridge:
            return withSummary(from: error, base: catalogScenario("MT5-001"))
        case .metaEditorNotFound, .eaSourceNotFound, .eaCompileFailed:
            return withSummary(from: error, base: catalogScenario("MT5-005"))
        }
    }

    private static func clickHouseInsertAdvice(_ error: ClickHouseInsertError) -> OperationalRecoveryAdvice {
        switch error {
        case .unverifiedCanonicalBar:
            return withSummary(from: error, base: catalogScenario("TIME-001"))
        default:
            return withSummary(from: error, base: catalogScenario("DATA-001"))
        }
    }

    private static func unknownAdvice(_ error: Error) -> OperationalRecoveryAdvice {
        scenario(
            code: "UNKNOWN-001",
            title: "Unexpected failure",
            summary: String(describing: error),
            automaticRecovery: "The current operation stops; supervised mode will retry safe cycles according to scheduler rules.",
            dataSafety: "Checkpoints only move after validated insert and canonical readback verification.",
            steps: ["Run startcheck.", "Run DB-only verify.", "If MT5-backed data is involved, run random-range verify before backtesting."],
            commands: [
                "FXExport startcheck --config-dir Config --migrations-dir Migrations",
                "FXExport verify --config-dir Config --random-ranges 0"
            ]
        )
    }

    private static func catalogScenario(_ code: String) -> OperationalRecoveryAdvice {
        if let advice = scenarioCatalog.first(where: { $0.code == code }) {
            return advice
        }
        return scenario(
            code: "GUIDE-001",
            title: "Failure guide catalog is incomplete",
            summary: "The program tried to map an error to missing recovery scenario \(code).",
            automaticRecovery: "The original operation still fails closed.",
            dataSafety: "No data safety decision depends on this missing help text.",
            steps: ["Update OperationalFailureGuide.swift so every typed error maps to a catalog scenario."],
            commands: ["swift test"]
        )
    }

    private static func withSummary(from error: Error, base: OperationalRecoveryAdvice) -> OperationalRecoveryAdvice {
        OperationalRecoveryAdvice(
            code: base.code,
            title: base.title,
            severity: base.severity,
            summary: String(describing: error),
            automaticRecovery: base.automaticRecovery,
            dataSafety: base.dataSafety,
            operatorSteps: base.operatorSteps,
            commands: base.commands
        )
    }

    private static func scenario(
        code: String,
        title: String,
        severity: RecoverySeverity = .stop,
        summary: String,
        automaticRecovery: String,
        dataSafety: String,
        steps: [String],
        commands: [String] = []
    ) -> OperationalRecoveryAdvice {
        OperationalRecoveryAdvice(
            code: code,
            title: title,
            severity: severity,
            summary: summary,
            automaticRecovery: automaticRecovery,
            dataSafety: dataSafety,
            operatorSteps: steps,
            commands: commands
        )
    }
}
