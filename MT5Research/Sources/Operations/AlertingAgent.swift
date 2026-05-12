import Foundation

public struct AlertingAgent: ProductionAgent {
    public let descriptor: AgentDescriptor
    private let lookbackSeconds: Int

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .alerting,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
        self.lookbackSeconds = max(300, intervalSeconds * 10)
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        var alerts: [String] = []
        alerts.append(contentsOf: try await recentWarningAndErrorAlerts(context: context))
        alerts.append(contentsOf: try await runtimeStateAlerts(context: context))
        alerts.append(contentsOf: try await verificationAndRepairAlerts(context: context))
        alerts.append(contentsOf: try await clickHouseDiskAlerts(context: context))
        alerts.append(contentsOf: localDiskAlerts(context: context))

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        guard !alerts.isEmpty else {
            return factory.ok("No operational alerts")
        }

        let details = alerts.prefix(30).joined(separator: " | ")
        return factory.warning(
            "Operational alerts require attention",
            details: "alerts=\(alerts.count); \(details)"
        )
    }

    private func recentWarningAndErrorAlerts(context: AgentRuntimeContext) async throws -> [String] {
        let since = utcNow().rawValue - Int64(lookbackSeconds)
        let sql = """
        SELECT
            agent_name,
            severity,
            message,
            finished_at_utc
        FROM \(context.config.clickHouse.database).runtime_agent_events
        WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
          AND finished_at_utc >= \(since)
          AND severity IN ('warning', 'error')
          AND agent_name != '\(ProductionAgentKind.alerting.rawValue)'
        ORDER BY finished_at_utc DESC
        LIMIT 20
        FORMAT TabSeparated
        """
        let body = try await context.clickHouse.execute(.select(sql))
        let rows = body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map(String.init)

        guard !rows.isEmpty else { return [] }
        return ["recent supervisor warnings/errors=\(rows.count): \(rows.prefix(10).joined(separator: " | "))"]
    }

    private func runtimeStateAlerts(context: AgentRuntimeContext) async throws -> [String] {
        let body = try await context.clickHouse.execute(.select("""
        SELECT agent_name, status, last_message, last_ok_at_utc, last_error_at_utc, updated_at_utc
        FROM \(context.config.clickHouse.database).runtime_agent_state FINAL
        WHERE broker_source_id = '\(SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue))'
        FORMAT TabSeparated
        """))
        let states = try parseRuntimeStateRows(body)
        let now = utcNow().rawValue
        let freshness = AgentFreshnessPolicy(config: context.config)
        var alerts: [String] = []

        for state in states.values.sorted(by: { $0.agent.rawValue < $1.agent.rawValue }) {
            if state.agent != .alerting && (state.status == .warning || state.status == .failed) {
                alerts.append("\(state.agent.rawValue) status=\(state.status.rawValue): \(state.message)")
            }
            if state.status == .failed,
               state.lastErrorAtUtc > 0,
               now - state.lastErrorAtUtc >= Int64(context.config.app.supervisor.mt5BridgeDownAlertSeconds),
               Self.isMT5SensitiveAgent(state.agent) {
                alerts.append("MT5 bridge appears down for \(now - state.lastErrorAtUtc)s; last failing agent=\(state.agent.rawValue)")
            }
        }

        for agent in AgentExecutionPolicy.backtestRequiredOkAgentKinds.sorted(by: { $0.priorityRank < $1.priorityRank }) {
            guard let state = states[agent], state.lastOkAtUtc > 0 else {
                alerts.append("backtest readiness blocked: \(agent.rawValue) has no successful supervisor run")
                continue
            }
            let maxAge = freshness.maxOkAgeSeconds(for: agent)
            if now - state.lastOkAtUtc > maxAge {
                alerts.append("backtest readiness blocked: \(agent.rawValue) last OK \(now - state.lastOkAtUtc)s ago, max \(maxAge)s")
            }
        }

        return alerts
    }

    private func verificationAndRepairAlerts(context: AgentRuntimeContext) async throws -> [String] {
        var alerts: [String] = []
        let broker = SQLText.literal(context.config.brokerTime.brokerSourceId.rawValue)
        let database = context.config.clickHouse.database

        let unresolvedVerification = try await scalar(context: context, sql: """
        SELECT count()
        FROM (
            SELECT logical_symbol, range_start_mt5_server_ts, range_end_mt5_server_ts,
                   argMax(result, checked_at_utc) AS latest_result
            FROM \(database).verification_results
            WHERE broker_source_id = '\(broker)'
            GROUP BY logical_symbol, range_start_mt5_server_ts, range_end_mt5_server_ts
        )
        WHERE latest_result != 'clean'
        FORMAT TabSeparated
        """)
        if unresolvedVerification > 0 {
            alerts.append("backtest readiness blocked: unresolved verification mismatches=\(unresolvedVerification)")
        }

        let failedRepairs = try await scalar(context: context, sql: """
        SELECT count()
        FROM (
            SELECT logical_symbol, range_start_mt5_server_ts, range_end_mt5_server_ts,
                   argMax(outcome, created_at_utc) AS latest_outcome
            FROM \(database).repair_log
            WHERE broker_source_id = '\(broker)'
            GROUP BY logical_symbol, range_start_mt5_server_ts, range_end_mt5_server_ts
        )
        WHERE latest_outcome = 'failed'
        FORMAT TabSeparated
        """)
        if failedRepairs > 0 {
            alerts.append("backtest readiness blocked: failed repair outcomes=\(failedRepairs)")
        }

        return alerts
    }

    private func clickHouseDiskAlerts(context: AgentRuntimeContext) async throws -> [String] {
        let threshold = context.config.app.supervisor.clickHouseDiskFreeAlertBytes
        let body = try await context.clickHouse.execute(.select("""
        SELECT name, path, free_space, total_space
        FROM system.disks
        WHERE free_space < \(threshold)
        ORDER BY free_space ASC
        FORMAT TabSeparated
        """, databaseOverride: "default"))
        return body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { row in "ClickHouse disk pressure: \(row)" }
    }

    private func localDiskAlerts(context: AgentRuntimeContext) -> [String] {
        do {
            let attributes = try FileManager.default.attributesOfFileSystem(forPath: FileManager.default.currentDirectoryPath)
            guard let freeNumber = attributes[.systemFreeSize] as? NSNumber else { return [] }
            let free = freeNumber.int64Value
            let threshold = context.config.app.supervisor.minimumFreeDiskBytes
            if free < threshold {
                return ["local disk free bytes \(free) below configured threshold \(threshold)"]
            }
            return []
        } catch {
            return ["local disk free-space check failed: \(error)"]
        }
    }

    private func parseRuntimeStateRows(_ body: String) throws -> [ProductionAgentKind: RuntimeStateRow] {
        var rows: [ProductionAgentKind: RuntimeStateRow] = [:]
        for line in body.split(separator: "\n", omittingEmptySubsequences: true) {
            let fields = line.split(separator: "\t", omittingEmptySubsequences: false).map(String.init)
            guard fields.count == 6,
                  let agent = ProductionAgentKind(rawValue: fields[0]),
                  let status = AgentStatus(rawValue: fields[1]),
                  let lastOk = Int64(fields[3]),
                  let lastError = Int64(fields[4]),
                  let updated = Int64(fields[5]) else {
                throw ProductionAgentError.invariant("invalid runtime_agent_state row for alerting: \(line)")
            }
            rows[agent] = RuntimeStateRow(
                agent: agent,
                status: status,
                message: fields[2],
                lastOkAtUtc: lastOk,
                lastErrorAtUtc: lastError,
                updatedAtUtc: updated
            )
        }
        return rows
    }

    private func scalar(context: AgentRuntimeContext, sql: String) async throws -> Int64 {
        let body = try await context.clickHouse.execute(.select(sql))
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let value = Int64(trimmed) else {
            throw ProductionAgentError.invariant("alerting scalar query returned invalid value: \(trimmed)")
        }
        return value
    }

    private static func isMT5SensitiveAgent(_ agent: ProductionAgentKind) -> Bool {
        switch agent {
        case .healthMonitor, .bridgeVersionGuard, .utcTimeAuthority, .symbolMetadataDrift, .sourceHistoryDrift, .historyImporter, .liveM1Updater, .databaseVerifierRepairer:
            return true
        case .supervisorCoordinator, .schemaDriftGuard, .verificationCoveragePlanner, .checkpointGapAuditor, .dataCertification, .backupReadiness, .backupRestoreVerifier, .alerting:
            return false
        }
    }
}

private struct RuntimeStateRow: Sendable {
    let agent: ProductionAgentKind
    let status: AgentStatus
    let message: String
    let lastOkAtUtc: Int64
    let lastErrorAtUtc: Int64
    let updatedAtUtc: Int64
}
