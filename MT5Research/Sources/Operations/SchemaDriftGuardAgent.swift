import ClickHouse
import Foundation

public struct SchemaDriftGuardAgent: ProductionAgent {
    public let descriptor: AgentDescriptor

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .schemaDriftGuard,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        let database = context.config.clickHouse.database
        let expected = Self.expectedColumns
        let tableNames = expected.keys.sorted()
        let quotedTables = tableNames.map { "'\(SQLText.literal($0))'" }.joined(separator: ",")
        let tableBody = try await context.clickHouse.execute(.select("""
        SELECT name, engine
        FROM system.tables
        WHERE database = '\(SQLText.literal(database))'
          AND name IN (\(quotedTables))
        FORMAT TabSeparated
        """, databaseOverride: "default"))
        let existingTables = Set(tableBody.split(separator: "\n", omittingEmptySubsequences: true).compactMap { line in
            line.split(separator: "\t", omittingEmptySubsequences: false).first.map(String.init)
        })

        let missingTables = tableNames.filter { !existingTables.contains($0) }
        guard missingTables.isEmpty else {
            return AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
                .failed("ClickHouse schema is missing required tables", details: "missing_tables=\(missingTables.joined(separator: ","))")
        }

        let columnBody = try await context.clickHouse.execute(.select("""
        SELECT table, name, type
        FROM system.columns
        WHERE database = '\(SQLText.literal(database))'
          AND table IN (\(quotedTables))
        ORDER BY table ASC, position ASC
        FORMAT TabSeparated
        """, databaseOverride: "default"))
        let actual = try parseColumns(columnBody)
        var missingColumns: [String] = []
        var typeMismatches: [String] = []
        for table in tableNames {
            let actualColumns = actual[table] ?? [:]
            for expectedColumn in expected[table] ?? [] {
                guard let actualType = actualColumns[expectedColumn.name] else {
                    missingColumns.append("\(table).\(expectedColumn.name)")
                    continue
                }
                if actualType != expectedColumn.type {
                    typeMismatches.append("\(table).\(expectedColumn.name):expected=\(expectedColumn.type),actual=\(actualType)")
                }
            }
        }

        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        if !missingColumns.isEmpty || !typeMismatches.isEmpty {
            return factory.failed(
                "ClickHouse schema drift found",
                details: "missing_columns=\(missingColumns.joined(separator: ",")); type_mismatches=\(typeMismatches.joined(separator: " | "))"
            )
        }
        return factory.ok("ClickHouse schema matches FXExport migrations", details: "tables_checked=\(tableNames.count)")
    }

    private func parseColumns(_ body: String) throws -> [String: [String: String]] {
        var result: [String: [String: String]] = [:]
        for line in body.split(separator: "\n", omittingEmptySubsequences: true) {
            let fields = line.split(separator: "\t", omittingEmptySubsequences: false).map(String.init)
            guard fields.count == 3 else {
                throw ProductionAgentError.invariant("invalid system.columns row for schema drift guard: \(line)")
            }
            result[fields[0], default: [:]][fields[1]] = fields[2]
        }
        return result
    }

    private static let expectedColumns: [String: [ExpectedColumn]] = [
        "mt5_ohlc_m1_raw": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("logical_symbol", "String"),
            ExpectedColumn("mt5_symbol", "String"),
            ExpectedColumn("timeframe", "LowCardinality(String)"),
            ExpectedColumn("mt5_server_ts_raw", "Int64"),
            ExpectedColumn("ts_utc", "Int64"),
            ExpectedColumn("offset_confidence", "LowCardinality(String)"),
            ExpectedColumn("bar_hash", "String")
        ],
        "ohlc_m1_canonical": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("logical_symbol", "String"),
            ExpectedColumn("mt5_symbol", "String"),
            ExpectedColumn("timeframe", "LowCardinality(String)"),
            ExpectedColumn("mt5_server_ts_raw", "Int64"),
            ExpectedColumn("ts_utc", "Int64"),
            ExpectedColumn("offset_confidence", "LowCardinality(String)"),
            ExpectedColumn("open_scaled", "Int64"),
            ExpectedColumn("high_scaled", "Int64"),
            ExpectedColumn("low_scaled", "Int64"),
            ExpectedColumn("close_scaled", "Int64"),
            ExpectedColumn("digits", "UInt8"),
            ExpectedColumn("bar_hash", "String")
        ],
        "broker_time_offsets": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("mt5_company", "String"),
            ExpectedColumn("mt5_server", "String"),
            ExpectedColumn("mt5_account_login", "Int64"),
            ExpectedColumn("valid_from_mt5_server_ts", "Int64"),
            ExpectedColumn("valid_to_mt5_server_ts", "Int64"),
            ExpectedColumn("offset_seconds", "Int32"),
            ExpectedColumn("confidence", "LowCardinality(String)"),
            ExpectedColumn("is_active", "UInt8")
        ],
        "ingest_state": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("logical_symbol", "String"),
            ExpectedColumn("mt5_symbol", "String"),
            ExpectedColumn("latest_ingested_closed_mt5_server_ts_raw", "Int64"),
            ExpectedColumn("latest_ingested_closed_ts_utc", "Int64"),
            ExpectedColumn("status", "LowCardinality(String)")
        ],
        "ingest_operations": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("logical_symbol", "String"),
            ExpectedColumn("operation_type", "LowCardinality(String)"),
            ExpectedColumn("batch_id", "String"),
            ExpectedColumn("status", "LowCardinality(String)"),
            ExpectedColumn("status_rank", "UInt8"),
            ExpectedColumn("hash_schema_version", "Nullable(String)"),
            ExpectedColumn("mt5_source_sha256", "Nullable(String)"),
            ExpectedColumn("canonical_readback_sha256", "Nullable(String)"),
            ExpectedColumn("offset_authority_sha256", "Nullable(String)")
        ],
        "ohlc_m1_verified_coverage": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("logical_symbol", "String"),
            ExpectedColumn("utc_range_start", "Int64"),
            ExpectedColumn("utc_range_end_exclusive", "Int64"),
            ExpectedColumn("hash_schema_version", "LowCardinality(String)"),
            ExpectedColumn("mt5_source_sha256", "String"),
            ExpectedColumn("canonical_readback_sha256", "String"),
            ExpectedColumn("offset_authority_sha256", "String")
        ],
        "data_certificates": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("logical_symbol", "String"),
            ExpectedColumn("certificate_sha256", "String"),
            ExpectedColumn("hash_schema_version", "LowCardinality(String)"),
            ExpectedColumn("certificate_status", "LowCardinality(String)")
        ],
        "broker_sources": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("mt5_company", "String"),
            ExpectedColumn("mt5_server", "String"),
            ExpectedColumn("mt5_account_login", "Int64"),
            ExpectedColumn("discovery_source", "LowCardinality(String)"),
            ExpectedColumn("status", "LowCardinality(String)"),
            ExpectedColumn("is_active", "UInt8")
        ],
        "runtime_agent_state": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("agent_name", "LowCardinality(String)"),
            ExpectedColumn("status", "LowCardinality(String)"),
            ExpectedColumn("last_ok_at_utc", "Int64"),
            ExpectedColumn("last_error_at_utc", "Int64")
        ],
        "verification_results": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("logical_symbol", "String"),
            ExpectedColumn("result", "LowCardinality(String)"),
            ExpectedColumn("checked_at_utc", "Int64")
        ],
        "repair_log": [
            ExpectedColumn("broker_source_id", "String"),
            ExpectedColumn("logical_symbol", "String"),
            ExpectedColumn("outcome", "LowCardinality(String)"),
            ExpectedColumn("created_at_utc", "Int64")
        ]
    ]
}

private struct ExpectedColumn: Sendable {
    let name: String
    let type: String

    init(_ name: String, _ type: String) {
        self.name = name
        self.type = type
    }
}
