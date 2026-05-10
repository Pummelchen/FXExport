import AppCore
import Config
import Foundation

public struct ClickHouseMigrator: Sendable {
    private let client: ClickHouseClientProtocol
    private let config: ClickHouseConfig
    private let logger: Logger

    public init(client: ClickHouseClientProtocol, config: ClickHouseConfig, logger: Logger) {
        self.client = client
        self.config = config
        self.logger = logger
    }

    public func migrate(migrationsDirectory: URL) async throws {
        let fileManager = FileManager.default
        let files = try fileManager.contentsOfDirectory(at: migrationsDirectory, includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "sql" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }

        for file in files {
            logger.db("Applying migration \(file.lastPathComponent)")
            let template = try String(contentsOf: file, encoding: .utf8)
            let sql = template.replacingOccurrences(of: "{database}", with: config.database)
            let statements = Self.splitSQLStatements(sql)
            for statement in statements {
                let databaseOverride = file.lastPathComponent.hasPrefix("001_") ? "default" : config.database
                _ = try await client.execute(.mutation(statement, idempotent: true, databaseOverride: databaseOverride))
            }
            logger.ok("Migration applied: \(file.lastPathComponent)")
        }
    }

    static func splitSQLStatements(_ sql: String) -> [String] {
        sql.split(separator: ";", omittingEmptySubsequences: true)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }
}
