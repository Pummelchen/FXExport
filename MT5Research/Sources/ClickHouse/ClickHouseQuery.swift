import Foundation

public struct ClickHouseQuery: Sendable {
    public let sql: String
    public let isIdempotent: Bool
    public let databaseOverride: String?

    public init(sql: String, isIdempotent: Bool, databaseOverride: String? = nil) {
        self.sql = sql
        self.isIdempotent = isIdempotent
        self.databaseOverride = databaseOverride
    }

    public static func select(_ sql: String, databaseOverride: String? = nil) -> ClickHouseQuery {
        ClickHouseQuery(sql: sql, isIdempotent: true, databaseOverride: databaseOverride)
    }

    public static func mutation(_ sql: String, idempotent: Bool, databaseOverride: String? = nil) -> ClickHouseQuery {
        ClickHouseQuery(sql: sql, isIdempotent: idempotent, databaseOverride: databaseOverride)
    }
}
