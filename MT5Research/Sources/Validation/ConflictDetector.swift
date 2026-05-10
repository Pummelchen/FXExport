import Domain
import Foundation

public struct BarConflict: Hashable, Sendable {
    public let existing: CanonicalBar
    public let incoming: CanonicalBar

    public init(existing: CanonicalBar, incoming: CanonicalBar) {
        self.existing = existing
        self.incoming = incoming
    }
}

public struct ConflictDetector: Sendable {
    public init() {}

    public func conflict(existing: CanonicalBar, incoming: CanonicalBar) -> BarConflict? {
        guard existing.brokerSourceId == incoming.brokerSourceId,
              existing.logicalSymbol == incoming.logicalSymbol,
              existing.utcTime == incoming.utcTime else {
            return nil
        }
        return existing.barHash == incoming.barHash ? nil : BarConflict(existing: existing, incoming: incoming)
    }
}
