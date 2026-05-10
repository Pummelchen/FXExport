import Foundation

public enum Timeframe: String, Codable, Hashable, Sendable, CaseIterable {
    case m1 = "M1"

    public var seconds: Int64 {
        switch self {
        case .m1:
            return 60
        }
    }
}
