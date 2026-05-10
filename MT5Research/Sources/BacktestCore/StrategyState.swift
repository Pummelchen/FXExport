import Foundation

public protocol StrategyState: Sendable {}

public struct EmptyStrategyState: StrategyState, Sendable {
    public init() {}
}
