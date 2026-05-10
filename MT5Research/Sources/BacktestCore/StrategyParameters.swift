import Foundation

public protocol StrategyParameters: Sendable {}

public struct EmptyStrategyParameters: StrategyParameters, Sendable {
    public init() {}
}
