import Foundation

public struct ExecutionModel: Sendable {
    public let spreadScaled: Int64
    public let commissionScaled: Int64

    public init(spreadScaled: Int64, commissionScaled: Int64) {
        self.spreadScaled = spreadScaled
        self.commissionScaled = commissionScaled
    }
}
