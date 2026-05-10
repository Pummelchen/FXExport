import Foundation

public enum RepairDecision: Equatable, Sendable {
    case noRepairNeeded
    case repairCanonicalOnly(reason: String)
    case refuse(reason: String)
}

public struct RepairPolicy: Sendable {
    public init() {}

    public func decide(verification: VerificationResult, mt5Available: Bool, utcMappingAmbiguous: Bool) -> RepairDecision {
        guard !verification.isClean else { return .noRepairNeeded }
        guard mt5Available else { return .refuse(reason: "MT5 source data is unavailable") }
        guard !utcMappingAmbiguous else { return .refuse(reason: "UTC mapping is ambiguous") }
        return .repairCanonicalOnly(reason: "MT5 source-of-truth comparison produced an unambiguous mismatch")
    }
}
