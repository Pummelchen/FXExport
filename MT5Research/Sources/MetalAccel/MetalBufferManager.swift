import Foundation

#if canImport(Metal)
import Metal
#endif

public enum MetalAccelerationError: Error, CustomStringConvertible, Sendable {
    case unavailable
    case setupFailed(String)
    case cpuVerificationFailed

    public var description: String {
        switch self {
        case .unavailable:
            return "Metal acceleration is unavailable on this machine."
        case .setupFailed(let reason):
            return "Metal setup failed: \(reason)"
        case .cpuVerificationFailed:
            return "GPU result did not match the CPU reference."
        }
    }
}

public struct MetalBufferManager: Sendable {
    public init() {}
}
