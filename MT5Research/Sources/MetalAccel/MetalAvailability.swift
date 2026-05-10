import Foundation

#if canImport(Metal)
import Metal
#endif

public struct MetalAvailability: Sendable {
    public let isAvailable: Bool
    public let deviceName: String?

    public init() {
        #if canImport(Metal)
        if let device = MTLCreateSystemDefaultDevice() {
            self.isAvailable = true
            self.deviceName = device.name
        } else {
            self.isAvailable = false
            self.deviceName = nil
        }
        #else
        self.isAvailable = false
        self.deviceName = nil
        #endif
    }
}
