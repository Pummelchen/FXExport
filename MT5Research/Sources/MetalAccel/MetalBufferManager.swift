import BacktestCore
import Foundation

#if canImport(Metal)
import Metal
#endif

public enum MetalAccelerationError: Error, CustomStringConvertible, Sendable {
    case unavailable
    case setupFailed(String)

    public var description: String {
        switch self {
        case .unavailable:
            return "Metal acceleration is unavailable on this machine."
        case .setupFailed(let reason):
            return "Metal setup failed: \(reason)"
        }
    }
}

public struct MetalBufferManager {
    public init() {}

    #if canImport(Metal)
    public func makeReadOnlyBuffers(
        series: ColumnarOhlcSeries,
        device: MTLDevice? = MTLCreateSystemDefaultDevice()
    ) throws -> MetalOhlcBuffers {
        guard let device else {
            throw MetalAccelerationError.unavailable
        }
        try OhlcSeriesValidator().validate(series)
        return MetalOhlcBuffers(
            deviceName: device.name,
            count: series.count,
            utcTimestamps: try makeBuffer(series.utcTimestamps, label: "fxexport.utc_timestamps", device: device),
            open: try makeBuffer(series.open, label: "fxexport.open", device: device),
            high: try makeBuffer(series.high, label: "fxexport.high", device: device),
            low: try makeBuffer(series.low, label: "fxexport.low", device: device),
            close: try makeBuffer(series.close, label: "fxexport.close", device: device)
        )
    }

    private func makeBuffer(_ values: [Int64], label: String, device: MTLDevice) throws -> MTLBuffer {
        guard values.count <= Int.max / MemoryLayout<Int64>.stride else {
            throw MetalAccelerationError.setupFailed("Metal buffer \(label) is too large.")
        }
        let byteCount = max(values.count * MemoryLayout<Int64>.stride, 1)
        guard let buffer = device.makeBuffer(length: byteCount, options: [.storageModeShared]) else {
            throw MetalAccelerationError.setupFailed("Could not allocate Metal buffer \(label).")
        }
        buffer.label = label
        if !values.isEmpty {
            try values.withUnsafeBytes { source in
                guard let baseAddress = source.baseAddress else {
                    throw MetalAccelerationError.setupFailed("Source bytes for Metal buffer \(label) were unavailable.")
                }
                buffer.contents().copyMemory(from: baseAddress, byteCount: source.count)
            }
        }
        return buffer
    }
    #else
    public func makeReadOnlyBuffers(series: ColumnarOhlcSeries) throws -> Never {
        throw MetalAccelerationError.unavailable
    }
    #endif
}

#if canImport(Metal)
public struct MetalOhlcBuffers {
    public let deviceName: String
    public let count: Int
    public let utcTimestamps: MTLBuffer
    public let open: MTLBuffer
    public let high: MTLBuffer
    public let low: MTLBuffer
    public let close: MTLBuffer
}
#endif
