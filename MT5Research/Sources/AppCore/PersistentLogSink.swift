import Foundation

public enum PersistentLogSinkError: Error, CustomStringConvertible, Sendable {
    case invalidPath(String)
    case createDirectoryFailed(URL, String)
    case createFileFailed(URL)

    public var description: String {
        switch self {
        case .invalidPath(let path):
            return "Invalid persistent log path: \(path)"
        case .createDirectoryFailed(let url, let reason):
            return "Could not create log directory \(url.path): \(reason)"
        case .createFileFailed(let url):
            return "Could not create log file \(url.path)"
        }
    }
}

public final class PersistentLogSink: @unchecked Sendable {
    private struct Entry: Encodable {
        let tsUtc: Int64
        let level: String
        let component: String
        let message: String

        enum CodingKeys: String, CodingKey {
            case tsUtc = "ts_utc"
            case level
            case component
            case message
        }
    }

    private let fileURL: URL
    private let maxFileBytes: UInt64
    private let maxRotatedFiles: Int
    private let fileManager: FileManager
    private let lock = NSLock()
    private var writeFailureEmitted = false

    public init(
        fileURL: URL,
        maxFileBytes: UInt64,
        maxRotatedFiles: Int,
        fileManager: FileManager = .default
    ) throws {
        guard !fileURL.path.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw PersistentLogSinkError.invalidPath(fileURL.path)
        }
        self.fileURL = fileURL
        self.maxFileBytes = max(1024, maxFileBytes)
        self.maxRotatedFiles = max(0, maxRotatedFiles)
        self.fileManager = fileManager
        try Self.ensureParentDirectoryExists(for: fileURL, fileManager: fileManager)
        if !fileManager.fileExists(atPath: fileURL.path) {
            guard fileManager.createFile(atPath: fileURL.path, contents: nil) else {
                throw PersistentLogSinkError.createFileFailed(fileURL)
            }
        }
    }

    public static func resolvedURL(path: String, baseDirectory: URL) throws -> URL {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { throw PersistentLogSinkError.invalidPath(path) }
        if trimmed.hasPrefix("/") {
            return URL(fileURLWithPath: trimmed)
        }
        return baseDirectory.appendingPathComponent(trimmed)
    }

    public func write(level: String, component: String, message: String, timestampUtc: Int64 = Int64(Date().timeIntervalSince1970)) {
        lock.lock()
        defer { lock.unlock() }

        do {
            try rotateIfNeeded()
            let entry = Entry(tsUtc: timestampUtc, level: level, component: component, message: message)
            let data = try JSONEncoder().encode(entry) + Data([0x0A])
            try append(data)
        } catch {
            emitWriteFailureOnce(error)
        }
    }

    private static func ensureParentDirectoryExists(for fileURL: URL, fileManager: FileManager) throws {
        let directory = fileURL.deletingLastPathComponent()
        do {
            try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        } catch {
            throw PersistentLogSinkError.createDirectoryFailed(directory, String(describing: error))
        }
    }

    private func append(_ data: Data) throws {
        let handle = try FileHandle(forWritingTo: fileURL)
        do {
            try handle.seekToEnd()
            try handle.write(contentsOf: data)
            try handle.close()
        } catch {
            do {
                try handle.close()
            } catch {
                throw error
            }
            throw error
        }
    }

    private func rotateIfNeeded() throws {
        guard try fileSize(fileURL) >= maxFileBytes else { return }
        if maxRotatedFiles == 0 {
            try removeIfExists(fileURL)
            guard fileManager.createFile(atPath: fileURL.path, contents: nil) else {
                throw PersistentLogSinkError.createFileFailed(fileURL)
            }
            return
        }

        for index in stride(from: maxRotatedFiles, through: 1, by: -1) {
            let source = index == 1 ? fileURL : rotatedURL(index - 1)
            let destination = rotatedURL(index)
            guard fileManager.fileExists(atPath: source.path) else { continue }
            try removeIfExists(destination)
            try fileManager.moveItem(at: source, to: destination)
        }
        guard fileManager.createFile(atPath: fileURL.path, contents: nil) else {
            throw PersistentLogSinkError.createFileFailed(fileURL)
        }
    }

    private func rotatedURL(_ index: Int) -> URL {
        URL(fileURLWithPath: "\(fileURL.path).\(index)")
    }

    private func fileSize(_ url: URL) throws -> UInt64 {
        guard fileManager.fileExists(atPath: url.path) else { return 0 }
        let attributes = try fileManager.attributesOfItem(atPath: url.path)
        return attributes[.size] as? UInt64 ?? 0
    }

    private func removeIfExists(_ url: URL) throws {
        if fileManager.fileExists(atPath: url.path) {
            try fileManager.removeItem(at: url)
        }
    }

    private func emitWriteFailureOnce(_ error: Error) {
        guard !writeFailureEmitted else { return }
        writeFailureEmitted = true
        let text = "[WARN] Persistent logging failed: \(error)\n"
        if let data = text.data(using: .utf8) {
            FileHandle.standardError.write(data)
        }
    }
}
