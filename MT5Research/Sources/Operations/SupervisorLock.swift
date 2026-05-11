import Darwin
import Foundation

public final class SupervisorLock: @unchecked Sendable {
    private let fileDescriptor: Int32
    public let path: String

    private init(fileDescriptor: Int32, path: String) {
        self.fileDescriptor = fileDescriptor
        self.path = path
    }

    deinit {
        _ = flock(fileDescriptor, LOCK_UN)
        _ = close(fileDescriptor)
    }

    public static func acquireDefault(brokerSourceId: String) throws -> SupervisorLock {
        try acquireRuntime(brokerSourceId: brokerSourceId, owner: "supervisor")
    }

    public static func acquireRuntime(brokerSourceId: String, owner: String) throws -> SupervisorLock {
        let safeId = brokerSourceId.map { character -> Character in
            character.isLetter || character.isNumber || character == "_" || character == "-" ? character : "_"
        }
        let path = "/tmp/fxexport-\(String(safeId))-runtime.lock"
        let fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
        guard fd >= 0 else {
            throw SupervisorError.lockUnavailable("Cannot open FXExport runtime lock at \(path): errno=\(errno)")
        }
        guard flock(fd, LOCK_EX | LOCK_NB) == 0 else {
            _ = close(fd)
            throw SupervisorError.lockUnavailable("Another FXExport writer/supervisor is already running for broker source \(brokerSourceId); \(owner) must wait")
        }
        return SupervisorLock(fileDescriptor: fd, path: path)
    }
}

public enum SupervisorError: Error, CustomStringConvertible, Sendable {
    case lockUnavailable(String)

    public var description: String {
        switch self {
        case .lockUnavailable(let message):
            return message
        }
    }
}
