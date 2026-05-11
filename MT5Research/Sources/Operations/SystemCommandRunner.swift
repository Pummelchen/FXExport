import Foundation

public struct SystemCommandRequest: Sendable, Equatable {
    public let executable: URL
    public let arguments: [String]
    public let timeoutSeconds: TimeInterval

    public init(executable: URL, arguments: [String], timeoutSeconds: TimeInterval = 30) {
        self.executable = executable
        self.arguments = arguments
        self.timeoutSeconds = timeoutSeconds
    }

    public var display: String {
        ([executable.path] + arguments).joined(separator: " ")
    }
}

public struct SystemCommandResult: Sendable, Equatable {
    public let request: SystemCommandRequest
    public let exitCode: Int32
    public let stdout: String
    public let stderr: String

    public init(request: SystemCommandRequest, exitCode: Int32, stdout: String, stderr: String) {
        self.request = request
        self.exitCode = exitCode
        self.stdout = stdout
        self.stderr = stderr
    }

    public var combinedOutput: String {
        [stdout, stderr].filter { !$0.isEmpty }.joined(separator: "\n")
    }
}

public enum SystemCommandError: Error, CustomStringConvertible, Sendable {
    case invalidTimeout(TimeInterval)
    case launchFailed(String, String)
    case timedOut(String, TimeInterval)

    public var description: String {
        switch self {
        case .invalidTimeout(let timeout):
            return "Invalid command timeout \(timeout)."
        case .launchFailed(let command, let reason):
            return "Could not run \(command): \(reason)"
        case .timedOut(let command, let timeout):
            return "\(command) did not finish within \(Int(timeout)) second(s)."
        }
    }
}

public protocol SystemCommandRunning: Sendable {
    func run(_ request: SystemCommandRequest) async throws -> SystemCommandResult
}

public struct ProcessCommandRunner: SystemCommandRunning {
    public init() {}

    public func run(_ request: SystemCommandRequest) async throws -> SystemCommandResult {
        guard request.timeoutSeconds > 0 else {
            throw SystemCommandError.invalidTimeout(request.timeoutSeconds)
        }

        let process = Process()
        process.executableURL = request.executable
        process.arguments = request.arguments

        let stdout = Pipe()
        let stderr = Pipe()
        process.standardOutput = stdout
        process.standardError = stderr

        do {
            try process.run()
        } catch {
            throw SystemCommandError.launchFailed(request.display, error.localizedDescription)
        }

        let deadline = Date().addingTimeInterval(request.timeoutSeconds)
        while process.isRunning && Date() < deadline {
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        if process.isRunning {
            process.terminate()
            process.waitUntilExit()
            throw SystemCommandError.timedOut(request.display, request.timeoutSeconds)
        }
        process.waitUntilExit()

        let stdoutText = String(data: stdout.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
        let stderrText = String(data: stderr.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
        return SystemCommandResult(
            request: request,
            exitCode: process.terminationStatus,
            stdout: stdoutText,
            stderr: stderrText
        )
    }
}
