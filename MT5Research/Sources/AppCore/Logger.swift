import Foundation

public enum LogLevel: String, Codable, Sendable, Comparable {
    case quiet
    case normal
    case verbose
    case debug

    public static func < (lhs: LogLevel, rhs: LogLevel) -> Bool {
        order(lhs) < order(rhs)
    }

    private static func order(_ level: LogLevel) -> Int {
        switch level {
        case .quiet: return 0
        case .normal: return 1
        case .verbose: return 2
        case .debug: return 3
        }
    }
}

public struct Logger: Sendable {
    public let level: LogLevel
    private let colorPolicy: TerminalColorPolicy

    public init(level: LogLevel = .normal, colorPolicy: TerminalColorPolicy = TerminalColorPolicy()) {
        self.level = level
        self.colorPolicy = colorPolicy
    }

    public func info(_ message: String) {
        guard level >= .normal else { return }
        emit("[INFO] ", message, color: .cyan)
    }

    public func ok(_ message: String) {
        guard level >= .normal else { return }
        emit("[OK]   ", message, color: .green)
    }

    public func warn(_ message: String) {
        guard level >= .quiet else { return }
        emit("[WARN] ", message, color: .yellow)
    }

    public func error(_ message: String) {
        emit("[ERROR]", " " + message, color: .red)
    }

    public func verify(_ message: String) {
        guard level >= .normal else { return }
        emit("[VERIFY]", " " + message, color: .magenta)
    }

    public func repair(_ message: String) {
        guard level >= .normal else { return }
        emit("[REPAIR]", " " + message, color: .magenta)
    }

    public func db(_ message: String) {
        guard level >= .normal else { return }
        emit("[DB]   ", message, color: .blue)
    }

    public func verbose(_ message: String) {
        guard level >= .verbose else { return }
        emit("[DETAIL]", " " + message, color: .gray)
    }

    public func debug(_ message: String) {
        guard level >= .debug else { return }
        emit("[DEBUG]", " " + message, color: .gray)
    }

    private func emit(_ prefix: String, _ message: String, color: TerminalColor) {
        print(colorPolicy.colorize(prefix, as: color) + message)
    }
}
