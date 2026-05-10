import Darwin
import Foundation

public enum TerminalColor: Sendable {
    case cyan
    case green
    case yellow
    case red
    case magenta
    case blue
    case gray

    var ansiCode: String {
        switch self {
        case .cyan: return "\u{001B}[36m"
        case .green: return "\u{001B}[32m"
        case .yellow: return "\u{001B}[33m"
        case .red: return "\u{001B}[31m"
        case .magenta: return "\u{001B}[35m"
        case .blue: return "\u{001B}[34m"
        case .gray: return "\u{001B}[90m"
        }
    }

    static let reset = "\u{001B}[0m"
}

public struct TerminalColorPolicy: Sendable {
    public let isEnabled: Bool

    public init(environment: [String: String] = ProcessInfo.processInfo.environment, stdoutIsTTY: Bool = isatty(STDOUT_FILENO) == 1) {
        self.isEnabled = stdoutIsTTY && environment["NO_COLOR"] == nil
    }

    public func colorize(_ text: String, as color: TerminalColor) -> String {
        guard isEnabled else { return text }
        return color.ansiCode + text + TerminalColor.reset
    }
}
