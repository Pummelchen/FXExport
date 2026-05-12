import Darwin
import Foundation

public enum TerminalColor: Equatable, Sendable {
    case cyan
    case green
    case yellow
    case red
    case magenta
    case blue
    case gray
    case brightCyan
    case brightGreen
    case brightYellow
    case brightMagenta
    case brightBlue
    case white

    var ansiCode: String {
        switch self {
        case .cyan: return "\u{001B}[36m"
        case .green: return "\u{001B}[32m"
        case .yellow: return "\u{001B}[33m"
        case .red: return "\u{001B}[31m"
        case .magenta: return "\u{001B}[35m"
        case .blue: return "\u{001B}[34m"
        case .gray: return "\u{001B}[90m"
        case .brightCyan: return "\u{001B}[96m"
        case .brightGreen: return "\u{001B}[92m"
        case .brightYellow: return "\u{001B}[93m"
        case .brightMagenta: return "\u{001B}[95m"
        case .brightBlue: return "\u{001B}[94m"
        case .white: return "\u{001B}[97m"
        }
    }

    static let reset = "\u{001B}[0m"
    static let foregroundReset = "\u{001B}[39m"
    static let blackBackground = "\u{001B}[40m"
}

public struct TerminalColorPolicy: Sendable {
    public let isEnabled: Bool

    public init(environment: [String: String] = ProcessInfo.processInfo.environment, stdoutIsTTY: Bool = isatty(STDOUT_FILENO) == 1) {
        self.isEnabled = stdoutIsTTY && environment["NO_COLOR"] == nil
    }

    public func colorize(_ text: String, as color: TerminalColor) -> String {
        guard isEnabled else { return text }
        return TerminalColor.blackBackground + color.ansiCode + text + TerminalColor.foregroundReset
    }
}
