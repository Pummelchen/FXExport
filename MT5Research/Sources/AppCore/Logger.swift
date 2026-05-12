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
    private let persistentLogSink: PersistentLogSink?
    private let alertSink: PersistentLogSink?

    public init(
        level: LogLevel = .normal,
        colorPolicy: TerminalColorPolicy = TerminalColorPolicy(),
        persistentLogSink: PersistentLogSink? = nil,
        alertSink: PersistentLogSink? = nil
    ) {
        self.level = level
        self.colorPolicy = colorPolicy
        self.persistentLogSink = persistentLogSink
        self.alertSink = alertSink
    }

    public func info(_ message: String) {
        guard level >= .normal else { return }
        emit("[INFO] ", message, color: .cyan, levelName: "info", component: "app")
    }

    public func ok(_ message: String) {
        guard level >= .normal else { return }
        emit("[OK]   ", message, color: .green, levelName: "ok", component: "app")
    }

    public func warn(_ message: String) {
        guard level >= .quiet else { return }
        emit("[WARN] ", message, color: .yellow, levelName: "warning", component: "app")
    }

    public func error(_ message: String) {
        emit("[ERROR]", " " + message, color: .red, levelName: "error", component: "app")
    }

    public func alert(_ message: String, details: String = "") {
        let fullMessage = details.isEmpty ? message : "\(message) | \(details)"
        emit("[ALERT]", " " + fullMessage, color: .yellow, levelName: "alert", component: "alert")
        alertSink?.write(level: "alert", component: "alert", message: fullMessage)
    }

    public func verify(_ message: String) {
        guard level >= .normal else { return }
        emit("[VERIFY]", " " + message, color: .magenta, levelName: "verify", component: "verification")
    }

    public func repair(_ message: String) {
        guard level >= .normal else { return }
        emit("[REPAIR]", " " + message, color: .magenta, levelName: "repair", component: "repair")
    }

    public func db(_ message: String) {
        guard level >= .normal else { return }
        emit("[DB]   ", message, color: .blue, levelName: "database", component: "clickhouse")
    }

    public func verbose(_ message: String) {
        guard level >= .verbose else { return }
        emit("[DETAIL]", " " + message, color: .gray, levelName: "detail", component: "app")
    }

    public func debug(_ message: String) {
        guard level >= .debug else { return }
        emit("[DEBUG]", " " + message, color: .gray, levelName: "debug", component: "app")
    }

    public func agentStatus(
        agentId: String,
        displayName: String,
        message: String,
        color: TerminalColor,
        levelName: String,
        details: String = "",
        isError: Bool = false,
        writeAlert: Bool = false,
        timestampUtc: Int64? = nil
    ) {
        guard level >= .normal || isError else { return }
        let fullMessage = details.isEmpty ? message : "\(message) | \(details)"
        let timestamp = Self.terminalTimestampString(timestampUtc: timestampUtc)
        let line = "\(timestamp) - Agent \(displayName) - \(fullMessage)"
        let terminalColor: TerminalColor = isError ? .red : color
        print(colorPolicy.colorize(line, as: terminalColor))
        persistentLogSink?.write(level: levelName, component: "agent.\(agentId)", message: fullMessage)
        if writeAlert {
            alertSink?.write(level: levelName, component: "agent.\(agentId)", message: fullMessage)
        }
    }

    private func emit(_ prefix: String, _ message: String, color: TerminalColor, levelName: String, component: String) {
        print(colorPolicy.colorize(prefix + message, as: color))
        persistentLogSink?.write(level: levelName, component: component, message: message)
    }

    private static func terminalTimestampString(timestampUtc: Int64?) -> String {
        let date: Date
        if let timestampUtc {
            date = Date(timeIntervalSince1970: TimeInterval(timestampUtc))
        } else {
            date = Date()
        }
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = .current
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter.string(from: date)
    }
}
