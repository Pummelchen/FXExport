import AppCore
import Darwin
import Foundation

struct InteractiveCommandSession: Sendable {
    private let coordinator: InteractiveCommandCoordinator

    init(startupArguments: [String]) {
        coordinator = InteractiveCommandCoordinator(startupArguments: startupArguments)
    }

    static func shouldStart(arguments: [String]) -> Bool {
        guard let first = arguments.first else { return true }
        let normalized = first.lowercased()
        if shellCommandNames.contains(normalized) { return true }
        if first.hasPrefix("--"), normalized != "--help", normalized != "--startcheck" {
            return true
        }
        return false
    }

    func run() async -> ExitCode {
        await coordinator.printBanner()
        while true {
            print("> ", terminator: "")
            fflush(stdout)
            guard let line = readLine() else {
                await coordinator.stopActiveCommand(reason: "terminal input closed")
                return .success
            }
            let shouldContinue = await coordinator.handle(line)
            if !shouldContinue {
                return .success
            }
        }
    }

    fileprivate static let shellCommandNames: Set<String> = ["shell", "interactive", "console"]
}

private actor InteractiveCommandCoordinator {
    private struct ActiveCommand {
        let id: UUID
        let displayName: String
        let startedAt: Date
        var task: Task<ExitCode, Never>?
    }

    private let tokenizer = CommandLineTokenizer()
    private let colorPolicy = TerminalColorPolicy()
    private let defaultOptions: [String]
    private let startupWarnings: [String]
    private var activeCommand: ActiveCommand?

    init(startupArguments: [String]) {
        let rawDefaults: [String]
        if let first = startupArguments.first,
           InteractiveCommandSession.shellCommandNames.contains(first.lowercased()) {
            rawDefaults = Array(startupArguments.dropFirst())
        } else {
            rawDefaults = startupArguments
        }
        let parsed = Self.parseStartupDefaults(rawDefaults)
        self.defaultOptions = parsed.options
        self.startupWarnings = parsed.warnings
    }

    func printBanner() {
        info("FXExport interactive command shell started")
        info("Type an FXExport command at the prompt, for example: supervise --with-backfill")
        info("Control commands: status, stop, wait, help, exit")
        if !defaultOptions.isEmpty {
            info("Session defaults applied to app commands: \(defaultOptions.joined(separator: " "))")
        }
        for warning in startupWarnings {
            warn(warning)
        }
    }

    func handle(_ line: String) async -> Bool {
        let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else { return true }

        let tokens: [String]
        do {
            tokens = try tokenizer.tokenize(trimmed)
        } catch {
            emitError("Command was not understood: \(error)")
            return true
        }
        guard !tokens.isEmpty else { return true }

        var commandTokens = stripBinaryPrefix(tokens)
        guard let first = commandTokens.first else { return true }
        switch first.lowercased() {
        case "exit", "quit":
            await stopActiveCommand(reason: "interactive shell is exiting")
            ok("FXExport interactive command shell stopped")
            return false
        case "help", "?":
            printHelp()
            return true
        case "status":
            printStatus()
            return true
        case "stop":
            await stopActiveCommand(reason: "operator requested stop")
            return true
        case "wait":
            await waitForActiveCommand()
            return true
        case "shell", "interactive", "console":
            info("Already running inside the FXExport interactive command shell")
            return true
        default:
            commandTokens = applyDefaultOptions(to: commandTokens)
            await startAppCommand(commandTokens)
            return true
        }
    }

    func stopActiveCommand(reason: String) async {
        guard let active = activeCommand else {
            info("No active FXExport command is running")
            return
        }
        warn("Gracefully stopping active command '\(active.displayName)' because \(reason)")
        active.task?.cancel()
        if let task = active.task {
            _ = await task.value
        }
        if activeCommand?.id == active.id {
            activeCommand = nil
        }
        ok("Active command '\(active.displayName)' stopped cleanly")
    }

    private func waitForActiveCommand() async {
        guard let active = activeCommand else {
            info("No active FXExport command is running")
            return
        }
        info("Waiting for active command '\(active.displayName)' to finish")
        if let task = active.task {
            _ = await task.value
        }
        if activeCommand?.id == active.id {
            activeCommand = nil
        }
    }

    private func startAppCommand(_ arguments: [String]) async {
        if let active = activeCommand {
            await stopActiveCommand(reason: "new command '\(arguments.joined(separator: " "))' was received")
            if activeCommand?.id == active.id {
                return
            }
        }

        let displayName = arguments.joined(separator: " ")
        let id = UUID()
        activeCommand = ActiveCommand(id: id, displayName: displayName, startedAt: Date(), task: nil)
        info("Starting command: \(displayName)")
        let task = Task.detached(priority: .userInitiated) { () -> ExitCode in
            let result = await MT5ResearchCLI.run(arguments: arguments)
            await self.finishCommand(id: id, displayName: displayName, result: result)
            return result
        }
        activeCommand?.task = task
    }

    private func finishCommand(id: UUID, displayName: String, result: ExitCode) {
        guard activeCommand?.id == id else { return }
        activeCommand = nil
        if result == .success {
            ok("Command completed: \(displayName)")
        } else {
            warn("Command finished with exit code \(result.rawValue): \(displayName)")
        }
    }

    private func printStatus() {
        guard let active = activeCommand else {
            ok("No active FXExport command is running; prompt is ready")
            return
        }
        let seconds = Int(Date().timeIntervalSince(active.startedAt).rounded())
        info("Active command '\(active.displayName)' has been running for \(seconds)s")
    }

    private func printHelp() {
        print("""
        FXExport interactive command shell

        Type commands without restarting the app:
          startcheck
          migrate
          bridge-check
          symbol-check
          backfill --symbols all
          live
          supervise --with-backfill
          verify --random-ranges 20
          repair --symbol EURUSD --from 2020-01-01 --to 2020-02-01
          data-check --config Config/history_data.json

        Control commands:
          status   show the active command
          stop     gracefully cancel the active command and wait for shutdown
          wait     wait until the active command finishes
          exit     gracefully stop the active command and close the shell

        Notes:
          - Pasting a new app command while live/supervise/backfill is active first requests graceful cancellation.
          - Checkpoints advance only inside the normal verified ingest path; stop does not mark unfinished data complete.
          - Quote paths or values with spaces using single or double quotes.
        """)
    }

    private func applyDefaultOptions(to tokens: [String]) -> [String] {
        guard !defaultOptions.isEmpty else { return tokens }
        var result = tokens
        var index = 0
        while index < defaultOptions.count {
            let option = defaultOptions[index]
            switch option {
            case "--config-dir", "--migrations-dir":
                if !result.contains(option), index + 1 < defaultOptions.count {
                    result.append(option)
                    result.append(defaultOptions[index + 1])
                }
                index += 2
            case "--verbose", "--debug":
                if !result.contains("--verbose"), !result.contains("--debug") {
                    result.append(option)
                }
                index += 1
            default:
                index += 1
            }
        }
        return result
    }

    private func stripBinaryPrefix(_ tokens: [String]) -> [String] {
        guard let first = tokens.first else { return tokens }
        let lower = first.lowercased()
        if lower == "fxexport" || lower == "./fxexport" || lower == "mt5research" {
            return Array(tokens.dropFirst())
        }
        return tokens
    }

    private static func parseStartupDefaults(_ arguments: [String]) -> (options: [String], warnings: [String]) {
        var options: [String] = []
        var warnings: [String] = []
        var index = 0
        while index < arguments.count {
            let argument = arguments[index]
            switch argument {
            case "--config-dir", "--migrations-dir":
                guard index + 1 < arguments.count else {
                    warnings.append("Ignoring \(argument): missing value")
                    index += 1
                    continue
                }
                options.append(argument)
                options.append(arguments[index + 1])
                index += 2
            case "--verbose", "--debug":
                options.append(argument)
                index += 1
            default:
                warnings.append("Ignoring startup shell option '\(argument)'; paste full app commands at the prompt instead")
                index += 1
            }
        }
        return (options, warnings)
    }

    private func info(_ message: String) {
        print(colorPolicy.colorize("[INFO]  \(message)", as: .cyan))
    }

    private func ok(_ message: String) {
        print(colorPolicy.colorize("[OK]    \(message)", as: .green))
    }

    private func warn(_ message: String) {
        print(colorPolicy.colorize("[WARN]  \(message)", as: .yellow))
    }

    private func emitError(_ message: String) {
        print(colorPolicy.colorize("[ERROR] \(message)", as: .red))
    }
}
