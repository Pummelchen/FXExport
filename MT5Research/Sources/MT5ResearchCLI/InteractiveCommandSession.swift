import AppCore
import Darwin
import Foundation

struct InteractiveCommandSession: Sendable {
    private let coordinator: InteractiveCommandCoordinator

    init(ignoredLaunchArguments: [String] = []) {
        coordinator = InteractiveCommandCoordinator(ignoredLaunchArguments: ignoredLaunchArguments)
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
    private let ignoredLaunchArguments: [String]
    private var activeCommand: ActiveCommand?

    init(ignoredLaunchArguments: [String]) {
        self.ignoredLaunchArguments = ignoredLaunchArguments
    }

    func printBanner() {
        info("FXExport interactive command shell started")
        info("Type an FXExport command at the prompt, for example: supervise --with-backfill")
        info("Control commands: status, stop, wait, help, exit")
        if !ignoredLaunchArguments.isEmpty {
            warn("Launch-time input is not accepted and was ignored: \(ignoredLaunchArguments.joined(separator: " "))")
            warn("Paste the command text at the `>` prompt instead.")
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

        let commandTokens = stripBinaryPrefix(tokens)
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
          fxbacktest-api --api-host 127.0.0.1 --api-port 5066
          health-api --api-host 127.0.0.1 --api-port 5067

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

    private func stripBinaryPrefix(_ tokens: [String]) -> [String] {
        guard let first = tokens.first else { return tokens }
        let lower = first.lowercased()
        if lower == "fxexport" || lower == "./fxexport" || lower == "mt5research" {
            return Array(tokens.dropFirst())
        }
        return tokens
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
