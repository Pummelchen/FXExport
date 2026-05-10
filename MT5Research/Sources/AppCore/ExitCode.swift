public enum ExitCode: Int32, Sendable {
    case success = 0
    case usage = 2
    case configuration = 3
    case mt5Bridge = 10
    case clickHouse = 11
    case validation = 12
    case verification = 13
    case repair = 14
    case backtest = 15
    case unknown = 99
}
