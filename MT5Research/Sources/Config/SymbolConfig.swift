import Domain
import Foundation

public struct SymbolMapping: Codable, Hashable, Sendable {
    public let logicalSymbol: LogicalSymbol
    public let mt5Symbol: MT5Symbol
    public let digits: Digits

    enum CodingKeys: String, CodingKey {
        case logicalSymbol = "logical_symbol"
        case mt5Symbol = "mt5_symbol"
        case digits
    }

    public init(logicalSymbol: LogicalSymbol, mt5Symbol: MT5Symbol, digits: Digits) {
        self.logicalSymbol = logicalSymbol
        self.mt5Symbol = mt5Symbol
        self.digits = digits
    }
}

public struct SymbolConfig: Codable, Sendable {
    public let symbols: [SymbolMapping]

    public init(symbols: [SymbolMapping]) {
        self.symbols = symbols
    }

    public func mapping(for logicalSymbol: LogicalSymbol) -> SymbolMapping? {
        symbols.first { $0.logicalSymbol == logicalSymbol }
    }
}
