import Domain
import Foundation

public enum VerificationMismatch: Equatable, Sendable {
    case rowCount(mt5: Int, database: Int)
    case brokerSource(index: Int, mt5: BrokerSourceId, database: BrokerSourceId)
    case logicalSymbol(index: Int, mt5: LogicalSymbol, database: LogicalSymbol)
    case mt5Symbol(index: Int, mt5: MT5Symbol, database: MT5Symbol)
    case mt5Timestamp(index: Int, mt5: MT5ServerSecond, database: MT5ServerSecond)
    case utcTimestamp(index: Int, mt5: UtcSecond, database: UtcSecond)
    case digits(index: Int, mt5: Digits, database: Digits)
    case offsetConfidence(index: Int, database: OffsetConfidence)
    case ohlc(index: Int)
    case hash(index: Int, mt5: BarHash, database: BarHash)
}

public struct VerificationResult: Sendable {
    public let isClean: Bool
    public let mismatches: [VerificationMismatch]

    public init(isClean: Bool, mismatches: [VerificationMismatch]) {
        self.isClean = isClean
        self.mismatches = mismatches
    }
}

public struct VerificationComparator: Sendable {
    public init() {}

    public func compare(mt5SourceBars: [VerificationBar], databaseBars: [VerificationBar]) -> VerificationResult {
        var mismatches: [VerificationMismatch] = []
        if mt5SourceBars.count != databaseBars.count {
            mismatches.append(.rowCount(mt5: mt5SourceBars.count, database: databaseBars.count))
        }
        let count = min(mt5SourceBars.count, databaseBars.count)
        for index in 0..<count {
            let mt5 = mt5SourceBars[index]
            let db = databaseBars[index]
            if mt5.brokerSourceId != db.brokerSourceId {
                mismatches.append(.brokerSource(index: index, mt5: mt5.brokerSourceId, database: db.brokerSourceId))
            }
            if mt5.logicalSymbol != db.logicalSymbol {
                mismatches.append(.logicalSymbol(index: index, mt5: mt5.logicalSymbol, database: db.logicalSymbol))
            }
            if mt5.mt5Symbol != db.mt5Symbol {
                mismatches.append(.mt5Symbol(index: index, mt5: mt5.mt5Symbol, database: db.mt5Symbol))
            }
            if mt5.mt5ServerTime != db.mt5ServerTime {
                mismatches.append(.mt5Timestamp(index: index, mt5: mt5.mt5ServerTime, database: db.mt5ServerTime))
            }
            if mt5.utcTime != db.utcTime {
                mismatches.append(.utcTimestamp(index: index, mt5: mt5.utcTime, database: db.utcTime))
            }
            if mt5.digits != db.digits {
                mismatches.append(.digits(index: index, mt5: mt5.digits, database: db.digits))
            }
            if db.offsetConfidence != .verified {
                mismatches.append(.offsetConfidence(index: index, database: db.offsetConfidence))
            }
            if mt5.open != db.open || mt5.high != db.high || mt5.low != db.low || mt5.close != db.close {
                mismatches.append(.ohlc(index: index))
            }
            if mt5.barHash != db.barHash {
                mismatches.append(.hash(index: index, mt5: mt5.barHash, database: db.barHash))
            }
        }
        return VerificationResult(isClean: mismatches.isEmpty, mismatches: mismatches)
    }
}
