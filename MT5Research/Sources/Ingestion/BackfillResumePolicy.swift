import Domain
import Foundation

public enum BackfillResumeAction: Equatable, Sendable {
    case freshBackfill
    case resumeFromCheckpoint
    case reprocessExpandedOlderHistory
    case resumeAfterPrunedHistory
}

public struct BackfillResumeDecision: Equatable, Sendable {
    public let cursor: MT5ServerSecond
    public let action: BackfillResumeAction

    public init(cursor: MT5ServerSecond, action: BackfillResumeAction) {
        self.cursor = cursor
        self.action = action
    }
}

public struct BackfillResumePolicy: Sendable {
    public init() {}

    public static func decide(
        logicalSymbol: LogicalSymbol,
        mt5Symbol: MT5Symbol,
        oldest: MT5ServerSecond,
        latestClosed: MT5ServerSecond,
        existingState: IngestState?
    ) throws -> BackfillResumeDecision {
        guard let existingState else {
            return BackfillResumeDecision(cursor: oldest, action: .freshBackfill)
        }
        guard existingState.mt5Symbol == mt5Symbol else {
            throw IngestError.checkpointSymbolMismatch(
                logicalSymbol: logicalSymbol.rawValue,
                expected: mt5Symbol.rawValue,
                actual: existingState.mt5Symbol.rawValue
            )
        }
        guard existingState.latestIngestedClosedMT5ServerTime.rawValue <= latestClosed.rawValue else {
            throw IngestError.checkpointAheadOfMT5(
                logicalSymbol: logicalSymbol.rawValue,
                checkpoint: existingState.latestIngestedClosedMT5ServerTime.rawValue,
                latestClosed: latestClosed.rawValue
            )
        }

        if oldest.rawValue < existingState.oldestMT5ServerTime.rawValue {
            return BackfillResumeDecision(cursor: oldest, action: .reprocessExpandedOlderHistory)
        }

        let nextFromCheckpoint = try nextClosedBar(after: existingState.latestIngestedClosedMT5ServerTime)
        if nextFromCheckpoint.rawValue < oldest.rawValue {
            return BackfillResumeDecision(cursor: oldest, action: .resumeAfterPrunedHistory)
        }

        return BackfillResumeDecision(cursor: nextFromCheckpoint, action: .resumeFromCheckpoint)
    }

    private static func nextClosedBar(after time: MT5ServerSecond) throws -> MT5ServerSecond {
        let result = time.rawValue.addingReportingOverflow(Timeframe.m1.seconds)
        guard !result.overflow else {
            throw IngestError.invalidChunk("checkpoint timestamp overflow while computing next M1 bar")
        }
        return MT5ServerSecond(rawValue: result.partialValue)
    }
}
