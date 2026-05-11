import AppCore
import Config
import Domain
import Foundation
import MT5Bridge
import TimeMapping

public enum BrokerOffsetRuntimeError: Error, CustomStringConvertible, Sendable {
    case invalidSnapshot(tradeServer: Int64, gmt: Int64)
    case observedOffsetNotAccepted(OffsetSeconds, accepted: [OffsetSeconds])
    case liveOffsetMismatch(observed: OffsetSeconds, configured: OffsetSeconds, serverTime: MT5ServerSecond)

    public var description: String {
        switch self {
        case .invalidSnapshot(let tradeServer, let gmt):
            return "MT5 server time snapshot is invalid. TimeTradeServer=\(tradeServer), TimeGMT=\(gmt)."
        case .observedOffsetNotAccepted(let observed, let accepted):
            let acceptedText = accepted.map(\.description).joined(separator: ", ")
            return "Observed MT5 live server UTC offset \(observed.rawValue) is not in accepted offsets [\(acceptedText)]."
        case .liveOffsetMismatch(let observed, let configured, let serverTime):
            return "Observed MT5 live server UTC offset \(observed.rawValue) does not match verified DB offset \(configured.rawValue) for server timestamp \(serverTime.rawValue)."
        }
    }
}

public struct BrokerOffsetRuntimeVerifier: Sendable {
    public init() {}

    public func verify(
        snapshot: ServerTimeSnapshotDTO,
        offsetMap: BrokerOffsetMap,
        acceptedLiveOffsetSeconds: [OffsetSeconds],
        logger: Logger
    ) throws {
        let observed = try Self.observedOffset(from: snapshot)
        if !acceptedLiveOffsetSeconds.isEmpty && !acceptedLiveOffsetSeconds.contains(observed) {
            throw BrokerOffsetRuntimeError.observedOffsetNotAccepted(observed, accepted: acceptedLiveOffsetSeconds)
        }

        let serverTime = MT5ServerSecond(rawValue: snapshot.timeTradeServer)
        let segment = try offsetMap.segment(containing: serverTime)
        guard segment.offset == observed else {
            throw BrokerOffsetRuntimeError.liveOffsetMismatch(observed: observed, configured: segment.offset, serverTime: serverTime)
        }
        logger.ok("MT5 live server UTC offset verified: \(observed.rawValue) seconds for \(offsetMap.terminalIdentity)")
    }

    public static func observedOffset(from snapshot: ServerTimeSnapshotDTO) throws -> OffsetSeconds {
        guard snapshot.timeTradeServer > 0, snapshot.timeGMT > 0 else {
            throw BrokerOffsetRuntimeError.invalidSnapshot(tradeServer: snapshot.timeTradeServer, gmt: snapshot.timeGMT)
        }
        let deltaResult = snapshot.timeTradeServer.subtractingReportingOverflow(snapshot.timeGMT)
        guard !deltaResult.overflow else {
            throw BrokerOffsetRuntimeError.invalidSnapshot(tradeServer: snapshot.timeTradeServer, gmt: snapshot.timeGMT)
        }
        let delta = deltaResult.partialValue
        guard (-86_400...86_400).contains(delta) else {
            throw BrokerOffsetRuntimeError.invalidSnapshot(tradeServer: snapshot.timeTradeServer, gmt: snapshot.timeGMT)
        }
        let rounded = roundToNearestMinute(delta)
        let remainder = delta.subtractingReportingOverflow(rounded)
        guard !remainder.overflow, abs(remainder.partialValue) <= 5 else {
            throw BrokerOffsetRuntimeError.invalidSnapshot(tradeServer: snapshot.timeTradeServer, gmt: snapshot.timeGMT)
        }
        return OffsetSeconds(rawValue: rounded)
    }

    private static func roundToNearestMinute(_ value: Int64) -> Int64 {
        if value >= 0 {
            return ((value + 30) / 60) * 60
        }
        return ((value - 30) / 60) * 60
    }
}
