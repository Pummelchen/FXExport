import Domain
import Foundation

public enum BrokerOffsetPolicy {
    /// Code-owned broker defaults replace fragile local broker-time configuration.
    /// Unknown brokers are not restricted here; they are still protected by the
    /// EA-observed live snapshot and audited ClickHouse offset authority.
    public static func acceptedLiveOffsets(for identity: BrokerServerIdentity) -> [OffsetSeconds] {
        let server = identity.server.lowercased()
        if server.contains("icmarkets") && server.contains("mt5") {
            return [
                OffsetSeconds(rawValue: 7_200),
                OffsetSeconds(rawValue: 10_800)
            ]
        }
        return []
    }
}
