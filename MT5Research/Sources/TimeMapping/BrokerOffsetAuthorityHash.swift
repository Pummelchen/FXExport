import Domain
import Foundation

public extension BrokerOffsetMap {
    func authoritySHA256() -> SHA256DigestHex {
        var hasher = SHA256ChunkHasher(namespace: "broker_offset_authority")
        hasher.appendField("broker_source_id", brokerSourceId.rawValue)
        hasher.appendField("mt5_company", terminalIdentity.company)
        hasher.appendField("mt5_server", terminalIdentity.server)
        hasher.appendField("mt5_account_login", terminalIdentity.accountLogin)
        hasher.appendField("segment_count", segments.count)

        for (index, segment) in segments.enumerated() {
            hasher.appendField("segment_index", index)
            hasher.appendField("valid_from_mt5_server_ts", segment.validFrom.rawValue)
            hasher.appendField("valid_to_mt5_server_ts", segment.validTo.rawValue)
            hasher.appendField("offset_seconds", segment.offset.rawValue)
            hasher.appendField("offset_source", segment.source.rawValue)
            hasher.appendField("offset_confidence", segment.confidence.rawValue)
        }

        return hasher.finalize()
    }
}
