import AppCore
import Domain
import Foundation
import Ingestion
import MT5Bridge
import Verification

public struct DatabaseVerifierRepairProductionAgent: ProductionAgent {
    public let descriptor: AgentDescriptor
    private let authority = BrokerAuthority()

    public init(intervalSeconds: Int) {
        self.descriptor = AgentDescriptor(
            kind: .databaseVerifierRepairer,
            intervalSeconds: intervalSeconds,
            requiresMT5Bridge: false
        )
    }

    public func run(context: AgentRuntimeContext, startedAt: Date) async throws -> AgentOutcome {
        try await VerificationAgent(
            config: context.config,
            bridge: nil,
            clickHouse: context.clickHouse,
            logger: context.logger
        ).startupChecks(randomRanges: 0)

        let configuredRandomRanges = context.config.app.verifierRandomRanges
        guard configuredRandomRanges > 0 else {
            return AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
                .ok("Database checks completed; MT5 random cross-check is disabled")
        }

        guard let bridge = context.bridge else {
            return AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
                .warning("Database checks completed; MT5 cross-check skipped because bridge is not connected")
        }

        let offsetMap = try await authority.verifyLiveOffset(context: context, bridge: bridge)
        let verifier = HistoricalRangeVerifier(
            config: context.config,
            bridge: bridge,
            clickHouse: context.clickHouse,
            offsetMap: offsetMap,
            logger: context.logger
        )
        let repairAgent = RepairAgent(
            clickHouse: context.clickHouse,
            database: context.config.clickHouse.database,
            logger: context.logger
        )
        let checkpointStore = context.checkpointStore()
        var generator = SystemRandomNumberGenerator()
        var checked = 0
        var repaired = 0
        var skipped = 0
        var mismatchCount = 0
        var warnings: [String] = []

        for _ in 0..<configuredRandomRanges {
            guard let mapping = context.config.symbols.symbols.randomElement(using: &generator) else {
                break
            }
            do {
                guard let state = try await checkpointStore.latestState(
                    brokerSourceId: context.config.brokerTime.brokerSourceId,
                    logicalSymbol: mapping.logicalSymbol
                ) else {
                    skipped += 1
                    continue
                }
                let range = try RandomRangeSelector().selectMonth(
                    brokerSourceId: context.config.brokerTime.brokerSourceId,
                    logicalSymbol: mapping.logicalSymbol,
                    oldest: state.oldestMT5ServerTime,
                    latestClosed: state.latestIngestedClosedMT5ServerTime,
                    random: &generator
                )
                let rangeLabel = OperatorStatusText.monthRangeLabel(start: range.mt5Start, endExclusive: range.mt5EndExclusive)
                let outcome = try await verifier.verify(range: range)
                checked += 1
                guard !outcome.result.isClean else {
                    context.logger.verify("\(mapping.logicalSymbol.rawValue) - \(rangeLabel) clean; no repair needed")
                    continue
                }
                mismatchCount += outcome.result.mismatches.count
                guard context.repairOnVerifierMismatch else {
                    context.logger.warn("\(mapping.logicalSymbol.rawValue) - \(rangeLabel) has MT5 mismatches; repair is disabled")
                    continue
                }
                let decision = RepairPolicy().decide(
                    verification: outcome.result,
                    mt5Available: !outcome.mt5Bars.isEmpty,
                    utcMappingAmbiguous: false
                )
                try await repairAgent.repairCanonicalRange(
                    range: range,
                    replacementBars: outcome.mt5Bars,
                    decision: decision
                )
                if case .repairCanonicalOnly = decision {
                    let recheck = try await verifier.verify(range: range)
                    guard recheck.result.isClean else {
                        throw RepairError.refused("post-repair verification still reports \(recheck.result.mismatches.count) mismatch(es)")
                    }
                    context.logger.repair("\(mapping.logicalSymbol.rawValue) - \(rangeLabel) repaired, reverified against MT5, UTC correct and all canonical data clean")
                    repaired += 1
                }
            } catch let error as RepairError {
                throw error
            } catch let error as MT5BridgeError {
                throw error
            } catch let error as ProtocolError {
                throw error
            } catch {
                skipped += 1
                warnings.append("\(mapping.logicalSymbol.rawValue): \(error)")
            }
        }

        let warningDetails = warnings.isEmpty ? "" : "; warnings=\(warnings.joined(separator: " | "))"
        let details = "checked=\(checked); repaired=\(repaired); skipped=\(skipped); mismatches=\(mismatchCount)\(warningDetails)"
        let factory = AgentOutcomeFactory(kind: descriptor.kind, startedAt: startedAt)
        if mismatchCount > 0 && repaired == 0 {
            return factory.warning("Database verifier found mismatches but did not repair", details: details)
        }
        if !warnings.isEmpty {
            return factory.warning("Database verification completed with skipped ranges", details: details)
        }
        return factory.ok("Database verification and repair cycle completed", details: details)
    }
}
