import Domain
import Foundation
import MT5Bridge

public enum MT5SourceRangeError: Error, CustomStringConvertible, Sendable {
    case missingManifest(String)
    case manifestMismatch(String)
    case unstableRange(String)
    case invalidRange(String)

    public var description: String {
        switch self {
        case .missingManifest(let reason):
            return "MT5 range response is missing completeness metadata: \(reason)"
        case .manifestMismatch(let reason):
            return "MT5 range response metadata does not match the request: \(reason)"
        case .unstableRange(let reason):
            return "MT5 range response was not stable across confirmation reads: \(reason)"
        case .invalidRange(let reason):
            return "MT5 range response is invalid: \(reason)"
        }
    }
}

public struct MT5RangeManifest: Equatable, Sendable {
    public let requestedFrom: MT5ServerSecond
    public let requestedToExclusive: MT5ServerSecond
    public let effectiveToExclusive: MT5ServerSecond
    public let latestClosed: MT5ServerSecond
    public let seriesSynchronized: Bool
    public let copiedCount: Int
    public let emittedCount: Int
    public let firstMT5ServerTime: MT5ServerSecond?
    public let lastMT5ServerTime: MT5ServerSecond?

    public init(
        requestedFrom: MT5ServerSecond,
        requestedToExclusive: MT5ServerSecond,
        effectiveToExclusive: MT5ServerSecond,
        latestClosed: MT5ServerSecond,
        seriesSynchronized: Bool,
        copiedCount: Int,
        emittedCount: Int,
        firstMT5ServerTime: MT5ServerSecond?,
        lastMT5ServerTime: MT5ServerSecond?
    ) {
        self.requestedFrom = requestedFrom
        self.requestedToExclusive = requestedToExclusive
        self.effectiveToExclusive = effectiveToExclusive
        self.latestClosed = latestClosed
        self.seriesSynchronized = seriesSynchronized
        self.copiedCount = copiedCount
        self.emittedCount = emittedCount
        self.firstMT5ServerTime = firstMT5ServerTime
        self.lastMT5ServerTime = lastMT5ServerTime
    }
}

public struct StableMT5SourceRange: Sendable {
    public let response: RatesResponseDTO
    public let manifest: MT5RangeManifest
    public let sourceHash: String

    public init(response: RatesResponseDTO, manifest: MT5RangeManifest, sourceHash: String) {
        self.response = response
        self.manifest = manifest
        self.sourceHash = sourceHash
    }
}

private struct MT5SourceSignature: Equatable, Sendable {
    let mt5Symbol: String
    let timeframe: String
    let requestedFrom: Int64
    let requestedToExclusive: Int64
    let effectiveToExclusive: Int64
    let seriesSynchronized: Bool
    let emittedCount: Int
    let first: Int64
    let last: Int64
    let sourceHash: String
}

public struct MT5SourceRangeVerifier: Sendable {
    public let maxAttempts: Int
    public let confirmationDelayNanoseconds: UInt64
    public let retryDelayNanoseconds: UInt64

    public init(
        maxAttempts: Int = 5,
        confirmationDelayNanoseconds: UInt64 = 100_000_000,
        retryDelayNanoseconds: UInt64 = 500_000_000
    ) {
        self.maxAttempts = max(1, maxAttempts)
        self.confirmationDelayNanoseconds = confirmationDelayNanoseconds
        self.retryDelayNanoseconds = retryDelayNanoseconds
    }

    public func fetchStableRange(
        mt5Symbol: MT5Symbol,
        from: MT5ServerSecond,
        toExclusive: MT5ServerSecond,
        maxBars: Int,
        request: () throws -> RatesResponseDTO
    ) async throws -> StableMT5SourceRange {
        var lastError: Error?
        for attempt in 1...maxAttempts {
            do {
                let first = try validate(
                    request(),
                    expectedMT5Symbol: mt5Symbol,
                    from: from,
                    toExclusive: toExclusive,
                    maxBars: maxBars
                )
                try await Task.sleep(nanoseconds: confirmationDelayNanoseconds)
                let second = try validate(
                    request(),
                    expectedMT5Symbol: mt5Symbol,
                    from: from,
                    toExclusive: toExclusive,
                    maxBars: maxBars
                )
                guard signature(first) == signature(second) else {
                    throw MT5SourceRangeError.unstableRange("attempt \(attempt) returned different source hashes or manifest counts")
                }
                return second
            } catch {
                lastError = error
                if attempt < maxAttempts {
                    try await Task.sleep(nanoseconds: retryDelay(attempt: attempt))
                }
            }
        }
        throw lastError ?? MT5SourceRangeError.unstableRange("no stable MT5 response after \(maxAttempts) attempt(s)")
    }

    public func validate(
        _ response: RatesResponseDTO,
        expectedMT5Symbol: MT5Symbol,
        from: MT5ServerSecond,
        toExclusive: MT5ServerSecond,
        maxBars: Int
    ) throws -> StableMT5SourceRange {
        guard response.mt5Symbol == expectedMT5Symbol.rawValue else {
            throw MT5SourceRangeError.manifestMismatch("expected \(expectedMT5Symbol.rawValue), got \(response.mt5Symbol)")
        }
        guard response.timeframe == Timeframe.m1.rawValue else {
            throw MT5SourceRangeError.manifestMismatch("expected M1 rates, got \(response.timeframe)")
        }
        guard from.rawValue < toExclusive.rawValue, from.isMinuteAligned, toExclusive.isMinuteAligned else {
            throw MT5SourceRangeError.invalidRange("requested range \(from.rawValue)..<\(toExclusive.rawValue) must be positive and minute-aligned")
        }
        let manifest = try manifest(from: response)
        guard manifest.requestedFrom == from else {
            throw MT5SourceRangeError.manifestMismatch("requested_from \(manifest.requestedFrom.rawValue), expected \(from.rawValue)")
        }
        guard manifest.requestedToExclusive == toExclusive else {
            throw MT5SourceRangeError.manifestMismatch("requested_to \(manifest.requestedToExclusive.rawValue), expected \(toExclusive.rawValue)")
        }
        guard manifest.effectiveToExclusive == toExclusive else {
            throw MT5SourceRangeError.manifestMismatch("effective_to \(manifest.effectiveToExclusive.rawValue), expected full closed range \(toExclusive.rawValue)")
        }
        guard manifest.seriesSynchronized else {
            throw MT5SourceRangeError.manifestMismatch("MT5 reports the M1 series is not synchronized")
        }
        guard manifest.emittedCount == response.rates.count else {
            throw MT5SourceRangeError.manifestMismatch("emitted_count \(manifest.emittedCount), decoded rows \(response.rates.count)")
        }
        guard manifest.copiedCount == manifest.emittedCount else {
            throw MT5SourceRangeError.manifestMismatch("copied_count \(manifest.copiedCount) differs from emitted_count \(manifest.emittedCount)")
        }
        guard manifest.emittedCount <= maxBars else {
            throw MT5SourceRangeError.manifestMismatch("emitted_count \(manifest.emittedCount) exceeds max_bars \(maxBars)")
        }
        if response.rates.isEmpty {
            guard manifest.firstMT5ServerTime == nil, manifest.lastMT5ServerTime == nil else {
                throw MT5SourceRangeError.manifestMismatch("empty response must not advertise first/last bar timestamps")
            }
        } else {
            guard let first = manifest.firstMT5ServerTime,
                  let last = manifest.lastMT5ServerTime else {
                throw MT5SourceRangeError.manifestMismatch("non-empty response is missing first/last bar timestamps")
            }
            guard response.rates.first?.mt5ServerTime == first.rawValue,
                  response.rates.last?.mt5ServerTime == last.rawValue else {
                throw MT5SourceRangeError.manifestMismatch("manifest first/last timestamps do not match decoded rows")
            }
        }

        var previous: Int64?
        for rate in response.rates {
            guard rate.mt5ServerTime >= from.rawValue,
                  rate.mt5ServerTime < toExclusive.rawValue else {
                throw MT5SourceRangeError.invalidRange("rate \(rate.mt5ServerTime) is outside \(from.rawValue)..<\(toExclusive.rawValue)")
            }
            guard rate.mt5ServerTime <= manifest.latestClosed.rawValue else {
                throw MT5SourceRangeError.invalidRange("rate \(rate.mt5ServerTime) is newer than latest closed \(manifest.latestClosed.rawValue)")
            }
            guard MT5ServerSecond(rawValue: rate.mt5ServerTime).isMinuteAligned else {
                throw MT5SourceRangeError.invalidRange("rate timestamp \(rate.mt5ServerTime) is not minute-aligned")
            }
            if let previous, rate.mt5ServerTime <= previous {
                throw MT5SourceRangeError.invalidRange("rates are not strictly increasing at \(previous) then \(rate.mt5ServerTime)")
            }
            previous = rate.mt5ServerTime
        }

        return StableMT5SourceRange(
            response: response,
            manifest: manifest,
            sourceHash: Self.sourceHash(response.rates)
        )
    }

    public static func sourceHash(_ rates: [MT5RateDTO]) -> String {
        var hasher = FNV1a64()
        for rate in rates {
            hasher.append(rate.mt5ServerTime)
            hasher.append(rate.open)
            hasher.append(rate.high)
            hasher.append(rate.low)
            hasher.append(rate.close)
        }
        return "fnv64:" + String(format: "%016llx", hasher.value)
    }

    private func manifest(from response: RatesResponseDTO) throws -> MT5RangeManifest {
        guard let requestedFrom = response.requestedFromMT5ServerTs else {
            throw MT5SourceRangeError.missingManifest("requested_from_mt5_server_ts")
        }
        guard let requestedTo = response.requestedToMT5ServerTsExclusive else {
            throw MT5SourceRangeError.missingManifest("requested_to_mt5_server_ts_exclusive")
        }
        guard let effectiveTo = response.effectiveToMT5ServerTsExclusive else {
            throw MT5SourceRangeError.missingManifest("effective_to_mt5_server_ts_exclusive")
        }
        guard let latestClosed = response.latestClosedMT5ServerTs else {
            throw MT5SourceRangeError.missingManifest("latest_closed_mt5_server_ts")
        }
        guard let seriesSynchronized = response.seriesSynchronized else {
            throw MT5SourceRangeError.missingManifest("series_synchronized")
        }
        guard let copiedCount = response.copiedCount else {
            throw MT5SourceRangeError.missingManifest("copied_count")
        }
        guard let emittedCount = response.emittedCount else {
            throw MT5SourceRangeError.missingManifest("emitted_count")
        }
        guard let first = response.firstMT5ServerTs else {
            throw MT5SourceRangeError.missingManifest("first_mt5_server_ts")
        }
        guard let last = response.lastMT5ServerTs else {
            throw MT5SourceRangeError.missingManifest("last_mt5_server_ts")
        }
        guard copiedCount >= 0, emittedCount >= 0 else {
            throw MT5SourceRangeError.manifestMismatch("copied_count and emitted_count must be non-negative")
        }
        return MT5RangeManifest(
            requestedFrom: MT5ServerSecond(rawValue: requestedFrom),
            requestedToExclusive: MT5ServerSecond(rawValue: requestedTo),
            effectiveToExclusive: MT5ServerSecond(rawValue: effectiveTo),
            latestClosed: MT5ServerSecond(rawValue: latestClosed),
            seriesSynchronized: seriesSynchronized,
            copiedCount: copiedCount,
            emittedCount: emittedCount,
            firstMT5ServerTime: first > 0 ? MT5ServerSecond(rawValue: first) : nil,
            lastMT5ServerTime: last > 0 ? MT5ServerSecond(rawValue: last) : nil
        )
    }

    private func signature(_ range: StableMT5SourceRange) -> MT5SourceSignature {
        MT5SourceSignature(
            mt5Symbol: range.response.mt5Symbol,
            timeframe: range.response.timeframe,
            requestedFrom: range.manifest.requestedFrom.rawValue,
            requestedToExclusive: range.manifest.requestedToExclusive.rawValue,
            effectiveToExclusive: range.manifest.effectiveToExclusive.rawValue,
            seriesSynchronized: range.manifest.seriesSynchronized,
            emittedCount: range.manifest.emittedCount,
            first: range.manifest.firstMT5ServerTime?.rawValue ?? 0,
            last: range.manifest.lastMT5ServerTime?.rawValue ?? 0,
            sourceHash: range.sourceHash
        )
    }

    private func retryDelay(attempt: Int) -> UInt64 {
        let result = retryDelayNanoseconds.multipliedReportingOverflow(by: UInt64(attempt))
        return result.overflow ? UInt64.max : result.partialValue
    }
}
