import Foundation
import FXBacktestAPI

public protocol FXBacktestHistoryProviding: Sendable {
    func loadM1History(_ request: FXBacktestM1HistoryRequest) async throws -> FXBacktestM1HistoryResponse
}

public protocol FXBacktestExecutionProviding: Sendable {
    func loadExecutionSpec(_ request: FXBacktestExecutionSpecRequest) async throws -> FXBacktestExecutionSpecResponse
}

public struct FXBacktestHTTPResponse: Sendable, Equatable {
    public let statusCode: Int
    public let contentType: String
    public let body: Data

    public init(statusCode: Int, contentType: String = "application/json; charset=utf-8", body: Data) {
        self.statusCode = statusCode
        self.contentType = contentType
        self.body = body
    }
}

public struct FXBacktestAPIHTTPHandler: Sendable {
    private let historyProvider: any FXBacktestHistoryProviding
    private let executionProvider: (any FXBacktestExecutionProviding)?

    public init(historyProvider: any FXBacktestHistoryProviding, executionProvider: (any FXBacktestExecutionProviding)? = nil) {
        self.historyProvider = historyProvider
        self.executionProvider = executionProvider
    }

    public func handle(method: String, path: String, body: Data) async -> FXBacktestHTTPResponse {
        do {
            switch (method.uppercased(), path) {
            case ("GET", FXBacktestAPIV1.statusPath):
                return try json(FXBacktestAPIStatusResponse())

            case ("POST", FXBacktestAPIV1.m1HistoryPath):
                let request = try JSONDecoder().decode(FXBacktestM1HistoryRequest.self, from: body)
                try request.validate()
                let response = try await historyProvider.loadM1History(request)
                try response.validate()
                return try json(response)

            case ("POST", FXBacktestAPIV1.executionSpecPath):
                guard let executionProvider else {
                    throw FXBacktestAPIServiceError.executionUnavailable("Execution provider is not configured.")
                }
                let request = try JSONDecoder().decode(FXBacktestExecutionSpecRequest.self, from: body)
                try request.validate()
                let response = try await executionProvider.loadExecutionSpec(request)
                try response.validate()
                return try json(response)

            default:
                return try error(status: 404, code: "not_found", message: "Unknown FXBacktest API endpoint \(method) \(path)")
            }
        } catch let error as FXBacktestAPIValidationError {
            return safeError(status: 400, code: "invalid_request", message: error.description)
        } catch let error as DecodingError {
            return safeError(status: 400, code: "invalid_json", message: String(describing: error))
        } catch let error as FXBacktestAPIServiceError {
            return safeError(status: error.httpStatus, code: error.code, message: error.description)
        } catch {
            return safeError(status: 500, code: "internal_error", message: String(describing: error))
        }
    }

    private func json<T: Encodable>(_ value: T, status: Int = 200) throws -> FXBacktestHTTPResponse {
        FXBacktestHTTPResponse(statusCode: status, body: try Self.makeEncoder().encode(value))
    }

    private func error(status: Int, code: String, message: String) throws -> FXBacktestHTTPResponse {
        try json(FXBacktestAPIErrorResponse(code: code, message: message), status: status)
    }

    private func safeError(status: Int, code: String, message: String) -> FXBacktestHTTPResponse {
        do {
            return try error(status: status, code: code, message: message)
        } catch {
            let fallback = #"{"api_version":"\#(FXBacktestAPIV1.version)","error":{"code":"encoding_error","message":"Could not encode API error response"}}"#
            return FXBacktestHTTPResponse(statusCode: 500, body: Data(fallback.utf8))
        }
    }

    private static func makeEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return encoder
    }
}

public enum FXBacktestAPIServiceError: Error, Equatable, CustomStringConvertible, Sendable {
    case invalidRequest(String)
    case unconfiguredSymbol(String)
    case brokerMismatch(expected: String, actual: String)
    case mt5SymbolMismatch(expected: String, actual: String)
    case digitsMismatch(expected: Int, actual: Int)
    case readinessBlocked(String)
    case historyUnavailable(String)
    case executionUnavailable(String)

    public var httpStatus: Int {
        switch self {
        case .invalidRequest, .unconfiguredSymbol, .brokerMismatch, .mt5SymbolMismatch, .digitsMismatch:
            return 400
        case .readinessBlocked:
            return 409
        case .historyUnavailable, .executionUnavailable:
            return 502
        }
    }

    public var code: String {
        switch self {
        case .invalidRequest:
            return "invalid_request"
        case .unconfiguredSymbol:
            return "unconfigured_symbol"
        case .brokerMismatch:
            return "broker_mismatch"
        case .mt5SymbolMismatch:
            return "mt5_symbol_mismatch"
        case .digitsMismatch:
            return "digits_mismatch"
        case .readinessBlocked:
            return "readiness_blocked"
        case .historyUnavailable:
            return "history_unavailable"
        case .executionUnavailable:
            return "execution_unavailable"
        }
    }

    public var description: String {
        switch self {
        case .invalidRequest(let reason):
            return reason
        case .unconfiguredSymbol(let symbol):
            return "\(symbol) is not configured in FXExport symbols.json."
        case .brokerMismatch(let expected, let actual):
            return "Requested broker_source_id \(actual) does not match FXExport broker_source_id \(expected)."
        case .mt5SymbolMismatch(let expected, let actual):
            return "Requested expected_mt5_symbol \(actual) does not match FXExport configured MT5 symbol \(expected)."
        case .digitsMismatch(let expected, let actual):
            return "Requested expected_digits \(actual) does not match FXExport configured digits \(expected)."
        case .readinessBlocked(let reason):
            return "FXExport backtest readiness gate blocked the request: \(reason)"
        case .historyUnavailable(let reason):
            return "FXExport could not load verified M1 history: \(reason)"
        case .executionUnavailable(let reason):
            return "FXExport could not load MT5 execution metadata: \(reason)"
        }
    }
}
