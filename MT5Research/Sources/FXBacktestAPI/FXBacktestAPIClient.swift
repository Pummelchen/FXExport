import Foundation

public struct FXBacktestAPIClient: Sendable {
    private let baseURL: URL
    private let session: URLSession
    private let requestTimeoutSeconds: Double

    public init(baseURL: URL, requestTimeoutSeconds: Double = 120, session: URLSession = .shared) {
        self.baseURL = baseURL
        self.session = session
        self.requestTimeoutSeconds = requestTimeoutSeconds
    }

    public func status() async throws -> FXBacktestAPIStatusResponse {
        var request = URLRequest(url: try endpoint(FXBacktestAPIV1.statusPath))
        request.httpMethod = "GET"
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        let data = try await perform(request)
        let response = try JSONDecoder().decode(FXBacktestAPIStatusResponse.self, from: data)
        guard response.apiVersion == FXBacktestAPIV1.version else {
            throw FXBacktestAPIClientError.apiVersionMismatch(response.apiVersion)
        }
        return response
    }

    public func loadM1History(_ historyRequest: FXBacktestM1HistoryRequest) async throws -> FXBacktestM1HistoryResponse {
        try historyRequest.validate()
        var request = URLRequest(url: try endpoint(FXBacktestAPIV1.m1HistoryPath))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.httpBody = try Self.makeEncoder().encode(historyRequest)
        let data = try await perform(request)
        let response = try JSONDecoder().decode(FXBacktestM1HistoryResponse.self, from: data)
        try response.validate()
        return response
    }

    public func loadExecutionSpec(_ specRequest: FXBacktestExecutionSpecRequest) async throws -> FXBacktestExecutionSpecResponse {
        try specRequest.validate()
        var request = URLRequest(url: try endpoint(FXBacktestAPIV1.executionSpecPath))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.httpBody = try Self.makeEncoder().encode(specRequest)
        let data = try await perform(request)
        let response = try JSONDecoder().decode(FXBacktestExecutionSpecResponse.self, from: data)
        try response.validate()
        return response
    }

    private func endpoint(_ path: String) throws -> URL {
        guard var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else {
            throw FXBacktestAPIClientError.invalidBaseURL(baseURL.absoluteString)
        }
        components.path = path
        components.query = nil
        guard let url = components.url else {
            throw FXBacktestAPIClientError.invalidBaseURL(baseURL.absoluteString)
        }
        return url
    }

    private func perform(_ request: URLRequest) async throws -> Data {
        var request = request
        request.timeoutInterval = requestTimeoutSeconds
        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw FXBacktestAPIClientError.transport(error.localizedDescription)
        }
        guard let httpResponse = response as? HTTPURLResponse else {
            throw FXBacktestAPIClientError.invalidResponse("Response was not HTTP.")
        }
        guard (200..<300).contains(httpResponse.statusCode) else {
            let errorResponse: FXBacktestAPIErrorResponse
            do {
                errorResponse = try JSONDecoder().decode(FXBacktestAPIErrorResponse.self, from: data)
            } catch {
                let body = String(data: data, encoding: .utf8) ?? ""
                let detail = body.isEmpty
                    ? "Could not decode FXExport API error response: \(error)"
                    : "\(body) (could not decode FXExport API error response: \(error))"
                throw FXBacktestAPIClientError.httpStatus(httpResponse.statusCode, detail)
            }
            throw FXBacktestAPIClientError.server(code: errorResponse.error.code, message: errorResponse.error.message)
        }
        return data
    }

    private static func makeEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return encoder
    }
}

public enum FXBacktestAPIClientError: Error, Equatable, CustomStringConvertible, Sendable {
    case invalidBaseURL(String)
    case transport(String)
    case invalidResponse(String)
    case apiVersionMismatch(String)
    case httpStatus(Int, String)
    case server(code: String, message: String)

    public var description: String {
        switch self {
        case .invalidBaseURL(let url):
            return "Invalid FXExport API base URL: \(url)"
        case .transport(let reason):
            return "FXExport API transport failed: \(reason)"
        case .invalidResponse(let reason):
            return "Invalid FXExport API response: \(reason)"
        case .apiVersionMismatch(let version):
            return "FXExport API version mismatch: got \(version), expected \(FXBacktestAPIV1.version)"
        case .httpStatus(let status, let body):
            return "FXExport API HTTP \(status): \(body)"
        case .server(let code, let message):
            return "FXExport API error \(code): \(message)"
        }
    }
}
