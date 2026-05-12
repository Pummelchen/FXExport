import AppCore
import Darwin
import Foundation

public final class FXBacktestAPIServer: @unchecked Sendable {
    private let host: String
    private let port: UInt16
    private let handler: FXBacktestAPIHTTPHandler
    private let logger: Logger
    private let clientSocketTimeoutSeconds: Double

    public init(
        host: String,
        port: UInt16,
        handler: FXBacktestAPIHTTPHandler,
        logger: Logger,
        clientSocketTimeoutSeconds: Double = 30
    ) {
        self.host = host
        self.port = port
        self.handler = handler
        self.logger = logger
        self.clientSocketTimeoutSeconds = clientSocketTimeoutSeconds
    }

    public func run() async throws {
        let serverFD = try makeServerSocket()
        defer { Darwin.close(serverFD) }
        logger.ok("FXBacktest API v1 listening at http://\(host):\(port)")
        logger.info("FXBacktest history endpoint: POST /v1/history/m1")
        logger.info("FXBacktest execution endpoint: POST /v1/execution/spec")

        while !Task.isCancelled {
            var pollFD = pollfd(fd: serverFD, events: Int16(POLLIN), revents: 0)
            let ready = poll(&pollFD, 1, 1_000)
            if ready < 0 {
                if errno == EINTR { continue }
                throw FXBacktestAPIServerError.acceptFailed(errno: errno)
            }
            guard ready > 0, pollFD.revents & Int16(POLLIN) != 0 else {
                continue
            }
            let clientFD = Darwin.accept(serverFD, nil, nil)
            if clientFD < 0 {
                if errno == EINTR { continue }
                throw FXBacktestAPIServerError.acceptFailed(errno: errno)
            }
            do {
                try configureClientSocket(clientFD)
            } catch {
                Darwin.close(clientFD)
                logger.warn("FXBacktest API rejected client socket: \(error)")
                continue
            }
            Task.detached(priority: .userInitiated) {
                await self.handle(clientFD: clientFD)
            }
        }
        logger.info("FXBacktest API v1 stopped")
    }

    private func handle(clientFD: Int32) async {
        defer { Darwin.close(clientFD) }
        do {
            let request = try readRequest(clientFD: clientFD)
            let response = await handler.handle(method: request.method, path: request.path, body: request.body)
            try writeResponse(response, clientFD: clientFD)
        } catch {
            let body = #"{"api_version":"fxexport.fxbacktest.history.v1","error":{"code":"bad_http_request","message":"\#(Self.jsonEscaped(String(describing: error)))"}}"#
            let response = FXBacktestHTTPResponse(statusCode: 400, body: Data(body.utf8))
            do {
                try writeResponse(response, clientFD: clientFD)
            } catch {
                logger.warn("FXBacktest API could not write error response: \(error)")
            }
        }
    }

    private func makeServerSocket() throws -> Int32 {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else {
            throw FXBacktestAPIServerError.socketCreateFailed(errno: errno)
        }
        var reuse: Int32 = 1
        guard setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, socklen_t(MemoryLayout<Int32>.size)) == 0 else {
            let saved = errno
            Darwin.close(fd)
            throw FXBacktestAPIServerError.bindFailed(errno: saved)
        }
        guard let address = IPv4Address(host: host, port: port) else {
            Darwin.close(fd)
            throw FXBacktestAPIServerError.invalidHost(host)
        }
        var socketAddress = address.sockaddr
        let bindResult = withUnsafePointer(to: &socketAddress) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindResult == 0 else {
            let saved = errno
            Darwin.close(fd)
            throw FXBacktestAPIServerError.bindFailed(errno: saved)
        }
        guard Darwin.listen(fd, 64) == 0 else {
            let saved = errno
            Darwin.close(fd)
            throw FXBacktestAPIServerError.listenFailed(errno: saved)
        }
        return fd
    }

    private func configureClientSocket(_ fd: Int32) throws {
        var noSigPipe: Int32 = 1
        _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &noSigPipe, socklen_t(MemoryLayout<Int32>.size))

        guard clientSocketTimeoutSeconds.isFinite,
              clientSocketTimeoutSeconds > 0,
              clientSocketTimeoutSeconds <= 3_600 else {
            throw FXBacktestAPIServerError.invalidTimeout(clientSocketTimeoutSeconds)
        }
        let seconds = Int(clientSocketTimeoutSeconds)
        let microseconds = Int((clientSocketTimeoutSeconds - Double(seconds)) * 1_000_000)
        var timeout = timeval(tv_sec: seconds, tv_usec: Int32(microseconds))
        guard setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size)) == 0 else {
            throw FXBacktestAPIServerError.socketOptionFailed(option: "SO_RCVTIMEO", errno: errno)
        }
        guard setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size)) == 0 else {
            throw FXBacktestAPIServerError.socketOptionFailed(option: "SO_SNDTIMEO", errno: errno)
        }
    }

    private func readRequest(clientFD: Int32) throws -> ParsedHTTPRequest {
        var data = Data()
        let maxRequestBytes = 1_048_576
        var expectedLength: Int?

        while data.count < maxRequestBytes {
            var buffer = [UInt8](repeating: 0, count: 8192)
            let bytesRead = Darwin.recv(clientFD, &buffer, buffer.count, 0)
            if bytesRead < 0 {
                if errno == EINTR { continue }
                if errno == EAGAIN || errno == EWOULDBLOCK {
                    throw FXBacktestAPIServerError.requestTimedOut
                }
                throw FXBacktestAPIServerError.readFailed(errno: errno)
            }
            guard bytesRead > 0 else {
                throw FXBacktestAPIServerError.connectionClosed
            }
            data.append(contentsOf: buffer.prefix(bytesRead))

            if expectedLength == nil, let parsedLength = try parseExpectedLength(data) {
                guard parsedLength <= maxRequestBytes else {
                    throw FXBacktestAPIServerError.requestTooLarge
                }
                expectedLength = parsedLength
            }
            if let expectedLength, data.count >= expectedLength {
                return try parseRequest(data)
            }
        }
        throw FXBacktestAPIServerError.requestTooLarge
    }

    private func parseExpectedLength(_ data: Data) throws -> Int? {
        guard let headerRange = data.range(of: Data("\r\n\r\n".utf8)) else { return nil }
        let headerEnd = headerRange.upperBound
        let headerData = data[..<headerRange.lowerBound]
        guard let headerText = String(data: headerData, encoding: .utf8) else {
            throw FXBacktestAPIServerError.invalidRequest("Headers are not UTF-8.")
        }
        let headers = parseHeaders(headerText)
        let contentLength = Int(headers["content-length"] ?? "0") ?? -1
        guard contentLength >= 0 else {
            throw FXBacktestAPIServerError.invalidRequest("Invalid Content-Length.")
        }
        return headerEnd + contentLength
    }

    private func parseRequest(_ data: Data) throws -> ParsedHTTPRequest {
        guard let headerRange = data.range(of: Data("\r\n\r\n".utf8)) else {
            throw FXBacktestAPIServerError.invalidRequest("Missing header terminator.")
        }
        guard let headerText = String(data: data[..<headerRange.lowerBound], encoding: .utf8) else {
            throw FXBacktestAPIServerError.invalidRequest("Headers are not UTF-8.")
        }
        let lines = headerText.components(separatedBy: "\r\n")
        guard let requestLine = lines.first else {
            throw FXBacktestAPIServerError.invalidRequest("Missing request line.")
        }
        let parts = requestLine.split(separator: " ", omittingEmptySubsequences: true)
        guard parts.count >= 2 else {
            throw FXBacktestAPIServerError.invalidRequest("Malformed request line.")
        }
        let headers = parseHeaders(headerText)
        let bodyStart = headerRange.upperBound
        let contentLength = Int(headers["content-length"] ?? "0") ?? 0
        let bodyEnd = bodyStart + contentLength
        guard bodyEnd <= data.count else {
            throw FXBacktestAPIServerError.invalidRequest("Body is shorter than Content-Length.")
        }
        return ParsedHTTPRequest(
            method: String(parts[0]),
            path: String(parts[1].split(separator: "?", maxSplits: 1, omittingEmptySubsequences: false)[0]),
            body: data[bodyStart..<bodyEnd]
        )
    }

    private func parseHeaders(_ headerText: String) -> [String: String] {
        var headers: [String: String] = [:]
        for line in headerText.components(separatedBy: "\r\n").dropFirst() {
            guard let colon = line.firstIndex(of: ":") else { continue }
            let key = line[..<colon].trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            let value = line[line.index(after: colon)...].trimmingCharacters(in: .whitespacesAndNewlines)
            headers[key] = value
        }
        return headers
    }

    private func writeResponse(_ response: FXBacktestHTTPResponse, clientFD: Int32) throws {
        let head = [
            "HTTP/1.1 \(response.statusCode) \(Self.reasonPhrase(response.statusCode))",
            "Content-Type: \(response.contentType)",
            "Content-Length: \(response.body.count)",
            "Connection: close",
            "",
            ""
        ].joined(separator: "\r\n")
        var payload = Data(head.utf8)
        payload.append(response.body)
        try payload.withUnsafeBytes { bytes in
            guard let base = bytes.baseAddress else { return }
            var written = 0
            while written < bytes.count {
                let result = Darwin.send(clientFD, base.advanced(by: written), bytes.count - written, 0)
                if result < 0 {
                    if errno == EINTR { continue }
                    if errno == EAGAIN || errno == EWOULDBLOCK {
                        throw FXBacktestAPIServerError.requestTimedOut
                    }
                    throw FXBacktestAPIServerError.writeFailed(errno: errno)
                }
                guard result > 0 else {
                    throw FXBacktestAPIServerError.connectionClosed
                }
                written += result
            }
        }
    }

    private static func reasonPhrase(_ statusCode: Int) -> String {
        switch statusCode {
        case 200: return "OK"
        case 400: return "Bad Request"
        case 404: return "Not Found"
        case 409: return "Conflict"
        case 500: return "Internal Server Error"
        case 502: return "Bad Gateway"
        default: return "HTTP"
        }
    }

    private static func jsonEscaped(_ value: String) -> String {
        do {
            let data = try JSONEncoder().encode(value)
            guard let encoded = String(data: data, encoding: .utf8), encoded.count >= 2 else {
                return fallbackJSONStringContent(value)
            }
            return String(encoded.dropFirst().dropLast())
        } catch {
            return fallbackJSONStringContent(value)
        }
    }

    private static func fallbackJSONStringContent(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
            .replacingOccurrences(of: "\t", with: "\\t")
    }
}

private struct ParsedHTTPRequest {
    let method: String
    let path: String
    let body: Data
}

private struct IPv4Address {
    let sockaddr: sockaddr_in

    init?(host: String, port: UInt16) {
        var address = in_addr()
        guard inet_pton(AF_INET, host, &address) == 1 else { return nil }
        self.sockaddr = sockaddr_in(
            sin_len: UInt8(MemoryLayout<sockaddr_in>.size),
            sin_family: sa_family_t(AF_INET),
            sin_port: port.bigEndian,
            sin_addr: address,
            sin_zero: (0, 0, 0, 0, 0, 0, 0, 0)
        )
    }
}

public enum FXBacktestAPIServerError: Error, CustomStringConvertible, Sendable {
    case invalidHost(String)
    case socketCreateFailed(errno: Int32)
    case bindFailed(errno: Int32)
    case listenFailed(errno: Int32)
    case acceptFailed(errno: Int32)
    case socketOptionFailed(option: String, errno: Int32)
    case invalidTimeout(Double)
    case readFailed(errno: Int32)
    case writeFailed(errno: Int32)
    case connectionClosed
    case requestTimedOut
    case requestTooLarge
    case invalidRequest(String)

    public var description: String {
        switch self {
        case .invalidHost(let host):
            return "FXBacktest API host must be an IPv4 address; got \(host)."
        case .socketCreateFailed(let errno):
            return "FXBacktest API socket creation failed with errno \(errno)."
        case .bindFailed(let errno):
            return "FXBacktest API bind failed with errno \(errno)."
        case .listenFailed(let errno):
            return "FXBacktest API listen failed with errno \(errno)."
        case .acceptFailed(let errno):
            return "FXBacktest API accept failed with errno \(errno)."
        case .socketOptionFailed(let option, let errno):
            return "FXBacktest API socket option \(option) failed with errno \(errno)."
        case .invalidTimeout(let timeout):
            return "FXBacktest API client socket timeout \(timeout) is invalid."
        case .readFailed(let errno):
            return "FXBacktest API read failed with errno \(errno)."
        case .writeFailed(let errno):
            return "FXBacktest API write failed with errno \(errno)."
        case .connectionClosed:
            return "FXBacktest API client closed the connection before sending a full request."
        case .requestTimedOut:
            return "FXBacktest API client socket timed out."
        case .requestTooLarge:
            return "FXBacktest API request exceeded 1 MiB."
        case .invalidRequest(let reason):
            return "Invalid FXBacktest HTTP request: \(reason)"
        }
    }
}
