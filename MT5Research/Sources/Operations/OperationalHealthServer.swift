import AppCore
import Darwin
import Foundation

public final class OperationalHealthServer: @unchecked Sendable {
    private let host: String
    private let port: UInt16
    private let service: OperationalHealthService
    private let logger: Logger

    public init(host: String, port: UInt16, service: OperationalHealthService, logger: Logger) {
        self.host = host
        self.port = port
        self.service = service
        self.logger = logger
    }

    public func run() async throws {
        let fd = try makeServerSocket()
        defer { Darwin.close(fd) }
        logger.ok("FXExport health API listening at http://\(host):\(port)/v1/health")
        while !Task.isCancelled {
            var pollFD = pollfd(fd: fd, events: Int16(POLLIN), revents: 0)
            let ready = poll(&pollFD, 1, 1_000)
            if ready < 0 {
                if errno == EINTR { continue }
                throw HealthServerError.acceptFailed(errno)
            }
            guard ready > 0 else { continue }
            let clientFD = Darwin.accept(fd, nil, nil)
            if clientFD < 0 {
                if errno == EINTR { continue }
                throw HealthServerError.acceptFailed(errno)
            }
            Task.detached(priority: .utility) {
                await self.handle(clientFD: clientFD)
            }
        }
    }

    private func handle(clientFD: Int32) async {
        defer { Darwin.close(clientFD) }
        do {
            let request = try readRequest(clientFD)
            guard request.method == "GET", request.path == "/v1/health" else {
                try write(status: 404, body: #"{"error":"not_found"}"#, clientFD: clientFD)
                return
            }
            let snapshot = await service.snapshot()
            let data = try JSONEncoder().encode(snapshot)
            try write(status: 200, body: String(data: data, encoding: .utf8) ?? "{}", clientFD: clientFD)
        } catch {
            do {
                try write(status: 400, body: #"{"error":"bad_request"}"#, clientFD: clientFD)
            } catch {
                logger.warn("Health API could not write error response: \(error)")
            }
        }
    }

    private func makeServerSocket() throws -> Int32 {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw HealthServerError.socketCreateFailed(errno) }
        var reuse: Int32 = 1
        _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, socklen_t(MemoryLayout<Int32>.size))
        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = port.bigEndian
        guard inet_pton(AF_INET, host, &address.sin_addr) == 1 else {
            Darwin.close(fd)
            throw HealthServerError.invalidHost(host)
        }
        let bindResult = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindResult == 0 else {
            let saved = errno
            Darwin.close(fd)
            throw HealthServerError.bindFailed(saved)
        }
        guard Darwin.listen(fd, 32) == 0 else {
            let saved = errno
            Darwin.close(fd)
            throw HealthServerError.listenFailed(saved)
        }
        return fd
    }

    private func readRequest(_ clientFD: Int32) throws -> HealthHTTPRequest {
        var buffer = [UInt8](repeating: 0, count: 4096)
        let read = Darwin.recv(clientFD, &buffer, buffer.count, 0)
        guard read > 0 else { throw HealthServerError.readFailed(errno) }
        guard let text = String(data: Data(buffer.prefix(read)), encoding: .utf8),
              let line = text.components(separatedBy: "\r\n").first else {
            throw HealthServerError.invalidRequest
        }
        let parts = line.split(separator: " ")
        guard parts.count >= 2 else { throw HealthServerError.invalidRequest }
        return HealthHTTPRequest(
            method: String(parts[0]).uppercased(),
            path: String(parts[1].split(separator: "?", maxSplits: 1)[0])
        )
    }

    private func write(status: Int, body: String, clientFD: Int32) throws {
        let reason = status == 200 ? "OK" : "Error"
        let response = "HTTP/1.1 \(status) \(reason)\r\n"
            + "Content-Type: application/json; charset=utf-8\r\n"
            + "Content-Length: \(body.utf8.count)\r\n"
            + "Connection: close\r\n"
            + "\r\n"
            + body
        let data = Array(response.utf8)
        var written = 0
        try data.withUnsafeBytes { raw in
            guard let base = raw.baseAddress else { return }
            while written < data.count {
                let count = Darwin.send(clientFD, base.advanced(by: written), data.count - written, 0)
                guard count > 0 else { throw HealthServerError.writeFailed(errno) }
                written += count
            }
        }
    }
}

private struct HealthHTTPRequest {
    let method: String
    let path: String
}

public enum HealthServerError: Error, CustomStringConvertible, Sendable {
    case invalidHost(String)
    case socketCreateFailed(Int32)
    case bindFailed(Int32)
    case listenFailed(Int32)
    case acceptFailed(Int32)
    case readFailed(Int32)
    case writeFailed(Int32)
    case invalidRequest

    public var description: String {
        switch self {
        case .invalidHost(let host): return "Invalid health API host \(host)."
        case .socketCreateFailed(let errno): return "Health API socket creation failed errno=\(errno)."
        case .bindFailed(let errno): return "Health API bind failed errno=\(errno)."
        case .listenFailed(let errno): return "Health API listen failed errno=\(errno)."
        case .acceptFailed(let errno): return "Health API accept failed errno=\(errno)."
        case .readFailed(let errno): return "Health API read failed errno=\(errno)."
        case .writeFailed(let errno): return "Health API write failed errno=\(errno)."
        case .invalidRequest: return "Invalid health API request."
        }
    }
}
