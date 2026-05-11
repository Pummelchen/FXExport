import Darwin
import Foundation

public enum MT5BridgeError: Error, CustomStringConvertible, Sendable {
    case invalidHost(String)
    case socketCreateFailed(errno: Int32)
    case connectFailed(host: String, port: UInt16, errno: Int32)
    case bindFailed(host: String, port: UInt16, errno: Int32)
    case listenFailed(errno: Int32)
    case acceptTimedOut(port: UInt16)
    case acceptFailed(errno: Int32)
    case readFailed(errno: Int32)
    case writeFailed(errno: Int32)
    case connectionClosed
    case invalidFrameLength(Int)
    case invalidTimeout(Double)

    public var description: String {
        switch self {
        case .invalidHost(let host):
            return "Invalid IPv4 host '\(host)'. Use 127.0.0.1 for the MT5 bridge."
        case .socketCreateFailed(let errno):
            return "Could not create TCP socket: errno \(errno)."
        case .connectFailed(let host, let port, let errno):
            return "Could not connect to MT5 bridge at \(host):\(port): errno \(errno)."
        case .bindFailed(let host, let port, let errno):
            return "Could not bind MT5 bridge listener at \(host):\(port): errno \(errno)."
        case .listenFailed(let errno):
            return "Could not listen for MT5 bridge connection: errno \(errno)."
        case .acceptTimedOut(let port):
            return "Timed out waiting for MT5 EA to connect on port \(port)."
        case .acceptFailed(let errno):
            return "Could not accept MT5 bridge connection: errno \(errno)."
        case .readFailed(let errno):
            return "Could not read from MT5 bridge socket: errno \(errno)."
        case .writeFailed(let errno):
            return "Could not write to MT5 bridge socket: errno \(errno)."
        case .connectionClosed:
            return "MT5 bridge closed the socket."
        case .invalidFrameLength(let length):
            return "MT5 bridge sent invalid frame length \(length)."
        case .invalidTimeout(let timeout):
            return "Invalid MT5 bridge socket timeout \(timeout)."
        }
    }
}

public final class MT5Connection: @unchecked Sendable {
    private let fd: Int32
    private let maxFrameBytes: Int
    private var isClosed = false

    private init(fd: Int32, maxFrameBytes: Int) {
        self.fd = fd
        self.maxFrameBytes = maxFrameBytes
    }

    deinit {
        close()
    }

    public static func connect(host: String, port: UInt16, timeoutSeconds: Double = 10, maxFrameBytes: Int = 16 * 1024 * 1024) throws -> MT5Connection {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw MT5BridgeError.socketCreateFailed(errno: errno) }
        do {
            try configureTimeouts(fd: fd, timeoutSeconds: timeoutSeconds)
        } catch {
            Darwin.close(fd)
            throw error
        }

        var address = try sockaddrIn(host: host, port: port)
        let result = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPointer in
                Darwin.connect(fd, sockaddrPointer, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard result == 0 else {
            let savedErrno = errno
            Darwin.close(fd)
            throw MT5BridgeError.connectFailed(host: host, port: port, errno: savedErrno)
        }
        return MT5Connection(fd: fd, maxFrameBytes: maxFrameBytes)
    }

    public static func listenOnce(host: String, port: UInt16, timeoutSeconds: Double = 30, maxFrameBytes: Int = 16 * 1024 * 1024) throws -> MT5Connection {
        let serverFD = socket(AF_INET, SOCK_STREAM, 0)
        guard serverFD >= 0 else { throw MT5BridgeError.socketCreateFailed(errno: errno) }
        var reuse: Int32 = 1
        setsockopt(serverFD, SOL_SOCKET, SO_REUSEADDR, &reuse, socklen_t(MemoryLayout<Int32>.size))

        var address = try sockaddrIn(host: host, port: port)
        let bindResult = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPointer in
                Darwin.bind(serverFD, sockaddrPointer, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindResult == 0 else {
            let savedErrno = errno
            Darwin.close(serverFD)
            throw MT5BridgeError.bindFailed(host: host, port: port, errno: savedErrno)
        }

        guard Darwin.listen(serverFD, 1) == 0 else {
            let savedErrno = errno
            Darwin.close(serverFD)
            throw MT5BridgeError.listenFailed(errno: savedErrno)
        }

        let isReadable: Bool
        do {
            isReadable = try waitForReadable(fd: serverFD, timeoutSeconds: timeoutSeconds)
        } catch {
            Darwin.close(serverFD)
            throw error
        }

        guard isReadable else {
            Darwin.close(serverFD)
            throw MT5BridgeError.acceptTimedOut(port: port)
        }

        var clientAddress = sockaddr()
        var clientLength = socklen_t(MemoryLayout<sockaddr>.size)
        let clientFD = Darwin.accept(serverFD, &clientAddress, &clientLength)
        Darwin.close(serverFD)
        guard clientFD >= 0 else { throw MT5BridgeError.acceptFailed(errno: errno) }
        do {
            try configureTimeouts(fd: clientFD, timeoutSeconds: timeoutSeconds)
        } catch {
            Darwin.close(clientFD)
            throw error
        }
        return MT5Connection(fd: clientFD, maxFrameBytes: maxFrameBytes)
    }

    public func close() {
        guard !isClosed else { return }
        isClosed = true
        Darwin.close(fd)
    }

    public func sendFrame(_ frame: Data) throws {
        var sent = 0
        try frame.withUnsafeBytes { rawBuffer in
            guard let baseAddress = rawBuffer.baseAddress else { return }
            while sent < frame.count {
                let written = Darwin.send(fd, baseAddress.advanced(by: sent), frame.count - sent, 0)
                guard written > 0 else {
                    if written == 0 { throw MT5BridgeError.connectionClosed }
                    throw MT5BridgeError.writeFailed(errno: errno)
                }
                sent += written
            }
        }
    }

    public func readFrameBody() throws -> Data {
        let prefix = try readExact(byteCount: 4)
        let length = try FramedProtocolCodec.bodyLength(from: prefix)
        guard length > 0, length <= maxFrameBytes else {
            throw MT5BridgeError.invalidFrameLength(length)
        }
        return try readExact(byteCount: length)
    }

    private func readExact(byteCount: Int) throws -> Data {
        var data = Data(count: byteCount)
        var received = 0
        try data.withUnsafeMutableBytes { rawBuffer in
            guard let baseAddress = rawBuffer.baseAddress else { return }
            while received < byteCount {
                let count = Darwin.recv(fd, baseAddress.advanced(by: received), byteCount - received, 0)
                guard count > 0 else {
                    if count == 0 { throw MT5BridgeError.connectionClosed }
                    throw MT5BridgeError.readFailed(errno: errno)
                }
                received += count
            }
        }
        return data
    }

    private static func configureTimeouts(fd: Int32, timeoutSeconds: Double) throws {
        guard timeoutSeconds.isFinite, timeoutSeconds > 0, timeoutSeconds <= 3600 else {
            throw MT5BridgeError.invalidTimeout(timeoutSeconds)
        }
        let seconds = Int(timeoutSeconds)
        let microseconds = Int((timeoutSeconds - Double(seconds)) * 1_000_000)
        var timeout = timeval(tv_sec: seconds, tv_usec: Int32(microseconds))
        guard setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size)) == 0 else {
            throw MT5BridgeError.readFailed(errno: errno)
        }
        guard setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size)) == 0 else {
            throw MT5BridgeError.writeFailed(errno: errno)
        }
    }

    private static func waitForReadable(fd: Int32, timeoutSeconds: Double) throws -> Bool {
        guard timeoutSeconds.isFinite, timeoutSeconds > 0, timeoutSeconds <= 3600 else {
            throw MT5BridgeError.invalidTimeout(timeoutSeconds)
        }
        var pollDescriptor = pollfd(fd: fd, events: Int16(POLLIN), revents: 0)
        let timeoutMilliseconds = Int32(max(0, timeoutSeconds * 1_000))
        let result = Darwin.poll(&pollDescriptor, 1, timeoutMilliseconds)
        if result < 0 { throw MT5BridgeError.acceptFailed(errno: errno) }
        return result > 0
    }

    private static func sockaddrIn(host: String, port: UInt16) throws -> sockaddr_in {
        let normalizedHost = host == "localhost" ? "127.0.0.1" : host
        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = port.bigEndian
        let conversion = normalizedHost.withCString { cString in
            inet_pton(AF_INET, cString, &address.sin_addr)
        }
        guard conversion == 1 else { throw MT5BridgeError.invalidHost(host) }
        return address
    }
}
