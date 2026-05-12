import Domain
import Foundation

public final class MT5BridgeClient: @unchecked Sendable {
    private let connection: MT5Connection
    private let codec: FramedProtocolCodec

    public init(connection: MT5Connection, codec: FramedProtocolCodec = FramedProtocolCodec()) {
        self.connection = connection
        self.codec = codec
    }

    public static func connect(
        host: String,
        port: UInt16,
        connectTimeoutSeconds: Double,
        requestTimeoutSeconds: Double
    ) throws -> MT5BridgeClient {
        let connection = try MT5Connection.connect(
            host: host,
            port: port,
            connectTimeoutSeconds: connectTimeoutSeconds,
            requestTimeoutSeconds: requestTimeoutSeconds
        )
        return MT5BridgeClient(connection: connection)
    }

    public static func listen(
        host: String,
        port: UInt16,
        connectTimeoutSeconds: Double,
        requestTimeoutSeconds: Double
    ) throws -> MT5BridgeClient {
        let connection = try MT5Connection.listenOnce(
            host: host,
            port: port,
            connectTimeoutSeconds: connectTimeoutSeconds,
            requestTimeoutSeconds: requestTimeoutSeconds
        )
        return MT5BridgeClient(connection: connection)
    }

    public func request<RequestPayload: Encodable, ResponsePayload: Decodable & Sendable>(
        command: MT5Command,
        payload: RequestPayload,
        responseType: ResponsePayload.Type
    ) throws -> ResponsePayload {
        let requestId = UUID().uuidString
        let frame = try codec.encode(
            command: command,
            requestId: requestId,
            timestampSentUtc: UtcSecond(rawValue: Int64(Date().timeIntervalSince1970)),
            payload: payload
        )
        try connection.sendFrame(frame)
        let responseBody = try connection.readFrameBody()
        let message = try codec.decode(responseBody, payloadType: responseType)
        guard message.requestId == requestId else {
            throw ProtocolError.invalidField("request_id")
        }
        guard message.command == command else {
            throw ProtocolError.invalidField("command")
        }
        return message.payload
    }

    public func hello() throws -> HelloResponseDTO {
        try request(command: .hello, payload: EmptyPayload(), responseType: HelloResponseDTO.self)
    }

    public func ping() throws -> EmptyPayload {
        try request(command: .ping, payload: EmptyPayload(), responseType: EmptyPayload.self)
    }

    public func terminalInfo() throws -> TerminalInfoDTO {
        try request(command: .getTerminalInfo, payload: EmptyPayload(), responseType: TerminalInfoDTO.self)
    }

    public func prepareSymbol(_ mt5Symbol: MT5Symbol) throws -> SymbolInfoDTO {
        try request(command: .prepareSymbol, payload: SymbolPayload(mt5Symbol: mt5Symbol.rawValue), responseType: SymbolInfoDTO.self)
    }

    public func symbolInfo(_ mt5Symbol: MT5Symbol) throws -> SymbolInfoDTO {
        try request(command: .getSymbolInfo, payload: SymbolPayload(mt5Symbol: mt5Symbol.rawValue), responseType: SymbolInfoDTO.self)
    }

    public func historyStatus(_ mt5Symbol: MT5Symbol) throws -> HistoryStatusDTO {
        try request(command: .getHistoryStatus, payload: SymbolPayload(mt5Symbol: mt5Symbol.rawValue), responseType: HistoryStatusDTO.self)
    }

    public func oldestM1BarTime(_ mt5Symbol: MT5Symbol) throws -> SingleTimeResponseDTO {
        try request(command: .getOldestM1BarTime, payload: SymbolPayload(mt5Symbol: mt5Symbol.rawValue), responseType: SingleTimeResponseDTO.self)
    }

    public func latestClosedM1Bar(_ mt5Symbol: MT5Symbol) throws -> SingleTimeResponseDTO {
        try request(command: .getLatestClosedM1Bar, payload: SymbolPayload(mt5Symbol: mt5Symbol.rawValue), responseType: SingleTimeResponseDTO.self)
    }

    public func ratesRange(mt5Symbol: MT5Symbol, from: MT5ServerSecond, toExclusive: MT5ServerSecond, maxBars: Int) throws -> RatesResponseDTO {
        try request(
            command: .getRatesRange,
            payload: RatesRangePayload(
                mt5Symbol: mt5Symbol.rawValue,
                fromMT5ServerTs: from.rawValue,
                toMT5ServerTsExclusive: toExclusive.rawValue,
                maxBars: maxBars
            ),
            responseType: RatesResponseDTO.self
        )
    }

    public func ratesFromPosition(mt5Symbol: MT5Symbol, startPosition: Int, count: Int) throws -> RatesResponseDTO {
        guard startPosition >= 1 else {
            throw ProtocolError.invalidField("start_pos")
        }
        guard count > 0 else {
            throw ProtocolError.invalidField("count")
        }
        return try request(
            command: .getRatesFromPosition,
            payload: RatesFromPositionPayload(
                mt5Symbol: mt5Symbol.rawValue,
                startPosition: startPosition,
                count: count
            ),
            responseType: RatesResponseDTO.self
        )
    }

    public func serverTimeSnapshot() throws -> ServerTimeSnapshotDTO {
        try request(command: .getServerTimeSnapshot, payload: EmptyPayload(), responseType: ServerTimeSnapshotDTO.self)
    }

    public func close() {
        connection.close()
    }
}
