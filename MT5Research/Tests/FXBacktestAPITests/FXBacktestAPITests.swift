import FXBacktestAPI
import FXBacktestAPIServer
import XCTest

final class FXBacktestAPITests: XCTestCase {
    func testHistoryRequestRequiresV1VersionAndMinuteAlignedRange() throws {
        XCTAssertThrowsError(try FXBacktestM1HistoryRequest(
            apiVersion: "old",
            brokerSourceId: "demo",
            logicalSymbol: "EURUSD",
            utcStartInclusive: 1_704_067_200,
            utcEndExclusive: 1_704_067_260
        ).validate()) { error in
            XCTAssertEqual(error as? FXBacktestAPIValidationError, .unsupportedVersion("old"))
        }

        XCTAssertThrowsError(try FXBacktestM1HistoryRequest(
            brokerSourceId: "demo",
            logicalSymbol: "EURUSD",
            utcStartInclusive: 1_704_067_201,
            utcEndExclusive: 1_704_067_260
        ).validate())
    }

    func testHTTPHandlerServesStatusAndHistoryWithV1Envelope() async throws {
        let handler = FXBacktestAPIHTTPHandler(historyProvider: MockHistoryProvider())

        let statusResponse = await handler.handle(method: "GET", path: FXBacktestAPIV1.statusPath, body: Data())
        XCTAssertEqual(statusResponse.statusCode, 200)
        let status = try JSONDecoder().decode(FXBacktestAPIStatusResponse.self, from: statusResponse.body)
        XCTAssertEqual(status.apiVersion, FXBacktestAPIV1.version)

        let request = FXBacktestM1HistoryRequest(
            brokerSourceId: "demo",
            logicalSymbol: "EURUSD",
            utcStartInclusive: 1_704_067_200,
            utcEndExclusive: 1_704_067_320,
            expectedMT5Symbol: "EURUSD",
            expectedDigits: 5,
            maximumRows: 2
        )
        let body = try JSONEncoder().encode(request)
        let historyResponse = await handler.handle(method: "POST", path: FXBacktestAPIV1.m1HistoryPath, body: body)
        XCTAssertEqual(historyResponse.statusCode, 200)
        let history = try JSONDecoder().decode(FXBacktestM1HistoryResponse.self, from: historyResponse.body)
        XCTAssertEqual(history.apiVersion, FXBacktestAPIV1.version)
        XCTAssertEqual(history.metadata.rowCount, 2)
        XCTAssertEqual(history.utcTimestamps, [1_704_067_200, 1_704_067_260])
    }

    func testHTTPHandlerRejectsUnsupportedAPIVersion() async throws {
        let handler = FXBacktestAPIHTTPHandler(historyProvider: MockHistoryProvider())
        let request = FXBacktestM1HistoryRequest(
            apiVersion: "v0",
            brokerSourceId: "demo",
            logicalSymbol: "EURUSD",
            utcStartInclusive: 1_704_067_200,
            utcEndExclusive: 1_704_067_260
        )
        let response = await handler.handle(
            method: "POST",
            path: FXBacktestAPIV1.m1HistoryPath,
            body: try JSONEncoder().encode(request)
        )

        XCTAssertEqual(response.statusCode, 400)
        let error = try JSONDecoder().decode(FXBacktestAPIErrorResponse.self, from: response.body)
        XCTAssertEqual(error.apiVersion, FXBacktestAPIV1.version)
        XCTAssertEqual(error.error.code, "invalid_request")
    }

    func testHistoryResponseValidationRejectsMetadataDrift() throws {
        let response = FXBacktestM1HistoryResponse(
            metadata: FXBacktestM1HistoryMetadata(
                brokerSourceId: "demo",
                logicalSymbol: "EURUSD",
                mt5Symbol: "EURUSD",
                digits: 5,
                requestedUtcStart: 1_704_067_200,
                requestedUtcEndExclusive: 1_704_067_260,
                firstUtc: 1_704_067_260,
                lastUtc: 1_704_067_260,
                rowCount: 1
            ),
            utcTimestamps: [1_704_067_200],
            open: [108_000],
            high: [108_020],
            low: [107_990],
            close: [108_010]
        )

        XCTAssertThrowsError(try response.validate())
    }

    func testHTTPHandlerMapsProviderInvalidRequestToV1Error() async throws {
        let handler = FXBacktestAPIHTTPHandler(historyProvider: InvalidRequestProvider())
        let request = FXBacktestM1HistoryRequest(
            brokerSourceId: "demo",
            logicalSymbol: "EURUSD",
            utcStartInclusive: 1_704_067_200,
            utcEndExclusive: 1_704_067_260
        )
        let response = await handler.handle(
            method: "POST",
            path: FXBacktestAPIV1.m1HistoryPath,
            body: try JSONEncoder().encode(request)
        )

        XCTAssertEqual(response.statusCode, 400)
        let error = try JSONDecoder().decode(FXBacktestAPIErrorResponse.self, from: response.body)
        XCTAssertEqual(error.apiVersion, FXBacktestAPIV1.version)
        XCTAssertEqual(error.error.code, "invalid_request")
    }
}

private struct MockHistoryProvider: FXBacktestHistoryProviding {
    func loadM1History(_ request: FXBacktestM1HistoryRequest) async throws -> FXBacktestM1HistoryResponse {
        FXBacktestM1HistoryResponse(
            metadata: FXBacktestM1HistoryMetadata(
                brokerSourceId: request.brokerSourceId,
                logicalSymbol: request.logicalSymbol,
                mt5Symbol: request.expectedMT5Symbol ?? request.logicalSymbol,
                digits: request.expectedDigits ?? 5,
                requestedUtcStart: request.utcStartInclusive,
                requestedUtcEndExclusive: request.utcEndExclusive,
                firstUtc: 1_704_067_200,
                lastUtc: 1_704_067_260,
                rowCount: 2
            ),
            utcTimestamps: [1_704_067_200, 1_704_067_260],
            open: [108_000, 108_010],
            high: [108_020, 108_030],
            low: [107_990, 108_000],
            close: [108_010, 108_020]
        )
    }
}

private struct InvalidRequestProvider: FXBacktestHistoryProviding {
    func loadM1History(_ request: FXBacktestM1HistoryRequest) async throws -> FXBacktestM1HistoryResponse {
        throw FXBacktestAPIServiceError.invalidRequest("Invalid logical symbol.")
    }
}
