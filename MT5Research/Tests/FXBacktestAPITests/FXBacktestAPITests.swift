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

        XCTAssertThrowsError(try FXBacktestM1HistoryRequest(
            brokerSourceId: "demo",
            logicalSymbol: "EURUSD",
            utcStartInclusive: 1_704_067_200,
            utcEndExclusive: 1_704_067_260,
            maximumRows: FXBacktestAPIV1.maximumRowsLimit + 1
        ).validate()) { error in
            guard case .invalidField(let message) = error as? FXBacktestAPIValidationError else {
                XCTFail("Expected invalidField, got \(error)")
                return
            }
            XCTAssertTrue(message.contains("maximum_rows"))
        }
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

    func testHTTPHandlerServesExecutionSpecWithV1Envelope() async throws {
        let handler = FXBacktestAPIHTTPHandler(
            historyProvider: MockHistoryProvider(),
            executionProvider: MockExecutionProvider()
        )
        let request = FXBacktestExecutionSpecRequest(
            brokerSourceId: "demo",
            symbols: [
                FXBacktestExecutionSymbolRequest(logicalSymbol: "EURUSD", expectedMT5Symbol: "EURUSD", expectedDigits: 5)
            ]
        )

        let response = await handler.handle(
            method: "POST",
            path: FXBacktestAPIV1.executionSpecPath,
            body: try JSONEncoder().encode(request)
        )

        XCTAssertEqual(response.statusCode, 200)
        let spec = try JSONDecoder().decode(FXBacktestExecutionSpecResponse.self, from: response.body)
        XCTAssertEqual(spec.apiVersion, FXBacktestAPIV1.version)
        XCTAssertEqual(spec.accountMode, "hedging")
        XCTAssertEqual(spec.symbols.first?.spreadPoints, 12)
        XCTAssertEqual(spec.symbols.first?.commissionSource, "not_exposed_by_mt5_symbol_info")
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

private struct MockExecutionProvider: FXBacktestExecutionProviding {
    func loadExecutionSpec(_ request: FXBacktestExecutionSpecRequest) async throws -> FXBacktestExecutionSpecResponse {
        FXBacktestExecutionSpecResponse(
            brokerSourceId: request.brokerSourceId,
            capturedAtUtc: 1_704_067_200,
            accountCurrency: "USD",
            accountLeverage: 100,
            accountMode: "hedging",
            mt5AccountMarginMode: 2,
            symbols: [
                FXBacktestExecutionSymbolSpec(
                    logicalSymbol: request.symbols[0].logicalSymbol,
                    mt5Symbol: request.symbols[0].expectedMT5Symbol ?? request.symbols[0].logicalSymbol,
                    selected: true,
                    digits: request.symbols[0].expectedDigits ?? 5,
                    bid: 1.08000,
                    ask: 1.08012,
                    point: 0.00001,
                    spreadPoints: 12,
                    spreadFloat: true,
                    contractSize: 100_000,
                    volumeMin: 0.01,
                    volumeStep: 0.01,
                    volumeMax: 100,
                    swapLongPerLot: -6.2,
                    swapShortPerLot: 1.4,
                    swapMode: 1,
                    marginInitialPerLot: 1_080,
                    marginMaintenancePerLot: nil,
                    marginBuyPerLot: 1_080,
                    marginSellPerLot: 1_079,
                    marginCalcLots: 1,
                    tradeCalcMode: 0,
                    tradeMode: 4,
                    tickSize: 0.00001,
                    tickValue: 1,
                    tickValueProfit: 1,
                    tickValueLoss: 1,
                    commissionPerLotPerSide: nil,
                    commissionSource: "not_exposed_by_mt5_symbol_info",
                    slippagePoints: 0,
                    slippageSource: "deterministic_zero_default"
                )
            ]
        )
    }
}
