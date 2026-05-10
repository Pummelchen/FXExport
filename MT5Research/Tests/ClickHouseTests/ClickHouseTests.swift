import ClickHouse
import XCTest

final class ClickHouseTests: XCTestCase {
    func testClickHouseExceptionParsing() {
        let parser = ClickHouseErrorParser()
        XCTAssertNotNil(parser.parseException(in: "Code: 62. DB::Exception: Syntax error"))
        XCTAssertNil(parser.parseException(in: "1\n"))
    }
}
