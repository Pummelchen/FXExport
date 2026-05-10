import Domain
import Verification
import XCTest

final class VerificationTests: XCTestCase {
    func testRepairDecisionLogic() {
        let policy = RepairPolicy()
        XCTAssertEqual(
            policy.decide(
                verification: VerificationResult(isClean: true, mismatches: []),
                mt5Available: true,
                utcMappingAmbiguous: false
            ),
            .noRepairNeeded
        )
        XCTAssertEqual(
            policy.decide(
                verification: VerificationResult(isClean: false, mismatches: [.rowCount(mt5: 1, database: 0)]),
                mt5Available: false,
                utcMappingAmbiguous: false
            ),
            .refuse(reason: "MT5 source data is unavailable")
        )
    }
}
