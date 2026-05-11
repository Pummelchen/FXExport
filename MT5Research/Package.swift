// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "MT5Research",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .executable(name: "mt5research", targets: ["MT5ResearchCLI"])
    ],
    targets: [
        .target(name: "Domain"),
        .target(name: "AppCore", dependencies: ["Domain"]),
        .target(name: "Config", dependencies: ["Domain", "AppCore"]),
        .target(name: "TimeMapping", dependencies: ["Domain"]),
        .target(name: "Validation", dependencies: ["Domain", "TimeMapping"]),
        .target(name: "MT5Bridge", dependencies: ["Domain", "AppCore"]),
        .target(name: "ClickHouse", dependencies: ["Domain", "AppCore", "Config"]),
        .target(name: "Ingestion", dependencies: ["Domain", "AppCore", "Config", "MT5Bridge", "ClickHouse", "TimeMapping", "Validation"]),
        .target(name: "Verification", dependencies: ["Domain", "AppCore", "Config", "MT5Bridge", "ClickHouse", "TimeMapping", "Validation", "Ingestion"]),
        .target(name: "BacktestCore", dependencies: ["Domain", "ClickHouse"]),
        .target(name: "MetalAccel", dependencies: ["Domain", "BacktestCore"]),
        .target(name: "Operations", dependencies: ["Domain", "AppCore", "Config", "MT5Bridge", "ClickHouse", "TimeMapping", "Validation", "Ingestion", "Verification"]),
        .executableTarget(
            name: "MT5ResearchCLI",
            dependencies: ["AppCore", "Config", "MT5Bridge", "ClickHouse", "Ingestion", "Verification", "BacktestCore", "MetalAccel", "TimeMapping", "Operations"]
        ),
        .testTarget(name: "DomainTests", dependencies: ["Domain"]),
        .testTarget(name: "ValidationTests", dependencies: ["Domain", "Validation", "TimeMapping", "Config"]),
        .testTarget(name: "TimeMappingTests", dependencies: ["Domain", "TimeMapping"]),
        .testTarget(name: "ProtocolTests", dependencies: ["MT5Bridge", "Domain"]),
        .testTarget(name: "ClickHouseTests", dependencies: ["ClickHouse", "Domain"]),
        .testTarget(name: "IngestionTests", dependencies: ["Domain", "Ingestion", "ClickHouse", "MT5Bridge", "TimeMapping"]),
        .testTarget(name: "VerificationTests", dependencies: ["Domain", "Verification", "TimeMapping"]),
        .testTarget(name: "OperationsTests", dependencies: ["Domain", "Config", "ClickHouse", "Operations"]),
        .testTarget(name: "BacktestTests", dependencies: ["Domain", "BacktestCore"])
    ],
    swiftLanguageModes: [.v6]
)
