# Assistant Guardrails & Coding Conventions

To ensure consistency and production-rigor throughout this project, adhere to the following standards when generating or reviewing code.

## 1. C++ Style & Conventions
- **Standard**: C++20.
- **Style Guide**: Follow the Google C++ Style Guide.
- **Formatting**: Provide `.clang-format` configurations; prefer 2 spaces for indentation.
- **Smart Pointers**: Never use raw `new`/`delete`. Use `std::unique_ptr` and `std::shared_ptr` exclusively for resource management.
- **Concurrency**: Use standard C++ concurrency primitives (`std::jthread`, `std::mutex`, `std::condition_variable`). Avoid manual thread lifecycle management where thread pools can be utilized.

## 2. Error Handling (Exceptions vs. Error Codes)
- **Decision**: Use **Exceptions** for unrecoverable errors (e.g., missing configuration, failure to bind to a port) and **`std::expected` (C++23) / `tl::expected` (C++20)** or return codes for expected, recoverable errors in hot paths (e.g., Redis cache miss, HTTP 404).
- **Justification**: The hot path of the pipeline (processing 1000 msgs/sec) cannot afford the overhead of stack unwinding for routine control flow like network timeouts or malformed JSON. Reserve exceptions for truly exceptional crashes.

## 3. Build System & Dependencies
- **CMake**: All targets must be defined using Modern CMake conventions (Target-based approach: `target_link_libraries`, `target_include_directories`). No global `include_directories`.
- **Package Management**: Use `vcpkg` or `FetchContent` to manage dependencies natively (`librdkafka`, `hiredis`, `cpp-httplib`, `nlohmann_json`, `spdlog`).

## 4. Testing Expectations
- **Framework**: `GoogleTest` (gtest).
- **Scope**:
  - Unit tests for all parsers, JSON transformations, and business logic.
  - Mocking: Use `gmock` to mock external dependencies (Kafka, Redis, ES) when testing consumer behavior (e.g. testing the backoff and DLQ logic).
  - No dummy/placeholder tests; edge cases (like malformed Debezium envelopes) must have explicit test coverage.

## 5. Logging Strategy
- **Library**: `spdlog`.
- **Format**: Strictly JSON. Logstash expects structured data.
- **Context**: Every log message related to an order must include the `order_id` to allow distributed tracing across the API, Kafka, and the Consumer logs in Kibana.
- **Levels**:
  - `DEBUG`: Verbose I/O details (disabled in prod).
  - `INFO`: Lifecycle milestones (e.g. "Consumer batch committed", "Server started").
  - `WARN`: Recoverable errors (e.g. "ES unavailable, backing off").
  - `ERROR`: DLQ routes, fatal crashes.
