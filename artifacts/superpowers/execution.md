# Execution Log: PocketQueue

## Step 1: Project Skeleton & Go Module Setup
- **Files changed**:
  - `go.mod`
  - `.gitignore`
  - `internal/platform/logger/logger.go`
  - `internal/platform/logger/logger_test.go`
- **What changed**:
  - Initialized Go module `github.com/pandusatrianura/pocketqueue` with Go 1.26.
  - Installed Charm TUI dependencies (`bubbletea`, `lipgloss`, `bubbles`).
  - Implemented structured `log/slog` logging platform with correlation ID propagation and operation duration tracking.
- **Verification command(s)**:
  - `go test -v ./internal/platform/logger/...`
  - `go build ./...`
- **Result**: PASS

## Step 2: Storage Engine Domain & In-Memory Ring Buffer (TDD)
- **Files changed**:
  - `internal/features/storage/storage.go`
  - `internal/features/storage/memory.go`
  - `internal/features/storage/memory_test.go`
- **What changed**:
  - Defined core storage domain contracts (`Record`, `Header`, `TopicPartition`, `Watermarks`, and `Engine` interface).
  - Implemented `MemoryEngine` with per-partition circular ring buffers, monotonic 64-bit offsets, atomic timestamps, and bounded memory eviction.
  - Implemented unit tests covering sequential append/read, ring buffer eviction, concurrent multi-goroutine writes, and missing topic/partition error handling.
- **Verification command(s)**:
  - `go test -v ./internal/features/storage/...`
- **Result**: PASS

## Step 3: Broker Domain Service & Real-Time EventBus (TDD)
- **Files changed**:
  - `internal/features/broker/eventbus.go`
  - `internal/features/broker/service.go`
  - `internal/features/broker/service_test.go`
- **What changed**:
  - Implemented non-blocking broadcast `EventBus` with non-blocking drops to insulate high-speed producers from slower UI subscribers.
  - Implemented `BrokerService` managing dynamic topic auto-creation, partition allocations, consumer group offset commits/lookups, dead-letter routing (`.dlq` suffix with tracing headers), and in-place message replay.
  - Added unit test suite covering real-time message pub/sub broadcast, consumer group offset tracking, and DLQ routing/replay.
- **Verification command(s)**:
  - `go test -v ./internal/features/broker/...`
- **Result**: PASS

## Step 4: Kafka Binary Wire Protocol Codec (TDD)
- **Files changed**:
  - `internal/features/protocol/kafka/codec.go`
  - `internal/features/protocol/kafka/codec_test.go`
- **What changed**:
  - Built zero-allocation Kafka binary big-endian encoder/decoder for standard protocol primitives (`Int8`, `Int16`, `Int32`, `Int64`, `String`, `NullableString`, `Bytes`, `NullableBytes`, `Varint`, `UVarint`).
  - Implemented CRC32-C (Castagnoli) checksumming function for Kafka RecordBatch validation.
  - Verified with table-driven tests checking round-trip serialization and exact Castagnoli CRC32 hash standards.
- **Verification command(s)**:
  - `go test -v ./internal/features/protocol/kafka/...`
- **Result**: PASS

## Step 5: Kafka Request & Response Schemas (TDD)
- **Files changed**:
  - `internal/features/protocol/kafka/messages.go`
  - `internal/features/protocol/kafka/messages_test.go`
- **What changed**:
  - Implemented request header parser (`ApiKey`, `ApiVersion`, `CorrelationId`, `ClientId`).
  - Implemented full encoders and decoders for Kafka v0–v8 protocol messages: `ApiVersions`, `Metadata`, `ProduceResponse`, `FetchResponse`, `FindCoordinatorResponse`, `OffsetCommitResponse`, `OffsetFetchResponse`, and `ListOffsetsResponse`.
  - Implemented Kafka RecordBatch v2 encoding/decoding with Castagnoli CRC32-C verification and legacy fallback.
  - Verified with comprehensive round-trip tests covering metadata negotiation and record batch extraction.
- **Verification command(s)**:
  - `go test -v ./internal/features/protocol/kafka/...`
- **Result**: PASS

## Step 6: Kafka TCP Server & Network Dispatcher
- **Files changed**:
  - `internal/features/protocol/kafka/handler.go`
  - `internal/features/protocol/kafka/server.go`
  - `internal/features/protocol/kafka/server_test.go`
- **What changed**:
  - Built Kafka protocol `Handler` implementing request dispatching for `ApiVersions`, `Metadata`, `Produce`, `Fetch`, `FindCoordinator`, `OffsetCommit`, `OffsetFetch`, and `ListOffsets`.
  - Built `Server` managing the TCP listener loop, 4-byte length-prefixed framing, goroutine-per-conn handling, write deadlines, and graceful connection draining.
  - Implemented end-to-end loopback TCP integration test verifying real TCP handshake, binary frame encoding/decoding, produce pipeline, and fetch responses.
- **Verification command(s)**:
  - `go test -v ./internal/features/protocol/kafka/...`
- **Result**: PASS

## Step 7: Interactive Terminal UI (Charm Bubbletea & Lipgloss)
- **Files changed**:
  - `internal/features/tui/styles.go`
  - `internal/features/tui/app.go`
  - `internal/features/tui/app_test.go`
  - `go.mod`, `go.sum`
- **What changed**:
  - Built modern terminal UI styles with custom vibrant dark palette, status badges, and rounded cards.
  - Implemented multi-tab `AppModel`:
    - Tab 1: Live Stream table with auto-scrolling, topic DLQ indicators, and payload preview.
    - Tab 2: Topics overview with partition counts, watermarks, and DLQ status.
    - Modal 1: Inspector modal with auto-indented JSON formatting and message headers.
    - Modal 2: Replay modal with in-terminal `bubbles/textarea` for editing payloads and `Ctrl+S` instant republishing.
  - Added unit test suite for lifecycle events, window resizing, tab switching, and quit signals.
- **Verification command(s)**:
  - `go test -v ./internal/features/tui/...`
- **Result**: PASS

## Step 8: CLI Entrypoint & Dual-Mode Execution
- **Files changed**:
  - `cmd/pocketqueue/config.go`
  - `cmd/pocketqueue/main.go`
- **What changed**:
  - Implemented configurable CLI flags (`--port`, `--host`, `--headless`, `--max-messages`, `--log-level`, `--json-log`).
  - Added auto-TTY detection via `isatty.IsTerminal`: launches the Charm Bubbletea interactive TUI with isolated logfile output if attached to a terminal; runs as a headless server daemon with structured `log/slog` logging if piped or `--headless`.
  - Added graceful shutdown handling (`SIGINT`, `SIGTERM`) releasing network listeners and channels.
- **Verification command(s)**:
  - `go build -o bin/pocketqueue.exe ./cmd/pocketqueue`
  - `./bin/pocketqueue.exe -h`
- **Result**: PASS

## Step 9: End-to-End Client Test with Real Kafka Client
- **Files changed**:
  - `test/e2e/e2e_test.go`
  - `internal/features/protocol/kafka/handler.go`
- **What changed**:
  - Created end-to-end integration test (`TestE2E_ProduceConsumeLifecycle`) running PocketQueue on an ephemeral TCP port.
  - Performed real client handshake (`ApiVersions`), produced 100 JSON messages with keys in a Kafka RecordBatch v2 over TCP, fetched all 100 records back in order, and asserted payload integrity.
  - Verified consumer group offset commitment and offset retrieval over TCP.
- **Verification command(s)**:
  - `go test -v ./test/e2e/...`
- **Result**: PASS

## Step 10: Binary Size, Startup Time, and Memory Footprint Audit
- **Files changed**:
  - `scripts/audit.ps1`
  - `cmd/pocketqueue/main.go`
- **What changed**:
  - Implemented automated resource audit script (`scripts/audit.ps1`) testing binary build size, process startup latency, and idle RAM consumption.
  - Added internal boot duration tracking in `main.go`.
  - Audited metrics against strict performance constraints:
    - **Binary Size**: **4.71 MB** (Target: `< 15.0 MB`) — **PASS**
    - **Idle RAM**: **7.71 MB** (Target: `< 25.0 MB`) — **PASS**
    - **Internal Boot Time**: **2.15 ms** (Target: `< 20 ms`) — **PASS**
- **Verification command(s)**:
  - `powershell -ExecutionPolicy Bypass -File ./scripts/audit.ps1`
- **Result**: PASS

## Phase 2 Expansion: Demo Tooling, Multi-Protocol, Persistence, and CI/CD
- **Files added/changed**:
  - `internal/features/demo/demo.go`, `demo_test.go`
  - `cmd/pocketqueue/producer.go`
  - `internal/features/storage/wal.go`, `wal_test.go`
  - `internal/features/protocol/redis/resp.go`, `server.go`, `server_test.go`
  - `cmd/pocketqueue/config.go`, `main.go`
  - `README.md`, `LICENSE`
  - `.github/workflows/ci.yml`, `.github/workflows/release.yml`
- **What changed**:
  1. **Demo & CLI Producer**:
     - Built domain event simulator emitting `orders`, `users.auth`, and `payments.dlq` events (`--demo` flag).
     - Built CLI producer (`-topic`, `-key`, `-value`) for instant terminal publishing over Kafka wire protocol.
  2. **WAL Persistence Engine**:
     - Implemented pure-Go append-only Write-Ahead Log engine with binary framing and offset indexing.
     - Verified persistence and offset continuity across simulated process restarts.
  3. **Redis Streams Dual-Protocol Server**:
     - Implemented RESP v2/v3 server on port `6379` supporting `PING`, `XADD`, `XLEN`, `XRANGE`, `XREAD`, `XACK`.
     - Enables Redis clients and Kafka clients to write to and read from the same message broker simultaneously.
  4. **Open-Source Polish & Automation**:
     - Created comprehensive `README.md` with comparison matrix, keybindings, and architecture diagrams.
     - Added GitHub Actions CI (`.github/workflows/ci.yml`) for Ubuntu, macOS, and Windows.
     - Added multi-platform automated release workflow (`.github/workflows/release.yml`).
- **Verification command(s)**:
  - `go vet ./...`
  - `go test -v ./...`
  - `powershell -ExecutionPolicy Bypass -File ./scripts/audit.ps1`
- **Result**: ALL PASS (Binary: 4.96 MB, Idle RAM: 7.9 MB, Cold Start: 1.79 ms)
