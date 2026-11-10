### Goal
Build **PocketQueue**: an ultra-lightweight (<15 MB binary, <25 MB RAM idle) embedded local message broker in pure Go with an embedded Kafka wire-compatible server (port 9092) and an interactive Charm Bubbletea TUI for real-time stream inspection, payload editing, and DLQ replay.

### Assumptions
- **Runtime**: Pure Go (`CGO_ENABLED=0`), cross-platform (Windows, Linux, macOS) without external dependencies.
- **Protocol**: Kafka Wire Protocol subset on port `9092` supporting standard Kafka SDKs (`Produce`, `Fetch`, `Metadata`, `ApiVersions`, `FindCoordinator`, `OffsetCommit`, `OffsetFetch`, `ListOffsets`).
- **Storage**: In-memory circular ring buffer by default (zero disk cruft, microsecond latency, strictly bounded memory) implementing a clean `storage.Engine` interface.
- **TUI & UX**: Dual-mode execution (interactive Charm Bubbletea TUI when attached to a TTY; `--headless` mode for CI/CD). Inline `textarea` modal for editing/replaying events, with `$EDITOR` fallback via `tea.ExecProcess`.
- **Testability**: Pure message broker logic and protocol decoders 100% testable in unit tests without network sockets or disk I/O.

---

### Plan

1. **Project Skeleton & Go Module Setup**
   - Files: `go.mod`, `.gitignore`, `internal/platform/logger/logger.go`
   - Change:
     - Initialize Go module `github.com/pandusatrianura/pocketqueue` with Go 1.26.
     - Add Charm dependencies (`charmbracelet/bubbletea`, `charmbracelet/lipgloss`, `charmbracelet/bubbles`).
     - Configure structured logger using standard library `log/slog` with correlation ID and duration helpers.
   - Verify: `go build ./...` exits 0.

2. **Storage Engine Domain & In-Memory Ring Buffer (TDD)**
   - Files:
     - `internal/features/storage/storage.go`
     - `internal/features/storage/memory.go`
     - `internal/features/storage/memory_test.go`
   - Change:
     - Define core domain types: `Record`, `Header`, `TopicPartition`, and `Engine` interface (`Append`, `ReadFrom`, `GetWatermarks`, `Purge`).
     - Implement `MemoryEngine` with per-partition circular ring buffers, atomic 64-bit monotonic offsets, and reader lock safety.
     - Add table-driven unit tests: append records, verify sequential offsets, test ring-buffer wrapping at max capacity, verify concurrent reader/writer safety.
   - Verify: `go test -v -race ./internal/features/storage/...`

3. **Broker Domain Service & Real-Time EventBus (TDD)**
   - Files:
     - `internal/features/broker/service.go`
     - `internal/features/broker/eventbus.go`
     - `internal/features/broker/service_test.go`
   - Change:
     - Implement `BrokerService` managing topic metadata, partition routing, consumer group committed offsets, and DLQ tagging.
     - Implement lock-free broadcast `EventBus` publishing events to TUI subscribers without blocking publisher threads.
     - Add unit tests: produce message, consume message with consumer group commit, broadcast delivery, and DLQ message isolation.
   - Verify: `go test -v -race ./internal/features/broker/...`

4. **Kafka Binary Wire Protocol Codec (TDD)**
   - Files:
     - `internal/features/protocol/kafka/codec.go`
     - `internal/features/protocol/kafka/codec_test.go`
   - Change:
     - Implement high-speed zero-alloc big-endian binary decoder/encoder for Kafka types: `Int8`, `Int16`, `Int32`, `Int64`, `Varint`, `UVarint`, `String`, `CompactString`, `Bytes`, `Array`.
     - Implement CRC32-C (Castagnoli) checksumming required for Kafka RecordBatch v2 verification.
     - Add unit tests verifying serialization and deserialization against raw byte fixtures.
   - Verify: `go test -v -race ./internal/features/protocol/kafka/...`

5. **Kafka Request & Response Schemas (TDD)**
   - Files:
     - `internal/features/protocol/kafka/messages.go`
     - `internal/features/protocol/kafka/messages_test.go`
   - Change:
     - Implement Kafka header parser (`ApiKey`, `ApiVersion`, `CorrelationId`, `ClientId`).
     - Implement encoder/decoder for core Kafka APIs:
       - `ApiVersions` (Key 18) - versions 0 to 3.
       - `Metadata` (Key 3) - versions 0 to 5.
       - `Produce` (Key 0) - RecordBatch v2 handling.
       - `Fetch` (Key 1) - streaming records back to consumers.
       - `FindCoordinator` (Key 10) - returning local broker as group coordinator.
       - `OffsetCommit` (Key 8) & `OffsetFetch` (Key 9).
       - `ListOffsets` (Key 2) - partition earliest/latest watermarks.
     - Add unit tests verifying parsing of real client handshakes and payload responses.
   - Verify: `go test -v -race ./internal/features/protocol/kafka/...`

6. **Kafka TCP Server & Network Dispatcher**
   - Files:
     - `internal/features/protocol/kafka/server.go`
     - `internal/features/protocol/kafka/handler.go`
     - `internal/features/protocol/kafka/server_test.go`
   - Change:
     - Implement TCP listener on `:9092` with connection pooling, goroutine-per-conn handling, and read/write timeouts.
     - Frame framing loop (`[4-byte Length][Header][Payload]`), dispatching to `BrokerService` and responding with proper correlation IDs.
     - Add integration test: open TCP connection, perform `ApiVersions` handshake, produce a record, fetch the record back over loopback TCP.
   - Verify: `go test -v -race ./internal/features/protocol/kafka/...`

7. **Interactive Terminal UI (Charm Bubbletea & Lipgloss)**
   - Files:
     - `internal/features/tui/app.go`
     - `internal/features/tui/stream_view.go`
     - `internal/features/tui/inspector_view.go`
     - `internal/features/tui/replay_view.go`
     - `internal/features/tui/styles.go`
   - Change:
     - Build multi-tab TUI with Charm `bubbletea` and `lipgloss`:
       - `Live Stream View`: Virtualized scrolling table of messages (Topic, Partition, Offset, Key, Payload Preview, Latency/Timestamp).
       - `Inspector Modal`: Full payload inspector with automatic JSON pretty-printing and header inspection.
       - `Replay & Edit Modal`: In-TUI `bubbles/textarea` allowing payload edits and `Ctrl+S` instant replay; `E` shortcut to spawn `$EDITOR` via `tea.ExecProcess`.
       - `Topics & Metrics View`: Message counters, ingress rate, and DLQ status.
     - Subscribe directly to Broker `EventBus`.
   - Verify: `go test ./internal/features/tui/...`

8. **CLI Entrypoint & Dual-Mode Execution**
   - Files:
     - `cmd/pocketqueue/main.go`
     - `cmd/pocketqueue/config.go`
   - Change:
     - Implement CLI flags: `--port` (default 9092), `--headless` (bool), `--max-messages` (default 5000), `--log-level`.
     - Implement auto-TTY detection:
       - If attached to terminal and not `--headless` -> launch Bubbletea TUI.
       - If piped/redirected or `--headless` -> run server in daemon mode with structured `slog` output.
     - Graceful shutdown handling (`SIGINT`, `SIGTERM`) releasing network ports and channels cleanly.
   - Verify: `go run ./cmd/pocketqueue --headless --port 9092`

9. **End-to-End Client Test with Real Kafka Client**
   - Files:
     - `test/e2e/e2e_test.go`
   - Change:
     - Write automated end-to-end integration test spinning up PocketQueue on an ephemeral port.
     - Use a standard Kafka client library to produce 100 messages with keys and JSON payloads.
     - Consume messages back and verify exact offset, key, value, and timestamp matching.
   - Verify: `go test -v ./test/e2e/...`

10. **Binary Size, Startup Time, and Memory Footprint Audit**
    - Files: `scripts/audit.ps1` (or automated test script)
    - Change:
      - Compile binary with stripped symbols: `go build -ldflags="-s -w" -o bin/pocketqueue.exe ./cmd/pocketqueue`.
      - Assert binary size is `< 15 MB`.
      - Measure idle RAM usage (`Get-Process pocketqueue | Select-Object WorkingSet64`) to assert `< 25 MB`.
      - Measure cold start time to assert `< 20 ms`.
    - Verify: Script passes all metric assertions.

---

### Risks & Mitigations
- **Subtle Kafka Client Differences**: Different client libraries (Sarama, Confluent, Franz-go, KafkaJS) send slightly different API versions during initial handshakes.
  - *Mitigation*: Our `ApiVersions` response explicitly advertises the supported version ranges (e.g. Produce v3–v7, Fetch v4–v11). Clients negotiate down to the intersection.
- **TUI Performance on High Throughput**: Fast message influx overwhelming the UI rendering loop.
  - *Mitigation*: Virtualized table rendering and a fixed-size UI buffer (e.g. 1,000 items) with throttling if ingress exceeds 500 msgs/sec in UI mode.
- **Windows Terminal ANSI Handling**: PowerShell / Windows Terminal rendering quirks.
  - *Mitigation*: Charm's `lipgloss` and `bubbletea` natively leverage Windows Virtual Terminal processing; verified natively on Windows.

### Rollback Plan
- Each step is encapsulated into isolated, single-responsibility Go packages (`storage`, `broker`, `protocol/kafka`, `tui`).
- No changes affect external files or system states.
- If any step encounters an issue, rollback is simply running `git checkout` on the affected package, with all previous steps remaining fully covered by their isolated unit tests.
