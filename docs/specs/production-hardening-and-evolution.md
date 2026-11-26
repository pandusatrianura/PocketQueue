# Spec: Production Hardening, Container DX & HTTP Ingress

## 1. Objective

Elevate PocketQueue from a local development & testing broker to a fully hardened, container-native edge broker and testbed while maintaining its foundational doctrine: **10x lighter and simpler** (< 15 MB binary, < 25 MB RAM idle, single static binary, zero CGo).

Following user architecture alignment, this initiative is phased into two distinct milestones:

### 🎯 Milestone 6.1: Data Resilience & Container DX (Immediate Implementation Target)
1. **Consumer Group Offset Persistence**: Save and restore committed offsets on WAL mode (`--data-dir`) using 50ms debounced atomic writes + graceful shutdown flush.
2. **Automated WAL Retention & FIFO Pruning**: Safe log rolling and oldest-frame pruning via `--retention-bytes` (default 100 MB) and `--retention-age`, advancing `Earliest` watermark cleanly.
3. **Built-in Healthcheck Probe**: Single CLI command `pocketqueue ping --broker 127.0.0.1:9092` with exit code 0/1 for instant Docker/Kubernetes container probes.
4. **Graceful Connection Draining**: Configurable drain period (`--drain-timeout`, default 500ms) on `SIGINT`/`SIGTERM` to allow in-flight fetch/produce requests to complete cleanly with EOF instead of abrupt TCP resets.

### 🚀 Milestone 7: HTTP Bridge, Webhooks & Automated Distribution (Next Release Target)
5. **HTTP REST & Server-Sent Events (SSE) Bridge**: Lightweight HTTP ingress (`POST /api/v1/topics/:topic`), real-time SSE stream (`GET /api/v1/topics/:topic/sse`), and `/healthz` (default disabled: `--http-port 0`).
6. **DLQ Webhook Alerting**: Optional forwarding of dead-letter queue records to external webhooks (`--webhook-dlq-url`).
7. **Release Workflow SHA-256 Automation**: Automated checksum generation and injection into Homebrew Formula and Scoop manifest during GitHub Actions release.

---

## 2. Tech Stack & Environment

- **Language**: Go 1.24+ (Standard Library only, `CGO_ENABLED=0`).
- **Target OS**: Linux (amd64, arm64), macOS (amd64, arm64), Windows (amd64, arm64).
- **Core Dependencies**:
  - `net`, `net/http` (Pure Go standard library HTTP & SSE, zero third-party web frameworks).
  - `encoding/json`, `os`, `sync`, `time`.
  - Existing protocol packages: `internal/features/protocol/kafka`, `redis`.

---

## 3. Assumptions Surfaced

```
ASSUMPTIONS I'M MAKING:
1. Offset Persistence in WAL Mode:
   - Consumer group offsets are stored inside a dedicated `offsets.json` metadata file within `--data-dir` with atomic write (write to temp file + atomic rename) and periodic fsync.
   - When running in pure memory mode (no `--data-dir`), offsets remain ephemeral in RAM.
2. WAL Retention & Rolling:
   - When a partition file reaches `--retention-bytes` (default 100 MB), the oldest records are pruned via segment truncation or rolling window, advancing the partition's `Earliest` watermark cleanly.
3. Healthcheck Probe (`pocketqueue ping`):
   - Communicates with the Kafka TCP listener using ApiVersions (Key 18) request.
   - Exits code 0 on responsive broker; exits code 1 on refused connection or timeout.
4. HTTP Bridge (`--http-port`):
   - Disabled by default (`--http-port 0`) to maintain zero socket overhead and zero attack surface unless explicitly enabled by developer (e.g. `--http-port 8080`).
   - Uses Go standard library `http.Server` with strict timeouts.
5. Invariants:
   - Total binary size must remain strictly under 6.0 MB (project ceiling < 15.0 MB).
   - Idle RAM must remain strictly under 10.0 MB (project ceiling < 25.0 MB).
   - Cold boot latency must remain strictly under 5.0 ms (project ceiling < 20.0 ms).
```

---

## 4. Commands

```bash
# Build
go build -ldflags="-s -w" -o bin/pocketqueue ./cmd/pocketqueue

# Run all unit & integration tests
go test -count=1 ./...

# Run healthcheck probe (CLI)
./bin/pocketqueue ping --broker 127.0.0.1:9092 --timeout 2s

# Run broker with WAL persistence and custom retention
./bin/pocketqueue --data-dir ./data --retention-bytes 104857600 --retention-age 24h

# Run broker with HTTP REST/SSE bridge enabled
./bin/pocketqueue --http-port 8080

# Audit performance and resource boundaries
powershell -ExecutionPolicy Bypass -File ./scripts/audit.ps1
```

---

## 5. Project Structure Additions

```
PocketQueue/
├── cmd/pocketqueue/
│   ├── main.go                # Subcommand dispatcher (adds 'ping')
│   ├── config.go              # Flags: --retention-bytes, --retention-age, --http-port, --drain-timeout
│   ├── ping_cmd.go            # Healthcheck probe CLI implementation
│   └── ping_cmd_test.go       # Healthcheck probe tests
├── internal/
│   ├── features/
│   │   ├── broker/
│   │   │   ├── service.go     # Wire offset persistence and retention management
│   │   │   └── offsets.go     # OffsetStorage interface & file-backed persistence
│   │   ├── storage/
│   │   │   ├── engine.go      # Add Prune(topic, partition, minOffset) to Engine interface
│   │   │   ├── wal.go         # Implement retention truncation / rolling segment
│   │   │   └── wal_test.go    # Test retention pruning and watermark advance
│   │   └── httpbridge/
│   │       ├── server.go      # REST produce, SSE stream, /healthz, webhook triggers
│   │       └── server_test.go # HTTP unit and integration tests
├── .github/workflows/
│   └── release.yml            # Dynamic SHA-256 computation and manifest patching
└── docs/specs/
    └── production-hardening-and-evolution.md # This specification
```

---

## 6. Code Style & Architectural Patterns

All new code adheres to the project's **Architectural Patterns — Testability-First Design** (`I/O Isolation`, `Pure Business Logic`, `Single Responsibility`).

Example idiomatic interface abstraction for Offset Storage:

```go
// OffsetStore abstracts consumer group offset persistence across restarts.
type OffsetStore interface {
	Save(ctx context.Context, offsets map[string]map[string]map[int32]int64) error
	Load(ctx context.Context) (map[string]map[string]map[int32]int64, error)
	Close() error
}
```

---

## 7. Boundaries & Guardrails

- **Always Do**:
  - Abstract I/O behind interfaces so unit tests run in-memory without touching disk or ports.
  - Flush OS file descriptors with `file.Sync()` on signal traps.
  - Enforce timeout boundaries on all network I/O (`context.WithTimeout`).
  - Maintain zero CGo (`CGO_ENABLED=0`) across all targets.
- **Ask First**:
  - Adding any third-party dependencies outside standard Go library.
  - Changing wire protocol framing format or default port numbers.
- **Never Do**:
  - Store uncompressed or non-atomic offset files that can corrupt on sudden power loss.
  - Block shutdown indefinitely when draining client connections (hard cap at drain timeout).
  - Enable HTTP port by default without user opt-in.

---

## 8. Success Criteria (Reframed Acceptance Conditions)

1. **Consumer Group Persistence**:
   - Given a running broker with `--data-dir /tmp/test-wal`, when a Kafka/Redis consumer group commits offset `42` on topic `orders` and the broker is killed via `SIGKILL`, upon subsequent restart with the same `--data-dir`, `GetOffset` immediately returns `42`.
2. **WAL Retention**:
   - Given a partition WAL file with `--retention-bytes 1048576` (1 MB), producing 2 MB of messages succeeds without error; file size does not exceed the budget, and earliest watermark advances to the oldest retained offset.
3. **Healthcheck Probe**:
   - `pocketqueue ping --broker 127.0.0.1:9092` exits with code `0` and prints latency when broker is running.
   - When broker is stopped, `pocketqueue ping` exits with code `1` within timeout without hanging.
4. **Graceful Draining**:
   - When `SIGINT` is received, active fetch requests finish within `--drain-timeout` (default 500ms) with clean EOF rather than abrupt TCP reset.
5. **HTTP & SSE Bridge**:
   - With `--http-port 8080`, `POST /api/v1/topics/chat` publishes messages to broker.
   - `GET /api/v1/topics/chat/sse` delivers messages in real time over Server-Sent Events.
   - `GET /healthz` returns `200 OK` with JSON status.
6. **Binary & Memory Budget**:
   - Stripped binary size remains `< 6.0 MB`.
   - Idle RAM footprint remains `< 10.0 MB`.
   - Cold boot latency remains `< 5.0 ms`.

---

## 9. Resolved Architectural Decisions

1. **Offset Persistence Granularity**:
   - **Decision**: Debounced atomic write (50ms) to `offsets.json` + immediate synchronous flush on shutdown (`SIGINT`/`SIGTERM`).
   - **Rationale**: Keeps consumer commit throughput high (> 10,000 commits/sec) without thrashing disk I/O, while guaranteeing zero offset loss on graceful exit.
2. **HTTP Bridge Port Default**:
   - **Decision**: Disabled by default (`--http-port 0`). Only starts if user explicitly passes `--http-port <port>`.
   - **Rationale**: Upholds the project's zero-attack-surface and zero-socket-overhead doctrine for developers who only use standard Kafka/Redis wire protocols.
3. **Phased Milestone Roadmap**:
   - **Decision**: Phased into two releases:
     - **Milestone 6.1 (Data Resilience & Container DX)**: Consumer group offset persistence, automated WAL retention pruning, `pocketqueue ping` probe, and graceful connection draining.
     - **Milestone 7 (HTTP Bridge & Webhooks)**: REST endpoint, SSE live stream, DLQ webhook forwarding, and automated release checksum injection.
