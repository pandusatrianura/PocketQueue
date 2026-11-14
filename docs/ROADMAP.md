# 🗺️ PocketQueue Milestone Roadmap

This roadmap outlines the evolution of PocketQueue while adhering strictly to the **10x Simpler & Lighter** constitution (< 15 MB binary, < 25 MB RAM idle, zero CGo).

---

## 🎯 Completed Milestones

### Milestone 1: Core Dual-Protocol Broker & TUI Studio ✅
- Single-binary pure Go architecture (`CGO_ENABLED=0`).
- Kafka Wire Protocol (`:9092`): ApiVersions, Metadata, Produce, Fetch.
- Redis Streams RESP (`:6379`): XADD, XREAD, XRANGE, XLEN.
- Pluggable Storage: In-Memory ring buffer & Write-Ahead Log (WAL).
- Charm Bubbletea TUI: Live stream viewport, JSON payload inspector, in-terminal DLQ Replay Studio (`r`/`e`).

### Milestone 2: Protocol Hardening & Resource Safeguards ✅
- **Pass 1: Resiliency & Protocol Foundations**
  - TCP framing length validation & OOM protection (16 MB cap).
  - Dynamic partition auto-expansion and non-negative partition guards.
  - Kafka consumer group protocol handshake stubs (`JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`).
  - Redis Streams `XREAD BLOCK` timeout (`*-1\r\n`) and non-blocking event-driven wakeups.
  - Bounded ring-buffer eviction clamping and corrupt trailing WAL frame auto-recovery.
  - TUI terminal resize protection (<40x10) and binary payload hex inspector.
- **Pass 2: Lifecycle & Resource Boundaries**
  - Kafka `acks = 0` fire-and-forget TCP socket preservation.
  - Cross-platform port collision fail-fast with guidance banner (`ErrPortInUse`).
  - Graceful `SIGINT`/`SIGTERM` OS signal shutdown with WAL `file.Sync()`.
  - Dynamic partition allocation cap (`MaxPartitionsPerTopic = 16`).
  - WAL file size cap safeguard (`100 MB` per partition).
  - Redis `QUIT` connection teardown and TUI ingress burst throttling ($\le 30$ FPS).
- **Architecture Doctrine**
  - Codified the **3-Tier Pre-Implementation Triage Framework** in `docs/architecture/triage-framework.md`.

### Milestone 3: Real-World Client SDK Compatibility Matrix (Go Ecosystem) ✅
*Objective: Guarantee out-of-the-box drop-in compatibility with official production client libraries.*
- **Official Go Kafka Client (`github.com/IBM/sarama`)**:
  - `sarama.NewSyncProducer`: Verified message production, topic creation, partition routing, and monotonic base offsets.
  - `sarama.NewConsumer`: Verified partition subscription, API version negotiation, watermark resolution, and FIFO consumption.
  - `sarama.NewConsumerGroup`: Verified high-level consumer group rebalance flow (`FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetFetch`, `OffsetCommit`).
- **Official Go Redis Client (`github.com/redis/go-redis/v9`)**:
  - `rdb.Ping()`: Verified connection handshake and `HELLO` RESP protocol negotiation.
  - `rdb.XAdd()` & `rdb.XLen()` & `rdb.XRange()`: Verified standard Redis Streams ingestion, stream length query, and ID range retrieval.
  - `rdb.XRead()` with `Block`: Verified blocking read timeouts (`redis.Nil`) and event-driven live pub/sub wakeups.
- **Zero Binary Bloat Guarantee**:
  - All SDK dependencies are strictly test-scoped in `test/compatibility/` — binary size remains **5.14 MB** (budget < 15.0 MB).

### Milestone 3b: Multi-Language Client SDK Compatibility Test Matrix ✅
*Objective: Expand client verification matrix across additional language runtimes.*
- **Python Ecosystem (`test/compatibility/python/`)**:
  - `redis-py` (v8.1+): Verified `xadd`, `xlen`, `xrange`, `xgroup_create` (`MKSTREAM`), `xreadgroup`, `xack`, `xrevrange`, and `xdel`.
  - Native Python Kafka Wire Protocol: Verified `ApiVersions` (Key 18), `CreateTopics` (Key 19), `Metadata` (Key 3), and `DeleteTopics` (Key 20).
- **Node.js / TypeScript Ecosystem (`test/compatibility/nodejs/`)**:
  - `ioredis` (v5.4+): Verified `xadd`, `xlen`, `xrange`, `xgroup CREATE`, `xreadgroup`, `xack`, `xrevrange`, and `xdel`.
  - Native Node.js `net` Kafka Wire Protocol: Verified `ApiVersions`, `CreateTopics`, `Metadata`, and `DeleteTopics`.
- Automated test suites run via `python test/compatibility/python/runner.py` and `cd test/compatibility/nodejs && npm test`.

### Milestone 4: Packaging, Containerization & Zero-Install Distribution ✅
*Objective: Instant zero-install usage for developers across macOS, Linux, and Windows.*
- Multi-stage scratch `Dockerfile` (< 8 MB image, zero attack surface, SSL certificates included).
- Drop-in `docker-compose.example.yml` microservice drop-in replacement template for Kafka/RabbitMQ.
- Homebrew formula (`Formula/pocketqueue.rb`) for macOS / Linux.
- Scoop manifest (`packaging/scoop/pocketqueue.json`) for Windows developers.

### Milestone 5: Developer Experience & Wire Protocol Completeness ✅
*Objective: Elevate terminal developer experience and wire protocol feature parity.*
- **Developer Experience & TUI Studio DX**:
  - **Live Filter & Quick Search (`/`)**: Real-time filtering by topic name or payload substring with cursor auto-clamping.
  - **Pause / Resume Auto-Scroll (`Space`)**: Freeze live viewport for detailed inspection while continuing non-blocking ingestion.
  - **Clear Stream Display (`c`)**: Clear viewport table without mutating broker storage or consumer group offsets.
  - **Event Snapshot Export (`w` & `--export`)**: Export in-memory event streams to formatted JSON on demand or at shutdown.
- **Wire Protocol Completeness**:
  - **Kafka Admin API**: Implemented `CreateTopics` (API 19) and `DeleteTopics` (API 20) with partition bounds validation ($\le 16$).
  - **Redis Streams Consumer Groups**: Implemented `XGROUP CREATE` (with `MKSTREAM`), `XGROUP DESTROY`, `XREADGROUP GROUP`, and `XACK`.
  - **Extended Redis Stream Commands**: Implemented `XREVRANGE` and `XDEL`.

---

## 🚀 Upcoming Milestones

### Milestone 6: Webhook / HTTP Ingress Bridge (Optional Headless Adapter)
- Expose lightweight REST / SSE endpoint on port `:8080` for event streaming into web browsers without custom clients.
- Webhook trigger destination for Dead-Letter Queue event notifications.

---

## 🛡️ Boundary Invariants (Always Enforced)
1. **Binary Size Ceiling**: `< 15.0 MB` (Current: `5.14 MB`).
2. **Idle Memory Ceiling**: `< 25.0 MB` (Current: `7.93 MB`).
3. **Cold Boot Ceiling**: `< 20.0 ms` (Current: `2.22 ms`).
4. **Zero External Runtime Dependencies**: Pure Go, zero CGo, zero JVM, zero Docker required for operation.

