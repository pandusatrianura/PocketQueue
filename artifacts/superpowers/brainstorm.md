# Brainstorm: PocketQueue / MiniBus
**Ultra-Lightweight Embedded Local Message Broker (<15MB, <25MB RAM) with Interactive Terminal UI**

## Goal
How might we eliminate the 1–2 GB RAM consumption and configuration friction of running Kafka or RabbitMQ on local development machines by creating **PocketQueue**—an ultra-lightweight (<15 MB single binary, <25 MB RAM idle), zero-config embedded message broker with a real-time Terminal UI (TUI) powered by Charm `bubbletea`, speaking a drop-in subset of the Kafka wire protocol and Redis Streams?

**Primary Value Drivers (Painkiller vs. Vitamin):**
- **Instant Dev Boot (<15ms vs. 45–90s)**: No Docker desktop, no JVM, no Zookeeper/KRaft, no Erlang VM. Just run `pocketqueue` and point your app to `localhost:9092`.
- **In-Terminal Event Observability**: Real-time stream inspection with auto-formatted JSON/Protobuf/String payloads, headers, partition offsets, and throughput gauges.
- **Dead-Letter Queue (DLQ) & Replay Studio**: One-keystroke message replay (`r`) or inline payload editing (`e`) to re-trigger failed downstream consumer logic without re-running entire business flows.
- **Zero-Friction Contribution**: Plug-and-play architecture for storage engines (In-Memory, Pure-Go SQLite, Pebble) and serializers (JSON, Avro, Protobuf).

## Constraints
1. **Binary & Memory Limits**:
   - Standalone binary size: **< 15 MB** (stripped, pure Go, no CGo).
   - Idle memory footprint: **< 25 MB**; active burst (10,000 msgs in-flight): **< 50 MB**.
   - Cold start latency: **< 20 ms** to port ready.
2. **Platform & Dependency Independence**:
   - Zero external runtime dependencies (no Docker, no external C libraries).
   - Cross-platform native binaries for Windows (`amd64`/`arm64`), Linux (`amd64`/`arm64`), and macOS (`arm64`/`amd64`).
3. **Wire Protocol Compatibility**:
   - Drop-in compatibility for standard Kafka SDKs (`sarama`, `confluent-kafka-go`, `kafka-python`, `kafkajs`, `spring-kafka`) using Kafka Wire Protocol v2–v8 for core primitives.
   - Core API Keys required: `ApiVersions` (v0–v3), `Metadata` (v0–v5), `Produce` (v3–v8), `Fetch` (v4–v11), `FindCoordinator` (v0–v2), `OffsetCommit` (v2–v8), `OffsetFetch` (v1–v6), `ListOffsets` (v1–v5).
4. **Interactive TUI Requirements**:
   - Built on Charm ecosystem (`bubbletea`, `lipgloss`, `bubbles`).
   - Virtualized viewport/table rendering to prevent terminal buffer exhaustion and memory leaks during high-throughput message bursts.
   - Dual execution modes: Interactive TUI (default when attached to a TTY) and Headless/Daemon mode (`--headless` or `pocketqueue server` for CI/CD or dockerized dev environments).
5. **Architectural Testability & Clean Boundaries**:
   - Follow strict I/O isolation: Broker core logic and state transitions must be 100% testable in pure Go unit tests without network sockets or disk I/O.

## Known context
- **Current Developer Pain**:
  - Running Kafka locally requires Docker containers, KRaft/Zookeeper images, and companion UIs (AKHQ, Kafka UI) taking 1.5–3 GB RAM.
  - On laptops with 8–16 GB RAM, this leads to heavy swapping, battery drain, and thermal throttling.
  - Resetting local state in Kafka or RabbitMQ often requires `docker compose down -v`, wiping all testing setups.
- **Existing Solutions**:
  - *Redpanda*: High performance in C++, but still consumes 500 MB–1 GB RAM and is optimized for production streaming rather than single-binary CLI developer ergonomics.
  - *NATS*: Ultra-light (<20 MB RAM) and fast, but uses its own bespoke protocol and lacks Kafka/Redis Streams wire compatibility—forcing developers to rewrite their client libraries.
  - *Embedded Kafka (Java)*: Heavy JVM memory consumption; only useful inside JVM test suites.
- **Ecosystem Leverage**:
  - Go's networking primitives (`net.Listener`, goroutines) allow high-throughput framing with near-zero allocation overhead.
  - Charm's TUI libraries (`bubbletea`, `lipgloss`) provide a rich developer experience previously only seen in tools like `k9s` or `lazygit`.

## Risks
1. **Kafka Wire Protocol Scope & Version Creep (High)**:
   - *Risk*: Kafka's binary protocol is large, complex, and evolving across versions. Attempting full coverage (SASL auth, consumer group coordinator heartbeats, transactions, schema registries) will cause endless maintenance and binary bloat.
   - *Mitigation*: Strictly limit the protocol implementation to the **Local Dev Subset**. Treat the broker as a single-node, single-broker cluster (`broker_id = 0`). Return deterministic static responses for unsupported management APIs (`CreateAcls`, `AlterConfigs`).
2. **TUI Memory Consumption on High-Throughput Streams (Medium)**:
   - *Risk*: If a producer dumps 50,000 messages/sec into PocketQueue, buffering all messages in the TUI state will quickly exceed the < 25 MB RAM ceiling and freeze the terminal event loop.
   - *Mitigation*: Fixed-capacity circular ring buffer (e.g., 2,000 latest events for live viewing) with non-blocking message dropping or sampling for the UI layer while the storage engine processes at full speed.
3. **Storage Engine Portability & CGo Traps (Medium)**:
   - *Risk*: Pulling in standard SQLite bindings (`github.com/mattn/go-sqlite3`) forces CGo, breaking single-binary cross-compilation and inflating binary size.
   - *Mitigation*: Use pure Go storage backends exclusively: an in-memory ring buffer with pure Go append-only WAL for default durability, and `modernc.org/sqlite` or CockroachDB's `pebble` if embedded persistent querying is needed.
4. **Scope Creep into Production Clustering (Low)**:
   - *Risk*: Community requests for Raft clustering, multi-node replication, and distributed consensus.
   - *Mitigation*: Firmly anchor PocketQueue's brand and architecture: "PocketQueue is the SQLite of Message Brokers—designed for local development, testing, and edge computing, not cloud-scale multi-datacenter clusters."

## Options (2–4)

### Option 1: Kafka-First Wire-Compatible Broker with Embedded Charm TUI (The Drop-In Replacement)
- **Core Design**:
  - Focuses 100% on the Kafka binary wire protocol on port `9092`.
  - Implements an optimized binary protocol parser for the top 8 Kafka request types.
  - Internal engine: Append-only partition log with in-memory indexes and optional disk WAL.
  - Embedded Bubbletea TUI with 4 interactive tabs: `[1] Streams`, `[2] Topics`, `[3] Message Inspector & Replay`, `[4] Dead Letter Queue`.
- **Pros**:
  - Maximum developer "drop-in" magic: Zero code changes for developers already using Kafka client libraries.
  - Highest market pull and immediate adoption.
- **Cons**:
  - Kafka binary framing parser requires careful implementation and unit testing against various client SDK versions.

### Option 2: Dual-Protocol Modular Engine: Kafka Wire + Redis Streams RESP (The Multi-Tenant Local Broker)
- **Core Design**:
  - Core engine implements an abstract `MessageStream` contract (Append, ReadAt, Subscribe, Ack, DLQ).
  - Two lightweight protocol adapters run side-by-side:
    - Kafka Wire Adapter on port `9092`.
    - Redis Streams RESP Adapter on port `6379` supporting `XADD`, `XREAD`, `XREADGROUP`, `XRANGE`, `XACK`.
  - Unified Bubbletea TUI displays events from both Kafka topics and Redis streams simultaneously.
- **Pros**:
  - Covers two dominant local queue paradigms (Kafka event streaming and Redis Streams microservices).
  - Clean separation between protocol frontends and storage backends.
- **Cons**:
  - Slightly larger initial development scope; risk of dividing focus across two protocols before Kafka compatibility is rock solid.

### Option 3: Redis Streams First + High-Level Kafka SDK Bridge
- **Core Design**:
  - Implements only the Redis RESP protocol on port `6379`.
  - Provides a lightweight, drop-in Go/Python/Node wrapper library that mimics Kafka's Producer/Consumer API on top of Redis Streams.
- **Pros**:
  - RESP protocol is dramatically simpler to implement than Kafka wire protocol, allowing an MVP in days.
- **Cons**:
  - **Fails the Zero Code Change test**: Developers must change their client libraries or configuration wrappers, drastically reducing adoption compared to native wire compatibility.

## Recommendation
**Adopt Option 1 (Kafka-First Drop-In Broker) with an Architecture Designed for Phase 2 Protocol & Storage Extensibility.**

- **Why**: The primary friction point driving developers crazy is Kafka's heavyweight local setup. A tool that lets a developer run `pocketqueue` in terminal, leave their existing application code untouched (`KAFKA_BROKERS=localhost:9092`), and immediately gives them a gorgeous real-time TUI is an irresistible 10x improvement.
- **Architectural Vertical Slice**:
  1. `pkg/protocol/kafka`: Minimal zero-alloc binary frame decoder for Kafka requests.
  2. `pkg/broker`: Core topic/partition management, offset tracking, consumer group coordination.
  3. `pkg/storage`: Pluggable storage interface (`StorageEngine`) with default `MemoryRing` and `FileWAL`.
  4. `pkg/tui`: Charm `bubbletea` application with keyboard-driven live stream, JSON syntax highlighting, and replay modal.
  5. `cmd/pocketqueue`: Entrypoint with auto-TTY detection (boots TUI if interactive terminal; runs headless daemon if piped or `--headless`).

### Explicit "Not Doing" (To preserve 10x Simplicity)
- **No Clustering or Raft Consensus**: Strictly single-node local execution.
- **No SASL / SSL Complexity**: Local plaintext communication only.
- **No Heavy Web UI**: 100% terminal-native; zero Node.js/browser bundle overhead.
- **No Schema Registry Daemon**: Embedded auto-detection for JSON / String; Protobuf schema loading from local `.proto` files if requested.

## Acceptance criteria
1. **Binary & Resource Compliance**:
   - `pocketqueue` builds to a single executable `< 15 MB` (stripped Go binary).
   - Idle memory usage is verified `< 25 MB` via process metrics.
   - Startup time from invocation to listening on `:9092` is `< 25 ms`.
2. **Protocol & Client Verification**:
   - Standard Kafka client SDKs in at least 3 major languages can produce and consume without error:
     - Go (`github.com/twmb/franz-go` or `IBM/sarama`)
     - Python (`kafka-python` or `confluent-kafka`)
     - Node.js (`kafkajs`)
3. **Interactive TUI Usability**:
   - Running `pocketqueue` in a terminal displays the Charm Bubbletea UI with live topic/message list.
   - Incoming messages appear in real-time with timestamp, topic, partition, key, and formatted value.
   - Pressing `Enter` opens the Inspector modal showing full headers and formatted payload.
   - Pressing `r` initiates a message replay back into the original or DLQ topic.
4. **Daemon / Headless Mode**:
   - Running `pocketqueue --headless` or `pocketqueue server` runs as a silent background daemon with structured logging (JSON/text) for CI pipelines.
5. **Codebase & Architecture Standards**:
   - Zero CGo dependencies (`CGO_ENABLED=0` builds clean on Windows, macOS, and Linux).
   - Core broker logic has >80% test coverage with isolated in-memory unit tests.
