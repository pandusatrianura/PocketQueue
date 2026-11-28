# Implementation Plan: Milestone 10 — MQTT v3.1.1 IoT Protocol Ingress Bridge (:1883)

## Overview
Decomposing the implementation of **MQTT v3.1.1** standard protocol support into PocketQueue on port `:1883`, turning it into a **Triple-Protocol Local Broker** (`:9092` Kafka, `:6379` Redis Streams, `:1883` MQTT) for IoT/Edge sensor data ingestion and cross-protocol event distribution.

---

## Architecture & Dependency Graph

```text
MQTT Packet Codec (packet.go)
    │
    ├── Topic Matching Engine (topic.go)
    │       │
    │       └── Client Session Manager (client.go)
    │               │
    │               └── Protocol Handler & Server (handler.go, server.go)
    │                       │
    │                       ├── Ingress -> broker.Service.Produce()
    │                       └── Egress <- broker.EventBus -> MQTT Subscribers
    │                               │
    │                               └── CLI Integration (config.go, main.go)
    │                                       │
    │                                       └── Cross-Protocol Compatibility & E2E Suites
```

---

## Detailed Task Breakdown

### Phase 1: Foundation & Packet Codec Engine

#### Task 1: MQTT v3.1.1 Binary Packet Codec
- **Description:** Implement pure Go encoder and decoder for MQTT v3.1.1 control packets, handling fixed header type/flags and variable remaining length (1–4 bytes continuation bit algorithm).
- **Acceptance criteria:**
  - [ ] Correctly encode and decode remaining length (0, 127, 128, 16383, 16384, 2097151, 268435455).
  - [ ] Support packet types: `CONNECT`, `CONNACK`, `PUBLISH` (QoS 0 and 1), `PUBACK`, `SUBSCRIBE`, `SUBACK`, `UNSUBSCRIBE`, `UNSUBACK`, `PINGREQ`, `PINGRESP`, `DISCONNECT`.
  - [ ] Reject packets exceeding max size limit (16 MB) or containing malformed headers.
  - [ ] Zero external dependencies; 100% pure Go standard library (`io`, `bytes`, `encoding/binary`).
- **Verification:**
  - [ ] `go test -v ./internal/features/protocol/mqtt -run TestPacket`
- **Dependencies:** None
- **Files likely touched:**
  - `internal/features/protocol/mqtt/packet.go`
  - `internal/features/protocol/mqtt/packet_test.go`
- **Estimated scope:** Medium (2 files, ~400 lines)

#### Task 2: MQTT Topic Filter & Wildcard Engine
- **Description:** Implement topic validation and matching engine supporting exact match, single-level wildcard (`+`), and multi-level wildcard (`#`) according to MQTT v3.1.1 Section 4.7 specifications.
- **Acceptance criteria:**
  - [ ] `MatchTopic(filter, topic)` returns true for exact matches (e.g. `factory/temp` matches `factory/temp`).
  - [ ] Single-level wildcard `+` matches exactly one level (e.g. `sensors/+/temp` matches `sensors/chiller/temp`, but not `sensors/building1/floor2/temp`).
  - [ ] Multi-level wildcard `#` matches current and all deeper levels (e.g. `factory/#` matches `factory`, `factory/line1`, `factory/line1/sensor1`).
  - [ ] Validates topic syntax (no `#` in the middle of a topic filter, non-empty topic segments).
- **Verification:**
  - [ ] `go test -v ./internal/features/protocol/mqtt -run TestTopic`
- **Dependencies:** None (can be built in parallel with Task 1)
- **Files likely touched:**
  - `internal/features/protocol/mqtt/topic.go`
  - `internal/features/protocol/mqtt/topic_test.go`
- **Estimated scope:** Small (2 files, ~150 lines)

🛑 **Checkpoint 1: Codec & Topic Engine Verified**
- [ ] All unit tests pass in `internal/features/protocol/mqtt`
- [ ] 0 compiler warnings and zero third-party dependencies

---

### Phase 2: Client Session & Protocol Server

#### Task 3: Client Session & Subscription State Manager
- **Description:** Manage per-client TCP connection state, credentials, ClientID, keepalive timer, registered topic subscriptions, and serialized connection writing.
- **Acceptance criteria:**
  - [ ] Track active client state: `net.Conn`, `ClientID`, `CleanSession`, `KeepAlive`, `Subscriptions` list.
  - [ ] Thread-safe `WritePacket(pkt Packet) error` with a mutex preventing interleaved binary packet corruption.
  - [ ] Non-blocking channel buffers preventing slow client stalls on the broker.
- **Verification:**
  - [ ] Unit tests in `internal/features/protocol/mqtt/client_test.go` or `server_test.go`
- **Dependencies:** Task 1, Task 2
- **Files likely touched:**
  - `internal/features/protocol/mqtt/client.go`
- **Estimated scope:** Small (1 file, ~120 lines)

#### Task 4: Protocol Handler, TCP Server & EventBus Ingress/Egress Pump
- **Description:** Implement TCP listener, packet read loop, protocol lifecycle handlers, and bidirectional bridge to `broker.Service` and `broker.EventBus`.
- **Acceptance criteria:**
  - [ ] `CONNECT`: Validate protocol name `"MQTT"` and protocol level `4`, respond with `CONNACK (0x00)`.
  - [ ] `PUBLISH`: Call `broker.Service.Produce(ctx, topic, 0, key, payload, headers)`. If QoS 1, return `PUBACK` with matching packet ID.
  - [ ] `SUBSCRIBE`: Add topic filter to client subscriptions, return `SUBACK` with granted QoS.
  - [ ] `UNSUBSCRIBE`: Remove topic filter, return `UNSUBACK`.
  - [ ] `PINGREQ`: Respond with `PINGRESP`.
  - [ ] `DISCONNECT`: Gracefully terminate connection.
  - [ ] EventBus Egress Pump: Subscribe to `broker.EventBus`. When messages are produced (from Kafka, Redis, HTTP, or MQTT), fan out matching messages as `PUBLISH` packets to active subscribers.
  - [ ] Graceful shutdown draining all client connections on `Close()`.
- **Verification:**
  - [ ] `go test -v ./internal/features/protocol/mqtt -run TestServer`
- **Dependencies:** Task 1, Task 2, Task 3
- **Files likely touched:**
  - `internal/features/protocol/mqtt/handler.go`
  - `internal/features/protocol/mqtt/server.go`
  - `internal/features/protocol/mqtt/server_test.go`
- **Estimated scope:** Medium (3 files, ~450 lines)

🛑 **Checkpoint 2: Server & Handler Lifecycle Verified**
- [ ] 100% unit tests pass across all MQTT server scenarios
- [ ] Connection handshake, publish QoS 0/1, keepalive, and subscription fan-out verified

---

### Phase 3: CLI & Broker Wiring

#### Task 5: CLI Flag `--mqtt-port` & Configuration
- **Description:** Add `--mqtt-port` configuration flag (default `1883`, set to `0` to disable) and environment variable support.
- **Acceptance criteria:**
  - [ ] CLI accepts `--mqtt-port <port>`, defaults to `1883`. Setting `--mqtt-port 0` disables MQTT server.
  - [ ] Unit tests verify default value and custom port parsing.
- **Verification:**
  - [ ] `go test -v ./cmd/pocketqueue -run TestConfig`
- **Dependencies:** None
- **Files likely touched:**
  - `cmd/pocketqueue/config.go`
  - `cmd/pocketqueue/config_test.go`
- **Estimated scope:** Small (2 files, ~40 lines)

#### Task 6: Main Bootstrapping & Graceful Drain
- **Description:** Wire and start `mqtt.Server` in `cmd/pocketqueue/main.go` alongside Kafka, Redis, and HTTP servers, with graceful shutdown draining.
- **Acceptance criteria:**
  - [ ] In headless and TUI modes, initialize and start `mqtt.NewServer(addr, svc, bus, logger)` if `cfg.MQTTPort > 0`.
  - [ ] Register MQTT server in graceful shutdown handler to close cleanly on SIGINT/SIGTERM.
  - [ ] Update console banner and log messages to show triple-protocol: `Kafka:127.0.0.1:9092 Redis:127.0.0.1:6379 MQTT:127.0.0.1:1883`.
- **Verification:**
  - [ ] `go test -v ./cmd/pocketqueue`
- **Dependencies:** Task 4, Task 5
- **Files likely touched:**
  - `cmd/pocketqueue/main.go`
- **Estimated scope:** Small (1 file, ~50 lines)

🛑 **Checkpoint 3: CLI & Server Wiring Verified**
- [ ] PocketQueue binary boots with Kafka, Redis, and MQTT listening concurrently
- [ ] Graceful shutdown terminates all 3 protocol listeners without leaks

---

### Phase 4: Cross-Protocol Compatibility, E2E Verification & Audit

#### Task 7: Protocol Compatibility Test Suite
- **Description:** Implement end-to-end compatibility suite testing client protocol behaviors with standard raw packet sequences.
- **Acceptance criteria:**
  - [ ] Test client handshake, Clean Session, KeepAlive PINGREQ/PINGRESP, QoS 0 and QoS 1 publish, multi-topic subscribe, and disconnect.
- **Verification:**
  - [ ] `go test -v ./test/compatibility -run TestMQTT`
- **Dependencies:** Task 6
- **Files likely touched:**
  - `test/compatibility/mqtt_compat_test.go`
- **Estimated scope:** Small (1 file, ~200 lines)

#### Task 8: Triple-Protocol Cross-Ingress E2E Suite
- **Description:** Implement comprehensive E2E tests validating bidirectional event flow across Kafka, Redis, HTTP, and MQTT.
- **Acceptance criteria:**
  - [ ] Scenario 1: MQTT client publishes to `sensors/temp` -> Kafka consumer fetches event at `:9092`.
  - [ ] Scenario 2: MQTT client publishes to `iot/metrics` -> Redis client reads event via `XREAD` at `:6379`.
  - [ ] Scenario 3: Kafka producer publishes to `factory/alarms` -> MQTT subscriber receives `PUBLISH` packet at `:1883`.
  - [ ] Scenario 4: MQTT client publishes to `telemetry` -> HTTP SSE stream (`GET /api/v1/topics/_all/sse`) delivers event.
- **Verification:**
  - [ ] `go test -v ./test/e2e -run TestMQTT`
- **Dependencies:** Task 6, Task 7
- **Files likely touched:**
  - `test/e2e/mqtt_ingress_e2e_test.go`
- **Estimated scope:** Medium (1 file, ~300 lines)

#### Task 9: Documentation in README.md & Resource Audit
- **Description:** Document Workflow 9 (MQTT IoT Ingress Bridge) in `README.md`, update CLI flags table, and run `scripts/audit.ps1`.
- **Acceptance criteria:**
  - [ ] Add "Workflow 9: MQTT v3.1.1 IoT Protocol Ingress Bridge" in `README.md` with Mosquitto CLI and Python Paho examples.
  - [ ] Add `--mqtt-port` to CLI Flag Reference table.
  - [ ] Verify resource constraints via `scripts/audit.ps1`:
    - Binary size $< 15.0\text{ MB}$
    - Idle RAM $< 25.0\text{ MB}$
    - Cold boot $< 5.0\text{ ms}$
  - [ ] Full regression pass across all packages: `go test -count=1 ./...`
- **Verification:**
  - [ ] `powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1`
  - [ ] `go test -count=1 ./...`
- **Dependencies:** Task 8
- **Files likely touched:**
  - `README.md`
- **Estimated scope:** Small (1 file, ~60 lines)

🛑 **Checkpoint 4: Milestone 10 Complete**
- [ ] 100% test pass rate across all packages
- [ ] Triple-protocol broker verified and documented

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Malformed or giant MQTT packet crashes broker or exhausts memory | High | Enforce strict max packet size (16 MB) and validate remaining length bounds during decode. Fail closed on malformed packets. |
| Slow MQTT subscriber stalls EventBus broadcasts | High | Non-blocking write channel with bounded buffer (256 packets) per client session; drop or disconnect unresponsive client without blocking broker. |
| Topic separator differences (MQTT `/` vs Kafka `.`) | Medium | PocketQueue storage engine accepts any valid string as topic; MQTT topics like `sensors/temp` are preserved verbatim as native topic names. |
| Port collision with system Mosquitto on `:1883` | Low | Configurable `--mqtt-port` flag; tests bind to random ephemeral port (`127.0.0.1:0`). |
