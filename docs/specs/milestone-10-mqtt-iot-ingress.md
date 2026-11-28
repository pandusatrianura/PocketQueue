# Spec: Milestone 10 — MQTT v3.1.1 IoT Protocol Ingress Bridge (:1883)

## 1. Objective

Transform PocketQueue into a **Triple-Protocol Local Broker** by adding native support for the **MQTT v3.1.1** standard protocol on port `1883` (`--mqtt-port 1883`), alongside the **Kafka Wire Protocol** (`:9092`) and **Redis Streams RESP** (`:6379`).

In Internet of Things (IoT), Edge computing, robotics, and embedded development, devices (e.g. ESP32, Raspberry Pi, Arduino, automotive microcontrollers, and edge gateways) communicate over resource-constrained networks using lightweight MQTT control packets rather than heavyweight Kafka TCP or HTTP REST.

### Core Capabilities:
1. **Zero-Dependency MQTT Protocol Engine**: Pure Go binary packet decoder for MQTT v3.1.1 control packets without external dependencies (no Eclipse Paho server or CGO).
2. **Cross-Protocol Ingress & Egress**:
   - Messages published by MQTT clients (`paho-mqtt`, `mqtt.js`, Mosquitto CLI) to topics like `sensors/factory/temp` are instantly appended to PocketQueue partitions and can be consumed simultaneously via:
     - Kafka Consumers (`IBM/sarama`, `confluent-kafka-python`)
     - Redis Streams (`XREAD`, `XREADGROUP`)
     - Web UI Dashboard & Real-Time SSE Stream (`GET /api/v1/topics/_all/sse`)
     - Charm Terminal UI (TUI)
   - Conversely, messages published via Kafka, Redis, or Web UI to topic `sensors/factory/temp` are fanned out in real time to connected MQTT subscribers!
3. **MQTT v3.1.1 Packet Support**:
   - `CONNECT` & `CONNACK`: Client handshake, clean session handling, keepalive timer.
   - `PUBLISH` & `PUBACK`: QoS 0 (fire-and-forget) and QoS 1 (at-least-once delivery with packet identifier ACK).
   - `SUBSCRIBE` & `SUBACK`: Topic subscription with exact topic matching and MQTT wildcards (`+` single-level, `#` multi-level).
   - `UNSUBSCRIBE` & `UNSUBACK`: Topic unsubscription.
   - `PINGREQ` & `PINGRESP`: Keepalive heartbeat probe and response.
   - `DISCONNECT`: Clean client shutdown.
4. **Resilience & Resource Limits**:
   - Zero allocs on idle connections, non-blocking broadcasts.
   - Respects PocketQueue's strict budget: Binary $< 15.0\text{ MB}$, Idle RAM $< 25.0\text{ MB}$, Boot $< 5.0\text{ ms}$.

---

## 2. Tech Stack & Commands

- **Language**: Pure Go 1.24+ standard library (`net`, `io`, `sync`, `bytes`, `encoding/binary`, `log/slog`).
- **Default Port**: `:1883` (configurable via `--mqtt-port <port>`, set to `0` to disable).
- **Default Host**: `127.0.0.1` (configurable via `--host <host>`).

### Commands:
```bash
# Build binary
go build -ldflags="-s -w" -o bin/pocketqueue ./cmd/pocketqueue

# Run PocketQueue with MQTT enabled on port 1883 (enabled by default with :1883)
./bin/pocketqueue --mqtt-port 1883 --port 9092 --redis-port 6379 --http-port 8080

# Run all unit tests
go test -v ./internal/features/protocol/mqtt/...

# Run compatibility & E2E tests
go test -v ./test/compatibility -run TestMQTT
go test -v ./test/e2e -run TestMQTT
```

---

## 3. MQTT v3.1.1 Protocol Specification & Packet Architecture

MQTT packets consist of:
1. **Fixed Header** (2 to 5 bytes):
   - Byte 1: Packet Type (bits 7-4), Flags: DUP (bit 3), QoS level (bits 2-1), RETAIN (bit 0).
   - Bytes 2-5: Variable-length Remaining Length (1 to 4 bytes using MQTT 7-bit continuation encoding).
2. **Variable Header** (packet-type specific):
   - `CONNECT`: Protocol Name (`"MQTT"`), Protocol Level (`4`), Connect Flags (Clean Session, Will, Username, Password), Keep Alive (uint16).
   - `CONNACK`: Session Present flag (byte 1), Connect Return Code (byte 2: 0 = Connection Accepted).
   - `PUBLISH`: Topic Name (string), Packet Identifier (uint16, only for QoS > 0).
   - `PUBACK`: Packet Identifier (uint16).
   - `SUBSCRIBE`: Packet Identifier (uint16).
   - `SUBACK`: Packet Identifier (uint16), Return Codes (granted QoS levels).
   - `UNSUBSCRIBE`: Packet Identifier (uint16).
   - `UNSUBACK`: Packet Identifier (uint16).
   - `PINGREQ` / `PINGRESP` / `DISCONNECT`: No variable header.
3. **Payload**:
   - `CONNECT`: Client ID, Will Topic/Msg, Username, Password.
   - `PUBLISH`: Raw application data/bytes.
   - `SUBSCRIBE`: List of Topic Filters + Requested QoS.
   - `UNSUBSCRIBE`: List of Topic Filters.

### Topic Filter Wildcard Matching:
- **Exact Match**: `finance/stocks` matches `finance/stocks`.
- **Single-Level Wildcard (`+`)**: `finance/+/price` matches `finance/GOOG/price` and `finance/AAPL/price`, but not `finance/GOOG/options/price`.
- **Multi-Level Wildcard (`#`)**: `sensors/#` matches `sensors`, `sensors/temp`, `sensors/temp/room1/celsius`.

---

## 4. Project Structure

```text
internal/features/protocol/mqtt/
  packet.go                # Binary encoder & decoder for MQTT v3.1.1 fixed header & packets
  packet_test.go           # Unit tests for packet serialization, variable length, and malformed frames
  topic.go                 # Topic matching logic (exact, +, # wildcards)
  topic_test.go            # Unit tests for MQTT topic filtering and wildcards
  client.go                # Client session state (connection, clientId, subscriptions, write mutex)
  handler.go               # Protocol handler: CONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT
  server.go                # TCP listener, graceful connection draining, event bus pump
  server_test.go           # Unit tests for end-to-end MQTT client-server interaction

test/compatibility/
  mqtt_compat_test.go      # Verified compatibility with standard MQTT packet flows

test/e2e/
  mqtt_ingress_e2e_test.go # Multi-protocol E2E: MQTT Publish -> Kafka Fetch & Redis XREAD & HTTP SSE
```

---

## 5. Code Style & Architecture Conventions

- **Pure Go**: No external CGO or third-party MQTT broker libraries.
- **I/O Isolation**: Network operations are separated from packet decoding (`packet.go` has pure decode/encode functions with no socket references).
- **Concurrency & Resource Safety**:
  - Each client connection runs a read loop goroutine.
  - Subscriptions subscribe to `broker.EventBus`.
  - Non-blocking client write channels or per-client mutex on `net.Conn` write to prevent slow consumer stalls.
  - Bounded buffers to guarantee memory footprint remains $< 25\text{ MB}$.
- **Logging**: Structured `log/slog` for connection events, errors, and disconnections with `clientId` and `remoteAddr`.

```go
// Example Idiomatic Packet Decoding Pattern
func DecodePacket(r io.Reader) (Packet, error) {
    var header [2]byte
    if _, err := io.ReadFull(r, header[:1]); err != nil {
        return nil, err
    }
    pktType := header[0] >> 4
    flags := header[0] & 0x0F

    remLen, err := decodeRemainingLength(r)
    if err != nil {
        return nil, err
    }

    body := make([]byte, remLen)
    if _, err := io.ReadFull(r, body); err != nil {
        return nil, err
    }

    return parsePacketBody(pktType, flags, body)
}
```

---

## 6. Testing Strategy

1. **Unit Tests (`internal/features/protocol/mqtt/packet_test.go`)**:
   - Variable length decoding boundary tests (0, 127, 128, 16383, 16384, 2097151, 268435455).
   - Exact binary round-trip tests for `CONNECT`, `CONNACK`, `PUBLISH` (QoS 0 and 1), `PUBACK`, `SUBSCRIBE`, `SUBACK`, `PINGREQ`, `PINGRESP`, `DISCONNECT`.
   - Handling of malformed, truncated, and oversized packets.
2. **Topic Filter Unit Tests (`topic_test.go`)**:
   - Exact matching, `+` wildcard, `#` wildcard, invalid filter syntax.
3. **Server Integration Tests (`server_test.go`)**:
   - Client connection lifecycle: `CONNECT` -> `CONNACK`.
   - Keepalive timeout and `PINGREQ` -> `PINGRESP`.
   - `PUBLISH` QoS 0 and QoS 1 -> `PUBACK`.
   - `SUBSCRIBE` -> broadcast receive -> `PUBLISH`.
4. **Cross-Protocol E2E Tests (`test/e2e/mqtt_ingress_e2e_test.go`)**:
   - Publish via MQTT -> Fetch via Kafka wire protocol.
   - Publish via MQTT -> Read via Redis Streams `XREAD`.
   - Publish via Kafka -> Receive on MQTT subscriber.
   - Publish via MQTT -> Receive on HTTP SSE stream (`/api/v1/topics/_all/sse`).

---

## 7. Boundaries

- **Always**:
  - Run all tests (`go test -count=1 ./...`) and lint checks before completing any phase.
  - Return proper protocol error codes on protocol violations rather than panicking.
  - Enforce max packet size limits (default 16 MB, matching Kafka and HTTP).
- **Ask First**:
  - Any change to existing Kafka or Redis protocol interfaces.
  - Adding any external third-party Go package dependency.
- **Never**:
  - Block the broker service or event bus when an MQTT client is slow.
  - Mutate stored payload bytes.
  - Exceed resource constraints ($< 15\text{ MB}$ binary, $< 25\text{ MB}$ RAM, $< 5\text{ ms}$ startup).

---

## 8. Success Criteria

- [ ] MQTT v3.1.1 server listens on `--mqtt-port` (default `1883`, `0` to disable).
- [ ] Clients can connect with `CONNECT`, receive `CONNACK (0x00)`.
- [ ] Clients can publish with `PUBLISH` (QoS 0 and QoS 1), receiving `PUBACK` on QoS 1.
- [ ] Published MQTT messages are stored in broker storage under the MQTT topic name (partition 0).
- [ ] Connected subscribers with matching topic filters (exact, `+`, `#`) receive fanned-out messages.
- [ ] Bidirectional cross-protocol interop verified with Kafka, Redis, HTTP SSE, and TUI.
- [ ] 100% test pass rate across all packages.
- [ ] Resource audit satisfied: Binary $< 15.0\text{ MB}$, Idle RAM $< 25.0\text{ MB}$, Boot $< 5.0\text{ ms}$.

---

## 9. Assumptions & Open Questions

### Assumptions:
1. MQTT v3.1.1 is the primary target standard (widely supported by 99% of IoT devices and libraries). MQTT v5.0 can be considered in a future milestone.
2. QoS 0 and QoS 1 are supported; QoS 2 requests are downgraded to QoS 1 or accepted as QoS 1 in `SUBACK`.
3. PocketQueue broker partitions default to 0 for MQTT published messages (MQTT has no concept of partitions). Key is optionally derived from ClientID or empty.
4. Clean Session = 1 (ephemeral sessions) is the default mode for local development.

### Questions for the human engineer:
- None blocking. We are ready to proceed to Phase 2: Plan.
