# Spec: Milestone 10 — MQTT v3.1.1 IoT Protocol Ingress Bridge (:1883)

## 1. Objective

Transform PocketQueue into a **Triple-Protocol Local Broker** by adding native support for the **MQTT v3.1.1** standard protocol on port `1883` (`--mqtt-port 1883`), alongside **Kafka Wire Protocol** (`:9092`) and **Redis Streams RESP** (`:6379`).

In Internet of Things (IoT), Edge computing, and embedded development, devices (e.g. ESP32, Raspberry Pi, automotive sensors, smart home gateways) communicate using lightweight MQTT packets rather than heavyweight Kafka TCP or HTTP REST.

### Core Capabilities:
1. **Zero-Dependency MQTT Engine**: Pure Go binary packet decoder for MQTT v3.1.1 control packets without external dependencies (no Eclipse Paho server or CGO).
2. **Cross-Protocol Ingress**: Messages published by MQTT clients (`paho-mqtt`, `mqtt.js`, Mosquitto CLI) to topics like `sensors/factory/temp` are instantly appended to PocketQueue partitions and can be consumed simultaneously via:
   - Kafka Consumers (`IBM/sarama`, `confluent-kafka-python`)
   - Redis Streams (`XREAD`, `XREADGROUP`)
   - Web UI Dashboard & Real-Time SSE Stream (`GET /api/v1/topics/_all/sse`)
   - Charm Terminal UI (TUI)
3. **IoT QoS 0 & QoS 1 Reliability**: Support for `PUBLISH`, `PUBACK`, `PINGREQ`/`PINGRESP` keepalive, and `SUBSCRIBE` fan-out.

---

## 2. Tech Stack & Commands

- **Language**: Pure Go standard library (`net`, `io`, `sync`)
- **Port**: Default `:1883` (configurable via `--mqtt-port <port>`, `0` to disable)
- **Run Command**:
  ```bash
  ./bin/pocketqueue --mqtt-port 1883 --port 9092 --http-port 8080
  ```
- **Test with Mosquitto CLI / Standard Clients**:
  ```bash
  # Publish sensor telemetry via MQTT
  mosquitto_pub -h 127.0.0.1 -p 1883 -t "factory/temp" -m '{"sensor_id":"esp32-1","celsius":23.8}'

  # Consume the same message via Kafka!
  ./bin/pocketqueue --topic "factory/temp"
  ```

---

## 3. Project Structure

```text
internal/features/protocol/mqtt/
  server.go                # TCP listener, client session manager, graceful drain
  packet.go                # MQTT v3.1.1 Fixed Header & Variable Header binary decoder/encoder
  handler.go               # Handles CONNECT, PUBLISH, SUBSCRIBE, PINGREQ, DISCONNECT
  server_test.go           # Unit tests with raw byte packets and concurrent sessions
test/compatibility/
  mqtt_compat_test.go      # Verified compatibility with standard MQTT clients
test/e2e/
  mqtt_ingress_e2e_test.go # E2E: MQTT publish -> Kafka fetch & Web SSE stream
```

---

## 4. Resource & Footprint Targets

- Single binary size increase: $< 250\text{ KB}$ (well under the 15 MB budget).
- Idle RAM impact: $< 1.0\text{ MB}$.
- Startup latency: $< 3\text{ ms}$.
