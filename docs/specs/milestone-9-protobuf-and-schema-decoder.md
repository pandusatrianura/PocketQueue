# Spec: Milestone 9 — Protobuf & Confluent Wire Format Payload Decoder

## 1. Objective

Enable PocketQueue to automatically detect, decode, and visualize binary-encoded messages in real time across the **Terminal UI (TUI)**, **Embedded Web Dashboard**, and **Server-Sent Events (SSE)** stream.

In enterprise Kafka architectures, messages are predominantly serialized as binary **Protobuf** or **Avro** wrapped in **Confluent Schema Registry Wire Format** rather than plain JSON. Currently, inspecting binary payloads in local dev environments yields garbled control characters or broken JSON parsing errors.

PocketQueue Milestone 9 solves this by introducing a **Universal Payload Decoder**:
1. **Confluent Wire Format Auto-Detection**: Identifies magic byte `0x00` and extracts the 4-byte Schema ID.
2. **Dynamic Protobuf Deserialization**: Accepts compiled FileDescriptorSet files (`--proto-desc <path>`) to dynamically decode binary protobuf records into human-readable JSON without compiling code.
3. **Structured Hex Dump Fallback**: When no schema matches or payload is raw binary, renders an aligned, formatted Hex & ASCII dump (like `hexdump -C`) with metadata badges instead of corrupted characters.
4. **Unified Multi-Interface Display**: Surfaces decoded payload and schema metadata in TUI Inspector (`Enter`), Web UI Inspector modal, and HTTP REST/SSE streams.

---

## 2. Assumptions & Architecture Boundaries

### Assumptions Made:
1. **Dynamic Deserialization via Protobuf FileDescriptorSet (`.desc`)**:
   - Developers compile their `.proto` files using standard `protoc --descriptor_set_out=schemas.desc --include_imports *.proto`.
   - PocketQueue loads the binary descriptor set at startup via `--proto-desc <path>`.
2. **Confluent Schema Registry Wire Format Specification**:
   - Byte 0: `0x00` (Magic Byte).
   - Bytes 1–4: `uint32` (Big-Endian Schema ID).
   - Bytes 5+: For Protobuf, an array of varint indexes identifying the root message type, followed by the protobuf serialized payload.
3. **Zero-Overhead Transparent Fallback**:
   - If a payload is already valid UTF-8 JSON or plain text, it is passed through as-is with zero decoding overhead.
   - If dynamic decoding fails or no matching descriptor is found, the payload is presented as a clean 16-byte-per-line hex dump with ASCII sidebar.
4. **Resource & Performance Invariants**:
   - Binary size must remain $< 15.0\text{ MB}$ (current: 8.85 MB).
   - Idle RAM must remain $< 25.0\text{ MB}$ (current: 8.45 MB).
   - Cold start latency must remain $< 20.0\text{ ms}$ (current: 2.17 ms).

---

## 3. Tech Stack & Dependencies

- **Language**: Go 1.22+ (`go.mod`)
- **Official Protobuf Library**: `google.golang.org/protobuf`
  - `reflect/protodesc`: Parse `FileDescriptorSet` bytes into `protoregistry.Files`.
  - `reflect/protoreflect`: Inspect message descriptors and field definitions dynamically.
  - `types/dynamicpb`: Instantiate and unmarshal dynamic messages from bytes at runtime without code generation.
  - `encoding/protojson`: Format dynamic protobuf messages to indented, valid JSON.
- **TUI Rendering**: Charm `bubbletea`, `lipgloss`
- **Web UI Rendering**: Native JavaScript DOM & embedded CSS (no npm/bundler)

---

## 4. Commands

- **Build PocketQueue**:
  ```bash
  go build -ldflags="-s -w" -o bin/pocketqueue ./cmd/pocketqueue
  ```
- **Run with Protobuf Descriptor Set**:
  ```bash
  ./bin/pocketqueue --proto-desc ./test/fixtures/schemas.desc --http-port 8080
  ```
- **Run in Demo Mode with Protobuf Payloads**:
  ```bash
  ./bin/pocketqueue --demo --proto-desc ./test/fixtures/schemas.desc --http-port 8080
  ```
- **Compile `.proto` to Descriptor Set for Testing**:
  ```bash
  protoc --descriptor_set_out=test/fixtures/schemas.desc --include_imports test/fixtures/order.proto
  ```
- **Run Unit and Compatibility Tests**:
  ```bash
  go test -v ./internal/features/decoder/...
  go test -v ./test/e2e -run TestProtobufDecoder
  ```
- **Audit Resource Budget**:
  ```powershell
  powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1
  ```

---

## 5. Project Structure & Vertical Slice Layout

Following our testability-first vertical slice convention:

```text
internal/features/decoder/
  decoder.go               # Universal PayloadDecoder interface and detection cascade
  confluent.go             # Confluent wire format parsing (magic byte, schema ID, index varints)
  protobuf.go              # Dynamic Protobuf loader (FileDescriptorSet) and dynamicpb deserializer
  hexdump.go               # Clean formatted 16-byte hex dump generator with ASCII representation
  decoder_test.go          # Unit tests for JSON passthrough, Confluent parsing, dynamic PB, and hex dump
test/fixtures/
  order.proto              # Reference protobuf schema for testing
  schemas.desc             # Compiled binary FileDescriptorSet fixture
cmd/pocketqueue/
  config.go                # Added --proto-desc flag
  main.go                  # Wire decoder into broker, TUI, and HTTP servers
internal/features/tui/
  app.go                   # Inspector modal renders decoded protobuf JSON & schema badges
internal/features/protocol/http/
  handler.go               # REST & SSE stream include decoded_value and schema_id metadata
  ui/assets/app.js         # Web modal displays decoded JSON with "[Protobuf: OrderEvent #1]" badge
test/e2e/
  protobuf_decoder_e2e_test.go # E2E tests: produce wire-format PB -> verify TUI & Web SSE output
```

---

## 6. Code Style & Technical Implementation Details

### A. Universal Decoded Result Contract
```go
package decoder

// PayloadKind describes how the payload was classified and decoded.
type PayloadKind string

const (
	KindJSON      PayloadKind = "json"
	KindText      PayloadKind = "text"
	KindProtobuf  PayloadKind = "protobuf"
	KindConfluent PayloadKind = "confluent_protobuf"
	KindHexDump   PayloadKind = "hex_dump"
)

// DecodedPayload contains the human-readable representation and schema metadata.
type DecodedPayload struct {
	Kind         PayloadKind `json:"kind"`
	SchemaID     uint32      `json:"schema_id,omitempty"`
	MessageName  string      `json:"message_name,omitempty"`
	Formatted    string      `json:"formatted"`
	IsBinary     bool        `json:"is_binary"`
	RawBytes     int         `json:"raw_bytes"`
}
```

### B. Auto-Detection Cascade
The decoder evaluates payloads in a deterministic, strict sequence:
1. **Empty Check**: If length == 0, return empty string.
2. **Valid JSON Check**: If payload begins with `{` or `[` and unmarshals cleanly as JSON, return `KindJSON` (pretty-printed).
3. **Confluent Wire Format Check**:
   - If `payload[0] == 0x00` and `len(payload) >= 5`:
     - Extract `SchemaID = binary.BigEndian.Uint32(payload[1:5])`.
     - Read message index array (varints).
     - Extract remaining protobuf binary slice.
     - Look up descriptor in loaded `DescriptorRegistry`.
     - If matching message descriptor found: `dynamicpb.NewMessage(desc)` $\to$ `proto.Unmarshal` $\to$ `protojson.Format` $\to$ return `KindConfluent`.
     - If schema not found: return `KindConfluent` with Schema ID badge and formatted hex dump of the payload slice.
4. **Raw Protobuf Check**:
   - If `--proto-desc` was supplied:
     - Iterate registered message types to find a schema that unmarshals cleanly without error or unconsumed trailing bytes.
     - On match, format via `protojson.Format` $\to$ return `KindProtobuf`.
5. **UTF-8 Plain Text Check**:
   - If `utf8.Valid(payload)` and contains printable characters, return `KindText`.
6. **Hex Dump Fallback**:
   - Render aligned 16-byte hex dump with offset and ASCII column $\to$ return `KindHexDump`.

---

## 7. Testing Strategy

1. **Unit Tests (`internal/features/decoder/`)**:
   - `TestDecoder_JSONPassthrough`: Ensure valid JSON is formatted cleanly with zero mutation.
   - `TestDecoder_ConfluentWireFormat_Header`: Test extraction of 4-byte Schema ID and index varints.
   - `TestDecoder_DynamicProtobuf_Unmarshal`: Unmarshal compiled `.desc` fixture and verify all proto fields (strings, ints, nested messages, enums).
   - `TestDecoder_HexDump`: Verify formatting, padding, alignment, and unprintable byte dots (`.`).
2. **End-to-End Tests (`test/e2e/protobuf_decoder_e2e_test.go`)**:
   - Start PocketQueue with `--proto-desc test/fixtures/schemas.desc`.
   - Publish Confluent-encoded binary protobuf message via Kafka producer.
   - Verify `GET /api/v1/topics/_all/sse` outputs decoded JSON and schema ID.
   - Verify Web UI and TUI inspector display decoded fields instead of binary noise.
3. **Resource & Invariant Verification**:
   - Verify binary size $< 15.0\text{ MB}$ after adding `google.golang.org/protobuf`.
   - Verify idle RAM $< 25.0\text{ MB}$ and cold boot $< 20.0\text{ ms}`.

---

## 8. Boundaries

- **Always Do**:
  - Abstract decoder behind `PayloadDecoder` interface for testability.
  - Handle malformed binary payloads gracefully without panic or memory leaks.
  - Sanitize ASCII output in hex dumps (replace control characters with `.`).
- **Ask First**:
  - Adding external Schema Registry HTTP client polling (e.g. connecting to remote `http://schema-registry:8081`).
  - Adding Apache Avro binary decoder (Milestone 9 focuses on Protobuf + Confluent Wire Format).
- **Never Do**:
  - Panic on corrupted protobuf payloads or out-of-bounds varint offsets.
  - Allocate unbounded memory on oversized binary payloads (enforce 16 MB max body limit).
  - Mutate original message bytes stored in WAL or MemoryEngine (decoding is purely visual/read-time).

---

## 9. Success Criteria

- [ ] Command `./bin/pocketqueue --proto-desc <file.desc>` accepts and loads valid protobuf descriptor sets.
- [ ] Confluent wire format (`0x00` + 4-byte ID) automatically extracts Schema ID.
- [ ] Protobuf payloads matching the descriptor set decode into indented JSON.
- [ ] Fallback for unrecognized binary generates a clean 16-byte aligned Hex/ASCII dump.
- [ ] TUI inspector (`Enter`) and Web UI modal display decoded payload with `[Protobuf: <Type> (#<ID>)]` badge.
- [ ] Stripped binary size $< 15.0\text{ MB}$, idle RAM $< 25.0\text{ MB}$, cold boot $< 20.0\text{ ms}$.
- [ ] 100% test pass rate across all packages with zero regressions.
