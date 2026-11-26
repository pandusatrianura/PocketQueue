# Implementation Plan: Milestone 9 — Protobuf & Confluent Wire Format Payload Decoder

## 1. Overview

This implementation plan defines the step-by-step engineering tasks to deliver **Milestone 9 (Protobuf & Confluent Wire Format Payload Decoder)** for PocketQueue following strict **Test-Driven Development (TDD)** and **Vertical Slicing**.

PocketQueue will automatically detect, decode, and visually format binary Protobuf and Confluent Wire Format messages across the **TUI**, **Embedded Web Dashboard**, and **Server-Sent Events (SSE)** stream, with clean Hex Dump fallbacks when schemas are omitted or unrecognized.

---

## 2. Architecture Decisions & Dependency Graph

```text
                      [google.golang.org/protobuf in go.mod]
                                      │
           ┌──────────────────────────┴──────────────────────────┐
           ▼                                                     ▼
[internal/features/decoder/hexdump.go]       [internal/features/decoder/protobuf.go]
(Clean 16-byte aligned Hex+ASCII dump)       (FileDescriptorSet loader & dynamicpb)
           │                                                     │
           │                                 [internal/features/decoder/confluent.go]
           │                                 (Magic 0x00, Schema ID, varint indexes)
           │                                                     │
           └──────────────────────────┬──────────────────────────┘
                                      ▼
                      [internal/features/decoder/decoder.go]
                      (Universal cascade: JSON -> Confluent -> Raw PB -> Text -> HexDump)
                                      │
           ┌──────────────────────────┴──────────────────────────┐
           ▼                                                     ▼
[cmd/pocketqueue/config.go & main.go]          [internal/features/protocol/http/handler.go]
(--proto-desc flag, wired to broker)           (REST & SSE payload decoration)
           │                                                     │
           ▼                                                     ▼
[internal/features/tui/app.go]                 [internal/features/protocol/http/ui/assets/app.js]
(TUI Inspector with decoded PB badge)          (Web modal with decoded JSON & badge)
                                      │
                                      ▼
                      [test/e2e/protobuf_decoder_e2e_test.go]
                      (Full end-to-end Kafka produce -> TUI & Web SSE verification)
```

---

## 3. Detailed Task Breakdown

### Phase 1: Decoder Foundation & Core Engines

#### Task 1: Dependencies & Test Fixtures Setup
- **Description:** Add `google.golang.org/protobuf` to `go.mod` and generate test schema fixtures (`test/fixtures/order.proto` and compiled `schemas.desc`).
- **Acceptance criteria:**
  - `go.mod` and `go.sum` include `google.golang.org/protobuf`.
  - `test/fixtures/order.proto` defines `OrderEvent` with diverse types (string, int64, double, enum, nested message).
  - Pre-compiled `test/fixtures/schemas.desc` checked into repo.
- **Verification:** `go build ./...` passes without missing dependency errors.
- **Dependencies:** None
- **Files touched:**
  - `go.mod`
  - `go.sum`
  - `test/fixtures/order.proto`
  - `test/fixtures/schemas.desc`
- **Scope:** Small (S)

#### Task 2: Formatted Hex Dump Engine
- **Description:** Implement clean 16-byte aligned Hex Dump generator with offset and ASCII sidebar (`internal/features/decoder/hexdump.go`).
- **Acceptance criteria:**
  - Formats any binary byte slice into standard 16-byte rows: `00000000  48 65 6c 6c 6f 20 57 6f  72 6c 64 21 00 00 00 01  |Hello World!....|`.
  - Replaces non-printable control characters with `.`.
  - Properly pads incomplete final rows.
- **Verification:** `go test -v ./internal/features/decoder -run TestHexDump`
- **Dependencies:** Task 1
- **Files touched:**
  - `internal/features/decoder/hexdump.go`
  - `internal/features/decoder/hexdump_test.go`
- **Scope:** Small (S)

#### Task 3: Dynamic Protobuf & Confluent Wire Format Deserializer
- **Description:** Implement `FileDescriptorSet` dynamic schema loader and Confluent wire format parser in `internal/features/decoder/protobuf.go` and `confluent.go`.
- **Acceptance criteria:**
  - `LoadDescriptorSet(path string)` parses binary `.desc` and registers `protoreflect.MessageDescriptor` entries.
  - `ParseConfluentHeader(data []byte)` extracts Schema ID and remaining payload slice.
  - Dynamically unmarshals raw protobuf bytes into `dynamicpb.Message` and formats to JSON via `protojson.Format`.
- **Verification:** `go test -v ./internal/features/decoder -run TestProtobuf`
- **Dependencies:** Task 1
- **Files touched:**
  - `internal/features/decoder/protobuf.go`
  - `internal/features/decoder/confluent.go`
  - `internal/features/decoder/protobuf_test.go`
- **Scope:** Medium (M)

#### Task 4: Universal Payload Decoder Cascade
- **Description:** Implement the `PayloadDecoder` interface and auto-detection cascade (`decoder.go`).
- **Acceptance criteria:**
  - Valid JSON/Text returns formatted string with `KindJSON` or `KindText`.
  - Confluent binary returns `KindConfluent` with `SchemaID` and decoded JSON (or Hex Dump if schema missing).
  - Raw Protobuf returns `KindProtobuf` with `MessageName` and decoded JSON.
  - Arbitrary binary returns `KindHexDump`.
- **Verification:** `go test -v ./internal/features/decoder -run TestDecoderCascade`
- **Dependencies:** Task 2, Task 3
- **Files touched:**
  - `internal/features/decoder/decoder.go`
  - `internal/features/decoder/decoder_test.go`
- **Scope:** Small (S)

### 🛑 Checkpoint 1: Decoder Core Verification
- All tests in `internal/features/decoder` pass.
- No memory leaks or panic on corrupted binary data.

---

### Phase 2: CLI Integration & Protocol Server Wiring

#### Task 5: CLI Configuration & Broker Wiring
- **Description:** Add `--proto-desc` CLI flag in `cmd/pocketqueue/config.go` and initialize the decoder in `cmd/pocketqueue/main.go`.
- **Acceptance criteria:**
  - Flag `--proto-desc <path>` loads the file descriptor set at startup.
  - Logs loaded message descriptors at INFO level.
  - Passes decoder instance into HTTP server and TUI model.
- **Verification:** `go test -v ./cmd/pocketqueue`
- **Dependencies:** Task 4
- **Files touched:**
  - `cmd/pocketqueue/config.go`
  - `cmd/pocketqueue/main.go`
  - `cmd/pocketqueue/config_test.go`
- **Scope:** Small (S)

#### Task 6: HTTP REST & SSE Stream Protocol Decoration
- **Description:** Decorate HTTP REST endpoints and SSE stream with decoded payload metadata (`internal/features/protocol/http/handler.go`).
- **Acceptance criteria:**
  - SSE events output `decoded` object containing `kind`, `schema_id`, `message_name`, and `formatted`.
  - Preserves raw base64 or string value for backwards compatibility.
- **Verification:** `go test -v ./internal/features/protocol/http -run TestHTTP_SSE_DecodedPayload`
- **Dependencies:** Task 5
- **Files touched:**
  - `internal/features/protocol/http/handler.go`
  - `internal/features/protocol/http/server_test.go`
- **Scope:** Small (S)

### 🛑 Checkpoint 2: CLI & Protocol Verification
- `./bin/pocketqueue --proto-desc ./test/fixtures/schemas.desc --headless` starts cleanly.
- SSE stream emits decoded fields.

---

### Phase 3: Visual UI Integration (TUI & Web Dashboard)

#### Task 7: Charm TUI Inspector Integration
- **Description:** Update TUI Inspector view (`internal/features/tui/app.go`) to render decoded protobuf JSON and schema badges.
- **Acceptance criteria:**
  - Hitting `Enter` on a binary event displays formatted JSON with title badge `[Protobuf: OrderEvent (Schema #1)]`.
  - For unmapped binary, displays the aligned Hex Dump view.
  - Allows copying decoded text to clipboard via `y`.
- **Verification:** `go test -v ./internal/features/tui`
- **Dependencies:** Task 5
- **Files touched:**
  - `internal/features/tui/app.go`
  - `internal/features/tui/app_test.go`
- **Scope:** Small (S)

#### Task 8: Web UI Dashboard Inspector Modal
- **Description:** Update Web Dashboard (`app.js` and `index.html`) to display decoded Protobuf JSON and Schema ID badges in the inspect modal.
- **Acceptance criteria:**
  - Clicking on a binary event in the Live Stream table displays decoded JSON with `[Protobuf: <Type> (#<ID>)]` pill.
  - Raw binary displays formatted Hex Dump with monospaced styling.
- **Verification:** Inspect modal in web browser shows formatted JSON.
- **Dependencies:** Task 6
- **Files touched:**
  - `internal/features/protocol/http/ui/assets/app.js`
  - `internal/features/protocol/http/ui/assets/index.html`
- **Scope:** Small (S)

### 🛑 Checkpoint 3: UI Verification
- Both TUI and Web Dashboard render decoded messages with badges.
- Assets bundle remains $< 35\text{ KB}$ uncompressed.

---

### Phase 4: E2E Verification & Documentation

#### Task 9: End-to-End Test Suite
- **Description:** Write comprehensive E2E test (`test/e2e/protobuf_decoder_e2e_test.go`) validating end-to-end binary production and decoded consumption.
- **Acceptance criteria:**
  - Produces Confluent Wire Format Protobuf message to topic `orders.proto`.
  - Verifies SSE client receives decoded JSON with matching Schema ID and fields.
  - Verifies hex dump fallback for corrupted binary.
- **Verification:** `go test -v ./test/e2e -run TestProtobufDecoderE2E`
- **Dependencies:** Task 7, Task 8
- **Files touched:**
  - `test/e2e/protobuf_decoder_e2e_test.go`
- **Scope:** Medium (M)

#### Task 10: Documentation & Resource Budget Audit
- **Description:** Update `README.md` with Protobuf decoding workflow and run `audit.ps1` to verify performance constraints.
- **Acceptance criteria:**
  - `README.md` documents `--proto-desc` and Confluent Wire Format auto-detection.
  - Stripped binary size $< 15.0\text{ MB}$.
  - Idle RAM $< 25.0\text{ MB}$.
  - Cold boot $< 20.0\text{ ms}$.
- **Verification:** `powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1`
- **Dependencies:** Task 9
- **Files touched:**
  - `README.md`
- **Scope:** Small (S)

### 🛑 Checkpoint 4: Final Milestone 9 Completion
- All 13 packages pass with 100% test coverage.
- Ready for Git commit, push, and release tag `v0.9.0`.
