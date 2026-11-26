# Todo: Milestone 7 — HTTP REST Ingress, SSE Bridge, Webhook DLQ & Automated Release

## Phase 1: HTTP Server & REST Ingress (TDD)

- [x] **Task 1.1: Implement HTTP Protocol Server & Handler**
  - **Description**: Create `internal/features/protocol/http/handler.go` and `server.go` implementing `POST /api/v1/topics/{topic}` (supporting JSON and raw body payloads) and `GET /healthz`. Implement `Start()` and `CloseWithTimeout()`.
  - **Acceptance criteria**:
    - `POST /api/v1/topics/{topic}` parses payload, calls `broker.Produce`, and returns `201 Created` with offset metadata.
    - `GET /healthz` returns `200 OK` with JSON health status.
    - CORS headers (`Access-Control-Allow-Origin: *`) are returned.
  - **Verification**: `go test -v ./internal/features/protocol/http -run TestHTTPProduce`
  - **Files**: `internal/features/protocol/http/handler.go`, `internal/features/protocol/http/server.go`, `internal/features/protocol/http/server_test.go`
  - **Scope**: Medium (3 files)

- [x] **Task 1.2: Wire HTTP Server into CLI & Main Lifecycle**
  - **Description**: Add `--http-port` flag (default 0 / disabled) in `cmd/pocketqueue/config.go`. In `cmd/pocketqueue/main.go`, instantiate and start `http.Server` if `cfg.HTTPPort > 0`, and drain gracefully on shutdown.
  - **Acceptance criteria**:
    - `--http-port 0` leaves HTTP server inactive.
    - `--http-port 8080` binds HTTP server on configured port.
    - Graceful shutdown stops accepting connections and drains in-flight HTTP requests within `--drain-timeout`.
  - **Verification**: `go test -v ./cmd/pocketqueue`
  - **Files**: `cmd/pocketqueue/config.go`, `cmd/pocketqueue/config_test.go`, `cmd/pocketqueue/main.go`
  - **Scope**: Small (3 files)

---

## Checkpoint 1: HTTP REST Ingress
- [x] All HTTP unit tests pass (`go test -v ./internal/features/protocol/http/...`)
- [x] `POST /api/v1/topics/:topic` returns 201 Created with stored offset

---

## Phase 2: Server-Sent Events (SSE) Stream (TDD)

- [x] **Task 2.1: Implement Server-Sent Events (SSE) Stream Handler**
  - **Description**: Implement `GET /api/v1/topics/{topic}/sse` in `internal/features/protocol/http/handler.go`. Subscribe to `broker.EventBus`, filter events matching `{topic}`, write `event: message\ndata: <json>\n\n`, and flush using `http.Flusher`.
  - **Acceptance criteria**:
    - Response headers include `text/event-stream`, `no-cache`, `keep-alive`, and `Access-Control-Allow-Origin: *`.
    - Real-time events are flushed immediately to connected clients.
    - Disconnecting client triggers context cancellation and cleanly unsubscribes channel without memory leaks.
  - **Verification**: `go test -v ./internal/features/protocol/http -run TestHTTPSSE`
  - **Files**: `internal/features/protocol/http/handler.go`, `internal/features/protocol/http/server_test.go`
  - **Scope**: Small (2 files)

---

## Checkpoint 2: SSE Stream
- [x] SSE stream tests pass (`go test -v ./internal/features/protocol/http -run TestHTTPSSE`)
- [x] Live events stream in real time with immediate flush

---

## Phase 3: Webhook DLQ Dispatcher (TDD)

- [x] **Task 3.1: Implement WebhookDLQDispatcher**
  - **Description**: Create `internal/features/broker/webhook.go` implementing asynchronous DLQ webhook notification. Subscribes to `EventBus`, filters messages on topics ending in `.dlq` or marked `IsDLQ`, and sends an HTTP POST request to configured webhook URL.
  - **Acceptance criteria**:
    - Dispatches HTTP POST with DLQ payload, original topic, and failure reason in headers.
    - Employs 3-second HTTP timeout and up to 2 retries.
    - Asynchronous worker queue ensures broker `Produce` latency is unaffected even if webhook endpoint is down or timing out.
  - **Verification**: `go test -v ./internal/features/broker -run TestWebhookDLQ`
  - **Files**: `internal/features/broker/webhook.go`, `internal/features/broker/webhook_test.go`
  - **Scope**: Medium (2 files)

- [x] **Task 3.2: Wire Webhook DLQ in CLI**
  - **Description**: Add `--dlq-webhook-url` flag (default `""` / disabled) in `cmd/pocketqueue/config.go`. In `cmd/pocketqueue/main.go`, start `WebhookDLQDispatcher` if URL is configured and close it upon shutdown.
  - **Acceptance criteria**:
    - When `--dlq-webhook-url` is specified, DLQ events trigger webhook POST.
    - When empty, zero background HTTP workers are created.
  - **Verification**: `go test -v ./cmd/pocketqueue`
  - **Files**: `cmd/pocketqueue/config.go`, `cmd/pocketqueue/main.go`
  - **Scope**: Small (2 files)

---

## Checkpoint 3: Webhook DLQ
- [x] Webhook DLQ tests pass (`go test -v ./internal/features/broker -run TestWebhookDLQ`)
- [x] DLQ events successfully dispatch webhook POST without blocking broker

---

## Phase 4: Release Workflow & Packaging Automation

- [x] **Task 4.1: Automate Archive Packaging & Checksum Calculation in GitHub Actions**
  - **Description**: Update `.github/workflows/release.yml` to package cross-platform binaries into `.tar.gz` (macOS/Linux) and `.zip` (Windows), compute SHA-256 hashes (`sha256sum`), and generate `checksums.txt`.
  - **Acceptance criteria**:
    - Generates release archives matching Homebrew (`.tar.gz`) and Scoop (`.zip`) conventions.
    - Calculates and outputs SHA-256 checksums file.
  - **Verification**: Local workflow lint and dry-run packaging check
  - **Files**: `.github/workflows/release.yml`
  - **Scope**: Small (1 file)

- [x] **Task 4.2: Automate Manifest Synchronization for Homebrew & Scoop**
  - **Description**: Add workflow step to `.github/workflows/release.yml` and `scripts/update-packaging.py` to inject computed SHA-256 hashes directly into `Formula/pocketqueue.rb` and `packaging/scoop/pocketqueue.json`.
  - **Acceptance criteria**:
    - Homebrew formula and Scoop manifest hashes are synchronized automatically upon tag release.
  - **Verification**: Script test on sample archives
  - **Files**: `.github/workflows/release.yml`, `scripts/update-packaging.py`, `Formula/pocketqueue.rb`, `packaging/scoop/pocketqueue.json`
  - **Scope**: Small (2-3 files)

---

## Checkpoint 4: Packaging Automation
- [x] Release workflow configuration validated
- [x] Manifest templates ready for automated tag injection

---

## Phase 5: Dual-Protocol E2E Verification, Resource Audit & Docs

- [x] **Task 5.1: Write Dual-Protocol E2E Tests**
  - **Description**: Create `test/e2e/http_bridge_e2e_test.go` verifying dual-protocol bridging:
    1. Produce message via HTTP POST $\to$ Fetch via native Kafka client.
    2. Produce message via native Redis `XADD` $\to$ Stream via HTTP SSE.
    3. Route message to `.dlq` $\to$ Catch webhook POST on local test server.
  - **Acceptance criteria**:
    - All 3 dual-protocol bridge flows pass reliably without flakes.
  - **Verification**: `go test -v ./test/e2e -run TestHTTPBridgeE2E`
  - **Files**: `test/e2e/http_bridge_e2e_test.go`
  - **Scope**: Small (1 file)

- [x] **Task 5.2: Full Zero-Cache Test Suite & Resource Audit**
  - **Description**: Execute `go test -count=1 ./...` and `scripts/audit.ps1`.
  - **Acceptance criteria**:
    - 100% pass across all packages.
    - Stripped binary size $< 15.0\text{ MB}$ (Current: 8.73 MB).
    - Idle RAM usage $< 25.0\text{ MB}$ (Current: 8.62 MB).
    - Cold boot latency $< 20.0\text{ ms}$ (Current: ~5.1 ms).
  - **Verification**: `go test -count=1 ./...; powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1`
  - **Files**: None (verification)
  - **Scope**: Small

- [x] **Task 5.3: Update Documentation & Roadmap**
  - **Description**: Update `README.md` and `docs/ROADMAP.md` documenting HTTP REST ingress, SSE streaming, DLQ webhooks, and marking Milestone 7 completed.
  - **Acceptance criteria**:
    - README includes cURL and JavaScript EventSource code snippets.
    - ROADMAP reflects Milestone 7 completion.
  - **Verification**: Markdown review
  - **Files**: `README.md`, `docs/ROADMAP.md`
  - **Scope**: Small (2 files)

---

## Final Checkpoint: Milestone 7 Complete
- [x] All 10 tasks completed and verified
- [x] Zero compiler warnings, zero lint errors
- [x] All operational and resource invariants strictly satisfied
