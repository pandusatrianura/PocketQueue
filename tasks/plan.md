# Implementation Plan: Milestone 7 — HTTP REST Ingress, SSE Bridge, Webhook DLQ & Automated Release

## 1. Overview

Milestone 7 expands PocketQueue's accessibility by introducing a native HTTP/REST ingress and Server-Sent Events (SSE) streaming bridge, an asynchronous Webhook DLQ notification dispatcher, and automated release archive packaging with SHA-256 checksum injection for Homebrew and Scoop.

All functionality is implemented using Go's standard library (`net/http`, zero external dependencies), keeping the single binary strictly $< 6.0\text{ MB}$, idle RAM $< 10.0\text{ MB}$, and cold boot $< 5.0\text{ ms}$.

---

## 2. Architecture Decisions & Rationale

### A. HTTP Protocol Slice (`internal/features/protocol/http`)
- **Location**: `internal/features/protocol/http/server.go`, `handler.go`
- **Pattern**: Standard library `http.Server` and `http.ServeMux` (Go 1.22+ method matching).
- **Default**: Disabled (`--http-port 0`) by default. Starts only when `--http-port <port>` is explicitly passed.
- **Endpoints**:
  - `POST /api/v1/topics/{topic}`: Publishes JSON or raw payload to the broker, returns `201 Created` with offset and partition metadata.
  - `GET /api/v1/topics/{topic}/sse`: Real-time Server-Sent Events stream delivering new records to web clients via `EventSource`.
  - `GET /healthz`: Returns `200 OK` with JSON health status for load balancers.
- **CORS**: `Access-Control-Allow-Origin: *` enabled on SSE to allow frontend dev servers (`localhost:3000`, `localhost:5173`) to connect seamlessly.

### B. EventBus Subscription for SSE
- **Pattern**: SSE handler calls `eventBus.Subscribe()`, filters records matching requested topic, writes `event: message\ndata: ...\n\n`, and flushes immediately using `http.Flusher`.
- **Resource Guard**: On client connection termination (`r.Context().Done()`), the subscription channel is immediately unsubscribed and closed to prevent goroutine and memory leaks.

### C. Webhook DLQ Dispatcher (`internal/features/broker/webhook.go`)
- **Pattern**: Asynchronous worker pool listening to `EventBus`. When a record on a `*.dlq` topic or with `rec.IsDLQ` arrives, it dispatches an HTTP POST request to `--dlq-webhook-url`.
- **Resilience**: Configured with strict 3-second timeout, 2 retries, and bounded queue capacity. Unreachable webhook endpoints will never block or degrade core broker ingestion throughput.

### D. Release Automation (`.github/workflows/release.yml`)
- **Pattern**: During release workflow, bundle binaries into `.tar.gz` (macOS/Linux) and `.zip` (Windows), compute SHA-256 hashes, generate `checksums.txt`, and automatically inject matching hashes into `Formula/pocketqueue.rb` and `packaging/scoop/pocketqueue.json`.

---

## 3. Implementation Phases & Task Breakdown

### Phase 1: HTTP Protocol Server & REST Ingress (TDD)
- **Task 1.1**: Create `internal/features/protocol/http/handler.go` and `server.go` implementing `POST /api/v1/topics/{topic}` and `GET /healthz`.
- **Task 1.2**: Write unit tests in `internal/features/protocol/http/server_test.go` verifying produce contract, error codes, and graceful shutdown.
- **Task 1.3**: Wire `--http-port` flag in `cmd/pocketqueue/config.go` and `cmd/pocketqueue/main.go`.

### Checkpoint 1: HTTP REST Ingress
- [x] Unit tests pass (`go test -v ./internal/features/protocol/http -run TestHTTPProduce`).
- [x] `POST /api/v1/topics/:topic` stores messages and returns `201 Created`.

### Phase 2: Server-Sent Events (SSE) Stream (TDD)
- **Task 2.1**: Implement `GET /api/v1/topics/{topic}/sse` in `internal/features/protocol/http/handler.go`.
- **Task 2.2**: Write unit tests in `server_test.go` verifying SSE headers, live event delivery from `EventBus`, and clean unsubscription on context cancellation.

### Checkpoint 2: SSE Streaming Bridge
- [x] Unit tests pass (`go test -v ./internal/features/protocol/http -run TestHTTPSSE`).
- [x] Real-time event streaming verified with immediate flush.

### Phase 3: Webhook DLQ Dispatcher (TDD)
- **Task 3.1**: Create `internal/features/broker/webhook.go` implementing `WebhookDLQDispatcher`.
- **Task 3.2**: Write unit tests in `internal/features/broker/webhook_test.go` testing async POST dispatch, payload formatting, retries, and timeout resilience.
- **Task 3.3**: Add `--dlq-webhook-url` flag in `cmd/pocketqueue/config.go` and wire into `main.go`.

### Checkpoint 3: Webhook DLQ
- [x] DLQ messages trigger asynchronous HTTP POST with metadata headers.
- [x] Slow/down webhooks do not degrade broker throughput.

### Phase 4: Release Workflow & Packaging Automation
- **Task 4.1**: Update `.github/workflows/release.yml` to package `.tar.gz` and `.zip` archives.
- **Task 4.2**: Automate SHA-256 calculation and manifest update for `Formula/pocketqueue.rb` and `packaging/scoop/pocketqueue.json`.

### Checkpoint 4: Automated Release Packaging
- [x] Release workflow script validated with archive generation and SHA-256 checksums.

### Phase 5: Dual-Protocol E2E Verification, Resource Audit & Docs
- **Task 5.1**: Create `test/e2e/http_bridge_e2e_test.go` testing dual-protocol bridging:
  - HTTP POST $\to$ Kafka Fetch
  - Redis XADD $\to$ HTTP SSE
  - DLQ produce $\to$ Webhook POST
- **Task 5.2**: Run zero-cache test suite: `go test -count=1 ./...`.
- **Task 5.3**: Run resource audit: `powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1`.
- **Task 5.4**: Synchronize documentation in `README.md` and `docs/ROADMAP.md`.
- [x] All Phase 5 verification and documentation steps completed.

---

## 4. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Unclosed SSE connections consuming memory | Memory leak in long-running broker | Unsubscribe from `EventBus` immediately when client request context is canceled (`<-r.Context().Done()`). |
| External webhook server slow/down | Goroutine explosion or blocked broker | Dedicated bounded worker pool (capacity 100), 3-second HTTP timeout, non-blocking fire-and-forget channel send. |
| CORS issues when called from browser | Browser blocks SSE or POST | Return standard CORS headers (`Access-Control-Allow-Origin: *`, `Access-Control-Allow-Headers: *`, `Access-Control-Allow-Methods: POST, GET, OPTIONS`). |
| Binary size expansion | Violates $< 15.0\text{ MB}$ project ceiling | Use `net/http` from standard library only. Zero third-party web frameworks. |
