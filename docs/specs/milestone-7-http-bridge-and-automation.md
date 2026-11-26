# Spec: Milestone 7 — HTTP REST Ingress, Server-Sent Events (SSE) Bridge, Webhook DLQ & Automated Release

## 1. Objective

Provide universal client connectivity to PocketQueue for browser applications, webhooks, and modern frontend frameworks (React, Vue, Svelte, Next.js) without requiring Kafka or Redis client SDKs, while maintaining PocketQueue's core identity:
- Single-binary executable (< 6 MB).
- Minimal idle memory (< 10 MB RAM).
- Sub-5ms boot time.
- Pure Go standard library (`net/http`, zero external frameworks/dependencies).

### User Stories
1. **Frontend / Webhook Publisher**: A developer or external system can send an event to PocketQueue via standard HTTP POST (`POST /api/v1/topics/:topic`) without configuring Kafka serializers or Redis connection pools.
2. **Browser Live Stream Consumer**: A web dashboard can open an SSE connection (`GET /api/v1/topics/:topic/sse`) and receive real-time JSON events via standard browser `EventSource`.
3. **Dead-Letter Queue Alerting**: When a poison pill message is sent to `.dlq`, PocketQueue can asynchronously trigger an external HTTP webhook alert (`--dlq-webhook-url https://my-alert.com/webhook`).
4. **Package Distribution**: When a git tag is released, GitHub Actions automatically packages platform archives (`.tar.gz` and `.zip`), calculates SHA-256 hashes, generates `checksums.txt`, and synchronizes the Homebrew formula and Scoop manifest.

---

## 2. Assumptions
1. **HTTP Server is Off by Default**: `--http-port 0` (disabled) by default to preserve the zero-attack-surface and zero-socket-overhead principle for developers only using Kafka/Redis. It starts only when `--http-port <port>` is explicitly passed.
2. **Standard Library Only**: Built entirely with Go's `net/http` and `http.ServeMux` (Go 1.22+ method-based route matching). Zero external dependencies (no Gin, Fiber, Chi, or Echo).
3. **EventBus Integration**: SSE streams subscribe to `broker.EventBus`. Client disconnection cleanly unsubscribes the channel, avoiding goroutine or memory leaks.
4. **Asynchronous Webhook Dispatch**: Webhook calls run in bounded worker pools with strict HTTP timeouts (default 3s) so slow or unreachable webhook endpoints never degrade broker ingestion throughput.
5. **Payload Handling**:
   - `POST /api/v1/topics/:topic` accepts `application/json` (e.g. `{"key": "k1", "value": {"item": 1}, "headers": {"env": "prod"}}`) or raw text/binary payloads.
   - Responds with `201 Created` and JSON metadata `{"topic": "...", "partition": 0, "offset": 105, "timestamp": "..."}`.
   - Includes standard `GET /healthz` endpoint returning `200 OK` `{"status": "UP", "version": "0.7.0"}`.

---

## 3. Commands

- **Build**: `go build -ldflags="-s -w" -o bin/pocketqueue ./cmd/pocketqueue`
- **Run with HTTP Bridge**: `./bin/pocketqueue --http-port 8080 --dlq-webhook-url https://webhook.site/test`
- **Publish via cURL**:
  ```bash
  curl -X POST http://localhost:8080/api/v1/topics/orders \
       -H "Content-Type: application/json" \
       -d '{"key": "user-1", "value": {"order_id": 42, "amount": 100}}'
  ```
- **Stream via SSE in Terminal**:
  ```bash
  curl -N http://localhost:8080/api/v1/topics/orders/sse
  ```
- **Stream via Browser JS**:
  ```javascript
  const es = new EventSource("http://localhost:8080/api/v1/topics/orders/sse");
  es.onmessage = (e) => console.log("Received:", JSON.parse(e.data));
  ```
- **Healthcheck**: `curl http://localhost:8080/healthz`
- **Test**: `go test -v ./...`
- **Audit**: `powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1`

---

## 4. Project Structure & Vertical Slice Layout

```text
cmd/pocketqueue/
  config.go                     # Added --http-port and --dlq-webhook-url flags
  main.go                       # Lifecycle wiring for HTTP server & DLQ webhook listener
internal/features/protocol/http/
  server.go                     # HTTPServer struct, graceful shutdown, listener management
  handler.go                    # REST endpoints (Produce, Healthz) and SSE stream handler
  server_test.go                # Unit and integration tests for HTTP & SSE endpoints
internal/features/broker/
  webhook.go                    # Async DLQ webhook dispatcher consuming EventBus
  webhook_test.go               # Tests for webhook dispatch, retries, and timeout
.github/workflows/
  release.yml                   # Automated archive packaging, SHA-256 calculation & manifest update
Formula/pocketqueue.rb          # Homebrew recipe (updated dynamically on release)
packaging/scoop/pocketqueue.json# Scoop manifest (updated dynamically on release)
test/e2e/
  http_bridge_e2e_test.go       # End-to-end integration: HTTP POST -> Kafka consume & Kafka produce -> SSE stream
```

---

## 5. Code Style & Idioms

### A. HTTP Produce Request & Response Contract
```go
// ProduceRequest represents standard JSON payload for HTTP ingress.
type ProduceRequest struct {
    Key       string            `json:"key"`
    Value     any               `json:"value"`
    Partition int32             `json:"partition"`
    Headers   map[string]string `json:"headers,omitempty"`
}

// ProduceResponse represents confirmation of persistent message storage.
type ProduceResponse struct {
    Topic     string `json:"topic"`
    Partition int32  `json:"partition"`
    Offset    int64  `json:"offset"`
    Timestamp string `json:"timestamp"`
}
```

### B. Server-Sent Events (SSE) Event Streaming Format
```go
func (h *Handler) handleSSEStream(w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    w.Header().Set("Access-Control-Allow-Origin", "*")

    subCh, unsub := h.eventBus.Subscribe()
    defer unsub()

    for {
        select {
        case <-r.Context().Done():
            return
        case rec, ok := <-subCh:
            if !ok {
                return
            }
            if rec.Topic == requestedTopic {
                data, _ := json.Marshal(rec)
                fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
                flusher.Flush()
            }
        }
    }
}
```

---

## 6. Testing Strategy

1. **Unit Testing (`internal/features/protocol/http`)**:
   - `httptest.NewServer` testing `POST /api/v1/topics/:topic` with structured JSON, raw string, and empty payloads.
   - `httptest.NewServer` testing `GET /api/v1/topics/:topic/sse` with live event publishing and client disconnection.
   - `GET /healthz` status code 200 and JSON contract.
2. **Webhook Dispatcher Testing (`internal/features/broker`)**:
   - Mock HTTP server verifying DLQ messages trigger POST requests with correct headers (`x-dlq-original-topic`, `x-dlq-reason`).
   - Verifying timeout handling when webhook target server is unresponsive.
3. **End-to-End Integration Testing (`test/e2e/http_bridge_e2e_test.go`)**:
   - Dual-protocol bridge test: Produce via HTTP POST $\to$ Consume via native Kafka protocol.
   - Dual-protocol bridge test: Produce via native Redis `XADD` $\to$ Stream via HTTP SSE.
4. **Performance & Invariant Audit**:
   - Stripped binary size strictly $< 6.0\text{ MB}$ (hard ceiling $< 15.0\text{ MB}$).
   - Idle RAM footprint $< 10.0\text{ MB}$ (hard ceiling $< 25.0\text{ MB}$).
   - Cold boot latency $< 5.0\text{ ms}$ (hard ceiling $< 20.0\text{ ms}$).

---

## 7. Boundaries

- **Always**:
  - Abstract all HTTP routing using Go standard library (`net/http`).
  - Gracefully shutdown the HTTP server using `srv.Shutdown(ctx)` during connection draining.
  - Return `Access-Control-Allow-Origin: *` on SSE streams to allow web apps on `localhost:3000` or `localhost:5173` to connect without CORS errors.
  - Unsubscribe from `EventBus` whenever an SSE client disconnects.
- **Ask First**:
  - Adding any external third-party HTTP routers or middleware.
  - Changing the default port or turning HTTP on by default.
- **Never**:
  - Allow an external webhook failure to block or slow down broker `Produce`/`Append` operations.
  - Commit plain release tokens or secrets to version control.
  - Exceed the 15 MB binary size budget.

---

## 8. Success Criteria & Verification Gates

- [ ] `POST /api/v1/topics/:topic` returns HTTP 201 and stores record in broker.
- [ ] `GET /api/v1/topics/:topic/sse` pushes new events in real time to connected browser clients.
- [ ] `GET /healthz` returns HTTP 200 OK with health status.
- [ ] DLQ events trigger asynchronous HTTP POST to `--dlq-webhook-url` with retry and timeout guards.
- [ ] Release workflow `.github/workflows/release.yml` produces `.tar.gz` and `.zip` archives with automated SHA-256 calculation.
- [ ] All unit, integration, and E2E tests pass (`go test -count=1 ./...`).
- [ ] Binary size remains $< 6.0\text{ MB}$, idle RAM $< 10.0\text{ MB}$, cold boot $< 5.0\text{ ms}$.
