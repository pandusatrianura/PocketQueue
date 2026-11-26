# Implementation Plan: Milestone 8 — Embedded Web UI Dashboard (Zero-Dependency)

## Overview
This plan implements a zero-dependency, embedded Web UI Dashboard for PocketQueue. When `--http-port <port>` is passed, PocketQueue will serve an interactive, modern browser dashboard at `http://localhost:<port>/` using Go's `//go:embed` filesystem. The dashboard enables live event streaming via SSE, real-time Canvas throughput sparklines, a web-based message producer, and DLQ inspection/replay, while keeping the binary under 10 MB and idle RAM under 10 MB.

---

## Architecture Decisions

1. **Pure Go Standard Library & `embed.FS`**:
   - Assets are compiled directly into the binary using `//go:embed ui/assets/*`.
   - Zero node/npm dependencies, zero external build steps, 100% offline & air-gapped capable.
2. **Micro-Sized Frontend Stack (< 50 KB total)**:
   - CSS: [Pico.css v2](https://picocss.com/) (~15 KB minified) for clean, responsive, dark/light theme styling without build tools.
   - JS: Vanilla ES6+ (~18 KB uncompressed) for tab switching, SSE streaming, form validation, and canvas charting.
   - Chart: Pure HTML5 Canvas 2D sparkline (< 3 KB JS) replacing heavy external libraries like Chart.js or D3.
3. **Transparent Compression & Caching**:
   - Static asset handler supports `ETag` matching (`If-None-Match` $\to$ `304 Not Modified`) and transparent gzip compression (`Content-Encoding: gzip`) when supported by the client.
4. **Single Ingress Architecture**:
   - The Web UI attaches directly to the existing `protocolhttp.Handler` and `http.ServeMux` created in Milestone 7, avoiding secondary ports or listener duplication.

---

## Dependency Graph

```text
Broker Domain Service (broker.Service & EventBus)
    │
    ├── 1. REST API Endpoints (GET /api/v1/topics, GET /api/v1/stats, POST /api/v1/dlq/replay)
    │       │
    │       ├── 2. Embedded Static Assets Engine (embed.FS, gzip, ETag, GET /, GET /ui, GET /static/*)
    │       │       │
    │       │       └── 3. Vanilla JS Controller & UI Views (SSE EventStream, Sparkline Canvas, Producer Form, DLQ Modal)
    │       │               │
    │       │               └── 4. End-to-End Integration Tests & Performance Invariant Audit
```

---

## Task Breakdown

### Phase 1: API Extensions & DLQ Replay

#### Task 1: Add Broker Topic Metadata & Telemetry APIs
- **Description:** Implement `GET /api/v1/topics` (lists all topics, partition count, and latest offsets) and `GET /api/v1/stats` (uptime, total message count, memory usage in bytes).
- **Acceptance criteria:**
  - `GET /api/v1/topics` returns JSON with array of topics, partitions, and offsets.
  - `GET /api/v1/stats` returns JSON with `uptime_seconds`, `total_messages`, and `memory_bytes`.
  - Proper CORS headers (`Access-Control-Allow-Origin: *`) on all responses.
- **Verification:**
  - `go test -v -run "TestAPI_ListTopics|TestAPI_BrokerStats" ./internal/features/protocol/http`
- **Dependencies:** None
- **Files likely touched:**
  - `internal/features/protocol/http/handler_api.go`
  - `internal/features/protocol/http/handler.go`
  - `internal/features/protocol/http/server_test.go`
- **Estimated scope:** S (3 files)

#### Task 2: Add DLQ Web Replay API
- **Description:** Implement `POST /api/v1/dlq/replay` allowing clients/web UI to republish a repaired payload into the target topic.
- **Acceptance criteria:**
  - Accepts JSON with `dlq_topic`, `target_topic` (auto-trimmed if ending in `.dlq`), `key`, `value`, `partition`, and optional `headers`.
  - Produces message into destination topic and returns HTTP 200 with new offset.
  - Returns HTTP 400 on malformed payload or missing topic.
- **Verification:**
  - `go test -v -run "TestAPI_DLQReplay" ./internal/features/protocol/http`
- **Dependencies:** Task 1
- **Files likely touched:**
  - `internal/features/protocol/http/handler_api.go`
  - `internal/features/protocol/http/server_test.go`
- **Estimated scope:** S (2 files)

---

### 🏁 Checkpoint 1: REST API Layer
- [ ] Topic metadata, stats telemetry, and DLQ replay endpoints verified with unit tests.
- [ ] All HTTP tests passing cleanly (`go test -v ./internal/features/protocol/http`).

---

### Phase 2: Static Asset Embedding & UI Handlers

#### Task 3: Assemble Minimal Web UI Assets (HTML, Pico.css, App CSS)
- **Description:** Create single-page dashboard assets (`index.html`, `pico.min.css`, `app.css`, `favicon.svg`) with dark/light mode toggle, 4 main view tabs (Live Stream, Topics & Watermarks, Web Publisher, DLQ Studio), and modal dialogs.
- **Acceptance criteria:**
  - Clean semantic HTML layout with responsive navigation and status pill.
  - Pico.css v2 bundled locally (< 15 KB).
  - SVG lightning favicon bundled locally.
  - Total asset bundle size $< 30\text{ KB}$.
- **Verification:**
  - Static HTML validates without external CDN references.
- **Dependencies:** None
- **Files likely touched:**
  - `internal/features/protocol/http/ui/assets/index.html`
  - `internal/features/protocol/http/ui/assets/pico.min.css`
  - `internal/features/protocol/http/ui/assets/app.css`
  - `internal/features/protocol/http/ui/assets/favicon.svg`
- **Estimated scope:** M (4 asset files)

#### Task 4: Implement Embedded Asset Server with ETag & Gzip
- **Description:** Wire `embed.FS` in `ui/embed.go` and implement `handler_ui.go` to serve `GET /`, `GET /ui`, and `GET /static/*` with `ETag` validation and transparent gzip compression.
- **Acceptance criteria:**
  - `GET /` and `GET /ui` return 200 OK with `text/html; charset=utf-8`.
  - `GET /static/{file}` returns correct MIME types (`text/css`, `application/javascript`, `image/svg+xml`).
  - Subsequent requests with `If-None-Match` matching ETag return `304 Not Modified`.
  - Clients sending `Accept-Encoding: gzip` receive gzipped content with `Content-Encoding: gzip`.
- **Verification:**
  - `go test -v -run "TestUI_" ./internal/features/protocol/http`
- **Dependencies:** Task 3
- **Files likely touched:**
  - `internal/features/protocol/http/ui/embed.go`
  - `internal/features/protocol/http/handler_ui.go`
  - `internal/features/protocol/http/handler.go`
  - `internal/features/protocol/http/server_test.go`
- **Estimated scope:** M (4 files)

---

### 🏁 Checkpoint 2: UI Asset Serving & Caching
- [ ] Browser can load `http://localhost:<http-port>/` and receive complete HTML/CSS without external network access.
- [ ] Static caching (`304 Not Modified`) and compression verified.

---

### Phase 3: Interactive Frontend Controller & Visual Features

#### Task 5: Implement Vanilla JS Controller (SSE, Sparkline, Forms & DLQ Modal)
- **Description:** Write `app.js` (< 20 KB) implementing the real-time client: SSE stream listener, live table rendering, pause/resume auto-scroll, native canvas sparkline, publisher form POST submission, and DLQ replay modal.
- **Acceptance criteria:**
  - Connects to `GET /api/v1/topics/:topic/sse` (or wildcard/all topics) and appends events into the live stream table.
  - Topic filter dropdown and text search filter events in real time.
  - Native Canvas sparkline renders events/sec throughput over a sliding 30-second window.
  - Web Publisher form submits payload to `POST /api/v1/topics/:topic` and displays result toast.
  - DLQ table displays poison pill messages with "Inspect & Replay" button opening modal.
- **Verification:**
  - Browser console logs 0 errors; SSE reconnects automatically on network drop.
- **Dependencies:** Task 4
- **Files likely touched:**
  - `internal/features/protocol/http/ui/assets/app.js`
  - `internal/features/protocol/http/ui/assets/index.html`
- **Estimated scope:** M (2 files)

---

### 🏁 Checkpoint 3: Full Dashboard Interactivity
- [ ] Live events stream into the UI table.
- [ ] Publishing from web form works and immediately reflects in the stream.
- [ ] DLQ replay works end-to-end.

---

### Phase 4: E2E Testing, Documentation & Audit

#### Task 6: End-to-End Integration Test Suite
- **Description:** Write comprehensive E2E tests in `test/e2e/web_dashboard_e2e_test.go` covering full dashboard lifecycle: static serving, API endpoints, web produce $\to$ Kafka fetch, and DLQ replay.
- **Acceptance criteria:**
  - E2E test runs with zero external dependencies.
  - Verifies HTML/CSS delivery, topic discovery, web publishing, and DLQ replay.
- **Verification:**
  - `go test -v ./test/e2e -run TestWebDashboard`
- **Dependencies:** Tasks 1-5
- **Files likely touched:**
  - `test/e2e/web_dashboard_e2e_test.go`
- **Estimated scope:** S (1 file)

#### Task 7: Documentation Update & Resource Audit
- **Description:** Update `README.md` with Web UI screenshots/documentation and run `scripts/audit.ps1` to ensure binary size $< 10.0\text{ MB}$, idle RAM $< 10.0\text{ MB}$, and boot time $< 5.0\text{ ms}$.
- **Acceptance criteria:**
  - `README.md` documents how to access the Web UI via `--http-port`.
  - `scripts/audit.ps1` passes with 100% green invariants.
- **Verification:**
  - `powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1`
  - `go test -v ./...`
- **Dependencies:** Task 6
- **Files likely touched:**
  - `README.md`
- **Estimated scope:** S (1 file)

---

## 🏁 Final Checkpoint: Milestone 8 Completion
- [ ] All 7 tasks implemented and verified.
- [ ] Full test suite passes: `go test -count=1 ./...`.
- [ ] Binary size $< 10.0\text{ MB}$, Idle RAM $< 10.0\text{ MB}$, Cold start $< 5.0\text{ ms}$.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| **Asset bloat exceeding binary size constraint** | High | Use Pico.css v2 (~15 KB) and Vanilla JS (~18 KB). Avoid any external JS frameworks or icon fonts. Total embedded size $< 50\text{ KB}$. |
| **SSE memory buildup with multiple browser tabs** | Medium | Bounded EventBus subscription channels (capacity 100). Automatic channel unsubscribe when client closes tab (`r.Context().Done()`). |
| **Cross-Origin / CORS issues during local dev** | Low | Handler automatically sets `Access-Control-Allow-Origin: *` and handles `OPTIONS` preflight requests. |
| **MIME type detection on Windows** | Low | Map `.css`, `.js`, `.svg`, and `.html` explicitly in `handler_ui.go` rather than relying on Windows registry `mime.TypeByExtension`. |
