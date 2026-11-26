# Spec: Milestone 8 — Embedded Web UI Dashboard (Zero-Dependency)

## 1. Objective

Provide an instant, zero-dependency, local **Web UI Dashboard** for PocketQueue when `--http-port` is enabled. 
Similar to how SQLite tools (e.g. SQLite Viewer) or Redis Commander empower developers to inspect databases without CLI friction, PocketQueue will serve an interactive, modern web console directly from its single binary without requiring external tools like AKHQ, Kafka UI, or Redpanda Console.

### Core User Stories
1. **Zero-Setup Browser Inspection**: A developer starts PocketQueue with `--http-port 8080`, opens `http://localhost:8080` in their browser, and immediately sees live topics, partition watermarks, and real-time streaming events.
2. **Real-Time Event Stream & Chart**: Developers can watch events stream across topics in real time via Server-Sent Events (SSE), with search/topic filtering, auto-scroll freeze, and real-time throughput metrics (events/sec).
3. **Web Message Producer**: Developers can compose and send JSON or plain text messages into any topic with custom keys, headers, and partition targeting directly from a web form.
4. **Visual DLQ Triage & Replay**: When consumers route failed messages to `.dlq` topics, developers can inspect the error context, edit the JSON payload in a browser modal, and click "Replay to Topic" with 1-click.
5. **Air-Gapped / Offline Local Dev**: All HTML, CSS, and JavaScript assets are bundled inside the Go binary via `//go:embed`. Zero CDN calls or npm runtimes are required.

---

## 2. Assumptions & Constraints

1. **Zero External Runtime / Build Step**:
   - No Node.js, npm, Webpack, or Vite required to build or distribute PocketQueue.
   - The UI is built using clean Semantic HTML5, [Pico.css v2](https://picocss.com/) (minified embedded ~15 KB), and Vanilla JavaScript (ES6+ ~18 KB). Total embedded asset size $< 50\text{ KB}$ (< $18\text{ KB}$ gzip).
2. **Air-Gapped Operation**:
   - All CSS, icons (inline SVG), and fonts (system native font stack) are served locally from `embed.FS`. No external requests to Google Fonts, CDNs, or telemetry endpoints.
3. **Single-Port Integration**:
   - The Web UI is hosted on the same HTTP server port configured via `--http-port <port>`.
   - Served at both `GET /` and `GET /ui` with HTML content type.
   - Static assets (`style.css`, `app.js`, `favicon.svg`) served under `GET /static/*`.
4. **Binary & Resource Invariant Enforcement**:
   - Stripped binary size increase must be $< 100\text{ KB}$ (well within the $< 15.0\text{ MB}$ repository limit).
   - Idle RAM usage remains $< 10.0\text{ MB}$ (hard limit $< 25.0\text{ MB}$).
   - Zero CPU consumption when no browser tabs are connected.

---

## 3. Commands

- **Build PocketQueue**:
  ```bash
  go build -ldflags="-s -w" -o bin/pocketqueue ./cmd/pocketqueue
  ```
- **Run with Web UI Dashboard**:
  ```bash
  ./bin/pocketqueue --http-port 8080 --demo
  ```
- **Open Dashboard**:
  Navigate to `http://localhost:8080` or `http://localhost:8080/ui` in any modern browser (Chrome, Firefox, Safari, Edge).
- **Run Tests**:
  ```bash
  go test -v ./...
  ```
- **Run Performance & Resource Audit**:
  ```powershell
  powershell -ExecutionPolicy Bypass -File .\scripts\audit.ps1
  ```

---

## 4. API Extensions for Web UI

To support the dashboard, the following lightweight REST endpoints will be added to `internal/features/protocol/http`:

| Endpoint | Method | Purpose | Response Format |
|---|---|---|---|
| `GET /` | GET | Serves embedded Single-Page Dashboard | `text/html; charset=utf-8` |
| `GET /ui` | GET | Alias for `GET /` | `text/html; charset=utf-8` |
| `GET /static/{file}` | GET | Serves embedded CSS/JS with ETag & Cache-Control | `text/css`, `application/javascript`, `image/svg+xml` |
| `GET /api/v1/topics` | GET | List all active topics, partition counts, and latest offsets | `{"topics": [{"name": "orders", "partitions": 1, "latest_offset": 42}]}` |
| `GET /api/v1/stats` | GET | Broker telemetry (uptime, total msgs, memory, active SSE clients) | `{"uptime_seconds": 120, "total_messages": 450, "memory_bytes": 8388608}` |
| `POST /api/v1/dlq/replay` | POST | Replay a failed message from DLQ to destination topic | `{"status": "replayed", "topic": "orders", "new_offset": 43}` |

---

## 5. Project Structure & Vertical Slice Layout

```text
internal/features/protocol/http/
  ui/
    assets/
      index.html                 # Semantic single-page HTML layout
      pico.min.css               # Embedded minimal responsive CSS (< 15 KB)
      app.css                    # Custom dashboard styling & dark/light theme adjustments
      app.js                     # Vanilla JS controller (SSE listener, tabs, forms, modals)
      favicon.svg                # Embedded SVG lightning icon
    embed.go                     # go:embed assets/* declaration and http.FS wrapper
  handler.go                     # RegisterRoutes: attach UI and new API endpoints
  handler_ui.go                  # UI static file serving, ETag validation, and gzip compression
  handler_api.go                 # GET /api/v1/topics, GET /api/v1/stats, POST /api/v1/dlq/replay
  server_test.go                 # Tests for UI static routes, API endpoints, and ETag/compression
test/e2e/
  web_dashboard_e2e_test.go      # End-to-end integration: produce -> verify via UI API & SSE
```

---

## 6. Code Style & Technical Implementation Details

### A. Static Asset Embedding (`embed.go`)
```go
package http

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed ui/assets/*
var uiAssets embed.FS

// UIFileSystem returns the sub-filesystem for UI assets.
func UIFileSystem() http.FileSystem {
	sub, err := fs.Sub(uiAssets, "ui/assets")
	if err != nil {
		panic("failed to create UI sub filesystem: " + err.Error())
	}
	return http.FS(sub)
}
```

### B. Lightweight Real-Time SVG/Canvas Throughput Sparkline (Pure Vanilla JS)
```javascript
// Render 30-second moving throughput sparkline with 0 external dependencies
function drawSparkline(canvas, history) {
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  if (history.length < 2) return;
  
  ctx.beginPath();
  ctx.strokeStyle = '#10b981';
  ctx.lineWidth = 2;
  const max = Math.max(...history, 10);
  history.forEach((val, i) => {
    const x = (i / (history.length - 1)) * w;
    const y = h - (val / max) * (h - 6) - 3;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}
```

### C. DLQ Replay Request Contract
```go
// DLQReplayRequest represents payload to replay a DLQ message.
type DLQReplayRequest struct {
    DLQTopic    string            `json:"dlq_topic"`    // e.g. "orders.dlq"
    TargetTopic string            `json:"target_topic"` // e.g. "orders" (inferred if omitted)
    Key         string            `json:"key"`
    Value       any               `json:"value"`
    Partition   int32             `json:"partition"`
    Headers     map[string]string `json:"headers,omitempty"`
}
```

---

## 7. Testing Strategy

1. **Unit Testing (`internal/features/protocol/http`)**:
   - `TestUI_ServeIndexHTML`: Verify `GET /` and `GET /ui` return 200 OK with `text/html` and content contains PocketQueue dashboard elements.
   - `TestUI_StaticAssets`: Verify `GET /static/pico.min.css`, `GET /static/app.js`, and `GET /static/favicon.svg` serve proper MIME types and non-empty content.
   - `TestUI_CacheHeaders`: Verify `ETag` and `Cache-Control` headers are set for static assets to ensure zero-overhead browser caching.
   - `TestAPI_ListTopics`: Verify `GET /api/v1/topics` returns list of topics and partition metadata.
   - `TestAPI_BrokerStats`: Verify `GET /api/v1/stats` returns accurate uptime, message counts, and memory usage.
   - `TestAPI_DLQReplay`: Verify `POST /api/v1/dlq/replay` republishes the edited message into the destination topic.
2. **End-to-End Integration Testing (`test/e2e/web_dashboard_e2e_test.go`)**:
   - Verify workflow: Start broker with `--http-port` $\to$ Fetch dashboard index $\to$ Produce via Web UI API $\to$ Verify event received on SSE stream.
3. **Resource & Invariant Audit**:
   - Binary size $< 10.0\text{ MB}$ (budget $< 15.0\text{ MB}$).
   - Idle RAM $< 10.0\text{ MB}$ (budget $< 25.0\text{ MB}$).
   - Cold start latency $< 5.0\text{ ms}$ (budget $< 20.0\text{ ms}$).

---

## 8. Boundaries

- **Always**:
  - Keep all frontend code strictly vanilla (HTML5, Pico.css, native JavaScript ES6+). Zero external frameworks (no React, no Vue, no jQuery).
  - Enable Gzip compression middleware on static assets and HTML responses when requested by browser (`Accept-Encoding: gzip`).
  - Set proper `Content-Security-Policy` and CORS headers.
  - Automatically close SSE connections in JavaScript when switching away or closing browser tabs.
- **Ask First**:
  - Introducing any npm / build-tool step for frontend compilation.
  - Adding external charting libraries (e.g. Chart.js, D3) that exceed 50 KB.
- **Never**:
  - Fetch any assets from remote CDNs (must work 100% offline in air-gapped dev environments).
  - Compromise the $< 15\text{ MB}$ binary limit or $< 25\text{ MB}$ RAM ceiling.

---

## 9. Success Criteria & Verification Gates

- [ ] Navigating to `http://localhost:<http-port>/` renders the PocketQueue Web UI with dark/light mode toggle.
- [ ] Live event stream displays incoming Kafka, Redis, and HTTP events in real time without browser refresh.
- [ ] Users can filter the event stream by topic name or payload keyword.
- [ ] Web message producer form successfully publishes records to Kafka/Redis topics via `POST /api/v1/topics/{topic}`.
- [ ] Dead-Letter Queue viewer displays `.dlq` records with a modal to edit payload and replay to the original topic.
- [ ] `GET /api/v1/topics` and `GET /api/v1/stats` provide RESTful JSON metadata for topics and broker health.
- [ ] All static assets are served from memory with gzip support and ETag validation.
- [ ] All unit, integration, and E2E tests pass (`go test -v ./...`).
- [ ] `scripts/audit.ps1` verifies binary size remains $< 10\text{ MB}$, RAM $< 10\text{ MB}$, and cold boot $< 5\text{ ms}$.
