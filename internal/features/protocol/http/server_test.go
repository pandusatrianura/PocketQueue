package http_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	protocolhttp "github.com/pandusatrianura/pocketqueue/internal/features/protocol/http"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func setupTestServer(t *testing.T) (*broker.Service, *protocolhttp.Server) {
	t.Helper()
	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(mem, bus, 1)

	server := protocolhttp.NewServer("127.0.0.1:0", svc, bus, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start http server: %v", err)
	}

	return svc, server
}

func TestHTTP_Healthz(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	resp, err := http.Get(server.URL() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK, got: %d", resp.StatusCode)
	}

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	if data["status"] != "UP" {
		t.Errorf("expected status 'UP', got: %v", data["status"])
	}
}

func TestHTTP_Produce_StructuredJSON(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	reqPayload := map[string]any{
		"key": "user-42",
		"value": map[string]any{
			"order_id": 1001,
			"total":    99.5,
		},
		"partition": 0,
		"headers": map[string]string{
			"env": "testing",
		},
	}
	rawJSON, _ := json.Marshal(reqPayload)

	resp, err := http.Post(server.URL()+"/api/v1/topics/orders", "application/json", bytes.NewReader(rawJSON))
	if err != nil {
		t.Fatalf("POST /api/v1/topics/orders failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 201 Created, got %d: %s", resp.StatusCode, string(body))
	}

	var res protocolhttp.ProduceResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode produce response: %v", err)
	}

	if res.Topic != "orders" || res.Partition != 0 || res.Offset != 0 {
		t.Errorf("unexpected produce response: %+v", res)
	}

	// Verify broker received the record
	records, err := svc.Fetch(context.Background(), "orders", 0, 0, 1)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record in broker, got %d", len(records))
	}
	if string(records[0].Key) != "user-42" {
		t.Errorf("expected key 'user-42', got: %s", string(records[0].Key))
	}
}

func TestHTTP_Produce_RawBody(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	rawText := "raw unformatted log line event"
	resp, err := http.Post(server.URL()+"/api/v1/topics/logs", "text/plain", bytes.NewBufferString(rawText))
	if err != nil {
		t.Fatalf("POST /api/v1/topics/logs failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got: %d", resp.StatusCode)
	}

	records, err := svc.Fetch(context.Background(), "logs", 0, 0, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("fetch failed: err=%v, count=%d", err, len(records))
	}
	if string(records[0].Value) != rawText {
		t.Errorf("expected value %q, got %q", rawText, string(records[0].Value))
	}
}

func TestHTTP_Produce_InvalidPartition(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	reqPayload := map[string]any{
		"value":     "some-val",
		"partition": 999, // Exceeds MaxPartitionsPerTopic (16)
	}
	rawJSON, _ := json.Marshal(reqPayload)

	resp, err := http.Post(server.URL()+"/api/v1/topics/test", "application/json", bytes.NewReader(rawJSON))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for partition 999, got: %d", resp.StatusCode)
	}
}

func TestHTTP_CORS_Options(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	req, _ := http.NewRequest(http.MethodOptions, server.URL()+"/api/v1/topics/test", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("OPTIONS request failed: %v", err)
	}
	defer resp.Body.Close()

	corsOrigin := resp.Header.Get("Access-Control-Allow-Origin")
	if corsOrigin != "*" {
		t.Errorf("expected Access-Control-Allow-Origin '*', got: %q", corsOrigin)
	}
}

func TestHTTP_Server_GracefulShutdown(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()

	start := time.Now()
	if err := server.CloseWithTimeout(300 * time.Millisecond); err != nil {
		t.Fatalf("CloseWithTimeout failed: %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Errorf("shutdown took too long: %v", time.Since(start))
	}
}

func TestHTTP_SSEStream(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL()+"/api/v1/topics/chat/sse", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect SSE stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got: %d", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Errorf("expected text/event-stream content type, got: %s", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected CORS origin '*', got: %s", resp.Header.Get("Access-Control-Allow-Origin"))
	}

	// Produce a message to 'chat' topic in background
	go func() {
		time.Sleep(30 * time.Millisecond)
		_, _ = svc.Produce(context.Background(), "chat", 0, []byte("author"), []byte("hello from SSE"), nil)
	}()

	// Read event from SSE stream
	scanner := bufio.NewScanner(resp.Body)
	var eventFound, dataFound bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: message") {
			eventFound = true
		}
		if strings.HasPrefix(line, "data: ") && strings.Contains(line, "hello from SSE") {
			dataFound = true
			break
		}
	}

	if !eventFound || !dataFound {
		t.Fatalf("expected SSE message with payload, got eventFound=%v, dataFound=%v", eventFound, dataFound)
	}
}

func TestHTTP_Produce_PayloadTooLarge(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	// 16 MB + 10 bytes
	hugeData := make([]byte, 16*1024*1024+10)
	resp, err := http.Post(server.URL()+"/api/v1/topics/oversized", "application/octet-stream", bytes.NewReader(hugeData))
	if err != nil {
		t.Fatalf("POST oversized failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413 StatusRequestEntityTooLarge, got: %d", resp.StatusCode)
	}
}

func TestHTTP_Produce_InvalidJSONSyntax(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	brokenJSON := []byte(`{"key": "test", "broken": `)
	resp, err := http.Post(server.URL()+"/api/v1/topics/bad-json", "application/json", bytes.NewReader(brokenJSON))
	if err != nil {
		t.Fatalf("POST bad-json failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 BadRequest for malformed JSON, got: %d", resp.StatusCode)
	}
}
