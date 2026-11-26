package http_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/decoder"
	protocolhttp "github.com/pandusatrianura/pocketqueue/internal/features/protocol/http"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
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

func TestAPI_ListTopics(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	// Seed 2 topics
	_, err := svc.Produce(context.Background(), "telemetry", 0, []byte("dev-1"), []byte("{\"temp\":22}"), nil)
	if err != nil {
		t.Fatalf("failed to produce to telemetry: %v", err)
	}
	_, err = svc.Produce(context.Background(), "alerts", 0, []byte("crit"), []byte("{\"level\":\"high\"}"), nil)
	if err != nil {
		t.Fatalf("failed to produce to alerts: %v", err)
	}

	resp, err := http.Get(server.URL() + "/api/v1/topics")
	if err != nil {
		t.Fatalf("GET /api/v1/topics failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK, got: %d", resp.StatusCode)
	}

	var result struct {
		Topics []struct {
			Name          string `json:"name"`
			Partitions    int32  `json:"partitions"`
			TotalMessages int64  `json:"total_messages"`
		} `json:"topics"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	if len(result.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(result.Topics))
	}

	foundTelemetry := false
	for _, top := range result.Topics {
		if top.Name == "telemetry" {
			foundTelemetry = true
			if top.TotalMessages != 1 {
				t.Errorf("expected 1 message in telemetry, got %d", top.TotalMessages)
			}
		}
	}
	if !foundTelemetry {
		t.Errorf("expected telemetry topic in response")
	}
}

func TestAPI_BrokerStats(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	// Produce 1 message
	_, _ = svc.Produce(context.Background(), "stats-topic", 0, []byte("k"), []byte("v"), nil)

	resp, err := http.Get(server.URL() + "/api/v1/stats")
	if err != nil {
		t.Fatalf("GET /api/v1/stats failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK, got: %d", resp.StatusCode)
	}

	var stats map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode JSON stats: %v", err)
	}

	if stats["status"] != "UP" {
		t.Errorf("expected status 'UP', got %v", stats["status"])
	}
	if uptime, ok := stats["uptime_seconds"].(float64); !ok || uptime < 0 {
		t.Errorf("invalid uptime_seconds: %v", stats["uptime_seconds"])
	}
	if total, ok := stats["total_messages"].(float64); !ok || total < 1 {
		t.Errorf("expected total_messages >= 1, got %v", stats["total_messages"])
	}
}

func TestAPI_DLQReplay(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	// 1. Send poison message to orders.dlq
	_, err := svc.SendToDLQ(context.Background(), "orders", 0, []byte("ord-bad"), []byte(`{"order_id": 999, "status": "malformed"}`), nil, "deserialization failed")
	if err != nil {
		t.Fatalf("failed to send to DLQ: %v", err)
	}

	// 2. Call POST /api/v1/dlq/replay with repaired payload
	replayReq := map[string]any{
		"dlq_topic":    "orders.dlq",
		"target_topic": "orders",
		"key":          "ord-bad",
		"value": map[string]any{
			"order_id": 999,
			"status":   "repaired",
		},
		"partition": 0,
		"headers": map[string]string{
			"repaired_by": "web-console",
		},
	}
	rawJSON, _ := json.Marshal(replayReq)

	resp, err := http.Post(server.URL()+"/api/v1/dlq/replay", "application/json", bytes.NewReader(rawJSON))
	if err != nil {
		t.Fatalf("POST /api/v1/dlq/replay failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(body))
	}

	var res struct {
		Status      string `json:"status"`
		SourceTopic string `json:"source_topic"`
		TargetTopic string `json:"target_topic"`
		Offset      int64  `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res.Status != "replayed" || res.TargetTopic != "orders" {
		t.Errorf("unexpected replay response: %+v", res)
	}

	// 3. Verify message is now present in destination 'orders' topic
	records, err := svc.Fetch(context.Background(), "orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("fetch orders failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record in orders, got %d", len(records))
	}
	if string(records[0].Key) != "ord-bad" {
		t.Errorf("expected key 'ord-bad', got %s", string(records[0].Key))
	}
	if !strings.Contains(string(records[0].Value), "repaired") {
		t.Errorf("expected value to contain 'repaired', got: %s", string(records[0].Value))
	}
}

func TestAPI_DLQReplay_Validation(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	// Missing dlq_topic and target_topic
	badReq := map[string]any{
		"value": "some-val",
	}
	rawJSON, _ := json.Marshal(badReq)

	resp, err := http.Post(server.URL()+"/api/v1/dlq/replay", "application/json", bytes.NewReader(rawJSON))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got: %d", resp.StatusCode)
	}
}

func TestUI_ServeIndexHTML(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	for _, path := range []string{"/", "/ui"} {
		resp, err := http.Get(server.URL() + path)
		if err != nil {
			t.Fatalf("GET %s failed: %v", path, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK for %s, got: %d", path, resp.StatusCode)
		}
		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "text/html") {
			t.Errorf("expected Content-Type text/html for %s, got: %s", path, contentType)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "PocketQueue") {
			t.Errorf("expected 'PocketQueue' in HTML body for %s", path)
		}
	}
}

func TestUI_ServeStaticAssets(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	cases := []struct {
		path        string
		expectedCode int
		expectedType string
	}{
		{"/static/pico.min.css", http.StatusOK, "text/css"},
		{"/static/app.css", http.StatusOK, "text/css"},
		{"/static/app.js", http.StatusOK, "application/javascript"},
		{"/static/favicon.svg", http.StatusOK, "image/svg+xml"},
		{"/static/unknown.xyz", http.StatusNotFound, ""},
	}

	for _, tc := range cases {
		resp, err := http.Get(server.URL() + tc.path)
		if err != nil {
			t.Fatalf("GET %s failed: %v", tc.path, err)
		}
		resp.Body.Close()

		if resp.StatusCode != tc.expectedCode {
			t.Errorf("expected %d for %s, got: %d", tc.expectedCode, tc.path, resp.StatusCode)
		}
		if tc.expectedType != "" && !strings.Contains(resp.Header.Get("Content-Type"), tc.expectedType) {
			t.Errorf("expected Content-Type %s for %s, got: %s", tc.expectedType, tc.path, resp.Header.Get("Content-Type"))
		}
	}
}

func TestUI_Caching_ETag(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	// 1. Initial request to obtain ETag
	resp, err := http.Get(server.URL() + "/static/app.css")
	if err != nil {
		t.Fatalf("initial GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got: %d", resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("expected non-empty ETag header")
	}

	// 2. Conditional request with matching If-None-Match
	req, err := http.NewRequest(http.MethodGet, server.URL()+"/static/app.css", nil)
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	req.Header.Set("If-None-Match", etag)

	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("conditional GET failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusNotModified {
		t.Errorf("expected 304 Not Modified, got: %d", resp2.StatusCode)
	}
}

func TestUI_GzipCompression(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL()+"/", nil)
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	req.Header.Set("Accept-Encoding", "gzip")

	// Use transport that does NOT automatically decompress so we can inspect Content-Encoding
	tr := &http.Transport{DisableCompression: true}
	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET with gzip failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("expected Content-Encoding gzip, got: %s", resp.Header.Get("Content-Encoding"))
	}

	gzReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("gzip.NewReader failed: %v", err)
	}
	defer gzReader.Close()

	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("read decompressed failed: %v", err)
	}

	if !strings.Contains(string(decompressed), "PocketQueue") {
		t.Errorf("expected 'PocketQueue' in decompressed content")
	}
}

func TestHTTP_SSEStream_Wildcard(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL()+"/api/v1/topics/_all/sse", nil)
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

	// Produce messages to 2 different topics
	go func() {
		time.Sleep(30 * time.Millisecond)
		_, _ = svc.Produce(context.Background(), "topic-a", 0, []byte("k1"), []byte("payload A"), nil)
		time.Sleep(20 * time.Millisecond)
		_, _ = svc.Produce(context.Background(), "topic-b", 0, []byte("k2"), []byte("payload B"), nil)
	}()

	scanner := bufio.NewScanner(resp.Body)
	var foundA, foundB bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "payload A") {
			foundA = true
		}
		if strings.Contains(line, "payload B") {
			foundB = true
		}
		if foundA && foundB {
			break
		}
	}

	if !foundA || !foundB {
		t.Fatalf("expected both topic-a and topic-b in wildcard SSE stream, got foundA=%v, foundB=%v", foundA, foundB)
	}
}

func TestHTTP_SSEStream_DecodedPayload(t *testing.T) {
	svc, server := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	reg := decoder.NewSchemaRegistry()
	if err := reg.LoadDescriptorSet("../../../../test/fixtures/schemas.desc"); err != nil {
		t.Fatalf("failed to load descriptor set: %v", err)
	}
	server.SetDecoder(decoder.NewUniversalDecoder(reg))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL()+"/api/v1/topics/_all/sse", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect SSE stream: %v", err)
	}
	defer resp.Body.Close()

	// Prepare Confluent Protobuf payload
	desc := reg.FindDescriptor("OrderEvent")
	msg := dynamicpb.NewMessage(desc)
	msg.Set(desc.Fields().ByName("order_id"), protoreflect.ValueOfString("ORD-DECODED-42"))
	msg.Set(desc.Fields().ByName("customer_id"), protoreflect.ValueOfString("CUST-42"))
	msg.Set(desc.Fields().ByName("total_amount"), protoreflect.ValueOfFloat64(750.50))
	pbBytes, _ := proto.Marshal(msg)

	confluentPayload := make([]byte, 5)
	confluentPayload[0] = 0x00
	binary.BigEndian.PutUint32(confluentPayload[1:5], 99)
	confluentPayload = append(confluentPayload, 0x00) // varint 0
	confluentPayload = append(confluentPayload, pbBytes...)

	go func() {
		time.Sleep(30 * time.Millisecond)
		_, _ = svc.Produce(context.Background(), "orders.proto", 0, []byte("ord-key"), confluentPayload, nil)
	}()

	scanner := bufio.NewScanner(resp.Body)
	var foundDecoded bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			dataJSON := strings.TrimPrefix(line, "data: ")
			var obj struct {
				Topic   string `json:"topic"`
				Decoded struct {
					Kind        string  `json:"kind"`
					SchemaID    uint32  `json:"schema_id"`
					MessageName string  `json:"message_name"`
					Formatted   string  `json:"formatted"`
					IsBinary    bool    `json:"is_binary"`
				} `json:"decoded"`
			}
			if err := json.Unmarshal([]byte(dataJSON), &obj); err == nil && obj.Topic == "orders.proto" {
				if obj.Decoded.Kind != "confluent_protobuf" {
					t.Errorf("expected kind 'confluent_protobuf', got: %s", obj.Decoded.Kind)
				}
				if obj.Decoded.SchemaID != 99 {
					t.Errorf("expected schema_id=99, got: %d", obj.Decoded.SchemaID)
				}
				if obj.Decoded.MessageName != "OrderEvent" {
					t.Errorf("expected message_name='OrderEvent', got: %s", obj.Decoded.MessageName)
				}
				if !strings.Contains(obj.Decoded.Formatted, "ORD-DECODED-42") {
					t.Errorf("expected formatted JSON to contain ORD-DECODED-42, got: %s", obj.Decoded.Formatted)
				}
				if !obj.Decoded.IsBinary {
					t.Errorf("expected is_binary=true")
				}
				foundDecoded = true
				break
			}
		}
	}

	if !foundDecoded {
		t.Fatalf("did not receive decoded confluent protobuf message in SSE stream")
	}
}
