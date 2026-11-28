package e2e_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	protocolhttp "github.com/pandusatrianura/pocketqueue/internal/features/protocol/http"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// TestWebDashboardE2E_FullLifecycle tests the complete end-to-end user workflows
// for Milestone 8 (Embedded Web UI Dashboard):
// 1. Serving single-page UI HTML and static embedded assets (ETag caching & Gzip).
// 2. Producing events via HTTP REST API.
// 3. Polling topics metadata and system stats via REST API.
// 4. Real-time wildcard SSE streaming across topics.
// 5. DLQ message inspection and 1-click replay verified via Kafka protocol fetch.
func TestWebDashboardE2E_FullLifecycle(t *testing.T) {
	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(mem, bus, 1)
	defer svc.Close()

	// 1. Start HTTP Server with UI Dashboard & REST Bridge enabled
	httpServer := protocolhttp.NewServer("127.0.0.1:0", svc, bus, nil)
	if err := httpServer.Start(); err != nil {
		t.Fatalf("failed to start http server: %v", err)
	}
	defer httpServer.Close()

	// 2. Start Kafka Server for cross-protocol verification
	kHandler := kafka.NewHandler(svc, "127.0.0.1", 0, nil)
	kServer := kafka.NewServer("127.0.0.1:0", kHandler, nil)
	if err := kServer.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer kServer.Close()

	baseURL := httpServer.URL()

	// -------------------------------------------------------------
	// Step 1: UI Dashboard HTML Serving (GET / and GET /ui)
	// -------------------------------------------------------------
	for _, path := range []string{"/", "/ui"} {
		resp, err := http.Get(baseURL + path)
		if err != nil {
			t.Fatalf("failed to fetch %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s returned status %d, expected 200", path, resp.StatusCode)
		}
		contentType := resp.Header.Get("Content-Type")
		if !strings.HasPrefix(contentType, "text/html") {
			t.Errorf("%s Content-Type expected text/html, got: %s", path, contentType)
		}
		if etag := resp.Header.Get("ETag"); etag == "" {
			t.Errorf("%s missing ETag header", path)
		}

		bodyStr := string(body)
		expectedElements := []string{
			"<title>PocketQueue Dashboard</title>",
			`canvas id="sparkline"`,
			`id="tab-stream"`,
			`id="tab-topics"`,
			`id="tab-producer"`,
			`id="tab-dlq"`,
			`id="btn-publish-submit"`,
			`id="modal-replay"`,
		}
		for _, el := range expectedElements {
			if !strings.Contains(bodyStr, el) {
				t.Errorf("%s missing expected element %q", path, el)
			}
		}
	}

	// -------------------------------------------------------------
	// Step 2: Static Embedded Assets, MIME types, ETag & Gzip
	// -------------------------------------------------------------
	assetTests := []struct {
		path         string
		expectedMime string
	}{
		{"/static/pico.min.css", "text/css; charset=utf-8"},
		{"/static/app.css", "text/css; charset=utf-8"},
		{"/static/app.js", "application/javascript; charset=utf-8"},
		{"/static/favicon.svg", "image/svg+xml"},
	}

	for _, tc := range assetTests {
		resp, err := http.Get(baseURL + tc.path)
		if err != nil {
			t.Fatalf("failed to fetch static asset %s: %v", tc.path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("asset %s returned %d", tc.path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != tc.expectedMime {
			t.Errorf("asset %s Content-Type expected %q, got: %q", tc.path, tc.expectedMime, ct)
		}
		etag := resp.Header.Get("ETag")
		if etag == "" {
			t.Errorf("asset %s missing ETag", tc.path)
		}
		if len(body) == 0 {
			t.Errorf("asset %s has empty body", tc.path)
		}

		// Test ETag Cache Validation (304 Not Modified)
		req304, _ := http.NewRequest(http.MethodGet, baseURL+tc.path, nil)
		req304.Header.Set("If-None-Match", etag)
		resp304, err := http.DefaultClient.Do(req304)
		if err != nil {
			t.Fatalf("conditional GET failed: %v", err)
		}
		_ = resp304.Body.Close()
		if resp304.StatusCode != http.StatusNotModified {
			t.Errorf("expected 304 Not Modified for %s, got: %d", tc.path, resp304.StatusCode)
		}
	}

	// Test Gzip Compression
	reqGzip, _ := http.NewRequest(http.MethodGet, baseURL+"/static/app.js", nil)
	reqGzip.Header.Set("Accept-Encoding", "gzip")
	respGzip, err := http.DefaultClient.Do(reqGzip)
	if err != nil {
		t.Fatalf("gzip request failed: %v", err)
	}
	defer respGzip.Body.Close()

	if ce := respGzip.Header.Get("Content-Encoding"); ce != "gzip" {
		t.Errorf("expected Content-Encoding: gzip, got: %s", ce)
	}
	gzReader, err := gzip.NewReader(respGzip.Body)
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("failed to read decompressed gzip body: %v", err)
	}
	_ = gzReader.Close()
	if !strings.Contains(string(decompressed), "DOMContentLoaded") {
		t.Errorf("decompressed app.js missing DOMContentLoaded handler")
	}

	// -------------------------------------------------------------
	// Step 3: Produce Messages via REST API (POST /api/v1/topics/{topic})
	// -------------------------------------------------------------
	producePayload := map[string]any{
		"key":   "ord-e2e-101",
		"value": map[string]any{"sku": "LAPTOP-01", "price": 1299},
	}
	bodyBytes, _ := json.Marshal(producePayload)
	pResp, err := http.Post(baseURL+"/api/v1/topics/e2e-orders", "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("produce message failed: %v", err)
	}
	defer pResp.Body.Close()
	if pResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created from POST /api/v1/topics/e2e-orders, got: %d", pResp.StatusCode)
	}

	// -------------------------------------------------------------
	// Step 4: Metadata and Telemetry Stats API
	// -------------------------------------------------------------
	// GET /api/v1/topics
	tResp, err := http.Get(baseURL + "/api/v1/topics")
	if err != nil {
		t.Fatalf("GET /api/v1/topics failed: %v", err)
	}
	defer tResp.Body.Close()
	if tResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/topics returned %d", tResp.StatusCode)
	}

	var topicsData struct {
		Topics []struct {
			Name          string `json:"name"`
			Partitions    int32  `json:"partitions"`
			TotalMessages int64  `json:"total_messages"`
		} `json:"topics"`
	}
	if err := json.NewDecoder(tResp.Body).Decode(&topicsData); err != nil {
		t.Fatalf("failed to decode /api/v1/topics JSON: %v", err)
	}
	var foundTopic bool
	for _, tp := range topicsData.Topics {
		if tp.Name == "e2e-orders" {
			foundTopic = true
			if tp.TotalMessages < 1 {
				t.Errorf("topic e2e-orders expected TotalMessages >= 1, got: %d", tp.TotalMessages)
			}
		}
	}
	if !foundTopic {
		t.Errorf("topic e2e-orders not found in /api/v1/topics")
	}

	// GET /api/v1/stats
	sResp, err := http.Get(baseURL + "/api/v1/stats")
	if err != nil {
		t.Fatalf("GET /api/v1/stats failed: %v", err)
	}
	defer sResp.Body.Close()
	if sResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/stats returned %d", sResp.StatusCode)
	}

	var statsData struct {
		Status        string `json:"status"`
		Version       string `json:"version"`
		UptimeSeconds int64  `json:"uptime_seconds"`
		TotalMessages int64  `json:"total_messages"`
		TopicsCount   int    `json:"topics_count"`
	}
	if err := json.NewDecoder(sResp.Body).Decode(&statsData); err != nil {
		t.Fatalf("failed to decode /api/v1/stats JSON: %v", err)
	}
	if statsData.Status != "UP" {
		t.Errorf("stats status expected UP, got: %s", statsData.Status)
	}
	if statsData.Version != "0.9.0" {
		t.Errorf("stats version expected 0.9.0, got: %s", statsData.Version)
	}
	if statsData.TotalMessages < 1 {
		t.Errorf("stats total_messages expected >= 1, got: %d", statsData.TotalMessages)
	}
	if statsData.TopicsCount < 1 {
		t.Errorf("stats topics_count expected >= 1, got: %d", statsData.TopicsCount)
	}

	// -------------------------------------------------------------
	// Step 5: Real-Time Wildcard SSE Stream (GET /api/v1/topics/_all/sse)
	// -------------------------------------------------------------
	sseCtx, sseCancel := context.WithCancel(context.Background())
	defer sseCancel()

	sseReq, _ := http.NewRequestWithContext(sseCtx, http.MethodGet, baseURL+"/api/v1/topics/_all/sse", nil)
	sseResp, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatalf("connect to wildcard SSE stream failed: %v", err)
	}
	defer sseResp.Body.Close()

	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("SSE stream returned status %d", sseResp.StatusCode)
	}

	// Publish another event to a different topic while SSE is connected
	go func() {
		time.Sleep(30 * time.Millisecond)
		pData, _ := json.Marshal(map[string]any{
			"key":   "k-sse-stream",
			"value": "v-sse-val",
		})
		_, _ = http.Post(baseURL+"/api/v1/topics/e2e-live-feed", "application/json", bytes.NewReader(pData))
	}()

	scanner := bufio.NewScanner(sseResp.Body)
	var receivedSSEPayload string
	sseDeadline := time.After(2 * time.Second)

	receivedChan := make(chan string, 1)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				receivedChan <- strings.TrimPrefix(line, "data: ")
				return
			}
		}
	}()

	select {
	case receivedSSEPayload = <-receivedChan:
		// success
	case <-sseDeadline:
		t.Fatalf("timed out waiting for wildcard SSE event")
	}

	var sseObj struct {
		Topic string `json:"topic"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(receivedSSEPayload), &sseObj); err != nil {
		t.Fatalf("failed to parse SSE payload JSON: %v", err)
	}
	if sseObj.Topic != "e2e-live-feed" {
		t.Errorf("expected SSE topic 'e2e-live-feed', got: %s", sseObj.Topic)
	}
	if sseObj.Key != "k-sse-stream" {
		t.Errorf("expected SSE key 'k-sse-stream', got: %s", sseObj.Key)
	}
	if sseObj.Value != "v-sse-val" {
		t.Errorf("expected SSE value 'v-sse-val', got: %s", sseObj.Value)
	}

	// -------------------------------------------------------------
	// Step 6: DLQ Replay Workflow with Cross-Protocol Kafka Parity
	// -------------------------------------------------------------
	// 6a. Publish dead-letter message to DLQ topic "e2e-payments.dlq"
	dlqProducePayload := map[string]any{
		"key":   "tx-err-999",
		"value": `{"status":"failed","error":"gateway_timeout"}`,
	}
	dlqBytes, _ := json.Marshal(dlqProducePayload)
	_, _ = http.Post(baseURL+"/api/v1/topics/e2e-payments.dlq", "application/json", bytes.NewReader(dlqBytes))

	// 6b. Call DLQ Replay endpoint (POST /api/v1/dlq/replay)
	replayPayload := map[string]any{
		"dlq_topic":    "e2e-payments.dlq",
		"target_topic": "e2e-payments",
		"key":          "tx-err-999",
		"value":        `{"status":"reprocessed_ok","amount":750}`,
		"partition":    0,
	}
	replayBytes, _ := json.Marshal(replayPayload)
	replayResp, err := http.Post(baseURL+"/api/v1/dlq/replay", "application/json", bytes.NewReader(replayBytes))
	if err != nil {
		t.Fatalf("POST /api/v1/dlq/replay failed: %v", err)
	}
	defer replayResp.Body.Close()

	if replayResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from DLQ replay, got: %d", replayResp.StatusCode)
	}

	var replayResult struct {
		Status      string `json:"status"`
		TargetTopic string `json:"target_topic"`
		Offset      int64  `json:"offset"`
	}
	if err := json.NewDecoder(replayResp.Body).Decode(&replayResult); err != nil {
		t.Fatalf("failed to decode replay response: %v", err)
	}
	if replayResult.Status != "replayed" {
		t.Errorf("replay status expected 'replayed', got: %s", replayResult.Status)
	}
	if replayResult.TargetTopic != "e2e-payments" {
		t.Errorf("replay target_topic expected 'e2e-payments', got: %s", replayResult.TargetTopic)
	}

	// 6c. Verify replayed message is consumable over Kafka Wire Protocol
	kConn, err := net.DialTimeout("tcp", kServer.Addr(), 3*time.Second)
	if err != nil {
		t.Fatalf("dial kafka server failed: %v", err)
	}
	defer kConn.Close()

	callKafka := func(req []byte) []byte {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(req)))
		if _, err := kConn.Write(lenBuf[:]); err != nil {
			t.Fatalf("kafka write len: %v", err)
		}
		if _, err := kConn.Write(req); err != nil {
			t.Fatalf("kafka write req: %v", err)
		}

		var respLenBuf [4]byte
		if _, err := io.ReadFull(kConn, respLenBuf[:]); err != nil {
			t.Fatalf("kafka read resp len: %v", err)
		}
		respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
		resp := make([]byte, respLen)
		if _, err := io.ReadFull(kConn, resp); err != nil {
			t.Fatalf("kafka read resp body: %v", err)
		}
		return resp
	}

	// Send Kafka Fetch Request for topic "e2e-payments", partition 0, offset 0
	fBuf := new(bytes.Buffer)
	fEnc := kafka.NewEncoder(fBuf)
	fEnc.PutInt16(kafka.ApiKeyFetch)
	fEnc.PutInt16(4)
	fEnc.PutInt32(505) // Correlation ID
	fEnc.PutString("e2e-dlq-verifier")
	fEnc.PutInt32(-1) // ReplicaId
	fEnc.PutInt32(50) // MaxWaitTime
	fEnc.PutInt32(1)  // MinBytes
	fEnc.PutInt32(1024 * 1024)
	fEnc.PutInt8(0) // IsolationLevel
	fEnc.PutInt32(1)
	fEnc.PutString("e2e-payments")
	fEnc.PutInt32(1)
	fEnc.PutInt32(0) // Partition 0
	fEnc.PutInt64(0) // FetchOffset 0
	fEnc.PutInt64(0)
	fEnc.PutInt32(1024 * 1024)

	fResp := callKafka(fBuf.Bytes())
	fDec := kafka.NewDecoder(fResp)
	corrId, _ := fDec.GetInt32()
	if corrId != 505 {
		t.Fatalf("unexpected kafka fetch corrId: %d", corrId)
	}

	_, _ = fDec.GetInt32() // ThrottleTimeMs
	tCount, _ := fDec.GetInt32()
	if tCount != 1 {
		t.Fatalf("expected 1 topic in fetch resp, got: %d", tCount)
	}
	fTopic, _ := fDec.GetString()
	if fTopic != "e2e-payments" {
		t.Fatalf("expected topic 'e2e-payments', got: %s", fTopic)
	}
	pCount, _ := fDec.GetInt32()
	if pCount != 1 {
		t.Fatalf("expected 1 partition in fetch resp, got: %d", pCount)
	}
	_, _ = fDec.GetInt32() // PartitionIndex
	pErr, _ := fDec.GetInt16()
	if pErr != 0 {
		t.Fatalf("partition fetch error code: %d", pErr)
	}
	_, _ = fDec.GetInt64() // HighWatermark
	_, _ = fDec.GetInt64() // LastStableOffset
	_, _ = fDec.GetInt64() // LogStartOffset
	_, _ = fDec.GetInt32() // AbortedTransactions

	recordsBytes, _ := fDec.GetBytes()
	records, err := kafka.DecodeRecordBatchV2(recordsBytes)
	if err != nil {
		t.Fatalf("failed to decode replayed records from kafka: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 replayed record, got: %d", len(records))
	}

	rec := records[0]
	if string(rec.Key) != "tx-err-999" {
		t.Errorf("expected replayed key 'tx-err-999', got: %s", string(rec.Key))
	}
	if !strings.Contains(string(rec.Value), "reprocessed_ok") {
		t.Errorf("expected replayed value containing 'reprocessed_ok', got: %s", string(rec.Value))
	}

	// 6d. Also verify internal record storage contains header "x-replayed-from: e2e-payments.dlq"
	storedRecs, err := mem.ReadFrom(context.Background(), "e2e-payments", 0, 0, 10)
	if err != nil {
		t.Fatalf("failed to read from storage engine: %v", err)
	}
	if len(storedRecs) == 0 {
		t.Fatalf("expected stored records in e2e-payments, got none")
	}
	var foundReplayHeader bool
	for _, h := range storedRecs[0].Headers {
		if h.Key == "x-replayed-from" && string(h.Value) == "e2e-payments.dlq" {
			foundReplayHeader = true
			break
		}
	}
	if !foundReplayHeader {
		t.Errorf("expected header 'x-replayed-from: e2e-payments.dlq' on replayed record")
	}
}
