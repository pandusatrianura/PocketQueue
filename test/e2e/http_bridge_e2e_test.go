package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	protocolhttp "github.com/pandusatrianura/pocketqueue/internal/features/protocol/http"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/redis"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// TestHTTPBridgeE2E_HTTPProduceToKafkaFetch tests:
// Publish event via HTTP POST -> Fetch event via Kafka binary wire protocol.
func TestHTTPBridgeE2E_HTTPProduceToKafkaFetch(t *testing.T) {
	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(mem, bus, 1)
	defer svc.Close()

	// 1. Start HTTP Server
	httpServer := protocolhttp.NewServer("127.0.0.1:0", svc, bus, nil)
	if err := httpServer.Start(); err != nil {
		t.Fatalf("failed to start http server: %v", err)
	}
	defer httpServer.Close()

	// 2. Start Kafka Server
	kHandler := kafka.NewHandler(svc, "127.0.0.1", 0, nil)
	kServer := kafka.NewServer("127.0.0.1:0", kHandler, nil)
	if err := kServer.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer kServer.Close()

	// 3. Publish message via HTTP POST
	postPayload := map[string]any{
		"key":   "k-http-42",
		"value": "v-payload-from-http",
	}
	bodyBytes, _ := json.Marshal(postPayload)

	httpResp, err := http.Post(httpServer.URL()+"/api/v1/topics/e2e-bridge", "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("HTTP produce failed: %v", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created from HTTP bridge, got: %d", httpResp.StatusCode)
	}

	// 4. Connect via TCP to Kafka Server
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

	// 5. Fetch message via Kafka Fetch API (ApiKey 1, v4)
	fBuf := new(bytes.Buffer)
	fEnc := kafka.NewEncoder(fBuf)
	fEnc.PutInt16(kafka.ApiKeyFetch)
	fEnc.PutInt16(4)
	fEnc.PutInt32(202) // Correlation ID
	fEnc.PutString("e2e-client")
	fEnc.PutInt32(-1) // ReplicaId (-1 for regular consumer)
	fEnc.PutInt32(50) // MaxWaitTime (ms)
	fEnc.PutInt32(1)  // MinBytes
	fEnc.PutInt32(1024 * 1024)
	fEnc.PutInt8(0) // IsolationLevel
	// 1 topic
	fEnc.PutInt32(1)
	fEnc.PutString("e2e-bridge")
	// 1 partition
	fEnc.PutInt32(1)
	fEnc.PutInt32(0) // Partition 0
	fEnc.PutInt64(0) // FetchOffset 0
	fEnc.PutInt64(0) // LogStartOffset
	fEnc.PutInt32(1024 * 1024)

	fResp := callKafka(fBuf.Bytes())
	fDec := kafka.NewDecoder(fResp)
	corrId, _ := fDec.GetInt32()
	if corrId != 202 {
		t.Fatalf("unexpected kafka fetch corrId: %d", corrId)
	}

	_, _ = fDec.GetInt32() // ThrottleTimeMs
	tCount, _ := fDec.GetInt32()
	if tCount != 1 {
		t.Fatalf("expected 1 topic in fetch resp, got: %d", tCount)
	}
	fTopic, _ := fDec.GetString()
	if fTopic != "e2e-bridge" {
		t.Fatalf("expected topic 'e2e-bridge', got: %s", fTopic)
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
	_, _ = fDec.GetInt32() // AbortedTransactions count

	recordsBytes, _ := fDec.GetBytes()
	records, err := kafka.DecodeRecordBatchV2(recordsBytes)
	if err != nil {
		t.Fatalf("failed to decode fetched record batch: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record from Kafka fetch, got: %d", len(records))
	}

	if string(records[0].Key) != "k-http-42" {
		t.Errorf("expected key 'k-http-42', got: %s", string(records[0].Key))
	}
	if string(records[0].Value) != "v-payload-from-http" {
		t.Errorf("expected value 'v-payload-from-http', got: %s", string(records[0].Value))
	}
}

// TestHTTPBridgeE2E_RedisProduceToHTTPSSE tests:
// Publish event via native Redis Streams XADD -> Stream event via HTTP SSE stream.
func TestHTTPBridgeE2E_RedisProduceToHTTPSSE(t *testing.T) {
	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(mem, bus, 1)
	defer svc.Close()

	// 1. Start Redis Server
	rServer := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := rServer.Start(); err != nil {
		t.Fatalf("failed to start redis server: %v", err)
	}
	defer rServer.Close()

	// 2. Start HTTP Server
	httpServer := protocolhttp.NewServer("127.0.0.1:0", svc, bus, nil)
	if err := httpServer.Start(); err != nil {
		t.Fatalf("failed to start http server: %v", err)
	}
	defer httpServer.Close()

	// 3. Connect to SSE stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sseReq, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL()+"/api/v1/topics/metrics/sse", nil)
	if err != nil {
		t.Fatalf("failed to build SSE request: %v", err)
	}

	sseResp, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatalf("failed to connect SSE stream: %v", err)
	}
	defer sseResp.Body.Close()

	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 for SSE, got: %d", sseResp.StatusCode)
	}

	// 4. Connect to Redis Server and run XADD
	rConn, err := net.DialTimeout("tcp", rServer.Addr(), 3*time.Second)
	if err != nil {
		t.Fatalf("failed to dial redis: %v", err)
	}
	defer rConn.Close()

	// RESP: XADD metrics * cpu 88.5
	xaddCmd := "*5\r\n$4\r\nXADD\r\n$7\r\nmetrics\r\n$1\r\n*\r\n$3\r\ncpu\r\n$4\r\n88.5\r\n"
	if _, err := rConn.Write([]byte(xaddCmd)); err != nil {
		t.Fatalf("failed to write XADD: %v", err)
	}

	rReader := bufio.NewReader(rConn)
	rLine, err := rReader.ReadString('\n')
	if err != nil || !strings.HasPrefix(rLine, "$") {
		t.Fatalf("unexpected XADD response: %s (err: %v)", rLine, err)
	}

	// 5. Read SSE event
	scanner := bufio.NewScanner(sseResp.Body)
	var eventFound, dataFound bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: message") {
			eventFound = true
		}
		if strings.HasPrefix(line, "data: ") && strings.Contains(line, "88.5") {
			dataFound = true
			break
		}
	}

	if !eventFound || !dataFound {
		t.Fatalf("expected SSE stream to receive Redis event, got eventFound=%v, dataFound=%v", eventFound, dataFound)
	}
}

// TestHTTPBridgeE2E_DLQRouteToWebhookPOST tests:
// Poison pill message routed to DLQ -> Webhook receives HTTP POST notification with headers and body.
func TestHTTPBridgeE2E_DLQRouteToWebhookPOST(t *testing.T) {
	var callCount int32
	var capturedHeaders http.Header
	var capturedBody []byte
	var mu sync.Mutex

	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		mu.Lock()
		capturedHeaders = r.Header.Clone()
		capturedBody, _ = io.ReadAll(r.Body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webhookServer.Close()

	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(mem, bus, 1)
	defer svc.Close()

	dispatcher := broker.NewWebhookDLQDispatcher(webhookServer.URL, bus, webhookServer.Client(), nil)
	dispatcher.Start()
	defer dispatcher.Close()

	// Route a poison pill event to DLQ
	_, err := svc.SendToDLQ(context.Background(), "transactions", 0, []byte("tx-999"), []byte(`{"error":"bad checksum"}`), nil, "CRC_MISMATCH")
	if err != nil {
		t.Fatalf("SendToDLQ failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&callCount) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if atomic.LoadInt32(&callCount) != 1 {
		t.Fatalf("expected webhook to be invoked 1 time, got %d", atomic.LoadInt32(&callCount))
	}

	mu.Lock()
	defer mu.Unlock()

	if capturedHeaders.Get("x-dlq-original-topic") != "transactions" {
		t.Errorf("expected original topic 'transactions', got: %s", capturedHeaders.Get("x-dlq-original-topic"))
	}
	if capturedHeaders.Get("x-dlq-reason") != "CRC_MISMATCH" {
		t.Errorf("expected reason 'CRC_MISMATCH', got: %s", capturedHeaders.Get("x-dlq-reason"))
	}

	var data map[string]any
	if err := json.Unmarshal(capturedBody, &data); err != nil {
		t.Fatalf("failed to decode webhook body: %v", err)
	}
	if data["topic"] != "transactions.dlq" {
		t.Errorf("expected topic 'transactions.dlq', got: %v", data["topic"])
	}
	if data["key"] != "tx-999" {
		t.Errorf("expected key 'tx-999', got: %v", data["key"])
	}
}
