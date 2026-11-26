package e2e_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
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

// TestProtobufDecoderE2E_FullPipeline tests end-to-end decoding across:
// 1. Confluent Wire Format (Magic 0x00 + 4-byte Schema ID + varint + Protobuf)
// 2. Raw Protobuf payload without header
// 3. Unrecognized binary payload (Hex Dump fallback)
// 4. Structured JSON payload (Auto-formatted)
// 5. UTF-8 Plain text
func TestProtobufDecoderE2E_FullPipeline(t *testing.T) {
	// 1. Setup Storage, Broker Service & Universal Decoder
	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(mem, bus, 1)
	defer svc.Close()

	reg := decoder.NewSchemaRegistry()
	if err := reg.LoadDescriptorSet("../fixtures/schemas.desc"); err != nil {
		t.Fatalf("failed to load descriptor set: %v", err)
	}

	orderDesc := reg.FindDescriptor("ecommerce.OrderEvent")
	if orderDesc == nil {
		orderDesc = reg.FindDescriptor("OrderEvent")
	}
	if orderDesc == nil {
		t.Fatalf("could not find OrderEvent descriptor in registry")
	}

	payloadDecoder := decoder.NewUniversalDecoder(reg)

	// 2. Start HTTP Server with Decoder attached
	httpServer := protocolhttp.NewServer("127.0.0.1:0", svc, bus, nil)
	httpServer.SetDecoder(payloadDecoder)
	if err := httpServer.Start(); err != nil {
		t.Fatalf("failed to start http server: %v", err)
	}
	defer httpServer.Close()

	// 3. Connect to SSE Stream (/api/v1/topics/_all/sse)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sseReq, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL()+"/api/v1/topics/_all/sse", nil)
	if err != nil {
		t.Fatalf("failed to build SSE request: %v", err)
	}

	sseResp, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatalf("failed to connect to SSE stream: %v", err)
	}
	defer sseResp.Body.Close()

	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 for SSE, got: %d", sseResp.StatusCode)
	}

	scanner := bufio.NewScanner(sseResp.Body)

	// Helper to read next SSE event payload
	readNextSSEEvent := func(timeout time.Duration) map[string]any {
		type readResult struct {
			data map[string]any
			err  error
		}
		ch := make(chan readResult, 1)
		go func() {
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "data: ") {
					jsonBytes := []byte(strings.TrimPrefix(line, "data: "))
					var payload map[string]any
					if err := json.Unmarshal(jsonBytes, &payload); err == nil {
						ch <- readResult{data: payload}
						return
					}
				}
			}
			ch <- readResult{err: scanner.Err()}
		}()

		select {
		case res := <-ch:
			if res.err != nil {
				t.Fatalf("error reading SSE stream: %v", res.err)
			}
			return res.data
		case <-time.After(timeout):
			t.Fatalf("timeout waiting for SSE event")
			return nil
		}
	}

	// Helper to create OrderEvent protobuf bytes
	createOrderProto := func(orderID, customerID string, amount float64) []byte {
		msg := dynamicpb.NewMessage(orderDesc)
		msg.Set(orderDesc.Fields().ByName("order_id"), protoreflect.ValueOfString(orderID))
		msg.Set(orderDesc.Fields().ByName("customer_id"), protoreflect.ValueOfString(customerID))
		msg.Set(orderDesc.Fields().ByName("total_amount"), protoreflect.ValueOfFloat64(amount))

		pbBytes, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("failed to marshal proto: %v", err)
		}
		return pbBytes
	}

	// ------------------------------------------------------------------------
	// Scenario A: Confluent Wire Format (Magic 0x00 + Schema ID 88 + Protobuf)
	// ------------------------------------------------------------------------
	t.Run("ConfluentWireFormat", func(t *testing.T) {
		pbData := createOrderProto("ORD-E2E-101", "CUST-E2E-909", 750.25)
		confluentHeader := make([]byte, 6)
		confluentHeader[0] = 0x00
		binary.BigEndian.PutUint32(confluentHeader[1:5], 88) // Schema ID = 88
		confluentHeader[5] = 0x00                            // varint 0 (top-level)
		confluentMsg := append(confluentHeader, pbData...)

		_, err := svc.Produce(context.Background(), "orders.confluent", 0, []byte("key-conf-1"), confluentMsg, nil)
		if err != nil {
			t.Fatalf("publish confluent message failed: %v", err)
		}

		event := readNextSSEEvent(3 * time.Second)
		if event["topic"] != "orders.confluent" {
			t.Errorf("expected topic 'orders.confluent', got: %v", event["topic"])
		}

		decoded, ok := event["decoded"].(map[string]any)
		if !ok || decoded == nil {
			t.Fatalf("expected decoded field in SSE event, got: %v", event["decoded"])
		}

		if decoded["kind"] != "confluent_protobuf" {
			t.Errorf("expected kind 'confluent_protobuf', got: %v", decoded["kind"])
		}
		if schemaID, ok := decoded["schema_id"].(float64); !ok || uint32(schemaID) != 88 {
			t.Errorf("expected schema_id 88, got: %v", decoded["schema_id"])
		}
		if msgName, ok := decoded["message_name"].(string); !ok || !strings.Contains(msgName, "OrderEvent") {
			t.Errorf("expected message_name to contain OrderEvent, got: %v", decoded["message_name"])
		}
		if isBinary, ok := decoded["is_binary"].(bool); !ok || !isBinary {
			t.Errorf("expected is_binary=true, got: %v", decoded["is_binary"])
		}

		formatted, _ := decoded["formatted"].(string)
		if !strings.Contains(formatted, "ORD-E2E-101") || !strings.Contains(formatted, "CUST-E2E-909") || !strings.Contains(formatted, "750.25") {
			t.Errorf("expected formatted JSON to contain order details, got: %s", formatted)
		}
	})

	// ------------------------------------------------------------------------
	// Scenario B: Raw Protobuf Payload (Without Confluent Header)
	// ------------------------------------------------------------------------
	t.Run("RawProtobuf", func(t *testing.T) {
		pbData := createOrderProto("ORD-RAW-202", "CUST-RAW-808", 129.99)

		_, err := svc.Produce(context.Background(), "orders.raw", 0, []byte("key-raw-2"), pbData, nil)
		if err != nil {
			t.Fatalf("publish raw proto failed: %v", err)
		}

		event := readNextSSEEvent(3 * time.Second)
		if event["topic"] != "orders.raw" {
			t.Errorf("expected topic 'orders.raw', got: %v", event["topic"])
		}

		decoded, ok := event["decoded"].(map[string]any)
		if !ok || decoded == nil {
			t.Fatalf("expected decoded field in SSE event")
		}

		if decoded["kind"] != "protobuf" {
			t.Errorf("expected kind 'protobuf', got: %v", decoded["kind"])
		}
		if msgName, ok := decoded["message_name"].(string); !ok || !strings.Contains(msgName, "OrderEvent") {
			t.Errorf("expected message_name to contain OrderEvent, got: %v", decoded["message_name"])
		}
		if isBinary, ok := decoded["is_binary"].(bool); !ok || !isBinary {
			t.Errorf("expected is_binary=true, got: %v", decoded["is_binary"])
		}

		formatted, _ := decoded["formatted"].(string)
		if !strings.Contains(formatted, "ORD-RAW-202") || !strings.Contains(formatted, "CUST-RAW-808") {
			t.Errorf("expected formatted JSON to contain order details, got: %s", formatted)
		}
	})

	// ------------------------------------------------------------------------
	// Scenario C: Unrecognized / Corrupt Binary (Hex Dump Fallback)
	// ------------------------------------------------------------------------
	t.Run("HexDumpFallback", func(t *testing.T) {
		corruptBinary := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0xCA, 0xFE, 0xBA, 0xBE, 0x88, 0x99, 0xAA, 0xBB, 0xCC}

		_, err := svc.Produce(context.Background(), "sensors.binary", 0, []byte("key-bin-3"), corruptBinary, nil)
		if err != nil {
			t.Fatalf("publish corrupt binary failed: %v", err)
		}

		event := readNextSSEEvent(3 * time.Second)
		if event["topic"] != "sensors.binary" {
			t.Errorf("expected topic 'sensors.binary', got: %v", event["topic"])
		}

		decoded, ok := event["decoded"].(map[string]any)
		if !ok || decoded == nil {
			t.Fatalf("expected decoded field in SSE event")
		}

		if decoded["kind"] != "hex_dump" {
			t.Errorf("expected kind 'hex_dump', got: %v", decoded["kind"])
		}
		if isBinary, ok := decoded["is_binary"].(bool); !ok || !isBinary {
			t.Errorf("expected is_binary=true, got: %v", decoded["is_binary"])
		}

		formatted, _ := decoded["formatted"].(string)
		if !strings.Contains(formatted, "00000000") || !strings.Contains(formatted, "de ad be ef") {
			t.Errorf("expected formatted hex dump with offset and bytes, got:\n%s", formatted)
		}
	})

	// ------------------------------------------------------------------------
	// Scenario D: Valid JSON Payload
	// ------------------------------------------------------------------------
	t.Run("JSONPayload", func(t *testing.T) {
		jsonRaw := []byte(`{"action":"sync","batch_id":555,"status":"ok"}`)

		_, err := svc.Produce(context.Background(), "events.json", 0, []byte("key-json-4"), jsonRaw, nil)
		if err != nil {
			t.Fatalf("publish json failed: %v", err)
		}

		event := readNextSSEEvent(3 * time.Second)
		decoded, ok := event["decoded"].(map[string]any)
		if !ok || decoded == nil {
			t.Fatalf("expected decoded field in SSE event")
		}

		if decoded["kind"] != "json" {
			t.Errorf("expected kind 'json', got: %v", decoded["kind"])
		}
		if isBinary, ok := decoded["is_binary"].(bool); !ok || isBinary {
			t.Errorf("expected is_binary=false, got: %v", decoded["is_binary"])
		}

		formatted, _ := decoded["formatted"].(string)
		if !strings.Contains(formatted, `"batch_id": 555`) {
			t.Errorf("expected indented JSON, got: %s", formatted)
		}
	})

	// ------------------------------------------------------------------------
	// Scenario E: Plain UTF-8 Text
	// ------------------------------------------------------------------------
	t.Run("PlainTextPayload", func(t *testing.T) {
		plainText := []byte("ping pong pocketqueue healthcheck")

		_, err := svc.Produce(context.Background(), "health.text", 0, []byte("key-txt-5"), plainText, nil)
		if err != nil {
			t.Fatalf("publish text failed: %v", err)
		}

		event := readNextSSEEvent(3 * time.Second)
		decoded, ok := event["decoded"].(map[string]any)
		if !ok || decoded == nil {
			t.Fatalf("expected decoded field in SSE event")
		}

		if decoded["kind"] != "text" {
			t.Errorf("expected kind 'text', got: %v", decoded["kind"])
		}
		if isBinary, ok := decoded["is_binary"].(bool); !ok || isBinary {
			t.Errorf("expected is_binary=false, got: %v", decoded["is_binary"])
		}

		formatted, _ := decoded["formatted"].(string)
		if formatted != "ping pong pocketqueue healthcheck" {
			t.Errorf("expected exact text, got: %s", formatted)
		}
	})
}
