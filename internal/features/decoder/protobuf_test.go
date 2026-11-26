package decoder_test

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/decoder"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestConfluent_ParseHeader(t *testing.T) {
	// Too short (< 5 bytes)
	_, _, _, ok := decoder.ParseConfluentHeader([]byte{0x00, 0x01, 0x02})
	if ok {
		t.Errorf("expected ok=false for short data")
	}

	// Wrong magic byte
	_, _, _, ok = decoder.ParseConfluentHeader([]byte{0x01, 0x00, 0x00, 0x00, 0x05, 0xaa})
	if ok {
		t.Errorf("expected ok=false when magic byte != 0x00")
	}

	// Valid header: Schema ID = 101, index [0] (varint 0), payload = "hello"
	raw := make([]byte, 5)
	raw[0] = 0x00
	binary.BigEndian.PutUint32(raw[1:5], 101)
	// Add index count = 0 (top-level message) or index [0]
	raw = append(raw, 0x00) // varint 0 (index count / index)
	raw = append(raw, []byte("hello")...)

	schemaID, indexes, payload, ok := decoder.ParseConfluentHeader(raw)
	if !ok {
		t.Fatalf("expected ok=true for valid confluent header")
	}
	if schemaID != 101 {
		t.Errorf("expected schemaID=101, got: %d", schemaID)
	}
	if len(indexes) == 0 && string(payload) != "hello" && !strings.Contains(string(payload), "hello") {
		t.Errorf("payload mismatch, got: %s", string(payload))
	}
}

func TestProtobuf_DynamicDecode(t *testing.T) {
	reg := decoder.NewSchemaRegistry()
	err := reg.LoadDescriptorSet("../../../test/fixtures/schemas.desc")
	if err != nil {
		t.Fatalf("failed to load descriptor set: %v", err)
	}

	desc := reg.FindDescriptor("OrderEvent")
	if desc == nil {
		desc = reg.FindDescriptor("ecommerce.OrderEvent")
	}
	if desc == nil {
		t.Fatalf("could not find OrderEvent descriptor in registry")
	}

	msg := dynamicpb.NewMessage(desc)
	orderIdField := desc.Fields().ByName("order_id")
	customerIdField := desc.Fields().ByName("customer_id")
	totalAmountField := desc.Fields().ByName("total_amount")

	msg.Set(orderIdField, protoreflect.ValueOfString("ORD-777"))
	msg.Set(customerIdField, protoreflect.ValueOfString("CUST-999"))
	msg.Set(totalAmountField, protoreflect.ValueOfFloat64(189.50))

	pbBytes, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal dynamic proto: %v", err)
	}

	// Test DecodeMessage
	jsonStr, err := reg.DecodeMessage(desc, pbBytes)
	if err != nil {
		t.Fatalf("failed to decode message: %v", err)
	}

	if !strings.Contains(jsonStr, "ORD-777") {
		t.Errorf("expected json to contain ORD-777, got: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, "CUST-999") {
		t.Errorf("expected json to contain CUST-999, got: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, "189.5") {
		t.Errorf("expected json to contain 189.5, got: %s", jsonStr)
	}

	// Test TryDecodeAny
	msgName, autoJson, ok := reg.TryDecodeAny(pbBytes)
	if !ok {
		t.Fatalf("expected TryDecodeAny to succeed")
	}
	if !strings.Contains(msgName, "OrderEvent") {
		t.Errorf("expected msgName to contain OrderEvent, got: %s", msgName)
	}
	if !strings.Contains(autoJson, "ORD-777") {
		t.Errorf("expected autoJson to contain ORD-777, got: %s", autoJson)
	}
}
