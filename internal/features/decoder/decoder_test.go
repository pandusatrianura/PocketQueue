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

func TestDecoderCascade_JSON(t *testing.T) {
	d := decoder.NewUniversalDecoder(nil)

	input := []byte(`{"event": "signup", "user_id": 42}`)
	res := d.Decode(input)

	if res.Kind != decoder.KindJSON {
		t.Errorf("expected KindJSON, got: %s", res.Kind)
	}
	if res.IsBinary {
		t.Errorf("expected IsBinary=false for JSON")
	}
	if !strings.Contains(res.Formatted, `"user_id": 42`) {
		t.Errorf("expected formatted JSON, got: %s", res.Formatted)
	}
}

func TestDecoderCascade_PlainText(t *testing.T) {
	d := decoder.NewUniversalDecoder(nil)

	input := []byte("Hello, PocketQueue text message!")
	res := d.Decode(input)

	if res.Kind != decoder.KindText {
		t.Errorf("expected KindText, got: %s", res.Kind)
	}
	if res.IsBinary {
		t.Errorf("expected IsBinary=false for text")
	}
	if res.Formatted != string(input) {
		t.Errorf("expected exact text, got: %s", res.Formatted)
	}
}

func TestDecoderCascade_ConfluentProtobuf(t *testing.T) {
	reg := decoder.NewSchemaRegistry()
	if err := reg.LoadDescriptorSet("../../../test/fixtures/schemas.desc"); err != nil {
		t.Fatalf("failed to load descriptor set: %v", err)
	}

	d := decoder.NewUniversalDecoder(reg)

	// Create dynamic protobuf payload
	desc := reg.FindDescriptor("OrderEvent")
	msg := dynamicpb.NewMessage(desc)
	msg.Set(desc.Fields().ByName("order_id"), protoreflect.ValueOfString("ORD-CONFLUENT-1"))
	msg.Set(desc.Fields().ByName("customer_id"), protoreflect.ValueOfString("CUST-CONFLUENT-1"))
	msg.Set(desc.Fields().ByName("total_amount"), protoreflect.ValueOfFloat64(999.0))

	pbBytes, _ := proto.Marshal(msg)

	// Wrap with Confluent Wire Format (0x00 + Schema ID 88 + varint 0)
	confluentBytes := make([]byte, 5)
	confluentBytes[0] = 0x00
	binary.BigEndian.PutUint32(confluentBytes[1:5], 88)
	confluentBytes = append(confluentBytes, 0x00) // varint index 0
	confluentBytes = append(confluentBytes, pbBytes...)

	res := d.Decode(confluentBytes)

	if res.Kind != decoder.KindConfluent {
		t.Errorf("expected KindConfluent, got: %s", res.Kind)
	}
	if res.SchemaID != 88 {
		t.Errorf("expected SchemaID=88, got: %d", res.SchemaID)
	}
	if res.MessageName != "OrderEvent" {
		t.Errorf("expected MessageName=OrderEvent, got: %s", res.MessageName)
	}
	if !res.IsBinary {
		t.Errorf("expected IsBinary=true for Confluent protobuf")
	}
	if !strings.Contains(res.Formatted, "ORD-CONFLUENT-1") {
		t.Errorf("expected decoded JSON in formatted, got: %s", res.Formatted)
	}
}

func TestDecoderCascade_ConfluentWithoutSchema_FallbackHex(t *testing.T) {
	d := decoder.NewUniversalDecoder(nil) // no schemas loaded

	// Confluent wire format with unknown schema ID 999
	raw := make([]byte, 5)
	raw[0] = 0x00
	binary.BigEndian.PutUint32(raw[1:5], 999)
	raw = append(raw, []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02}...)

	res := d.Decode(raw)

	if res.Kind != decoder.KindConfluent {
		t.Errorf("expected KindConfluent, got: %s", res.Kind)
	}
	if res.SchemaID != 999 {
		t.Errorf("expected SchemaID=999, got: %d", res.SchemaID)
	}
	if !strings.Contains(res.Formatted, "00000000  ") {
		t.Errorf("expected hex dump in formatted, got: %s", res.Formatted)
	}
}

func TestDecoderCascade_RawProtobuf(t *testing.T) {
	reg := decoder.NewSchemaRegistry()
	_ = reg.LoadDescriptorSet("../../../test/fixtures/schemas.desc")
	d := decoder.NewUniversalDecoder(reg)

	desc := reg.FindDescriptor("OrderEvent")
	msg := dynamicpb.NewMessage(desc)
	msg.Set(desc.Fields().ByName("order_id"), protoreflect.ValueOfString("RAW-PB-123"))
	msg.Set(desc.Fields().ByName("customer_id"), protoreflect.ValueOfString("CUST-RAW-123"))
	pbBytes, _ := proto.Marshal(msg)

	res := d.Decode(pbBytes)

	if res.Kind != decoder.KindProtobuf {
		t.Errorf("expected KindProtobuf, got: %s", res.Kind)
	}
	if res.MessageName != "OrderEvent" {
		t.Errorf("expected MessageName=OrderEvent, got: %s", res.MessageName)
	}
	if !strings.Contains(res.Formatted, "RAW-PB-123") {
		t.Errorf("expected formatted JSON, got: %s", res.Formatted)
	}
}

func TestDecoderCascade_ArbitraryBinary_HexDump(t *testing.T) {
	d := decoder.NewUniversalDecoder(nil)

	// Random binary that is not valid JSON, text, or Confluent
	binaryData := []byte{0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01, 0x01, 0x00, 0x00, 0x00}
	res := d.Decode(binaryData)

	if res.Kind != decoder.KindHexDump {
		t.Errorf("expected KindHexDump, got: %s", res.Kind)
	}
	if !res.IsBinary {
		t.Errorf("expected IsBinary=true for arbitrary binary")
	}
	if !strings.Contains(res.Formatted, "00000000  7f 45 4c 46") {
		t.Errorf("expected formatted hex dump, got: %s", res.Formatted)
	}
}
