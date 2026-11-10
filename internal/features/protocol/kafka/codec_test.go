package kafka_test

import (
	"bytes"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
)

func TestCodec_Primitives(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := kafka.NewEncoder(buf)

	// Write primitives
	enc.PutInt8(42)
	enc.PutInt16(1024)
	enc.PutInt32(65536)
	enc.PutInt64(12345678901234)
	enc.PutString("pocketqueue")
	enc.PutBytes([]byte("hello-kafka"))
	enc.PutVarint(-42)
	enc.PutVarint(100)
	enc.PutUVarint(300)

	dec := kafka.NewDecoder(buf.Bytes())

	if v, err := dec.GetInt8(); err != nil || v != 42 {
		t.Errorf("Int8 expected 42, got %d (%v)", v, err)
	}
	if v, err := dec.GetInt16(); err != nil || v != 1024 {
		t.Errorf("Int16 expected 1024, got %d (%v)", v, err)
	}
	if v, err := dec.GetInt32(); err != nil || v != 65536 {
		t.Errorf("Int32 expected 65536, got %d (%v)", v, err)
	}
	if v, err := dec.GetInt64(); err != nil || v != 12345678901234 {
		t.Errorf("Int64 expected 12345678901234, got %d (%v)", v, err)
	}
	if v, err := dec.GetString(); err != nil || v != "pocketqueue" {
		t.Errorf("String expected 'pocketqueue', got %q (%v)", v, err)
	}
	if v, err := dec.GetBytes(); err != nil || string(v) != "hello-kafka" {
		t.Errorf("Bytes expected 'hello-kafka', got %q (%v)", v, err)
	}
	if v, err := dec.GetVarint(); err != nil || v != -42 {
		t.Errorf("Varint expected -42, got %d (%v)", v, err)
	}
	if v, err := dec.GetVarint(); err != nil || v != 100 {
		t.Errorf("Varint expected 100, got %d (%v)", v, err)
	}
	if v, err := dec.GetUVarint(); err != nil || v != 300 {
		t.Errorf("UVarint expected 300, got %d (%v)", v, err)
	}
}

func TestCodec_Nullables(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := kafka.NewEncoder(buf)

	enc.PutString("") // empty
	enc.PutNullableString(nil)
	enc.PutNullableBytes(nil)

	dec := kafka.NewDecoder(buf.Bytes())

	s, err := dec.GetString()
	if err != nil || s != "" {
		t.Errorf("expected empty string, got %q (%v)", s, err)
	}

	ns, err := dec.GetNullableString()
	if err != nil || ns != nil {
		t.Errorf("expected nil string, got %v (%v)", ns, err)
	}

	nb, err := dec.GetNullableBytes()
	if err != nil || nb != nil {
		t.Errorf("expected nil bytes, got %v (%v)", nb, err)
	}
}

func TestCodec_CRC32Castagnoli(t *testing.T) {
	data := []byte("123456789")
	checksum := kafka.ChecksumCRC32(data)
	// Castagnoli CRC32 for "123456789" is standard 0xe3069283
	if checksum != 0xe3069283 {
		t.Errorf("expected CRC32-C 0xe3069283, got 0x%08x", checksum)
	}
}

