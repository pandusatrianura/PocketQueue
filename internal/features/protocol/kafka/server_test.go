package kafka_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestKafkaServer_TCPHandshakeAndProduceFetch(t *testing.T) {
	// 1. Setup broker & server
	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	// 2. Connect client via TCP
	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	// Helper to send frame and read response
	sendAndReceive := func(payload []byte) []byte {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))

		_, err := conn.Write(lenBuf[:])
		if err != nil {
			t.Fatalf("write len: %v", err)
		}
		_, err = conn.Write(payload)
		if err != nil {
			t.Fatalf("write payload: %v", err)
		}

		// Read response
		var respLenBuf [4]byte
		_, err = io.ReadFull(conn, respLenBuf[:])
		if err != nil {
			t.Fatalf("read resp len: %v", err)
		}
		respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
		respBytes := make([]byte, respLen)
		_, err = io.ReadFull(conn, respBytes)
		if err != nil {
			t.Fatalf("read resp payload: %v", err)
		}
		return respBytes
	}

	// 3. Test ApiVersions (Key 18)
	reqBuf := new(bytes.Buffer)
	enc := kafka.NewEncoder(reqBuf)
	enc.PutInt16(kafka.ApiKeyApiVersions)
	enc.PutInt16(3)       // version 3
	enc.PutInt32(777)     // correlation ID
	enc.PutString("unit-client")

	respBytes := sendAndReceive(reqBuf.Bytes())
	dec := kafka.NewDecoder(respBytes)

	corrId, _ := dec.GetInt32()
	if corrId != 777 {
		t.Errorf("expected correlation ID 777, got %d", corrId)
	}
	errCode, _ := dec.GetInt16()
	if errCode != 0 {
		t.Errorf("expected ApiVersions error code 0, got %d", errCode)
	}

	// 4. Test Produce (Key 0)
	prodBuf := new(bytes.Buffer)
	pEnc := kafka.NewEncoder(prodBuf)
	pEnc.PutInt16(kafka.ApiKeyProduce)
	pEnc.PutInt16(3)     // version 3
	pEnc.PutInt32(888)   // correlation ID
	pEnc.PutString("unit-client")

	// Produce body
	pEnc.PutNullableString(nil) // transactional_id
	pEnc.PutInt16(1)            // acks = 1
	pEnc.PutInt32(1500)         // timeout_ms = 1500

	// 1 topic
	pEnc.PutInt32(1)
	pEnc.PutString("orders")

	// 1 partition
	pEnc.PutInt32(1)
	pEnc.PutInt32(0) // partition 0

	// Encoded RecordBatch
	records := []kafka.RecordItem{
		{Key: []byte("ord-1"), Value: []byte(`{"status":"new"}`)},
	}
	batchBytes := kafka.EncodeRecordBatchV2(0, records)
	pEnc.PutBytes(batchBytes)

	prodRespBytes := sendAndReceive(prodBuf.Bytes())
	pDec := kafka.NewDecoder(prodRespBytes)

	pCorrId, _ := pDec.GetInt32()
	if pCorrId != 888 {
		t.Errorf("expected produce correlation ID 888, got %d", pCorrId)
	}
	topicsCount, _ := pDec.GetInt32()
	if topicsCount != 1 {
		t.Fatalf("expected 1 topic response, got %d", topicsCount)
	}
	tName, _ := pDec.GetString()
	if tName != "orders" {
		t.Errorf("expected topic 'orders', got %s", tName)
	}
	pCount, _ := pDec.GetInt32()
	if pCount != 1 {
		t.Fatalf("expected 1 partition response, got %d", pCount)
	}
	partId, _ := pDec.GetInt32()
	pErr, _ := pDec.GetInt16()
	baseOffset, _ := pDec.GetInt64()

	if partId != 0 || pErr != 0 || baseOffset != 0 {
		t.Errorf("unexpected produce result: part=%d, err=%d, baseOffset=%d", partId, pErr, baseOffset)
	}

	// 5. Test Fetch (Key 1)
	fetchBuf := new(bytes.Buffer)
	fEnc := kafka.NewEncoder(fetchBuf)
	fEnc.PutInt16(kafka.ApiKeyFetch)
	fEnc.PutInt16(4)     // version 4
	fEnc.PutInt32(999)   // correlation ID
	fEnc.PutString("unit-client")

	fEnc.PutInt32(-1)    // replica_id
	fEnc.PutInt32(500)   // max_wait_ms
	fEnc.PutInt32(1)     // min_bytes
	fEnc.PutInt32(65536) // max_bytes
	fEnc.PutInt8(0)      // isolation_level

	// 1 topic
	fEnc.PutInt32(1)
	fEnc.PutString("orders")

	// 1 partition
	fEnc.PutInt32(1)
	fEnc.PutInt32(0)     // partition 0
	fEnc.PutInt64(0)     // fetch_offset = 0
	fEnc.PutInt32(65536) // max_bytes

	fetchRespBytes := sendAndReceive(fetchBuf.Bytes())
	fDec := kafka.NewDecoder(fetchRespBytes)

	fCorrId, _ := fDec.GetInt32()
	if fCorrId != 999 {
		t.Errorf("expected fetch correlation ID 999, got %d", fCorrId)
	}
	_, _ = fDec.GetInt32() // throttle_time_ms
	fTopicsCount, _ := fDec.GetInt32()
	if fTopicsCount != 1 {
		t.Fatalf("expected 1 fetch topic response, got %d", fTopicsCount)
	}
	fTopicName, _ := fDec.GetString()
	if fTopicName != "orders" {
		t.Errorf("expected fetch topic 'orders', got %s", fTopicName)
	}
	fPartCount, _ := fDec.GetInt32()
	if fPartCount != 1 {
		t.Fatalf("expected 1 fetch partition response, got %d", fPartCount)
	}
	_, _ = fDec.GetInt32() // partition_id
	_, _ = fDec.GetInt16() // error_code
	_, _ = fDec.GetInt64() // high_watermark
	_, _ = fDec.GetInt64() // last_stable_offset
	_, _ = fDec.GetInt64() // log_start_offset
	_, _ = fDec.GetInt32() // aborted_transactions_count

	fetchedBatchBytes, err := fDec.GetBytes()
	if err != nil || len(fetchedBatchBytes) == 0 {
		t.Fatalf("expected non-empty record batch, got %d bytes (err: %v)", len(fetchedBatchBytes), err)
	}

	fetchedItems, err := kafka.DecodeRecordBatchV2(fetchedBatchBytes)
	if err != nil {
		t.Fatalf("failed to decode fetched record batch: %v", err)
	}

	if len(fetchedItems) != 1 {
		t.Fatalf("expected 1 fetched record, got %d", len(fetchedItems))
	}
	if string(fetchedItems[0].Key) != "ord-1" || string(fetchedItems[0].Value) != `{"status":"new"}` {
		t.Errorf("unexpected record contents: %+v", fetchedItems[0])
	}
}
