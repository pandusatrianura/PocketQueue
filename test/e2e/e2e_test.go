package e2e_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestE2E_ProduceConsumeLifecycle(t *testing.T) {
	// 1. Boot up PocketQueue components
	engine := storage.NewMemoryEngine(5000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 0, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Close()

	// 2. Dial TCP connection
	conn, err := net.DialTimeout("tcp", server.Addr(), 3*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	call := func(reqBytes []byte) []byte {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(reqBytes)))

		if _, err := conn.Write(lenBuf[:]); err != nil {
			t.Fatalf("send len: %v", err)
		}
		if _, err := conn.Write(reqBytes); err != nil {
			t.Fatalf("send body: %v", err)
		}

		var respLenBuf [4]byte
		if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
			t.Fatalf("read resp len: %v", err)
		}
		respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
		resp := make([]byte, respLen)
		if _, err := io.ReadFull(conn, resp); err != nil {
			t.Fatalf("read resp body: %v", err)
		}
		return resp
	}

	// 3. ApiVersions Handshake
	hBuf := new(bytes.Buffer)
	hEnc := kafka.NewEncoder(hBuf)
	hEnc.PutInt16(kafka.ApiKeyApiVersions)
	hEnc.PutInt16(3)
	hEnc.PutInt32(101)
	hEnc.PutString("e2e-client")

	vResp := call(hBuf.Bytes())
	vDec := kafka.NewDecoder(vResp)
	cId, _ := vDec.GetInt32()
	errCode, _ := vDec.GetInt16()
	if cId != 101 || errCode != 0 {
		t.Fatalf("handshake failed: corr=%d err=%d", cId, errCode)
	}

	// 4. Produce 100 messages to "e2e-orders"
	const totalMsgs = 100
	topicName := "e2e-orders"

	records := make([]kafka.RecordItem, totalMsgs)
	for i := 0; i < totalMsgs; i++ {
		records[i] = kafka.RecordItem{
			Key:   []byte(fmt.Sprintf("order-%03d", i)),
			Value: []byte(fmt.Sprintf(`{"id": %d, "amount": %d}`, i, i*10)),
		}
	}

	batch := kafka.EncodeRecordBatchV2(0, records)

	pBuf := new(bytes.Buffer)
	pEnc := kafka.NewEncoder(pBuf)
	pEnc.PutInt16(kafka.ApiKeyProduce)
	pEnc.PutInt16(3)
	pEnc.PutInt32(202)
	pEnc.PutString("e2e-client")
	pEnc.PutNullableString(nil)
	pEnc.PutInt16(1)
	pEnc.PutInt32(1000)

	// Topics
	pEnc.PutInt32(1)
	pEnc.PutString(topicName)
	// Partitions
	pEnc.PutInt32(1)
	pEnc.PutInt32(0) // partition 0
	pEnc.PutBytes(batch)

	prodResp := call(pBuf.Bytes())
	pDec := kafka.NewDecoder(prodResp)
	pCorrId, _ := pDec.GetInt32()
	if pCorrId != 202 {
		t.Fatalf("unexpected produce corrId %d", pCorrId)
	}

	// 5. Fetch messages back
	fBuf := new(bytes.Buffer)
	fEnc := kafka.NewEncoder(fBuf)
	fEnc.PutInt16(kafka.ApiKeyFetch)
	fEnc.PutInt16(4)
	fEnc.PutInt32(303)
	fEnc.PutString("e2e-client")
	fEnc.PutInt32(-1)
	fEnc.PutInt32(100)
	fEnc.PutInt32(1)
	fEnc.PutInt32(1048576)
	fEnc.PutInt8(0)

	// Topics
	fEnc.PutInt32(1)
	fEnc.PutString(topicName)
	// Partitions
	fEnc.PutInt32(1)
	fEnc.PutInt32(0)
	fEnc.PutInt64(0) // start offset
	fEnc.PutInt32(1048576)

	fResp := call(fBuf.Bytes())
	fDec := kafka.NewDecoder(fResp)
	fCorrId, _ := fDec.GetInt32()
	if fCorrId != 303 {
		t.Fatalf("unexpected fetch corrId %d", fCorrId)
	}
	_, _ = fDec.GetInt32() // throttle
	topicsCount, _ := fDec.GetInt32()
	if topicsCount != 1 {
		t.Fatalf("expected 1 topic, got %d", topicsCount)
	}
	_, _ = fDec.GetString() // topic
	_, _ = fDec.GetInt32()  // partitions count
	_, _ = fDec.GetInt32()  // partition id
	fErrCode, _ := fDec.GetInt16()
	if fErrCode != 0 {
		t.Fatalf("fetch error code: %d", fErrCode)
	}
	hw, _ := fDec.GetInt64()
	if hw < int64(totalMsgs) {
		t.Fatalf("expected high watermark >= %d, got %d", totalMsgs, hw)
	}

	_, _ = fDec.GetInt64() // last stable offset
	_, _ = fDec.GetInt64() // log start offset
	_, _ = fDec.GetInt32() // aborted tx count

	batchBytes, err := fDec.GetBytes()
	if err != nil || len(batchBytes) == 0 {
		t.Fatalf("empty fetched batch: err=%v", err)
	}

	fetchedItems, err := kafka.DecodeRecordBatchV2(batchBytes)
	if err != nil {
		t.Fatalf("failed to decode fetched batch: %v", err)
	}

	if len(fetchedItems) != totalMsgs {
		t.Fatalf("expected %d fetched items, got %d", totalMsgs, len(fetchedItems))
	}

	// Verify exact content of item 0 and item 99
	expectedFirstKey := "order-000"
	expectedFirstVal := `{"id": 0, "amount": 0}`
	if string(fetchedItems[0].Key) != expectedFirstKey || string(fetchedItems[0].Value) != expectedFirstVal {
		t.Errorf("record 0 mismatch: got key=%q val=%q", fetchedItems[0].Key, fetchedItems[0].Value)
	}

	expectedLastKey := "order-099"
	expectedLastVal := `{"id": 99, "amount": 990}`
	if string(fetchedItems[99].Key) != expectedLastKey || string(fetchedItems[99].Value) != expectedLastVal {
		t.Errorf("record 99 mismatch: got key=%q val=%q", fetchedItems[99].Key, fetchedItems[99].Value)
	}

	// 6. Test Consumer Group Commit and Fetch
	cBuf := new(bytes.Buffer)
	cEnc := kafka.NewEncoder(cBuf)
	cEnc.PutInt16(kafka.ApiKeyOffsetCommit)
	cEnc.PutInt16(2)
	cEnc.PutInt32(404)
	cEnc.PutString("e2e-client")
	cEnc.PutString("e2e-workers") // Group ID
	cEnc.PutInt32(1)              // Generation ID
	cEnc.PutString("member-1")
	// 1 topic
	cEnc.PutInt32(1)
	cEnc.PutString(topicName)
	// 1 partition
	cEnc.PutInt32(1)
	cEnc.PutInt32(0)
	cEnc.PutInt64(100) // Committed offset = 100
	cEnc.PutString("") // Metadata

	commitResp := call(cBuf.Bytes())
	cDec := kafka.NewDecoder(commitResp)
	cCorrId, _ := cDec.GetInt32()
	if cCorrId != 404 {
		t.Fatalf("unexpected commit corrId %d", cCorrId)
	}

	// Fetch committed offset
	ofBuf := new(bytes.Buffer)
	ofEnc := kafka.NewEncoder(ofBuf)
	ofEnc.PutInt16(kafka.ApiKeyOffsetFetch)
	ofEnc.PutInt16(1)
	ofEnc.PutInt32(505)
	ofEnc.PutString("e2e-client")
	ofEnc.PutString("e2e-workers") // Group ID
	// 1 topic
	ofEnc.PutInt32(1)
	ofEnc.PutString(topicName)
	// 1 partition
	ofEnc.PutInt32(1)
	ofEnc.PutInt32(0)

	ofResp := call(ofBuf.Bytes())
	ofDec := kafka.NewDecoder(ofResp)
	ofCorrId, _ := ofDec.GetInt32()
	if ofCorrId != 505 {
		t.Fatalf("unexpected offset fetch corrId %d", ofCorrId)
	}
	_, _ = ofDec.GetInt32()  // topics count
	_, _ = ofDec.GetString() // topic name
	_, _ = ofDec.GetInt32()  // parts count
	_, _ = ofDec.GetInt32()  // part ID
	committedOffset, _ := ofDec.GetInt64()
	if committedOffset != 100 {
		t.Errorf("expected committed offset 100, got %d", committedOffset)
	}
}
