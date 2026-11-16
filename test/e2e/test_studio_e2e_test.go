package e2e_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
	"github.com/pandusatrianura/pocketqueue/internal/features/testrunner"
)

func TestStudioE2E_RecordAndReplayLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	dumpPath := filepath.Join(tmpDir, "traffic.pqrec")

	// 1. Broker 1 with TrafficRecorder
	engine1 := storage.NewMemoryEngine(1000)
	bus1 := broker.NewEventBus(100)
	svc1 := broker.NewService(engine1, bus1, 1)

	recorder, err := broker.NewTrafficRecorder(dumpPath)
	if err != nil {
		t.Fatalf("failed to create recorder: %v", err)
	}

	subCh, unsub := bus1.Subscribe()
	defer unsub()
	stopRec := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopRec:
				return
			case r, ok := <-subCh:
				if !ok {
					return
				}
				_ = recorder.Record(r)
			}
		}
	}()

	handler1 := kafka.NewHandler(svc1, "127.0.0.1", 0, nil)
	server1 := kafka.NewServer("127.0.0.1:0", handler1, nil)
	if err := server1.Start(); err != nil {
		t.Fatalf("failed to start server1: %v", err)
	}
	defer server1.Close()

	// 2. Connect client and produce 5 messages
	conn, err := net.DialTimeout("tcp", server1.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial server1: %v", err)
	}
	defer conn.Close()

	for i := 0; i < 5; i++ {
		pBuf := new(bytes.Buffer)
		pEnc := kafka.NewEncoder(pBuf)
		pEnc.PutInt16(kafka.ApiKeyProduce)
		pEnc.PutInt16(3)
		pEnc.PutInt32(int32(100 + i))
		pEnc.PutString("e2e-studio-client")
		pEnc.PutNullableString(nil)
		pEnc.PutInt16(1)
		pEnc.PutInt32(1500)
		pEnc.PutInt32(1)
		pEnc.PutString("studio-topic")
		pEnc.PutInt32(1)
		pEnc.PutInt32(0)

		records := []kafka.RecordItem{
			{Key: []byte(fmt.Sprintf("k-%d", i)), Value: []byte(fmt.Sprintf(`{"seq":%d}`, i))},
		}
		pEnc.PutBytes(kafka.EncodeRecordBatchV2(int64(i), records))

		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(pBuf.Len()))
		_, _ = conn.Write(lenBuf[:])
		_, _ = conn.Write(pBuf.Bytes())

		var respLenBuf [4]byte
		_, _ = io.ReadFull(conn, respLenBuf[:])
		respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
		respPayload := make([]byte, respLen)
		_, _ = io.ReadFull(conn, respPayload)
	}

	time.Sleep(50 * time.Millisecond)
	close(stopRec)
	if err := recorder.Close(); err != nil {
		t.Fatalf("recorder close failed: %v", err)
	}

	// 3. Verify .pqrec file content
	data, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("failed to read .pqrec file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 NDJSON lines, got %d", len(lines))
	}

	// 4. Broker 2: Replay recorded session
	engine2 := storage.NewMemoryEngine(1000)
	bus2 := broker.NewEventBus(100)
	svc2 := broker.NewService(engine2, bus2, 1)

	replayer := broker.NewTrafficReplayer(dumpPath, svc2, 10.0, true) // fast speed, reset offsets
	replayedCount, err := replayer.Replay(context.Background())
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if replayedCount != 5 {
		t.Fatalf("expected 5 replayed messages, got %d", replayedCount)
	}

	// 5. Verify records in Broker 2
	fetched, err := svc2.Fetch(context.Background(), "studio-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if len(fetched) != 5 {
		t.Fatalf("expected 5 fetched records in broker 2, got %d", len(fetched))
	}
	for i, r := range fetched {
		expectedKey := fmt.Sprintf("k-%d", i)
		if string(r.Key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(r.Key))
		}
	}
}

func TestStudioE2E_TestRunnerAssertion(t *testing.T) {
	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 0, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Close()

	// Produce matching message in background after 40ms
	go func() {
		time.Sleep(40 * time.Millisecond)
		_, _ = svc.Produce(
			context.Background(),
			"user-logins",
			0,
			[]byte("auth-user-99"),
			[]byte(`{"event":"login_success","ip":"127.0.0.1"}`),
			nil,
		)
	}()

	spec := testrunner.AssertionSpec{
		BrokerAddr:         server.Addr(),
		Topic:              "user-logins",
		ExpectedKey:        "auth-user-99",
		ExpectedPayloadSub: "login_success",
		Timeout:            2 * time.Second,
		PollInterval:       20 * time.Millisecond,
	}

	result, err := testrunner.RunAssertion(context.Background(), spec)
	if err != nil {
		t.Fatalf("RunAssertion failed: %v", err)
	}

	if result.Topic != "user-logins" {
		t.Errorf("expected topic user-logins, got %s", result.Topic)
	}
	if result.Key != "auth-user-99" {
		t.Errorf("expected key auth-user-99, got %s", result.Key)
	}
	if !strings.Contains(result.Payload, "login_success") {
		t.Errorf("expected payload to contain login_success, got: %s", result.Payload)
	}
	if result.Offset != 0 {
		t.Errorf("expected offset 0, got %d", result.Offset)
	}
}
