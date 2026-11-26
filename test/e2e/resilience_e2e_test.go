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
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestE2E_Resilience_OffsetPersistenceAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	// 1. Session 1: Boot broker with WAL and FileOffsetStore
	wal1, err := storage.NewWALEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create wal1: %v", err)
	}
	store1, err := broker.NewFileOffsetStore(dataDir)
	if err != nil {
		t.Fatalf("failed to create store1: %v", err)
	}
	svc1 := broker.NewService(wal1, nil, 1)
	if err := svc1.SetOffsetStore(store1); err != nil {
		t.Fatalf("failed to set store1: %v", err)
	}

	handler1 := kafka.NewHandler(svc1, "127.0.0.1", 0, nil)
	server1 := kafka.NewServer("127.0.0.1:0", handler1, nil)
	if err := server1.Start(); err != nil {
		t.Fatalf("failed to start server1: %v", err)
	}

	// Commit offset 77
	if err := svc1.CommitOffset(ctx, "payment-workers", "orders", 0, 77); err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Graceful shutdown
	if err := server1.Close(); err != nil {
		t.Fatalf("failed to close server1: %v", err)
	}
	if err := svc1.Close(); err != nil {
		t.Fatalf("failed to close svc1: %v", err)
	}

	// Verify offsets.json was written to dataDir
	offsetsPath := filepath.Join(dataDir, "offsets.json")
	if _, err := os.Stat(offsetsPath); err != nil {
		t.Fatalf("expected offsets.json to exist in %s: %v", dataDir, err)
	}

	// 2. Session 2: Boot new broker on same dataDir
	wal2, err := storage.NewWALEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create wal2: %v", err)
	}
	store2, err := broker.NewFileOffsetStore(dataDir)
	if err != nil {
		t.Fatalf("failed to create store2: %v", err)
	}
	svc2 := broker.NewService(wal2, nil, 1)
	if err := svc2.SetOffsetStore(store2); err != nil {
		t.Fatalf("failed to set store2: %v", err)
	}
	defer svc2.Close()

	// Verify committed offset restored
	offset, err := svc2.GetOffset(ctx, "payment-workers", "orders", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset != 77 {
		t.Fatalf("expected restored offset 77, got: %d", offset)
	}
}

func TestE2E_Resilience_RetentionPruning(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	// Small limit: 3 KB per partition
	wal, err := storage.NewWALEngineWithLimit(dataDir, 3072)
	if err != nil {
		t.Fatalf("failed to create wal: %v", err)
	}
	svc := broker.NewService(wal, nil, 1)
	defer svc.Close()

	// Produce 60 messages of 80 bytes each (total ~4.8 KB > 3 KB)
	payload := make([]byte, 80)
	for i := range payload {
		payload[i] = 'Z'
	}

	for i := 0; i < 60; i++ {
		_, err := svc.Produce(ctx, "telemetry", 0, []byte(fmt.Sprintf("k-%d", i)), payload, nil)
		if err != nil {
			t.Fatalf("produce %d failed: %v", i, err)
		}
	}

	meta, err := svc.GetTopicMetadata(ctx, "telemetry")
	if err != nil {
		t.Fatalf("failed to get topic metadata: %v", err)
	}

	p0 := meta.Partitions[0]
	if p0.Earliest == 0 {
		t.Fatalf("expected earliest watermark to advance > 0, got: %d", p0.Earliest)
	}
	if p0.Latest != 60 {
		t.Fatalf("expected latest watermark 60, got: %d", p0.Latest)
	}

	// Verify fetch reads surviving records cleanly
	records, err := svc.Fetch(ctx, "telemetry", 0, p0.Earliest, 100)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if int64(len(records)) != p0.TotalCount {
		t.Fatalf("expected %d records, got: %d", p0.TotalCount, len(records))
	}
}

func TestE2E_Resilience_PingProbeAndDrain(t *testing.T) {
	mem := storage.NewMemoryEngine(100)
	svc := broker.NewService(mem, nil, 1)
	defer svc.Close()

	handler := kafka.NewHandler(svc, "127.0.0.1", 0, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// 1. Send Ping Probe
	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	reqBuf := new(bytes.Buffer)
	enc := kafka.NewEncoder(reqBuf)
	enc.PutInt16(kafka.ApiKeyApiVersions)
	enc.PutInt16(0)
	enc.PutInt32(123)
	enc.PutString("e2e-ping")

	payload := reqBuf.Bytes()
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(payload)))

	_, _ = conn.Write(sizeBuf[:])
	_, _ = conn.Write(payload)

	_, err = io.ReadFull(conn, sizeBuf[:])
	if err != nil {
		t.Fatalf("read ping resp size: %v", err)
	}
	respSize := binary.BigEndian.Uint32(sizeBuf[:])
	respPayload := make([]byte, respSize)
	_, err = io.ReadFull(conn, respPayload)
	if err != nil {
		t.Fatalf("read ping resp payload: %v", err)
	}

	dec := kafka.NewDecoder(respPayload)
	corrID, _ := dec.GetInt32()
	errCode, _ := dec.GetInt16()
	if corrID != 123 || errCode != 0 {
		t.Fatalf("unexpected ping response: corrID=%d, errCode=%d", corrID, errCode)
	}

	// 2. Test Graceful Drain
	drainStart := time.Now()
	if err := server.CloseWithTimeout(300 * time.Millisecond); err != nil {
		t.Fatalf("server CloseWithTimeout failed: %v", err)
	}
	drainDuration := time.Since(drainStart)
	if drainDuration > 500*time.Millisecond {
		t.Fatalf("drain took too long: %v", drainDuration)
	}
}
