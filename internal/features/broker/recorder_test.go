package broker_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestTrafficRecorder_RecordAndReadBack(t *testing.T) {
	tmpDir := t.TempDir()
	dumpPath := filepath.Join(tmpDir, "test_session.pqrec")

	rec, err := broker.NewTrafficRecorder(dumpPath)
	if err != nil {
		t.Fatalf("NewTrafficRecorder failed: %v", err)
	}

	if !rec.IsRecording() {
		t.Errorf("expected recorder to be active")
	}

	// 1. Write records
	sample1 := storage.Record{
		Topic:     "orders",
		Partition: 0,
		Offset:    101,
		Timestamp: time.UnixMilli(1700000000000),
		Key:       []byte("ord-101"),
		Value:     []byte(`{"status":"paid","amount":99.5}`),
		Headers: []storage.Header{
			{Key: "source", Value: []byte("checkout")},
		},
		IsDLQ: false,
	}

	sample2 := storage.Record{
		Topic:     "notifications",
		Partition: 1,
		Offset:    202,
		Timestamp: time.UnixMilli(1700000000500),
		Key:       []byte("user-42"),
		Value:     []byte(`{"type":"email","subject":"Receipt"}`),
		IsDLQ:     true,
	}

	if err := rec.Record(sample1); err != nil {
		t.Fatalf("Record sample1 failed: %v", err)
	}
	if err := rec.Record(sample2); err != nil {
		t.Fatalf("Record sample2 failed: %v", err)
	}

	if count := rec.RecordedCount(); count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}

	if err := rec.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if rec.IsRecording() {
		t.Errorf("expected recorder to be inactive after Close")
	}

	// 2. Read back and verify NDJSON
	f, err := os.Open(dumpPath)
	if err != nil {
		t.Fatalf("failed to open dump file: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var items []broker.RecordItem
	for scanner.Scan() {
		var item broker.RecordItem
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			t.Fatalf("unmarshal NDJSON line failed: %v", err)
		}
		items = append(items, item)
	}

	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}

	if items[0].Topic != "orders" || items[0].Key != "ord-101" || items[0].Headers["source"] != "checkout" {
		t.Errorf("unexpected item 0: %+v", items[0])
	}
	if items[1].Topic != "notifications" || items[1].IsDLQ != true {
		t.Errorf("unexpected item 1: %+v", items[1])
	}
}

func TestTrafficRecorder_ConcurrentThreadSafety(t *testing.T) {
	tmpDir := t.TempDir()
	dumpPath := filepath.Join(tmpDir, "concurrent.pqrec")

	rec, err := broker.NewTrafficRecorder(dumpPath)
	if err != nil {
		t.Fatalf("NewTrafficRecorder failed: %v", err)
	}

	const numWorkers = 10
	const msgsPerWorker = 50

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < msgsPerWorker; i++ {
				_ = rec.Record(storage.Record{
					Topic:     "stress",
					Partition: int32(workerID),
					Offset:    int64(i),
					Key:       []byte("key"),
					Value:     []byte("stress-value"),
				})
			}
		}(w)
	}

	wg.Wait()
	_ = rec.Close()

	if rec.RecordedCount() != int64(numWorkers*msgsPerWorker) {
		t.Errorf("expected count %d, got %d", numWorkers*msgsPerWorker, rec.RecordedCount())
	}
}
