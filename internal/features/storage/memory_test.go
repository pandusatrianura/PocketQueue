package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestMemoryEngine_AppendAndRead(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine(100)
	defer engine.Close()

	topic := "orders"
	partition := int32(0)

	// Append 5 messages
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("value-%d", i))
		headers := []storage.Header{{Key: "source", Value: []byte("test")}}

		rec, err := engine.Append(ctx, topic, partition, key, val, headers)
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
		if rec.Offset != int64(i) {
			t.Errorf("expected offset %d, got %d", i, rec.Offset)
		}
		if string(rec.Key) != string(key) {
			t.Errorf("expected key %s, got %s", key, rec.Key)
		}
	}

	// Verify watermarks
	wm, err := engine.GetWatermarks(ctx, topic, partition)
	if err != nil {
		t.Fatalf("failed to get watermarks: %v", err)
	}
	if wm.Earliest != 0 || wm.Latest != 5 {
		t.Errorf("expected watermarks [0, 5], got [%d, %d]", wm.Earliest, wm.Latest)
	}

	// Read from offset 2
	records, err := engine.ReadFrom(ctx, topic, partition, 2, 10)
	if err != nil {
		t.Fatalf("failed to read from offset 2: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[0].Offset != 2 || string(records[0].Key) != "key-2" {
		t.Errorf("expected first record offset 2 key-2, got %d %s", records[0].Offset, records[0].Key)
	}
	if records[2].Offset != 4 || string(records[2].Key) != "key-4" {
		t.Errorf("expected third record offset 4 key-4, got %d %s", records[2].Offset, records[2].Key)
	}
}

func TestMemoryEngine_RingBufferEviction(t *testing.T) {
	ctx := context.Background()
	// Ring buffer capacity is strictly 3 items
	capacity := 3
	engine := storage.NewMemoryEngine(capacity)
	defer engine.Close()

	topic := "events"
	partition := int32(0)

	// Append 5 items into capacity-3 buffer
	for i := 0; i < 5; i++ {
		_, err := engine.Append(ctx, topic, partition, []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)), nil)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	// Earliest should now be 2, latest high-watermark 5
	wm, err := engine.GetWatermarks(ctx, topic, partition)
	if err != nil {
		t.Fatalf("failed to get watermarks: %v", err)
	}
	if wm.Earliest != 2 || wm.Latest != 5 {
		t.Errorf("expected watermarks [2, 5], got [%d, %d]", wm.Earliest, wm.Latest)
	}

	// Reading from offset 0 should automatically read from earliest available (offset 2)
	records, err := engine.ReadFrom(ctx, topic, partition, 0, 10)
	if err != nil {
		t.Fatalf("failed to read from offset 0: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[0].Offset != 2 {
		t.Errorf("expected first available record offset 2, got %d", records[0].Offset)
	}
}

func TestMemoryEngine_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine(5000)
	defer engine.Close()

	topic := "concurrent"
	partition := int32(0)
	numWriters := 10
	messagesPerWriter := 100

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			for m := 0; m < messagesPerWriter; m++ {
				_, err := engine.Append(ctx, topic, partition,
					[]byte(fmt.Sprintf("w%d-m%d", writerID, m)),
					[]byte("payload"), nil)
				if err != nil {
					t.Errorf("writer %d message %d error: %v", writerID, m, err)
				}
			}
		}(w)
	}

	wg.Wait()

	wm, err := engine.GetWatermarks(ctx, topic, partition)
	if err != nil {
		t.Fatalf("failed to get watermarks: %v", err)
	}

	totalExpected := int64(numWriters * messagesPerWriter)
	if wm.Latest != totalExpected {
		t.Errorf("expected latest watermark %d, got %d", totalExpected, wm.Latest)
	}
}

func TestMemoryEngine_Errors(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine(10)
	defer engine.Close()

	// Topic doesn't exist
	_, err := engine.ReadFrom(ctx, "nonexistent", 0, 0, 1)
	if err != storage.ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}

	_, err = engine.GetWatermarks(ctx, "nonexistent", 0)
	if err != storage.ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}

	// Topic exists but partition doesn't
	_, err = engine.Append(ctx, "existing", 0, []byte("k"), []byte("v"), nil)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	_, err = engine.ReadFrom(ctx, "existing", 99, 0, 1)
	if err != storage.ErrPartitionNotFound {
		t.Errorf("expected ErrPartitionNotFound, got %v", err)
	}

	_, err = engine.GetWatermarks(ctx, "existing", 99)
	if err != storage.ErrPartitionNotFound {
		t.Errorf("expected ErrPartitionNotFound, got %v", err)
	}

	// ListTopics
	topics, err := engine.ListTopics(ctx)
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}
	if topics["existing"] != 1 {
		t.Errorf("expected 1 partition for 'existing', got %d", topics["existing"])
	}
}
