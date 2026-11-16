package broker_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestTrafficReplayer_RecordAndReplay(t *testing.T) {
	tmpDir := t.TempDir()
	dumpPath := filepath.Join(tmpDir, "replay_session.pqrec")

	// 1. Record 3 events
	rec, err := broker.NewTrafficRecorder(dumpPath)
	if err != nil {
		t.Fatalf("NewTrafficRecorder failed: %v", err)
	}

	now := time.Now()
	for i := 0; i < 3; i++ {
		_ = rec.Record(storage.Record{
			Topic:     "orders",
			Partition: 0,
			Offset:    int64(i),
			Timestamp: now.Add(time.Duration(i*50) * time.Millisecond),
			Key:       []byte("ord"),
			Value:     []byte("payload"),
		})
	}
	_ = rec.Close()

	// 2. Setup fresh broker service
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	// Pre-commit an offset to test reset
	_ = svc.CommitOffset(context.Background(), "mygroup", "orders", 0, 999)
	oldOffset, _ := svc.GetOffset(context.Background(), "mygroup", "orders", 0)
	if oldOffset != 999 {
		t.Fatalf("expected old offset 999, got %d", oldOffset)
	}

	// 3. Replay with resetOffsets = true and speed = 100.0 (fast)
	replayer := broker.NewTrafficReplayer(dumpPath, svc, 100.0, true)
	count, err := replayer.Replay(context.Background())
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 replayed records, got %d", count)
	}

	// Verify offset was reset
	resetOffset, _ := svc.GetOffset(context.Background(), "mygroup", "orders", 0)
	if resetOffset != 0 {
		t.Errorf("expected offset reset to 0, got %d", resetOffset)
	}

	// Verify records are readable from broker
	records, err := svc.Fetch(context.Background(), "orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("expected 3 records in broker, got %d", len(records))
	}
}

func TestTrafficReplayer_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	dumpPath := filepath.Join(tmpDir, "cancel.pqrec")

	rec, err := broker.NewTrafficRecorder(dumpPath)
	if err != nil {
		t.Fatalf("NewTrafficRecorder failed: %v", err)
	}

	now := time.Now()
	for i := 0; i < 5; i++ {
		_ = rec.Record(storage.Record{
			Topic:     "stream",
			Partition: 0,
			Offset:    int64(i),
			Timestamp: now.Add(time.Duration(i*500) * time.Millisecond),
			Key:       []byte("k"),
			Value:     []byte("v"),
		})
	}
	_ = rec.Close()

	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Replay at 1x speed (each event 500ms apart, so should cancel before all finish)
	replayer := broker.NewTrafficReplayer(dumpPath, svc, 1.0, false)
	count, err := replayer.Replay(ctx)
	if err == nil {
		t.Errorf("expected context cancellation error, got nil")
	}
	if count >= 5 {
		t.Errorf("expected early abort, got all %d records", count)
	}
}
