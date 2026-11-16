package broker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestChaosEngine_DisabledZeroOverhead(t *testing.T) {
	mem := storage.NewMemoryEngine(100)
	svc := broker.NewService(mem, nil, 1)

	// No chaos configured
	rec, err := svc.Produce(context.Background(), "test-topic", 0, []byte("k"), []byte("v"), nil)
	if err != nil {
		t.Fatalf("expected produce success, got: %v", err)
	}
	if rec.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", rec.Offset)
	}

	recs, err := svc.Fetch(context.Background(), "test-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("expected fetch success, got: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
}

func TestChaosEngine_Drop(t *testing.T) {
	mem := storage.NewMemoryEngine(100)
	svc := broker.NewService(mem, nil, 1)

	chaos := broker.NewChaosEngine(broker.ChaosConfig{
		DropRate: 1.0, // 100% drops
	})
	svc.SetChaos(chaos)

	// Produce should drop
	_, err := svc.Produce(context.Background(), "drop-topic", 0, []byte("k"), []byte("v"), nil)
	if !errors.Is(err, broker.ErrChaosDrop) {
		t.Fatalf("expected ErrChaosDrop on produce, got: %v", err)
	}

	// Fetch should drop
	_, err = svc.Fetch(context.Background(), "drop-topic", 0, 0, 10)
	if !errors.Is(err, broker.ErrChaosDrop) {
		t.Fatalf("expected ErrChaosDrop on fetch, got: %v", err)
	}
}

func TestChaosEngine_Latency(t *testing.T) {
	mem := storage.NewMemoryEngine(100)
	svc := broker.NewService(mem, nil, 1)

	chaos := broker.NewChaosEngine(broker.ChaosConfig{
		LatencyMs:   60,
		LatencyRate: 1.0, // 100% latency
	})
	svc.SetChaos(chaos)

	start := time.Now()
	rec, err := svc.Produce(context.Background(), "lat-topic", 0, []byte("k"), []byte("v"), nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected produce success with latency, got: %v", err)
	}
	if rec == nil {
		t.Fatal("expected non-nil record")
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected elapsed >= 50ms, got: %v", elapsed)
	}
}

func TestChaosEngine_Duplicate(t *testing.T) {
	mem := storage.NewMemoryEngine(100)
	svc := broker.NewService(mem, nil, 1)

	chaos := broker.NewChaosEngine(broker.ChaosConfig{
		DuplicateRate: 1.0, // 100% duplicate
	})
	svc.SetChaos(chaos)

	rec, err := svc.Produce(context.Background(), "dup-topic", 0, []byte("k"), []byte("val"), nil)
	if err != nil {
		t.Fatalf("expected produce success with duplicate, got: %v", err)
	}
	if rec.Offset != 1 {
		t.Fatalf("expected second duplicate offset 1, got: %d", rec.Offset)
	}

	// Verify storage has 2 records
	recs, err := mem.ReadFrom(context.Background(), "dup-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records in storage, got: %d", len(recs))
	}
	if recs[0].Offset != 0 || recs[1].Offset != 1 {
		t.Fatalf("unexpected offsets: %d, %d", recs[0].Offset, recs[1].Offset)
	}
}

func TestChaosEngine_ContextCancellation(t *testing.T) {
	mem := storage.NewMemoryEngine(100)
	svc := broker.NewService(mem, nil, 1)

	chaos := broker.NewChaosEngine(broker.ChaosConfig{
		LatencyMs:   1000,
		LatencyRate: 1.0,
	})
	svc.SetChaos(chaos)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err := svc.Produce(ctx, "cancel-topic", 0, []byte("k"), []byte("v"), nil)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context error on timeout, got: %v", err)
	}
}

func TestChaosEngine_UpdateConfig(t *testing.T) {
	chaos := broker.NewChaosEngine(broker.ChaosConfig{
		DropRate: 1.0,
	})
	if !chaos.Enabled() {
		t.Fatal("expected chaos to be enabled")
	}

	// Turn off chaos
	chaos.UpdateConfig(broker.ChaosConfig{})
	if chaos.Enabled() {
		t.Fatal("expected chaos to be disabled after reset")
	}

	drop, dup, delay := chaos.EvaluateProduce(context.Background())
	if drop || dup || delay != 0 {
		t.Fatalf("expected all false/0 after reset, got drop=%v, dup=%v, delay=%v", drop, dup, delay)
	}
}
