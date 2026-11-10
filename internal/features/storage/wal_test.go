package storage_test

import (
	"context"
	"os"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestWALEngine_PersistenceAndRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pocketqueue-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()

	// 1. Initial engine session
	wal1, err := storage.NewWALEngine(tempDir)
	if err != nil {
		t.Fatalf("failed to create wal1: %v", err)
	}

	headers := []storage.Header{
		{Key: "env", Value: []byte("prod")},
		{Key: "source", Value: []byte("checkout")},
	}

	r1, err := wal1.Append(ctx, "orders", 0, []byte("k1"), []byte("v1"), headers)
	if err != nil || r1.Offset != 0 {
		t.Fatalf("wal1 append 1 failed: offset=%d err=%v", r1.Offset, err)
	}

	r2, err := wal1.Append(ctx, "orders", 0, []byte("k2"), []byte("v2"), nil)
	if err != nil || r2.Offset != 1 {
		t.Fatalf("wal1 append 2 failed: offset=%d err=%v", r2.Offset, err)
	}

	// Close session 1
	if err := wal1.Close(); err != nil {
		t.Fatalf("wal1 close failed: %v", err)
	}

	// 2. Open session 2 on same dir (Simulating process restart)
	wal2, err := storage.NewWALEngine(tempDir)
	if err != nil {
		t.Fatalf("failed to create wal2: %v", err)
	}
	defer wal2.Close()

	// Verify watermarks recovered
	wm, err := wal2.GetWatermarks(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("wal2 get watermarks failed: %v", err)
	}
	if wm.Earliest != 0 || wm.Latest != 2 {
		t.Errorf("expected watermarks [0, 2], got [%d, %d]", wm.Earliest, wm.Latest)
	}

	// Read records back
	records, err := wal2.ReadFrom(ctx, "orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("wal2 read failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 recovered records, got %d", len(records))
	}

	if string(records[0].Key) != "k1" || string(records[0].Value) != "v1" {
		t.Errorf("record 0 mismatch: %+v", records[0])
	}
	if len(records[0].Headers) != 2 || records[0].Headers[0].Key != "env" {
		t.Errorf("record 0 headers mismatch: %+v", records[0].Headers)
	}

	// Append 3rd record in session 2 to verify nextOffset continuity
	r3, err := wal2.Append(ctx, "orders", 0, []byte("k3"), []byte("v3"), nil)
	if err != nil || r3.Offset != 2 {
		t.Fatalf("wal2 append 3 failed: offset=%d err=%v", r3.Offset, err)
	}
}
