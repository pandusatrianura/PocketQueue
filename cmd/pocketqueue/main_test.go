package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestDataDirOffsetPersistenceIntegration(t *testing.T) {
	dataDir := t.TempDir()

	// 1. Initialize first instance with WAL and FileOffsetStore
	wal1, err := storage.NewWALEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create WAL1: %v", err)
	}
	store1, err := broker.NewFileOffsetStore(dataDir)
	if err != nil {
		t.Fatalf("failed to create store1: %v", err)
	}
	svc1 := broker.NewService(wal1, nil, 1)
	if err := svc1.SetOffsetStore(store1); err != nil {
		t.Fatalf("failed to set store1: %v", err)
	}

	ctx := context.Background()
	if err := svc1.CommitOffset(ctx, "billing-group", "invoices", 0, 108); err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Close first instance
	if err := svc1.Close(); err != nil {
		t.Fatalf("failed to close svc1: %v", err)
	}

	// Verify offsets.json file physically exists
	offsetsPath := filepath.Join(dataDir, "offsets.json")
	if _, err := os.Stat(offsetsPath); err != nil {
		t.Fatalf("offsets.json was not created in %s: %v", dataDir, err)
	}

	// 2. Initialize second instance simulating restart with same dataDir
	wal2, err := storage.NewWALEngine(dataDir)
	if err != nil {
		t.Fatalf("failed to create WAL2: %v", err)
	}
	store2, err := broker.NewFileOffsetStore(dataDir)
	if err != nil {
		t.Fatalf("failed to create store2: %v", err)
	}
	svc2 := broker.NewService(wal2, nil, 1)
	defer svc2.Close()
	if err := svc2.SetOffsetStore(store2); err != nil {
		t.Fatalf("failed to set store2: %v", err)
	}

	offset, err := svc2.GetOffset(ctx, "billing-group", "invoices", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset != 108 {
		t.Fatalf("expected restored offset 108, got %d", offset)
	}
}
