package broker_test

import (
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
)

func TestFileOffsetStore_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := broker.NewFileOffsetStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FileOffsetStore: %v", err)
	}

	// 1. Initial load on empty dir returns empty map
	initial, err := store.Load()
	if err != nil {
		t.Fatalf("expected nil error on empty load, got: %v", err)
	}
	if len(initial) != 0 {
		t.Fatalf("expected empty map, got: %v", initial)
	}

	// 2. Save offset data
	data := map[string]map[string]map[int32]int64{
		"consumer-group-alpha": {
			"orders": {
				0: 42,
				1: 84,
			},
			"payments": {
				0: 10,
			},
		},
	}

	if err := store.Save(data); err != nil {
		t.Fatalf("failed to save offsets: %v", err)
	}

	// Close store to trigger immediate flush
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// 3. Re-open store from same directory and load
	store2, err := broker.NewFileOffsetStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store2: %v", err)
	}
	defer store2.Close()

	loaded, err := store2.Load()
	if err != nil {
		t.Fatalf("failed to load offsets: %v", err)
	}

	grp, ok := loaded["consumer-group-alpha"]
	if !ok {
		t.Fatalf("expected group consumer-group-alpha in loaded data")
	}
	if grp["orders"][0] != 42 || grp["orders"][1] != 84 {
		t.Errorf("unexpected orders offsets: %v", grp["orders"])
	}
	if grp["payments"][0] != 10 {
		t.Errorf("unexpected payments offset: %v", grp["payments"])
	}
}

func TestFileOffsetStore_DebouncedFlush(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := broker.NewFileOffsetStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FileOffsetStore: %v", err)
	}
	defer store.Close()

	// Rapidly save 5 times
	for i := int64(1); i <= 5; i++ {
		data := map[string]map[string]map[int32]int64{
			"grp": {
				"topic": {
					0: i,
				},
			},
		}
		_ = store.Save(data)
	}

	// Wait for debounced flush to complete on disk (>50ms debounce threshold, up to 2s timeout)
	deadline := time.Now().Add(2 * time.Second)
	var lastOffset int64
	for time.Now().Before(deadline) {
		storeCheck, err := broker.NewFileOffsetStore(tmpDir)
		if err == nil {
			loaded, err := storeCheck.Load()
			_ = storeCheck.Close()
			if err == nil && loaded != nil && loaded["grp"] != nil && loaded["grp"]["topic"] != nil {
				lastOffset = loaded["grp"]["topic"][0]
				if lastOffset == 5 {
					break
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	if lastOffset != 5 {
		t.Errorf("expected offset 5 after debounce flush, got: %d", lastOffset)
	}
}

func TestNoopOffsetStore(t *testing.T) {
	store := &broker.NoopOffsetStore{}

	if err := store.Save(nil); err != nil {
		t.Errorf("expected nil error on Noop Save, got: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Errorf("expected nil error on Noop Load, got: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("expected empty map, got: %v", loaded)
	}

	if err := store.Close(); err != nil {
		t.Errorf("expected nil error on Noop Close, got: %v", err)
	}
}
