package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OffsetStore abstracts consumer group offset persistence across restarts.
type OffsetStore interface {
	Save(offsets map[string]map[string]map[int32]int64) error
	Load() (map[string]map[string]map[int32]int64, error)
	Close() error
}

// NoopOffsetStore provides an in-memory/no-op implementation of OffsetStore.
type NoopOffsetStore struct{}

func (n *NoopOffsetStore) Save(offsets map[string]map[string]map[int32]int64) error { return nil }
func (n *NoopOffsetStore) Load() (map[string]map[string]map[int32]int64, error) {
	return make(map[string]map[string]map[int32]int64), nil
}
func (n *NoopOffsetStore) Close() error { return nil }

// FileOffsetStore persists consumer group offsets to offsets.json using atomic writes
// and a 50ms debounced disk flush.
type FileOffsetStore struct {
	mu           sync.Mutex
	dir          string
	filePath     string
	tmpPath      string
	data         map[string]map[string]map[int32]int64
	dirty        bool
	flushTimer   *time.Timer
	flushPending bool
	closed       bool
}

// NewFileOffsetStore creates a FileOffsetStore for the given directory.
func NewFileOffsetStore(dir string) (*FileOffsetStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("directory cannot be empty")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create offset dir: %w", err)
	}

	return &FileOffsetStore{
		dir:      dir,
		filePath: filepath.Join(dir, "offsets.json"),
		tmpPath:  filepath.Join(dir, "offsets.json.tmp"),
		data:     make(map[string]map[string]map[int32]int64),
	}, nil
}

// Load reads persisted offsets from offsets.json if present.
func (f *FileOffsetStore) Load() (map[string]map[string]map[int32]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	raw, err := os.ReadFile(f.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]map[string]map[int32]int64), nil
		}
		return nil, fmt.Errorf("read offsets.json: %w", err)
	}

	var loaded map[string]map[string]map[int32]int64
	if err := json.Unmarshal(raw, &loaded); err != nil {
		return nil, fmt.Errorf("unmarshal offsets.json: %w", err)
	}
	if loaded == nil {
		loaded = make(map[string]map[string]map[int32]int64)
	}

	f.data = deepCopyOffsets(loaded)
	return loaded, nil
}

// Save schedules a debounced write of consumer group offsets.
func (f *FileOffsetStore) Save(offsets map[string]map[string]map[int32]int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return fmt.Errorf("offset store is closed")
	}

	f.data = deepCopyOffsets(offsets)
	f.dirty = true

	if !f.flushPending {
		f.flushPending = true
		f.flushTimer = time.AfterFunc(50*time.Millisecond, func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.flushPending = false
			if f.dirty && !f.closed {
				_ = f.flushLocked()
			}
		})
	}

	return nil
}

// Close flushes any pending offsets synchronously and releases resources.
func (f *FileOffsetStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}
	f.closed = true

	if f.flushTimer != nil {
		f.flushTimer.Stop()
		f.flushPending = false
	}

	if f.dirty {
		return f.flushLocked()
	}
	return nil
}

func (f *FileOffsetStore) flushLocked() error {
	raw, err := json.MarshalIndent(f.data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal offsets: %w", err)
	}

	tmpFile, err := os.OpenFile(f.tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create tmp offsets file: %w", err)
	}

	if _, err := tmpFile.Write(raw); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("write tmp offsets file: %w", err)
	}
	_ = tmpFile.Sync()
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close tmp offsets file: %w", err)
	}

	if err := os.Rename(f.tmpPath, f.filePath); err != nil {
		return fmt.Errorf("atomic rename offsets file: %w", err)
	}

	f.dirty = false
	return nil
}

func deepCopyOffsets(src map[string]map[string]map[int32]int64) map[string]map[string]map[int32]int64 {
	dst := make(map[string]map[string]map[int32]int64, len(src))
	for grp, topics := range src {
		tDst := make(map[string]map[int32]int64, len(topics))
		for topic, parts := range topics {
			pDst := make(map[int32]int64, len(parts))
			for part, off := range parts {
				pDst[part] = off
			}
			tDst[topic] = pDst
		}
		dst[grp] = tDst
	}
	return dst
}
