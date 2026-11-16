package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// RecordItem represents a serialized message in NDJSON format (.pqrec).
type RecordItem struct {
	Version   int               `json:"v"`
	Timestamp int64             `json:"t"` // unix millisecond
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Payload   string            `json:"payload"`
	Headers   map[string]string `json:"headers,omitempty"`
	IsDLQ     bool              `json:"is_dlq,omitempty"`
}

// TrafficRecorder writes streaming broker records to an NDJSON file safely.
type TrafficRecorder struct {
	filePath string
	file     *os.File
	writer   *bufio.Writer
	mu       sync.Mutex
	count    atomic.Int64
	closed   atomic.Bool
}

// NewTrafficRecorder creates or appends to a .pqrec file for recording.
func NewTrafficRecorder(filePath string) (*TrafficRecorder, error) {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open record file %s: %w", filePath, err)
	}

	return &TrafficRecorder{
		filePath: filePath,
		file:     f,
		writer:   bufio.NewWriterSize(f, 64*1024),
	}, nil
}

// Record serializes a storage.Record as an NDJSON line.
func (r *TrafficRecorder) Record(rec storage.Record) error {
	if r.closed.Load() {
		return nil
	}

	var headersMap map[string]string
	if len(rec.Headers) > 0 {
		headersMap = make(map[string]string, len(rec.Headers))
		for _, h := range rec.Headers {
			headersMap[h.Key] = string(h.Value)
		}
	}

	ts := rec.Timestamp.UnixMilli()
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}

	item := RecordItem{
		Version:   1,
		Timestamp: ts,
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       string(rec.Key),
		Payload:   string(rec.Value),
		Headers:   headersMap,
		IsDLQ:     rec.IsDLQ,
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal record item failed: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return nil
	}

	if _, err := r.writer.Write(data); err != nil {
		return err
	}
	if err := r.writer.WriteByte('\n'); err != nil {
		return err
	}

	r.count.Add(1)
	return nil
}

// Flush flushes buffered data to disk.
func (r *TrafficRecorder) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writer != nil {
		if err := r.writer.Flush(); err != nil {
			return err
		}
	}
	if r.file != nil {
		return r.file.Sync()
	}
	return nil
}

// Close flushes and closes the underlying recording file.
func (r *TrafficRecorder) Close() error {
	if r.closed.Swap(true) {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error
	if r.writer != nil {
		if err := r.writer.Flush(); err != nil {
			firstErr = err
		}
	}
	if r.file != nil {
		if err := r.file.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := r.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// IsRecording returns true if the recorder is active and not closed.
func (r *TrafficRecorder) IsRecording() bool {
	return !r.closed.Load()
}

// RecordedCount returns the number of records successfully written.
func (r *TrafficRecorder) RecordedCount() int64 {
	return r.count.Load()
}

// FilePath returns the destination file path.
func (r *TrafficRecorder) FilePath() string {
	return r.filePath
}
