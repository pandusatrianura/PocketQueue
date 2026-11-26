package broker_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestWebhookDLQDispatcher_DispatchSuccess(t *testing.T) {
	var receivedCount int32
	var lastHeaders http.Header
	var lastBody []byte
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&receivedCount, 1)
		mu.Lock()
		lastHeaders = r.Header.Clone()
		lastBody, _ = io.ReadAll(r.Body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bus := broker.NewEventBus(100)
	defer bus.Close()

	dispatcher := broker.NewWebhookDLQDispatcher(server.URL, bus, server.Client(), nil)
	dispatcher.Start()
	defer dispatcher.Close()

	// Publish a DLQ record
	dlqRec := storage.Record{
		Topic:     "payments.dlq",
		Partition: 0,
		Offset:    42,
		Key:       []byte("txn-99"),
		Value:     []byte(`{"amount": 500, "status": "failed"}`),
		Timestamp: time.Now().UTC(),
		Headers: []storage.Header{
			{Key: "x-dlq-original-topic", Value: []byte("payments")},
			{Key: "x-dlq-reason", Value: []byte("insufficient_funds")},
		},
		IsDLQ: true,
	}

	bus.Publish(dlqRec)

	// Wait for dispatcher to send HTTP request
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&receivedCount) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if atomic.LoadInt32(&receivedCount) != 1 {
		t.Fatalf("expected 1 webhook request, got %d", atomic.LoadInt32(&receivedCount))
	}

	mu.Lock()
	defer mu.Unlock()

	if lastHeaders.Get("x-dlq-original-topic") != "payments" {
		t.Errorf("expected header x-dlq-original-topic 'payments', got %q", lastHeaders.Get("x-dlq-original-topic"))
	}
	if lastHeaders.Get("x-dlq-reason") != "insufficient_funds" {
		t.Errorf("expected header x-dlq-reason 'insufficient_funds', got %q", lastHeaders.Get("x-dlq-reason"))
	}

	var payload map[string]any
	if err := json.Unmarshal(lastBody, &payload); err != nil {
		t.Fatalf("failed to decode webhook json body: %v", err)
	}
	if payload["topic"] != "payments.dlq" {
		t.Errorf("expected topic 'payments.dlq', got %v", payload["topic"])
	}
	if payload["key"] != "txn-99" {
		t.Errorf("expected key 'txn-99', got %v", payload["key"])
	}
}

func TestWebhookDLQDispatcher_RetriesOnFailure(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att < 3 {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bus := broker.NewEventBus(100)
	defer bus.Close()

	client := server.Client()
	client.Timeout = 1 * time.Second

	dispatcher := broker.NewWebhookDLQDispatcher(server.URL, bus, client, nil)
	dispatcher.Start()
	defer dispatcher.Close()

	dlqRec := storage.Record{
		Topic:     "orders.dlq",
		Partition: 0,
		Offset:    1,
		Key:       []byte("order-1"),
		Value:     []byte("test"),
		Timestamp: time.Now().UTC(),
		IsDLQ:     true,
	}

	bus.Publish(dlqRec)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&attempts) >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("expected 3 attempts (initial + 2 retries), got %d", atomic.LoadInt32(&attempts))
	}
}

func TestWebhookDLQDispatcher_NonDLQIgnored(t *testing.T) {
	var count int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bus := broker.NewEventBus(100)
	defer bus.Close()

	dispatcher := broker.NewWebhookDLQDispatcher(server.URL, bus, server.Client(), nil)
	dispatcher.Start()
	defer dispatcher.Close()

	// Normal non-DLQ record
	bus.Publish(storage.Record{
		Topic:     "orders",
		Partition: 0,
		Offset:    1,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Timestamp: time.Now().UTC(),
		IsDLQ:     false,
	})

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&count) != 0 {
		t.Errorf("expected non-DLQ message to be ignored, but got %d requests", atomic.LoadInt32(&count))
	}
}

func TestWebhookDLQDispatcher_IntegrationWithService(t *testing.T) {
	var count int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mem := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(mem, bus, 1)
	defer svc.Close()

	dispatcher := broker.NewWebhookDLQDispatcher(server.URL, bus, server.Client(), nil)
	dispatcher.Start()
	defer dispatcher.Close()

	_, err := svc.SendToDLQ(context.Background(), "auth", 0, []byte("user-1"), []byte("invalid password"), nil, "auth_failed")
	if err != nil {
		t.Fatalf("SendToDLQ failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&count) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if atomic.LoadInt32(&count) != 1 {
		t.Errorf("expected 1 webhook call via SendToDLQ, got %d", atomic.LoadInt32(&count))
	}
}

func TestWebhookDLQDispatcher_GracefulQueueDrain(t *testing.T) {
	var count int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bus := broker.NewEventBus(100)
	defer bus.Close()

	dispatcher := broker.NewWebhookDLQDispatcher(server.URL, bus, server.Client(), nil)
	dispatcher.Start()

	// Publish 5 DLQ records quickly
	for i := 0; i < 5; i++ {
		bus.Publish(storage.Record{
			Topic:     "queue.dlq",
			Partition: 0,
			Offset:    int64(i),
			Key:       []byte("k"),
			Value:     []byte("v"),
			Timestamp: time.Now().UTC(),
			IsDLQ:     true,
		})
	}

	// Close immediately and assert that all buffered messages drain cleanly
	if err := dispatcher.Close(); err != nil {
		t.Fatalf("dispatcher.Close failed: %v", err)
	}

	if atomic.LoadInt32(&count) != 5 {
		t.Errorf("expected all 5 records to be drained on close, got %d", atomic.LoadInt32(&count))
	}
}
