package testrunner_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
	"github.com/pandusatrianura/pocketqueue/internal/features/testrunner"
)

func TestRunAssertion_Validation(t *testing.T) {
	_, err := testrunner.RunAssertion(context.Background(), testrunner.AssertionSpec{
		BrokerAddr: "127.0.0.1:9092",
		Topic:      "", // empty topic
	})
	if !errors.Is(err, testrunner.ErrTopicRequired) {
		t.Fatalf("expected ErrTopicRequired, got: %v", err)
	}
}

func TestRunAssertion_Success(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(50)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	// Produce matching message asynchronously after 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = svc.Produce(context.Background(), "ci-events", 0, []byte("ord-99"), []byte(`{"order_id":99,"status":"completed"}`), nil)
	}()

	res, err := testrunner.RunAssertion(context.Background(), testrunner.AssertionSpec{
		BrokerAddr:         server.Addr(),
		Topic:              "ci-events",
		ExpectedKey:        "ord-99",
		ExpectedPayloadSub: "completed",
		Timeout:            2 * time.Second,
		PollInterval:       20 * time.Millisecond,
	})

	if err != nil {
		t.Fatalf("assertion failed: %v", err)
	}
	if res.Topic != "ci-events" {
		t.Errorf("expected topic ci-events, got %s", res.Topic)
	}
	if res.Key != "ord-99" {
		t.Errorf("expected key ord-99, got %s", res.Key)
	}
	if res.Offset != 0 {
		t.Errorf("expected offset 0, got %d", res.Offset)
	}
}

func TestRunAssertion_Timeout(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(50)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	_, err := testrunner.RunAssertion(context.Background(), testrunner.AssertionSpec{
		BrokerAddr:         server.Addr(),
		Topic:              "non-existent-topic",
		ExpectedPayloadSub: "will-not-match",
		Timeout:            150 * time.Millisecond,
		PollInterval:       25 * time.Millisecond,
	})

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, testrunner.ErrAssertionTimeout) {
		t.Fatalf("expected ErrAssertionTimeout, got: %v", err)
	}
}
