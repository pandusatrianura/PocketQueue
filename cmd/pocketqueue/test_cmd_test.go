package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestRunTestCommand_MissingTopic(t *testing.T) {
	err := runTestCommand([]string{"--broker", "127.0.0.1:9092"})
	if err == nil {
		t.Fatal("expected error when --expect-topic is missing")
	}
	if !strings.Contains(err.Error(), "--expect-topic is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunTestCommand_SuccessWithBroker(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(50)
	svc := broker.NewService(engine, bus, 1)

	handler := kafka.NewHandler(svc, "127.0.0.1", 9092, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	// Produce matching message in background
	go func() {
		time.Sleep(30 * time.Millisecond)
		_, _ = svc.Produce(context.Background(), "payment-events", 0, []byte("user-1"), []byte(`{"event":"paid","amount":50}`), nil)
	}()

	err := runTestCommand([]string{
		"--broker", server.Addr(),
		"--expect-topic", "payment-events",
		"--expect-key", "user-1",
		"--expect-payload", "paid",
		"--timeout", "2s",
		"--poll", "20ms",
	})

	if err != nil {
		t.Fatalf("expected test command success, got: %v", err)
	}
}
