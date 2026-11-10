package demo_test

import (
	"context"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/demo"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestDemoGenerator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(50)
	svc := broker.NewService(engine, bus, 1)
	defer svc.Close()

	gen := demo.NewGenerator(svc)

	// Run generator for 100ms at fast interval (10ms)
	go gen.Start(ctx, 10*time.Millisecond)

	time.Sleep(80 * time.Millisecond)
	cancel()

	// Verify topics exist and have messages
	topics, err := svc.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}

	if len(topics) == 0 {
		t.Error("expected demo generator to populate topics, found 0")
	}
}

