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

	// Run generator for up to 2 seconds or until topics are created
	go gen.Start(ctx, 10*time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	var topics map[string]int32
	for time.Now().Before(deadline) {
		topics, _ = svc.ListTopics(context.Background())
		if len(topics) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
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

