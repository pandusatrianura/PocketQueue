package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestBrokerService_ProduceAndFetch(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)
	defer svc.Close()

	// Subscribe to event bus to verify real-time broadcast
	subCh, unsub := bus.Subscribe()
	defer unsub()

	// Produce message to auto-created topic
	rec, err := svc.Produce(ctx, "payments", 0, []byte("pay-101"), []byte(`{"amount": 50}`), nil)
	if err != nil {
		t.Fatalf("failed to produce: %v", err)
	}

	if rec.Topic != "payments" || rec.Partition != 0 || rec.Offset != 0 {
		t.Errorf("unexpected record metadata: %+v", rec)
	}

	// Verify EventBus received the broadcast
	select {
	case event := <-subCh:
		if event.Topic != "payments" || string(event.Key) != "pay-101" {
			t.Errorf("unexpected event on bus: %+v", event)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for event bus notification")
	}

	// Fetch message back
	records, err := svc.Fetch(ctx, "payments", 0, 0, 10)
	if err != nil {
		t.Fatalf("failed to fetch: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if string(records[0].Value) != `{"amount": 50}` {
		t.Errorf("unexpected record value: %s", records[0].Value)
	}
}

func TestBrokerService_ConsumerGroupOffsets(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)
	defer svc.Close()

	groupID := "billing-service"
	topic := "invoices"

	// Commit offset 42
	err := svc.CommitOffset(ctx, groupID, topic, 0, 42)
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Fetch offset
	offset, err := svc.GetOffset(ctx, groupID, topic, 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset != 42 {
		t.Errorf("expected offset 42, got %d", offset)
	}

	// Fetch unknown group offset -> returns -1
	unkOffset, err := svc.GetOffset(ctx, "unknown-group", topic, 0)
	if err != nil {
		t.Fatalf("unexpected error on unknown group: %v", err)
	}
	if unkOffset != -1 {
		t.Errorf("expected -1 for unknown group offset, got %d", unkOffset)
	}
}

func TestBrokerService_DLQAndReplay(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)
	defer svc.Close()

	// Route a failed message to DLQ
	dlqRec, err := svc.SendToDLQ(ctx, "orders", 0, []byte("order-99"), []byte(`{"error":"validation failed"}`), nil, "Invalid SKU")
	if err != nil {
		t.Fatalf("failed to send to DLQ: %v", err)
	}

	if dlqRec.Topic != "orders.dlq" {
		t.Errorf("expected DLQ topic 'orders.dlq', got %s", dlqRec.Topic)
	}
	if !dlqRec.IsDLQ {
		t.Error("expected IsDLQ to be true")
	}

	// Replay: re-publish to original topic with edited payload
	replayed, err := svc.Replay(ctx, "orders", 0, []byte("order-99"), []byte(`{"fixed": true}`), nil)
	if err != nil {
		t.Fatalf("failed to replay: %v", err)
	}
	if replayed.Topic != "orders" {
		t.Errorf("expected replay topic 'orders', got %s", replayed.Topic)
	}

	// Verify original topic now has the replayed message
	msgs, err := svc.Fetch(ctx, "orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("failed to fetch replayed message: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Value) != `{"fixed": true}` {
		t.Errorf("unexpected message in topic: %+v", msgs)
	}
}

