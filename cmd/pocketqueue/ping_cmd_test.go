package main

import (
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestPingCommand_SuccessWithBroker(t *testing.T) {
	mem := storage.NewMemoryEngine(100)
	svc := broker.NewService(mem, nil, 1)
	defer svc.Close()

	handler := kafka.NewHandler(svc, "127.0.0.1", 0, nil)
	server := kafka.NewServer("127.0.0.1:0", handler, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer server.Close()

	// Wait brief moment for listener to accept
	time.Sleep(20 * time.Millisecond)

	err := runPingCommand([]string{"--broker", server.Addr(), "--timeout", "2s"})
	if err != nil {
		t.Fatalf("expected ping command to succeed on %s, got: %v", server.Addr(), err)
	}
}

func TestPingCommand_UnreachableBroker(t *testing.T) {
	err := runPingCommand([]string{"--broker", "127.0.0.1:59999", "--timeout", "100ms"})
	if err == nil {
		t.Fatalf("expected ping command to fail on unreachable broker, got nil")
	}
	if !strings.Contains(err.Error(), "failed to connect") {
		t.Errorf("expected connection error, got: %v", err)
	}
}

func TestPingCommand_Help(t *testing.T) {
	err := runPingCommand([]string{"--help"})
	if err != nil {
		t.Errorf("expected nil error on --help, got: %v", err)
	}
}
