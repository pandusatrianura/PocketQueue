package redis_test

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/redis"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func TestRedisServer_StreamsLifecycle(t *testing.T) {
	engine := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(100)
	svc := broker.NewService(engine, bus, 1)

	server := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start redis server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial redis server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	sendCmd := func(cmd string) string {
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read line error: %v", err)
		}
		return strings.TrimRight(line, "\r\n")
	}

	// 1. PING
	pong := sendCmd("PING\r\n")
	if pong != "+PONG" {
		t.Errorf("expected +PONG, got %q", pong)
	}

	// 2. XADD
	// *5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$4\r\ntemp\r\n$4\r\n25.5\r\n
	xaddResp := sendCmd("*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$4\r\ntemp\r\n$4\r\n25.5\r\n")
	if xaddResp != "$3" { // bulk string header for "0-0"
		t.Errorf("expected $3 for stream ID length, got %q", xaddResp)
	}
	streamID, _ := reader.ReadString('\n')
	if strings.TrimRight(streamID, "\r\n") != "0-0" {
		t.Errorf("expected 0-0, got %q", streamID)
	}

	// 3. XLEN
	xlenResp := sendCmd("*2\r\n$4\r\nXLEN\r\n$8\r\nmystream\r\n")
	if xlenResp != ":1" {
		t.Errorf("expected :1 for stream length, got %q", xlenResp)
	}

	// 4. XRANGE
	xrangeResp := sendCmd("*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$1\r\n-\r\n$1\r\n+\r\n")
	if xrangeResp != "*1" {
		t.Errorf("expected *1 for xrange array, got %q", xrangeResp)
	}
}

