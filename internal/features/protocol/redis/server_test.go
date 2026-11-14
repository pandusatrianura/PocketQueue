package redis_test

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/redis"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func readRESPValue(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return "", nil
	}
	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		length, err := strconv.Atoi(line[1:])
		if err != nil || length == -1 {
			return "", nil
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return string(buf[:length]), nil
	case '*':
		count, err := strconv.Atoi(line[1:])
		if err != nil || count <= 0 {
			return line, nil
		}
		var items []string
		for i := 0; i < count; i++ {
			item, err := readRESPValue(r)
			if err != nil {
				return "", err
			}
			items = append(items, item)
		}
		return "[" + strings.Join(items, ", ") + "]", nil
	default:
		return line, nil
	}
}

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

func TestRedisServer_XReadBlockTimeout(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(10)
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

	// Send XREAD BLOCK 100 STREAMS mystream $
	start := time.Now()
	// *6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n100\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n
	cmd := "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n100\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n"
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatalf("failed to write XREAD command: %v", err)
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read XREAD response: %v", err)
	}
	elapsed := time.Since(start)

	if strings.TrimRight(resp, "\r\n") != "*-1" {
		t.Errorf("expected *-1 for timeout nil array, got %q", resp)
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("expected at least ~100ms wait, got %v", elapsed)
	}
}

func TestRedisServer_XReadBlockWakeup(t *testing.T) {
	engine := storage.NewMemoryEngine(100)
	bus := broker.NewEventBus(10)
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

	// Start a background producer that publishes after 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		pConn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
		if err != nil {
			return
		}
		defer pConn.Close()
		// XADD jobs * task email
		pCmd := "*5\r\n$4\r\nXADD\r\n$4\r\njobs\r\n$1\r\n*\r\n$4\r\ntask\r\n$5\r\nemail\r\n"
		_, _ = pConn.Write([]byte(pCmd))
	}()

	// Read with BLOCK 1500 STREAMS jobs $
	cmd := "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$4\r\n1500\r\n$7\r\nSTREAMS\r\n$4\r\njobs\r\n$1\r\n$\r\n"
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatalf("failed to write XREAD command: %v", err)
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read XREAD response: %v", err)
	}

	if strings.TrimRight(resp, "\r\n") != "*1" {
		t.Errorf("expected *1 for stream envelope array, got %q", resp)
	}
}

func TestRedisServer_PortInUse(t *testing.T) {
	engine := storage.NewMemoryEngine(10)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	server1 := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := server1.Start(); err != nil {
		t.Fatalf("failed to start server1: %v", err)
	}
	defer server1.Close()

	// Try starting a second server on the exact same port
	server2 := redis.NewServer(server1.Addr(), svc, nil)
	err := server2.Start()
	if err == nil {
		server2.Close()
		t.Fatalf("expected port in use error, got nil")
	}

	if !errors.Is(err, redis.ErrPortInUse) {
		t.Errorf("expected error wrapping redis.ErrPortInUse, got: %v", err)
	}
}

func TestRedisServer_QuitAndConnectionClosure(t *testing.T) {
	engine := storage.NewMemoryEngine(10)
	bus := broker.NewEventBus(10)
	svc := broker.NewService(engine, bus, 1)

	server := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial server failed: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send QUIT command
	if _, err := conn.Write([]byte("QUIT\r\n")); err != nil {
		t.Fatalf("write QUIT failed: %v", err)
	}

	// Should receive +OK\r\n
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read QUIT response failed: %v", err)
	}
	if strings.TrimRight(resp, "\r\n") != "+OK" {
		t.Errorf("expected +OK, got %q", resp)
	}

	// Server must close connection promptly (next read returns EOF)
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var dummy [1]byte
	_, readErr := reader.Read(dummy[:])
	if readErr == nil || !errors.Is(readErr, io.EOF) {
		t.Errorf("expected EOF after QUIT, got: %v", readErr)
	}
}

func TestRedisServer_ConsumerGroupsAndExtendedOps(t *testing.T) {
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
		if _, err := conn.Write([]byte(cmd)); err != nil {
			t.Fatalf("write error: %v", err)
		}
		val, err := readRESPValue(reader)
		if err != nil {
			t.Fatalf("readRESPValue error: %v", err)
		}
		return val
	}

	// 1. XGROUP CREATE mystream mygroup $ MKSTREAM
	cmdGroupCreate := "*6\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n$\r\n$8\r\nMKSTREAM\r\n"
	resp := sendCmd(cmdGroupCreate)
	if resp != "+OK" {
		t.Fatalf("expected +OK from XGROUP CREATE, got %q", resp)
	}

	// 2. XADD mystream * k1 v1
	cmdXAdd1 := "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$2\r\nk1\r\n$2\r\nv1\r\n"
	if r := sendCmd(cmdXAdd1); r != "0-0" {
		t.Fatalf("expected 0-0 from XADD 1, got %q", r)
	}

	// 3. XADD mystream * k2 v2
	cmdXAdd2 := "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$2\r\nk2\r\n$2\r\nv2\r\n"
	if r := sendCmd(cmdXAdd2); r != "1-0" {
		t.Fatalf("expected 1-0 from XADD 2, got %q", r)
	}

	// 4. XREADGROUP GROUP mygroup c1 COUNT 10 STREAMS mystream >
	cmdReadGroup := "*9\r\n$10\r\nXREADGROUP\r\n$5\r\nGROUP\r\n$7\r\nmygroup\r\n$2\r\nc1\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n>\r\n"
	rgResp := sendCmd(cmdReadGroup)
	if !strings.Contains(rgResp, "mystream") || !strings.Contains(rgResp, "0-0") || !strings.Contains(rgResp, "1-0") {
		t.Fatalf("expected XREADGROUP to contain mystream, 0-0, 1-0, got %q", rgResp)
	}

	// 5. XACK mystream mygroup 0-0 1-0
	cmdAck := "*5\r\n$4\r\nXACK\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$3\r\n0-0\r\n$3\r\n1-0\r\n"
	ackResp := sendCmd(cmdAck)
	if ackResp != ":2" {
		t.Fatalf("expected :2 from XACK, got %q", ackResp)
	}

	// 6. XREVRANGE mystream + - COUNT 1
	cmdRevRange := "*6\r\n$9\r\nXREVRANGE\r\n$8\r\nmystream\r\n$1\r\n+\r\n$1\r\n-\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n"
	revResp := sendCmd(cmdRevRange)
	if !strings.Contains(revResp, "1-0") || strings.Contains(revResp, "0-0") {
		t.Fatalf("expected XREVRANGE to contain 1-0 and not 0-0 (count 1), got %q", revResp)
	}

	// 7. XDEL mystream 0-0
	cmdXDel := "*3\r\n$4\r\nXDEL\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n"
	delResp := sendCmd(cmdXDel)
	if delResp != ":1" {
		t.Fatalf("expected :1 from XDEL, got %q", delResp)
	}

	// 8. XGROUP DESTROY mystream mygroup
	cmdDestroy := "*4\r\n$6\r\nXGROUP\r\n$7\r\nDESTROY\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n"
	destResp := sendCmd(cmdDestroy)
	if destResp != ":1" {
		t.Fatalf("expected :1 from XGROUP DESTROY, got %q", destResp)
	}
	if destResp != ":1" {
		t.Fatalf("expected :1 from XGROUP DESTROY, got %q", destResp)
	}
}


