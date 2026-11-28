package mqtt_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/mqtt"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

func setupTestServer(t *testing.T) (*mqtt.Server, *broker.Service, *broker.EventBus) {
	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(mem, bus, 1)

	server := mqtt.NewServer("127.0.0.1:0", svc, bus, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start mqtt server: %v", err)
	}

	return server, svc, bus
}

func dialClient(t *testing.T, addr, clientID string) (net.Conn, *mqtt.ConnackPacket) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial mqtt server: %v", err)
	}

	connectPkt := &mqtt.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		CleanSession:  true,
		ClientID:      clientID,
	}

	if err := mqtt.WritePacket(conn, connectPkt); err != nil {
		_ = conn.Close()
		t.Fatalf("failed to write CONNECT: %v", err)
	}

	readPkt, err := mqtt.ReadPacket(conn)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("failed to read CONNACK: %v", err)
	}

	connack, ok := readPkt.(*mqtt.ConnackPacket)
	if !ok {
		_ = conn.Close()
		t.Fatalf("expected *ConnackPacket, got: %T", readPkt)
	}

	return conn, connack
}

func TestServer_ConnectAndDisconnect(t *testing.T) {
	server, svc, _ := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	conn, connack := dialClient(t, server.Addr(), "client-connect-1")
	defer conn.Close()

	if connack.ReturnCode != mqtt.ConnAccepted {
		t.Fatalf("expected ConnAccepted, got: %d", connack.ReturnCode)
	}

	// Send DISCONNECT
	if err := mqtt.WritePacket(conn, &mqtt.DisconnectPacket{}); err != nil {
		t.Fatalf("failed to write DISCONNECT: %v", err)
	}
}

func TestServer_Pingreq(t *testing.T) {
	server, svc, _ := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	conn, _ := dialClient(t, server.Addr(), "client-ping-1")
	defer conn.Close()

	if err := mqtt.WritePacket(conn, &mqtt.PingreqPacket{}); err != nil {
		t.Fatalf("failed to write PINGREQ: %v", err)
	}

	resp, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("failed to read PINGRESP: %v", err)
	}
	if resp.Type() != mqtt.TypePingresp {
		t.Fatalf("expected PINGRESP, got: %d", resp.Type())
	}
}

func TestServer_Publish_QoS0_And_QoS1(t *testing.T) {
	server, svc, _ := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	conn, _ := dialClient(t, server.Addr(), "client-pub-1")
	defer conn.Close()

	// 1. Publish QoS 0
	pub0 := &mqtt.PublishPacket{
		Topic:   "sensors/weather/temp",
		QoS:     mqtt.QoS0,
		Payload: []byte("28.3C"),
	}
	if err := mqtt.WritePacket(conn, pub0); err != nil {
		t.Fatalf("failed to send PUBLISH QoS 0: %v", err)
	}

	// Wait briefly for broker append
	time.Sleep(50 * time.Millisecond)

	records, err := svc.Fetch(context.Background(), "sensors/weather/temp", 0, 0, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("expected 1 record in broker, got len=%d, err=%v", len(records), err)
	}
	if string(records[0].Value) != "28.3C" {
		t.Errorf("expected payload '28.3C', got: %s", string(records[0].Value))
	}

	// 2. Publish QoS 1 (Expect PUBACK)
	pub1 := &mqtt.PublishPacket{
		Topic:    "factory/alarm",
		PacketID: 88,
		QoS:      mqtt.QoS1,
		Payload:  []byte("CRITICAL_PRESSURE"),
	}
	if err := mqtt.WritePacket(conn, pub1); err != nil {
		t.Fatalf("failed to send PUBLISH QoS 1: %v", err)
	}

	ackPkt, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("failed to read PUBACK: %v", err)
	}
	puback, ok := ackPkt.(*mqtt.PubackPacket)
	if !ok || puback.PacketID != 88 {
		t.Fatalf("expected PUBACK with PacketID 88, got: %+v", ackPkt)
	}

	records1, err := svc.Fetch(context.Background(), "factory/alarm", 0, 0, 10)
	if err != nil || len(records1) != 1 {
		t.Fatalf("expected 1 record in broker for factory/alarm, got len=%d", len(records1))
	}
	if string(records1[0].Value) != "CRITICAL_PRESSURE" {
		t.Errorf("expected payload 'CRITICAL_PRESSURE', got: %s", string(records1[0].Value))
	}
}

func TestServer_Subscribe_And_Receive_Wildcard(t *testing.T) {
	server, svc, _ := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	subConn, _ := dialClient(t, server.Addr(), "sub-client-1")
	defer subConn.Close()

	// Subscribe to "sensors/+/celsius" and "factory/#"
	subPkt := &mqtt.SubscribePacket{
		PacketID: 44,
		Subscriptions: []mqtt.TopicSubscription{
			{Topic: "sensors/+/celsius", QoS: mqtt.QoS0},
			{Topic: "factory/#", QoS: mqtt.QoS1},
		},
	}
	if err := mqtt.WritePacket(subConn, subPkt); err != nil {
		t.Fatalf("failed to write SUBSCRIBE: %v", err)
	}

	subackPkt, err := mqtt.ReadPacket(subConn)
	if err != nil {
		t.Fatalf("failed to read SUBACK: %v", err)
	}
	suback, ok := subackPkt.(*mqtt.SubackPacket)
	if !ok || suback.PacketID != 44 || len(suback.ReturnCodes) != 2 {
		t.Fatalf("unexpected SUBACK: %+v", subackPkt)
	}

	// Connect second client to publish
	pubConn, _ := dialClient(t, server.Addr(), "pub-client-1")
	defer pubConn.Close()

	// 1. Publish matching single-level wildcard '+'
	_ = mqtt.WritePacket(pubConn, &mqtt.PublishPacket{
		Topic:   "sensors/chiller1/celsius",
		QoS:     mqtt.QoS0,
		Payload: []byte("14.2"),
	})

	// 2. Publish matching multi-level wildcard '#'
	_ = mqtt.WritePacket(pubConn, &mqtt.PublishPacket{
		Topic:   "factory/line2/press/state",
		QoS:     mqtt.QoS0,
		Payload: []byte("ONLINE"),
	})

	// 3. Publish non-matching topic
	_ = mqtt.WritePacket(pubConn, &mqtt.PublishPacket{
		Topic:   "unrelated/topic",
		QoS:     mqtt.QoS0,
		Payload: []byte("IGNORE_ME"),
	})

	// Read first received PUBLISH on subConn
	_ = subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg1, err := mqtt.ReadPacket(subConn)
	if err != nil {
		t.Fatalf("failed to receive msg 1: %v", err)
	}
	pubMsg1, ok := msg1.(*mqtt.PublishPacket)
	if !ok {
		t.Fatalf("expected *PublishPacket, got: %T", msg1)
	}
	if pubMsg1.Topic != "sensors/chiller1/celsius" || string(pubMsg1.Payload) != "14.2" {
		t.Errorf("msg 1 mismatch: topic=%s payload=%s", pubMsg1.Topic, string(pubMsg1.Payload))
	}

	// Read second received PUBLISH on subConn
	_ = subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg2, err := mqtt.ReadPacket(subConn)
	if err != nil {
		t.Fatalf("failed to receive msg 2: %v", err)
	}
	pubMsg2, ok := msg2.(*mqtt.PublishPacket)
	if !ok {
		t.Fatalf("expected *PublishPacket, got: %T", msg2)
	}
	if pubMsg2.Topic != "factory/line2/press/state" || string(pubMsg2.Payload) != "ONLINE" {
		t.Errorf("msg 2 mismatch: topic=%s payload=%s", pubMsg2.Topic, string(pubMsg2.Payload))
	}
}

func TestServer_InvalidHandshake(t *testing.T) {
	server, svc, _ := setupTestServer(t)
	defer svc.Close()
	defer server.Close()

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send PINGREQ before CONNECT (Violation of MQTT spec 3.1.0)
	_ = mqtt.WritePacket(conn, &mqtt.PingreqPacket{})

	// Server should close connection
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var buf [10]byte
	n, _ := conn.Read(buf[:])
	if n != 0 {
		t.Errorf("expected server to immediately close without sending data, got %d bytes", n)
	}
}
