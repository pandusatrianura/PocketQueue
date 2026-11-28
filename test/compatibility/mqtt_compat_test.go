package compatibility_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/mqtt"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// TestMQTT_Compat_FullLifecycle tests standard MQTT v3.1.1 client packet interactions:
// Handshake -> Ping -> Publish QoS 0 -> Publish QoS 1 (PUBACK) -> Subscribe (SUBACK) -> Message Fanout -> Unsubscribe (UNSUBACK) -> Disconnect.
func TestMQTT_Compat_FullLifecycle(t *testing.T) {
	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(mem, bus, 1)
	defer svc.Close()

	server := mqtt.NewServer("127.0.0.1:0", svc, bus, nil)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start mqtt server: %v", err)
	}
	defer server.Close()

	// 1. Establish TCP connection
	conn, err := net.DialTimeout("tcp", server.Addr(), 3*time.Second)
	if err != nil {
		t.Fatalf("failed to dial mqtt server: %v", err)
	}
	defer conn.Close()

	// 2. Send CONNECT packet
	connectPkt := &mqtt.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		CleanSession:  true,
		KeepAlive:     15,
		ClientID:      "compat-tester-v311",
		Username:      "iot-device",
		Password:      "secret-token",
	}
	if err := mqtt.WritePacket(conn, connectPkt); err != nil {
		t.Fatalf("failed to send CONNECT: %v", err)
	}

	// 3. Receive CONNACK
	pkt, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("failed to read CONNACK: %v", err)
	}
	connack, ok := pkt.(*mqtt.ConnackPacket)
	if !ok || connack.ReturnCode != mqtt.ConnAccepted {
		t.Fatalf("expected ConnAccepted, got: %+v", pkt)
	}

	// 4. Send PINGREQ -> Receive PINGRESP
	if err := mqtt.WritePacket(conn, &mqtt.PingreqPacket{}); err != nil {
		t.Fatalf("failed to send PINGREQ: %v", err)
	}
	pingResp, err := mqtt.ReadPacket(conn)
	if err != nil || pingResp.Type() != mqtt.TypePingresp {
		t.Fatalf("expected PINGRESP, got: %v (err: %v)", pingResp, err)
	}

	// 5. Send QoS 0 PUBLISH
	pub0 := &mqtt.PublishPacket{
		Topic:   "iot/telemetry/voltage",
		QoS:     mqtt.QoS0,
		Payload: []byte("3.30V"),
	}
	if err := mqtt.WritePacket(conn, pub0); err != nil {
		t.Fatalf("failed to send QoS 0 PUBLISH: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	recs, err := svc.Fetch(context.Background(), "iot/telemetry/voltage", 0, 0, 10)
	if err != nil || len(recs) != 1 || string(recs[0].Value) != "3.30V" {
		t.Fatalf("broker did not store QoS 0 message correctly: len=%d, err=%v", len(recs), err)
	}

	// 6. Send QoS 1 PUBLISH -> Expect PUBACK
	pub1 := &mqtt.PublishPacket{
		Topic:    "iot/alerts/battery",
		PacketID: 101,
		QoS:      mqtt.QoS1,
		Payload:  []byte("LOW_BATTERY_15_PERCENT"),
	}
	if err := mqtt.WritePacket(conn, pub1); err != nil {
		t.Fatalf("failed to send QoS 1 PUBLISH: %v", err)
	}

	ackPkt, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("failed to read PUBACK: %v", err)
	}
	puback, ok := ackPkt.(*mqtt.PubackPacket)
	if !ok || puback.PacketID != 101 {
		t.Fatalf("expected PUBACK with PacketID 101, got: %+v", ackPkt)
	}

	// 7. Subscribe to "iot/command/#"
	subPkt := &mqtt.SubscribePacket{
		PacketID: 202,
		Subscriptions: []mqtt.TopicSubscription{
			{Topic: "iot/command/#", QoS: mqtt.QoS1},
		},
	}
	if err := mqtt.WritePacket(conn, subPkt); err != nil {
		t.Fatalf("failed to send SUBSCRIBE: %v", err)
	}

	subackPkt, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("failed to read SUBACK: %v", err)
	}
	suback, ok := subackPkt.(*mqtt.SubackPacket)
	if !ok || suback.PacketID != 202 || len(suback.ReturnCodes) != 1 || suback.ReturnCodes[0] != mqtt.SubackQoS1 {
		t.Fatalf("expected SUBACK with QoS 1, got: %+v", subackPkt)
	}

	// 8. Publish to "iot/command/reboot" via broker -> Expect client to receive it
	_, err = svc.Produce(context.Background(), "iot/command/reboot", 0, []byte("admin"), []byte("REBOOT_NOW"), nil)
	if err != nil {
		t.Fatalf("produce to broker failed: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	fanoutPkt, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("failed to receive fanned-out PUBLISH: %v", err)
	}
	fannedPub, ok := fanoutPkt.(*mqtt.PublishPacket)
	if !ok || fannedPub.Topic != "iot/command/reboot" || string(fannedPub.Payload) != "REBOOT_NOW" {
		t.Fatalf("fanned-out packet mismatch: %+v", fanoutPkt)
	}

	// 9. UNSUBSCRIBE
	unsubPkt := &mqtt.UnsubscribePacket{
		PacketID: 303,
		Topics:   []string{"iot/command/#"},
	}
	if err := mqtt.WritePacket(conn, unsubPkt); err != nil {
		t.Fatalf("failed to send UNSUBSCRIBE: %v", err)
	}
	unsubackPkt, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("failed to read UNSUBACK: %v", err)
	}
	unsuback, ok := unsubackPkt.(*mqtt.UnsubackPacket)
	if !ok || unsuback.PacketID != 303 {
		t.Fatalf("expected UNSUBACK with PacketID 303, got: %+v", unsubackPkt)
	}

	// 10. DISCONNECT
	if err := mqtt.WritePacket(conn, &mqtt.DisconnectPacket{}); err != nil {
		t.Fatalf("failed to send DISCONNECT: %v", err)
	}
}
