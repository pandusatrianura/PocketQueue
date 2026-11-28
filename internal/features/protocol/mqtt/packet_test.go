package mqtt_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/mqtt"
)

func TestPacket_RemainingLength(t *testing.T) {
	testCases := []struct {
		length   int
		expected []byte
	}{
		{0, []byte{0x00}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{16383, []byte{0xFF, 0x7F}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{2097151, []byte{0xFF, 0xFF, 0x7F}},
		{2097152, []byte{0x80, 0x80, 0x80, 0x01}},
		{268435455, []byte{0xFF, 0xFF, 0xFF, 0x7F}},
	}

	for _, tc := range testCases {
		encoded := mqtt.EncodeRemainingLength(tc.length)
		if !bytes.Equal(encoded, tc.expected) {
			t.Errorf("EncodeRemainingLength(%d) = %v, expected %v", tc.length, encoded, tc.expected)
		}

		decoded, bytesRead, err := mqtt.DecodeRemainingLength(bytes.NewReader(tc.expected))
		if err != nil {
			t.Fatalf("DecodeRemainingLength failed for %d: %v", tc.length, err)
		}
		if decoded != tc.length {
			t.Errorf("DecodeRemainingLength() = %d, expected %d", decoded, tc.length)
		}
		if bytesRead != len(tc.expected) {
			t.Errorf("DecodeRemainingLength() bytesRead = %d, expected %d", bytesRead, len(tc.expected))
		}
	}

	// Malformed remaining length (> 4 bytes with MSB set)
	malformed := []byte{0x80, 0x80, 0x80, 0x80, 0x01}
	_, _, err := mqtt.DecodeRemainingLength(bytes.NewReader(malformed))
	if err == nil {
		t.Errorf("expected error for malformed 5-byte remaining length")
	}
}

func TestPacket_ConnectConnack_Roundtrip(t *testing.T) {
	connPkt := &mqtt.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		CleanSession:  true,
		KeepAlive:     60,
		ClientID:      "test-client-123",
		Username:      "iot-user",
		Password:      "iot-pass",
	}

	buf := new(bytes.Buffer)
	if err := mqtt.WritePacket(buf, connPkt); err != nil {
		t.Fatalf("WritePacket(CONNECT) failed: %v", err)
	}

	readPkt, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(CONNECT) failed: %v", err)
	}

	parsedConn, ok := readPkt.(*mqtt.ConnectPacket)
	if !ok {
		t.Fatalf("expected *ConnectPacket, got %T", readPkt)
	}

	if parsedConn.ProtocolName != "MQTT" || parsedConn.ProtocolLevel != 4 {
		t.Errorf("protocol mismatch: %s %d", parsedConn.ProtocolName, parsedConn.ProtocolLevel)
	}
	if parsedConn.ClientID != "test-client-123" {
		t.Errorf("clientID mismatch: %s", parsedConn.ClientID)
	}
	if parsedConn.KeepAlive != 60 {
		t.Errorf("keepalive mismatch: %d", parsedConn.KeepAlive)
	}
	if !parsedConn.CleanSession {
		t.Errorf("expected clean session true")
	}
	if parsedConn.Username != "iot-user" || parsedConn.Password != "iot-pass" {
		t.Errorf("credentials mismatch: %s / %s", parsedConn.Username, parsedConn.Password)
	}

	// CONNACK
	connackPkt := &mqtt.ConnackPacket{
		SessionPresent: false,
		ReturnCode:     mqtt.ConnAccepted,
	}
	buf.Reset()
	if err := mqtt.WritePacket(buf, connackPkt); err != nil {
		t.Fatalf("WritePacket(CONNACK) failed: %v", err)
	}

	readConnack, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(CONNACK) failed: %v", err)
	}
	parsedConnack, ok := readConnack.(*mqtt.ConnackPacket)
	if !ok {
		t.Fatalf("expected *ConnackPacket, got %T", readConnack)
	}
	if parsedConnack.ReturnCode != mqtt.ConnAccepted {
		t.Errorf("expected ConnAccepted, got %d", parsedConnack.ReturnCode)
	}
}

func TestPacket_PublishPuback_Roundtrip(t *testing.T) {
	// QoS 0 Publish
	pub0 := &mqtt.PublishPacket{
		Topic:   "sensors/temperature",
		QoS:     mqtt.QoS0,
		Payload: []byte(`{"celsius": 24.5}`),
	}
	buf := new(bytes.Buffer)
	if err := mqtt.WritePacket(buf, pub0); err != nil {
		t.Fatalf("WritePacket(PUBLISH QoS 0) failed: %v", err)
	}

	readPub0, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(PUBLISH QoS 0) failed: %v", err)
	}
	parsedPub0, ok := readPub0.(*mqtt.PublishPacket)
	if !ok {
		t.Fatalf("expected *PublishPacket, got %T", readPub0)
	}
	if parsedPub0.Topic != "sensors/temperature" || parsedPub0.QoS != 0 {
		t.Errorf("publish QoS 0 mismatch: topic=%s qos=%d", parsedPub0.Topic, parsedPub0.QoS)
	}
	if string(parsedPub0.Payload) != `{"celsius": 24.5}` {
		t.Errorf("payload mismatch: %s", string(parsedPub0.Payload))
	}

	// QoS 1 Publish & PUBACK
	pub1 := &mqtt.PublishPacket{
		Topic:    "factory/line1/pressure",
		PacketID: 42,
		QoS:      mqtt.QoS1,
		Dup:      false,
		Retain:   false,
		Payload:  []byte("101.3 kPa"),
	}
	buf.Reset()
	if err := mqtt.WritePacket(buf, pub1); err != nil {
		t.Fatalf("WritePacket(PUBLISH QoS 1) failed: %v", err)
	}

	readPub1, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(PUBLISH QoS 1) failed: %v", err)
	}
	parsedPub1, ok := readPub1.(*mqtt.PublishPacket)
	if !ok {
		t.Fatalf("expected *PublishPacket, got %T", readPub1)
	}
	if parsedPub1.PacketID != 42 || parsedPub1.QoS != 1 {
		t.Errorf("publish QoS 1 mismatch: packetID=%d qos=%d", parsedPub1.PacketID, parsedPub1.QoS)
	}

	// PUBACK
	puback := &mqtt.PubackPacket{PacketID: 42}
	buf.Reset()
	if err := mqtt.WritePacket(buf, puback); err != nil {
		t.Fatalf("WritePacket(PUBACK) failed: %v", err)
	}
	readPuback, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(PUBACK) failed: %v", err)
	}
	parsedPuback, ok := readPuback.(*mqtt.PubackPacket)
	if !ok {
		t.Fatalf("expected *PubackPacket, got %T", readPuback)
	}
	if parsedPuback.PacketID != 42 {
		t.Errorf("puback PacketID mismatch: %d", parsedPuback.PacketID)
	}
}

func TestPacket_SubscribeSuback_Roundtrip(t *testing.T) {
	sub := &mqtt.SubscribePacket{
		PacketID: 101,
		Subscriptions: []mqtt.TopicSubscription{
			{Topic: "sensors/+/temperature", QoS: mqtt.QoS0},
			{Topic: "factory/#", QoS: mqtt.QoS1},
		},
	}
	buf := new(bytes.Buffer)
	if err := mqtt.WritePacket(buf, sub); err != nil {
		t.Fatalf("WritePacket(SUBSCRIBE) failed: %v", err)
	}

	readSub, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(SUBSCRIBE) failed: %v", err)
	}
	parsedSub, ok := readSub.(*mqtt.SubscribePacket)
	if !ok {
		t.Fatalf("expected *SubscribePacket, got %T", readSub)
	}
	if parsedSub.PacketID != 101 || len(parsedSub.Subscriptions) != 2 {
		t.Fatalf("subscribe mismatch: id=%d len=%d", parsedSub.PacketID, len(parsedSub.Subscriptions))
	}
	if parsedSub.Subscriptions[0].Topic != "sensors/+/temperature" || parsedSub.Subscriptions[0].QoS != 0 {
		t.Errorf("sub 0 mismatch: %+v", parsedSub.Subscriptions[0])
	}
	if parsedSub.Subscriptions[1].Topic != "factory/#" || parsedSub.Subscriptions[1].QoS != 1 {
		t.Errorf("sub 1 mismatch: %+v", parsedSub.Subscriptions[1])
	}

	// SUBACK
	suback := &mqtt.SubackPacket{
		PacketID:    101,
		ReturnCodes: []byte{mqtt.SubackQoS0, mqtt.SubackQoS1},
	}
	buf.Reset()
	if err := mqtt.WritePacket(buf, suback); err != nil {
		t.Fatalf("WritePacket(SUBACK) failed: %v", err)
	}
	readSuback, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(SUBACK) failed: %v", err)
	}
	parsedSuback, ok := readSuback.(*mqtt.SubackPacket)
	if !ok {
		t.Fatalf("expected *SubackPacket, got %T", readSuback)
	}
	if parsedSuback.PacketID != 101 || !reflect.DeepEqual(parsedSuback.ReturnCodes, []byte{0x00, 0x01}) {
		t.Errorf("suback mismatch: %+v", parsedSuback)
	}
}

func TestPacket_UnsubscribeUnsuback_Roundtrip(t *testing.T) {
	unsub := &mqtt.UnsubscribePacket{
		PacketID: 202,
		Topics:   []string{"sensors/temp", "factory/#"},
	}
	buf := new(bytes.Buffer)
	if err := mqtt.WritePacket(buf, unsub); err != nil {
		t.Fatalf("WritePacket(UNSUBSCRIBE) failed: %v", err)
	}

	readUnsub, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(UNSUBSCRIBE) failed: %v", err)
	}
	parsedUnsub, ok := readUnsub.(*mqtt.UnsubscribePacket)
	if !ok {
		t.Fatalf("expected *UnsubscribePacket, got %T", readUnsub)
	}
	if parsedUnsub.PacketID != 202 || len(parsedUnsub.Topics) != 2 {
		t.Fatalf("unsub mismatch: id=%d len=%d", parsedUnsub.PacketID, len(parsedUnsub.Topics))
	}

	// UNSUBACK
	unsuback := &mqtt.UnsubackPacket{PacketID: 202}
	buf.Reset()
	if err := mqtt.WritePacket(buf, unsuback); err != nil {
		t.Fatalf("WritePacket(UNSUBACK) failed: %v", err)
	}
	readUnsuback, err := mqtt.ReadPacket(buf)
	if err != nil {
		t.Fatalf("ReadPacket(UNSUBACK) failed: %v", err)
	}
	parsedUnsuback, ok := readUnsuback.(*mqtt.UnsubackPacket)
	if !ok {
		t.Fatalf("expected *UnsubackPacket, got %T", readUnsuback)
	}
	if parsedUnsuback.PacketID != 202 {
		t.Errorf("unsuback id mismatch: %d", parsedUnsuback.PacketID)
	}
}

func TestPacket_ControlPackets_Roundtrip(t *testing.T) {
	buf := new(bytes.Buffer)

	// PINGREQ
	if err := mqtt.WritePacket(buf, &mqtt.PingreqPacket{}); err != nil {
		t.Fatalf("WritePacket(PINGREQ) failed: %v", err)
	}
	if p, err := mqtt.ReadPacket(buf); err != nil || p.Type() != mqtt.TypePingreq {
		t.Fatalf("ReadPacket(PINGREQ) failed: %v", err)
	}

	// PINGRESP
	buf.Reset()
	if err := mqtt.WritePacket(buf, &mqtt.PingrespPacket{}); err != nil {
		t.Fatalf("WritePacket(PINGRESP) failed: %v", err)
	}
	if p, err := mqtt.ReadPacket(buf); err != nil || p.Type() != mqtt.TypePingresp {
		t.Fatalf("ReadPacket(PINGRESP) failed: %v", err)
	}

	// DISCONNECT
	buf.Reset()
	if err := mqtt.WritePacket(buf, &mqtt.DisconnectPacket{}); err != nil {
		t.Fatalf("WritePacket(DISCONNECT) failed: %v", err)
	}
	if p, err := mqtt.ReadPacket(buf); err != nil || p.Type() != mqtt.TypeDisconnect {
		t.Fatalf("ReadPacket(DISCONNECT) failed: %v", err)
	}
}

func TestPacket_ErrorsAndEdgeCases(t *testing.T) {
	// Truncated packet
	truncated := []byte{0x10, 0x0A, 0x00, 0x04, 'M', 'Q'}
	_, err := mqtt.ReadPacket(bytes.NewReader(truncated))
	if err == nil {
		t.Errorf("expected error for truncated packet")
	}

	// Unsupported packet type (e.g. 0 or 15)
	unsupported := []byte{0xF0, 0x00}
	_, err = mqtt.ReadPacket(bytes.NewReader(unsupported))
	if err == nil {
		t.Errorf("expected error for unsupported packet type")
	}
}

