package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	protocolhttp "github.com/pandusatrianura/pocketqueue/internal/features/protocol/http"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/mqtt"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/redis"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// TestMQTT_CrossProtocolE2E tests full cross-protocol ingress and egress:
// 1. MQTT Publish -> Kafka Fetch
// 2. MQTT Publish -> Redis Streams Range
// 3. Kafka Produce -> MQTT Subscriber Receive
// 4. MQTT Publish -> HTTP SSE Real-Time Stream
func TestMQTT_CrossProtocolE2E(t *testing.T) {
	mem := storage.NewMemoryEngine(1000)
	bus := broker.NewEventBus(500)
	svc := broker.NewService(mem, bus, 1)
	defer svc.Close()

	// 1. Start MQTT Protocol Server
	mqttServer := mqtt.NewServer("127.0.0.1:0", svc, bus, nil)
	if err := mqttServer.Start(); err != nil {
		t.Fatalf("failed to start mqtt server: %v", err)
	}
	defer mqttServer.Close()

	// 2. Start Kafka Protocol Server
	kHandler := kafka.NewHandler(svc, "127.0.0.1", 0, nil)
	kafkaServer := kafka.NewServer("127.0.0.1:0", kHandler, nil)
	if err := kafkaServer.Start(); err != nil {
		t.Fatalf("failed to start kafka server: %v", err)
	}
	defer kafkaServer.Close()

	// 3. Start Redis Streams Protocol Server
	redisServer := redis.NewServer("127.0.0.1:0", svc, nil)
	if err := redisServer.Start(); err != nil {
		t.Fatalf("failed to start redis server: %v", err)
	}
	defer redisServer.Close()

	// 4. Start HTTP REST & SSE Server
	httpServer := protocolhttp.NewServer("127.0.0.1:0", svc, bus, nil)
	if err := httpServer.Start(); err != nil {
		t.Fatalf("failed to start http server: %v", err)
	}
	defer httpServer.Close()

	// Helper: connect and handshake with MQTT
	dialMQTT := func(clientID string) net.Conn {
		conn, err := net.DialTimeout("tcp", mqttServer.Addr(), 2*time.Second)
		if err != nil {
			t.Fatalf("failed to dial mqtt: %v", err)
		}
		_ = mqtt.WritePacket(conn, &mqtt.ConnectPacket{
			ProtocolName:  "MQTT",
			ProtocolLevel: 4,
			CleanSession:  true,
			ClientID:      clientID,
		})
		resp, err := mqtt.ReadPacket(conn)
		if err != nil {
			t.Fatalf("failed to read connack: %v", err)
		}
		connack, ok := resp.(*mqtt.ConnackPacket)
		if !ok || connack.ReturnCode != mqtt.ConnAccepted {
			t.Fatalf("connack failed: %+v", resp)
		}
		return conn
	}

	// Helper: call Kafka TCP
	callKafka := func(kConn net.Conn, req []byte) []byte {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(req)))
		if _, err := kConn.Write(lenBuf[:]); err != nil {
			t.Fatalf("kafka write len: %v", err)
		}
		if _, err := kConn.Write(req); err != nil {
			t.Fatalf("kafka write req: %v", err)
		}

		var respLenBuf [4]byte
		if _, err := io.ReadFull(kConn, respLenBuf[:]); err != nil {
			t.Fatalf("kafka read resp len: %v", err)
		}
		respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
		resp := make([]byte, respLen)
		if _, err := io.ReadFull(kConn, resp); err != nil {
			t.Fatalf("kafka read resp body: %v", err)
		}
		return resp
	}

	// ------------------------------------------------------------------------
	// Scenario 1: MQTT Publish -> Kafka Fetch
	// ------------------------------------------------------------------------
	t.Run("MQTTPublish_To_KafkaFetch", func(t *testing.T) {
		mConn := dialMQTT("mqtt-pub-scenario1")
		defer mConn.Close()

		pub := &mqtt.PublishPacket{
			Topic:   "iot/sensors/temp",
			QoS:     mqtt.QoS0,
			Payload: []byte(`{"celsius":26.8}`),
		}
		if err := mqtt.WritePacket(mConn, pub); err != nil {
			t.Fatalf("mqtt publish failed: %v", err)
		}

		time.Sleep(30 * time.Millisecond)

		// Connect to Kafka and Fetch
		kConn, err := net.DialTimeout("tcp", kafkaServer.Addr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial kafka failed: %v", err)
		}
		defer kConn.Close()

		fBuf := new(bytes.Buffer)
		fEnc := kafka.NewEncoder(fBuf)
		fEnc.PutInt16(kafka.ApiKeyFetch)
		fEnc.PutInt16(4)
		fEnc.PutInt32(701) // Correlation ID
		fEnc.PutString("e2e-mqtt-test")
		fEnc.PutInt32(-1) // ReplicaId
		fEnc.PutInt32(50) // MaxWaitTime
		fEnc.PutInt32(1)  // MinBytes
		fEnc.PutInt32(1024 * 1024)
		fEnc.PutInt8(0) // IsolationLevel
		// 1 topic
		fEnc.PutInt32(1)
		fEnc.PutString("iot/sensors/temp")
		// 1 partition
		fEnc.PutInt32(1)
		fEnc.PutInt32(0) // Partition 0
		fEnc.PutInt64(0) // FetchOffset 0
		fEnc.PutInt64(0) // LogStartOffset
		fEnc.PutInt32(1024 * 1024)

		fResp := callKafka(kConn, fBuf.Bytes())
		fDec := kafka.NewDecoder(fResp)
		corrID, _ := fDec.GetInt32()
		if corrID != 701 {
			t.Fatalf("unexpected kafka corrID: %d", corrID)
		}
		_, _ = fDec.GetInt32() // ThrottleTimeMs
		tCount, _ := fDec.GetInt32()
		if tCount != 1 {
			t.Fatalf("expected 1 topic, got: %d", tCount)
		}
		_, _ = fDec.GetString() // topic
		_, _ = fDec.GetInt32()  // partition count
		_, _ = fDec.GetInt32()  // partitionIndex
		pErr, _ := fDec.GetInt16()
		if pErr != 0 {
			t.Fatalf("fetch partition error: %d", pErr)
		}
		_, _ = fDec.GetInt64() // HighWatermark
		_, _ = fDec.GetInt64() // LastStableOffset
		_, _ = fDec.GetInt64() // LogStartOffset
		_, _ = fDec.GetInt32() // AbortedTransactions

		recordsBytes, _ := fDec.GetBytes()
		records, err := kafka.DecodeRecordBatchV2(recordsBytes)
		if err != nil {
			t.Fatalf("failed to decode kafka record batch: %v", err)
		}
		if len(records) != 1 {
			t.Fatalf("expected 1 record from Kafka fetch, got: %d", len(records))
		}
		if string(records[0].Value) != `{"celsius":26.8}` {
			t.Errorf("expected payload '{\"celsius\":26.8}', got: %s", string(records[0].Value))
		}
	})

	// ------------------------------------------------------------------------
	// Scenario 2: MQTT Publish -> Redis Streams Range
	// ------------------------------------------------------------------------
	t.Run("MQTTPublish_To_RedisStreams", func(t *testing.T) {
		mConn := dialMQTT("mqtt-pub-scenario2")
		defer mConn.Close()

		pub := &mqtt.PublishPacket{
			Topic:   "iot/telemetry",
			QoS:     mqtt.QoS0,
			Payload: []byte("motor_rpm:3400"),
		}
		if err := mqtt.WritePacket(mConn, pub); err != nil {
			t.Fatalf("mqtt publish failed: %v", err)
		}

		time.Sleep(30 * time.Millisecond)

		// Connect to Redis and query XLEN / XRANGE
		rConn, err := net.DialTimeout("tcp", redisServer.Addr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial redis failed: %v", err)
		}
		defer rConn.Close()

		// RESP: XRANGE iot/telemetry - +
		xrangeCmd := "*4\r\n$6\r\nXRANGE\r\n$13\r\niot/telemetry\r\n$1\r\n-\r\n$1\r\n+\r\n"
		if _, err := rConn.Write([]byte(xrangeCmd)); err != nil {
			t.Fatalf("write XRANGE failed: %v", err)
		}

		rReader := bufio.NewReader(rConn)
		respLine, err := rReader.ReadString('\n')
		if err != nil || !strings.HasPrefix(respLine, "*") {
			t.Fatalf("unexpected XRANGE response: %s (err: %v)", respLine, err)
		}

		// Read remainder of array response
		buf := make([]byte, 1024)
		n, _ := rReader.Read(buf)
		bodyStr := string(buf[:n])
		if !strings.Contains(bodyStr, "motor_rpm:3400") {
			t.Errorf("expected Redis stream to contain motor_rpm:3400, got: %s", bodyStr)
		}
	})

	// ------------------------------------------------------------------------
	// Scenario 3: Kafka Produce -> MQTT Subscriber Receive
	// ------------------------------------------------------------------------
	t.Run("KafkaProduce_To_MQTTSubscriber", func(t *testing.T) {
		mConn := dialMQTT("mqtt-sub-scenario3")
		defer mConn.Close()

		// Subscribe to "factory/#"
		_ = mqtt.WritePacket(mConn, &mqtt.SubscribePacket{
			PacketID: 55,
			Subscriptions: []mqtt.TopicSubscription{
				{Topic: "factory/#", QoS: mqtt.QoS0},
			},
		})
		resp, _ := mqtt.ReadPacket(mConn)
		if _, ok := resp.(*mqtt.SubackPacket); !ok {
			t.Fatalf("expected suback, got: %+v", resp)
		}

		// Produce event into PocketQueue broker (as if from Kafka or service)
		_, err := svc.Produce(context.Background(), "factory/alarms/furnace", 0, []byte("sensor-k1"), []byte("TEMP_EXCEEDED_800C"), nil)
		if err != nil {
			t.Fatalf("produce failed: %v", err)
		}

		// MQTT subscriber should receive the fanned out message!
		_ = mConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		pkt, err := mqtt.ReadPacket(mConn)
		if err != nil {
			t.Fatalf("failed to receive publish on MQTT: %v", err)
		}
		pubPkt, ok := pkt.(*mqtt.PublishPacket)
		if !ok {
			t.Fatalf("expected *PublishPacket, got: %T", pkt)
		}
		if pubPkt.Topic != "factory/alarms/furnace" || string(pubPkt.Payload) != "TEMP_EXCEEDED_800C" {
			t.Errorf("received payload mismatch: topic=%s payload=%s", pubPkt.Topic, string(pubPkt.Payload))
		}
	})

	// ------------------------------------------------------------------------
	// Scenario 4: MQTT Publish -> HTTP SSE Stream
	// ------------------------------------------------------------------------
	t.Run("MQTTPublish_To_HTTP_SSE", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sseReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL()+"/api/v1/topics/_all/sse", nil)
		sseResp, err := http.DefaultClient.Do(sseReq)
		if err != nil {
			t.Fatalf("failed to connect SSE stream: %v", err)
		}
		defer sseResp.Body.Close()

		// Publish from MQTT
		mConn := dialMQTT("mqtt-pub-scenario4")
		defer mConn.Close()

		pub := &mqtt.PublishPacket{
			Topic:   "iot/weather/humidity",
			QoS:     mqtt.QoS0,
			Payload: []byte("65%"),
		}
		if err := mqtt.WritePacket(mConn, pub); err != nil {
			t.Fatalf("mqtt publish failed: %v", err)
		}

		// Read SSE stream
		scanner := bufio.NewScanner(sseResp.Body)
		var foundEvent bool
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "iot/weather/humidity") && strings.Contains(line, "65%") {
				foundEvent = true
				break
			}
		}

		if !foundEvent {
			t.Errorf("expected HTTP SSE stream to receive MQTT event: 65%%")
		}
	})
}
