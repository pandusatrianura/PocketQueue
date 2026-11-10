package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
)

// PublishCLI sends a message via Kafka TCP protocol to a running broker.
func PublishCLI(brokerAddr, topic, key, value string) error {
	conn, err := net.DialTimeout("tcp", brokerAddr, 3*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to broker at %s: %w", brokerAddr, err)
	}
	defer conn.Close()

	records := []kafka.RecordItem{
		{Key: []byte(key), Value: []byte(value)},
	}
	batch := kafka.EncodeRecordBatchV2(0, records)

	buf := new(bytes.Buffer)
	enc := kafka.NewEncoder(buf)
	enc.PutInt16(kafka.ApiKeyProduce)
	enc.PutInt16(3)
	enc.PutInt32(1)
	enc.PutString("pocketqueue-cli")
	enc.PutNullableString(nil)
	enc.PutInt16(1)
	enc.PutInt32(1500)

	enc.PutInt32(1) // 1 topic
	enc.PutString(topic)
	enc.PutInt32(1) // 1 partition
	enc.PutInt32(0) // partition 0
	enc.PutBytes(batch)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(buf.Len()))

	if _, err := conn.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("failed to send frame length: %w", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to send produce frame: %w", err)
	}

	var respLenBuf [4]byte
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		return fmt.Errorf("failed to read response length: %w", err)
	}
	respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
	respBytes := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBytes); err != nil {
		return fmt.Errorf("failed to read response payload: %w", err)
	}

	dec := kafka.NewDecoder(respBytes)
	_, _ = dec.GetInt32() // correlation_id
	topicsCount, _ := dec.GetInt32()
	if topicsCount > 0 {
		_, _ = dec.GetString()
		partsCount, _ := dec.GetInt32()
		if partsCount > 0 {
			_, _ = dec.GetInt32() // part ID
			errCode, _ := dec.GetInt16()
			baseOffset, _ := dec.GetInt64()
			if errCode != 0 {
				return fmt.Errorf("broker returned kafka error code: %d", errCode)
			}
			fmt.Printf("✅ Published to %s (offset %d)\n", topic, baseOffset)
			return nil
		}
	}

	fmt.Printf("✅ Published to %s\n", topic)
	return nil
}

