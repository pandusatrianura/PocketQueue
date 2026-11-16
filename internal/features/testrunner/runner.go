package testrunner

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
)

// Common errors returned by testrunner.
var (
	ErrAssertionTimeout = errors.New("testrunner: timeout waiting for matching message")
	ErrTopicRequired    = errors.New("testrunner: topic name is required")
)

// AssertionSpec defines matching criteria for CI/CD test assertions.
type AssertionSpec struct {
	BrokerAddr         string        // e.g. "127.0.0.1:9092"
	Topic              string        // required topic
	Partition          int32         // defaults to 0
	ExpectedKey        string        // if non-empty, must match message key
	ExpectedPayloadSub string        // if non-empty, must be substring of payload
	Timeout            time.Duration // default 5s if <= 0
	PollInterval       time.Duration // default 50ms if <= 0
}

// AssertionResult captures details of the matched record.
type AssertionResult struct {
	Topic     string        `json:"topic"`
	Partition int32         `json:"partition"`
	Offset    int64         `json:"offset"`
	Key       string        `json:"key"`
	Payload   string        `json:"payload"`
	Timestamp time.Time     `json:"timestamp"`
	Latency   time.Duration `json:"latency"`
}

// RunAssertion connects to the Kafka protocol port and polls until a matching record is found or timeout.
func RunAssertion(ctx context.Context, spec AssertionSpec) (*AssertionResult, error) {
	if spec.Topic == "" {
		return nil, ErrTopicRequired
	}
	if spec.BrokerAddr == "" {
		spec.BrokerAddr = "127.0.0.1:9092"
	}
	if spec.Timeout <= 0 {
		spec.Timeout = 5 * time.Second
	}
	if spec.PollInterval <= 0 {
		spec.PollInterval = 50 * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(ctx, spec.Timeout)
	defer cancel()

	startTime := time.Now()
	var currentOffset int64 = 0
	var corrID int32 = 1

	var conn net.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: topic=%q, expectedKey=%q, expectedSub=%q (elapsed: %v)",
				ErrAssertionTimeout, spec.Topic, spec.ExpectedKey, spec.ExpectedPayloadSub, time.Since(startTime))
		default:
		}

		if conn == nil {
			var dialer net.Dialer
			c, err := dialer.DialContext(ctx, "tcp", spec.BrokerAddr)
			if err != nil {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("%w: failed to connect to %s: %v", ErrAssertionTimeout, spec.BrokerAddr, err)
				case <-time.After(spec.PollInterval):
					continue
				}
			}
			conn = c
		}

		corrID++
		hw, items, err := fetchPartition(conn, corrID, spec.Topic, spec.Partition, currentOffset)
		if err != nil {
			_ = conn.Close()
			conn = nil
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: fetch error: %v", ErrAssertionTimeout, err)
			case <-time.After(spec.PollInterval):
				continue
			}
		}

		if len(items) > 0 {
			for idx, item := range items {
				itemOffset := currentOffset + int64(idx)
				keyStr := string(item.Key)
				valStr := string(item.Value)

				keyMatch := spec.ExpectedKey == "" || keyStr == spec.ExpectedKey
				payloadMatch := spec.ExpectedPayloadSub == "" || strings.Contains(valStr, spec.ExpectedPayloadSub)

				if keyMatch && payloadMatch {
					return &AssertionResult{
						Topic:     spec.Topic,
						Partition: spec.Partition,
						Offset:    itemOffset,
						Key:       keyStr,
						Payload:   valStr,
						Timestamp: time.Now(),
						Latency:   time.Since(startTime),
					}, nil
				}
			}
			currentOffset += int64(len(items))
		} else if hw > currentOffset {
			currentOffset = hw
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: topic=%q, expectedKey=%q, expectedSub=%q (elapsed: %v)",
				ErrAssertionTimeout, spec.Topic, spec.ExpectedKey, spec.ExpectedPayloadSub, time.Since(startTime))
		case <-time.After(spec.PollInterval):
		}
	}
}

func fetchPartition(conn net.Conn, corrID int32, topic string, partition int32, fetchOffset int64) (int64, []kafka.RecordItem, error) {
	buf := new(bytes.Buffer)
	enc := kafka.NewEncoder(buf)
	enc.PutInt16(kafka.ApiKeyFetch)
	enc.PutInt16(4) // version 4
	enc.PutInt32(corrID)
	enc.PutString("pocketqueue-testrunner")

	enc.PutInt32(-1)    // replica_id
	enc.PutInt32(200)   // max_wait_ms
	enc.PutInt32(1)     // min_bytes
	enc.PutInt32(65536) // max_bytes
	enc.PutInt8(0)      // isolation_level

	// 1 topic
	enc.PutInt32(1)
	enc.PutString(topic)

	// 1 partition
	enc.PutInt32(1)
	enc.PutInt32(partition)
	enc.PutInt64(fetchOffset)
	enc.PutInt32(65536) // max_bytes

	respBytes, err := sendAndReceive(conn, buf.Bytes())
	if err != nil {
		return 0, nil, err
	}

	dec := kafka.NewDecoder(respBytes)
	_, _ = dec.GetInt32() // correlation_id
	_, _ = dec.GetInt32() // throttle_time_ms
	topicsCount, err := dec.GetInt32()
	if err != nil || topicsCount == 0 {
		return 0, nil, nil
	}
	_, _ = dec.GetString() // topic name
	partsCount, err := dec.GetInt32()
	if err != nil || partsCount == 0 {
		return 0, nil, nil
	}
	_, _ = dec.GetInt32() // partition_id
	errCode, _ := dec.GetInt16()
	if errCode != 0 {
		return 0, nil, nil
	}
	highWatermark, _ := dec.GetInt64()
	_, _ = dec.GetInt64() // last_stable_offset
	_, _ = dec.GetInt64() // log_start_offset
	_, _ = dec.GetInt32() // aborted_tx_count

	batchBytes, err := dec.GetBytes()
	if err != nil || len(batchBytes) == 0 {
		return highWatermark, nil, nil
	}

	items, err := kafka.DecodeRecordBatchV2(batchBytes)
	if err != nil {
		return highWatermark, nil, err
	}

	return highWatermark, items, nil
}

func sendAndReceive(conn net.Conn, payload []byte) ([]byte, error) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))

	if _, err := conn.Write(lenBuf[:]); err != nil {
		return nil, fmt.Errorf("write frame length: %w", err)
	}
	if _, err := conn.Write(payload); err != nil {
		return nil, fmt.Errorf("write payload: %w", err)
	}

	var respLenBuf [4]byte
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		return nil, fmt.Errorf("read resp length: %w", err)
	}
	respLen := int(binary.BigEndian.Uint32(respLenBuf[:]))
	if respLen <= 0 || respLen > 16*1024*1024 {
		return nil, fmt.Errorf("invalid response length: %d", respLen)
	}

	respBytes := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBytes); err != nil {
		return nil, fmt.Errorf("read resp payload: %w", err)
	}
	return respBytes, nil
}
