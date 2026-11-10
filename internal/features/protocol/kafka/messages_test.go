package kafka_test

import (
	"bytes"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
)

func TestRequestHeader_Decode(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := kafka.NewEncoder(buf)

	enc.PutInt16(kafka.ApiKeyApiVersions)
	enc.PutInt16(3)       // version 3
	enc.PutInt32(999)     // correlation ID
	enc.PutString("test-client")

	header, err := kafka.DecodeRequestHeader(kafka.NewDecoder(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to decode header: %v", err)
	}

	if header.ApiKey != kafka.ApiKeyApiVersions {
		t.Errorf("expected ApiKey %d, got %d", kafka.ApiKeyApiVersions, header.ApiKey)
	}
	if header.ApiVersion != 3 {
		t.Errorf("expected ApiVersion 3, got %d", header.ApiVersion)
	}
	if header.CorrelationId != 999 {
		t.Errorf("expected CorrelationId 999, got %d", header.CorrelationId)
	}
	if header.ClientId != "test-client" {
		t.Errorf("expected ClientId 'test-client', got %q", header.ClientId)
	}
}

func TestApiVersions_Roundtrip(t *testing.T) {
	resp := kafka.BuildApiVersionsResponse(42)
	buf := resp.Encode()

	dec := kafka.NewDecoder(buf)
	corrId, err := dec.GetInt32()
	if err != nil || corrId != 42 {
		t.Errorf("expected correlation ID 42, got %d (%v)", corrId, err)
	}

	errCode, err := dec.GetInt16()
	if err != nil || errCode != 0 {
		t.Errorf("expected error code 0, got %d (%v)", errCode, err)
	}

	apiCount, err := dec.GetInt32()
	if err != nil || apiCount <= 0 {
		t.Fatalf("expected api count > 0, got %d (%v)", apiCount, err)
	}

	// Verify ApiKeyProduce (0) is present
	foundProduce := false
	for i := 0; i < int(apiCount); i++ {
		key, _ := dec.GetInt16()
		minV, _ := dec.GetInt16()
		maxV, _ := dec.GetInt16()
		if key == kafka.ApiKeyProduce {
			foundProduce = true
			if minV > maxV {
				t.Errorf("invalid version range: min %d > max %d", minV, maxV)
			}
		}
	}

	if !foundProduce {
		t.Error("expected ApiKeyProduce in ApiVersions response")
	}
}

func TestMetadata_Encode(t *testing.T) {
	resp := kafka.MetadataResponse{
		CorrelationId: 101,
		Brokers: []kafka.BrokerInfo{
			{NodeId: 0, Host: "127.0.0.1", Port: 9092},
		},
		Topics: []kafka.TopicMetadataResponse{
			{
				ErrorCode: 0,
				Topic:     "test-topic",
				Partitions: []kafka.PartitionMetadataResponse{
					{
						ErrorCode:   0,
						PartitionId: 0,
						Leader:      0,
						Replicas:    []int32{0},
						Isr:         []int32{0},
					},
				},
			},
		},
	}

	buf := resp.Encode()
	dec := kafka.NewDecoder(buf)

	corrId, _ := dec.GetInt32()
	if corrId != 101 {
		t.Errorf("expected corrId 101, got %d", corrId)
	}

	// Broker count
	bCount, _ := dec.GetInt32()
	if bCount != 1 {
		t.Errorf("expected 1 broker, got %d", bCount)
	}
}

func TestRecordBatchV2_EncodeDecode(t *testing.T) {
	records := []kafka.RecordItem{
		{
			Key:   []byte("k1"),
			Value: []byte("v1"),
		},
		{
			Key:   []byte("k2"),
			Value: []byte("v2"),
		},
	}

	batchBytes := kafka.EncodeRecordBatchV2(0, records)
	if len(batchBytes) == 0 {
		t.Fatal("encoded batch is empty")
	}

	decoded, err := kafka.DecodeRecordBatchV2(batchBytes)
	if err != nil {
		t.Fatalf("failed to decode record batch: %v", err)
	}

	if len(decoded) != 2 {
		t.Fatalf("expected 2 records, got %d", len(decoded))
	}
	if string(decoded[0].Key) != "k1" || string(decoded[0].Value) != "v1" {
		t.Errorf("unexpected record 0: %+v", decoded[0])
	}
	if string(decoded[1].Key) != "k2" || string(decoded[1].Value) != "v2" {
		t.Errorf("unexpected record 1: %+v", decoded[1])
	}
}


func TestMessages_ProduceFetchRoundtrip(t *testing.T) {
	prodResp := kafka.ProduceResponse{
		CorrelationId: 202,
		Topics: []kafka.ProduceTopicResponse{
			{
				Topic: "metrics",
				Partitions: []kafka.ProducePartitionResponse{
					{
						Partition:      0,
						ErrorCode:      0,
						BaseOffset:     15,
						LogAppendTime:  1000,
						LogStartOffset: 0,
					},
				},
			},
		},
	}

	prodBytes := prodResp.Encode()
	dec := kafka.NewDecoder(prodBytes)
	corrId, _ := dec.GetInt32()
	if corrId != 202 {
		t.Errorf("expected corrId 202, got %d", corrId)
	}

	fetchResp := kafka.FetchResponse{
		CorrelationId: 203,
		Topics: []kafka.FetchTopicResponse{
			{
				Topic: "metrics",
				Partitions: []kafka.FetchPartitionResponse{
					{
						Partition:     0,
						ErrorCode:     0,
						HighWatermark: 16,
						RecordBatch:   []byte("sample-batch"),
					},
				},
			},
		},
	}

	fetchBytes := fetchResp.Encode()
	dec2 := kafka.NewDecoder(fetchBytes)
	corrId2, _ := dec2.GetInt32()
	if corrId2 != 203 {
		t.Errorf("expected corrId 203, got %d", corrId2)
	}
}
