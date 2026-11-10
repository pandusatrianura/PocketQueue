package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// TopicMetadata holds runtime information about a topic and its partitions.
type TopicMetadata struct {
	Name       string
	Partitions []PartitionMetadata
}

// PartitionMetadata holds partition ID, leader, and offset watermarks.
type PartitionMetadata struct {
	ID         int32
	Leader     int32
	Earliest   int64
	Latest     int64
	TotalCount int64
}

// Service manages topics, partitions, message lifecycle, consumer group offsets, and DLQ.
type Service struct {
	storage           storage.Engine
	eventBus          *EventBus
	defaultPartitions int32

	mu            sync.RWMutex
	topics        map[string]int32 // topicName -> partitionCount
	groupOffsets  map[string]map[string]map[int32]int64 // groupID -> topic -> partition -> offset
	closed        bool
}

// NewService instantiates a BrokerService.
func NewService(engine storage.Engine, eventBus *EventBus, defaultPartitions int32) *Service {
	if defaultPartitions <= 0 {
		defaultPartitions = 1
	}
	return &Service{
		storage:           engine,
		eventBus:          eventBus,
		defaultPartitions: defaultPartitions,
		topics:            make(map[string]int32),
		groupOffsets:      make(map[string]map[string]map[int32]int64),
	}
}

// EnsureTopic guarantees a topic exists with at least the default number of partitions.
func (s *Service) EnsureTopic(topic string, partitions int32) int32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cur, exists := s.topics[topic]; exists {
		return cur
	}

	if partitions <= 0 {
		partitions = s.defaultPartitions
	}
	s.topics[topic] = partitions
	return partitions
}

// Produce appends a record to the specified topic partition and broadcasts it to the EventBus.
func (s *Service) Produce(ctx context.Context, topic string, partition int32, key, value []byte, headers []storage.Header) (*storage.Record, error) {
	s.EnsureTopic(topic, partition+1)

	rec, err := s.storage.Append(ctx, topic, partition, key, value, headers)
	if err != nil {
		return nil, fmt.Errorf("storage append failed for topic %s: %w", topic, err)
	}

	if s.eventBus != nil {
		s.eventBus.Publish(*rec)
	}

	return rec, nil
}

// Fetch retrieves records starting from startOffset up to maxRecords.
func (s *Service) Fetch(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int) ([]storage.Record, error) {
	return s.storage.ReadFrom(ctx, topic, partition, startOffset, maxRecords)
}

// CommitOffset updates the committed offset for a consumer group.
func (s *Service) CommitOffset(ctx context.Context, groupID, topic string, partition int32, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	grp, exists := s.groupOffsets[groupID]
	if !exists {
		grp = make(map[string]map[int32]int64)
		s.groupOffsets[groupID] = grp
	}

	tOffsets, exists := grp[topic]
	if !exists {
		tOffsets = make(map[int32]int64)
		grp[topic] = tOffsets
	}

	tOffsets[partition] = offset
	return nil
}

// GetOffset retrieves the committed offset for a consumer group. Returns -1 if not committed.
func (s *Service) GetOffset(ctx context.Context, groupID, topic string, partition int32) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	grp, exists := s.groupOffsets[groupID]
	if !exists {
		return -1, nil
	}

	tOffsets, exists := grp[topic]
	if !exists {
		return -1, nil
	}

	offset, exists := tOffsets[partition]
	if !exists {
		return -1, nil
	}

	return offset, nil
}

// SendToDLQ routes a failed message to a designated dead-letter queue topic (e.g. topic.dlq).
func (s *Service) SendToDLQ(ctx context.Context, topic string, partition int32, key, value []byte, headers []storage.Header, reason string) (*storage.Record, error) {
	dlqTopic := topic + ".dlq"
	if headers == nil {
		headers = make([]storage.Header, 0, 2)
	}
	headers = append(headers,
		storage.Header{Key: "x-dlq-original-topic", Value: []byte(topic)},
		storage.Header{Key: "x-dlq-reason", Value: []byte(reason)},
		storage.Header{Key: "x-dlq-timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)

	s.EnsureTopic(dlqTopic, 1)

	rec, err := s.storage.Append(ctx, dlqTopic, 0, key, value, headers)
	if err != nil {
		return nil, fmt.Errorf("failed to append to DLQ topic %s: %w", dlqTopic, err)
	}

	rec.IsDLQ = true
	if s.eventBus != nil {
		s.eventBus.Publish(*rec)
	}

	return rec, nil
}

// Replay re-publishes a record into targetTopic with optional payload modification.
func (s *Service) Replay(ctx context.Context, targetTopic string, partition int32, key, value []byte, headers []storage.Header) (*storage.Record, error) {
	if headers == nil {
		headers = make([]storage.Header, 0, 1)
	}
	headers = append(headers, storage.Header{Key: "x-replayed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))})
	return s.Produce(ctx, targetTopic, partition, key, value, headers)
}

// GetTopicMetadata returns partition list, leader, and offset watermarks for a topic.
func (s *Service) GetTopicMetadata(ctx context.Context, topic string) (*TopicMetadata, error) {
	s.mu.RLock()
	partsCount, exists := s.topics[topic]
	s.mu.RUnlock()

	if !exists {
		return nil, storage.ErrTopicNotFound
	}

	meta := &TopicMetadata{
		Name:       topic,
		Partitions: make([]PartitionMetadata, partsCount),
	}

	for p := int32(0); p < partsCount; p++ {
		wm, err := s.storage.GetWatermarks(ctx, topic, p)
		var earliest, latest int64
		if err == nil {
			earliest = wm.Earliest
			latest = wm.Latest
		}
		meta.Partitions[p] = PartitionMetadata{
			ID:         p,
			Leader:     0, // PocketQueue is a single-broker local engine (Broker ID 0)
			Earliest:   earliest,
			Latest:     latest,
			TotalCount: latest - earliest,
		}
	}

	return meta, nil
}

// ListTopics returns all known topics and their partition counts.
func (s *Service) ListTopics(ctx context.Context) (map[string]int32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make(map[string]int32, len(s.topics))
	for t, p := range s.topics {
		res[t] = p
	}
	return res, nil
}

// Close shuts down the broker service and stops the event bus.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.eventBus != nil {
		s.eventBus.Close()
	}
	return s.storage.Close()
}

