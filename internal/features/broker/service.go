package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// MaxPartitionsPerTopic limits dynamic partition allocation per topic to prevent
// OS file descriptor exhaustion from runaway client loops.
const MaxPartitionsPerTopic int32 = 16

// ErrPartitionLimitExceeded indicates requested partition exceeds the maximum allowable limit.
var (
	ErrPartitionLimitExceeded = errors.New("partition limit exceeded")
	ErrTopicAlreadyExists     = errors.New("topic already exists")
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

	mu           sync.RWMutex
	topics       map[string]int32                      // topicName -> partitionCount
	groupOffsets map[string]map[string]map[int32]int64 // groupID -> topic -> partition -> offset
	offsetStore  OffsetStore                           // optional consumer group offset persistence
	chaos        *ChaosEngine                          // optional chaos / fault injection engine
	closed       bool
}

// NewService instantiates a BrokerService.
func NewService(engine storage.Engine, eventBus *EventBus, defaultPartitions int32) *Service {
	if defaultPartitions <= 0 {
		defaultPartitions = 1
	}
	svc := &Service{
		storage:           engine,
		eventBus:          eventBus,
		defaultPartitions: defaultPartitions,
		topics:            make(map[string]int32),
		groupOffsets:      make(map[string]map[string]map[int32]int64),
	}

	// Synchronize any pre-existing topics from persistent storage
	if engine != nil {
		if existing, err := engine.ListTopics(context.Background()); err == nil && existing != nil {
			for t, p := range existing {
				svc.topics[t] = p
			}
		}
	}

	return svc
}

// SetChaos sets the chaos / fault injection engine.
func (s *Service) SetChaos(c *ChaosEngine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chaos = c
}

// Chaos returns the active chaos engine if configured.
func (s *Service) Chaos() *ChaosEngine {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.chaos
}

// SetOffsetStore configures the persistent offset store and restores any saved offsets.
func (s *Service) SetOffsetStore(store OffsetStore) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsetStore = store
	if store != nil {
		loaded, err := store.Load()
		if err != nil {
			return err
		}
		if loaded != nil {
			for grp, topics := range loaded {
				if _, exists := s.groupOffsets[grp]; !exists {
					s.groupOffsets[grp] = make(map[string]map[int32]int64)
				}
				for t, parts := range topics {
					if _, exists := s.groupOffsets[grp][t]; !exists {
						s.groupOffsets[grp][t] = make(map[int32]int64)
					}
					for p, off := range parts {
						s.groupOffsets[grp][t][p] = off
					}
				}
			}
		}
	}
	return nil
}

// OffsetStore returns the active offset store if configured.
func (s *Service) OffsetStore() OffsetStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offsetStore
}

// EventBus returns the active EventBus instance for real-time pub/sub notifications.
func (s *Service) EventBus() *EventBus {
	return s.eventBus
}

// EnsureTopic guarantees a topic exists with at least the requested number of partitions.
func (s *Service) EnsureTopic(topic string, partitions int32) int32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if partitions <= 0 {
		partitions = s.defaultPartitions
	}
	if partitions > MaxPartitionsPerTopic {
		partitions = MaxPartitionsPerTopic
	}

	cur, exists := s.topics[topic]
	if !exists {
		s.topics[topic] = partitions
		return partitions
	}

	if partitions > cur {
		s.topics[topic] = partitions
		return partitions
	}

	return cur
}

// CreateTopic explicitly registers a new topic with requested partition count.
func (s *Service) CreateTopic(topic string, partitions int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[topic]; exists {
		return ErrTopicAlreadyExists
	}
	if partitions <= 0 {
		partitions = s.defaultPartitions
	}
	if partitions > MaxPartitionsPerTopic {
		return ErrPartitionLimitExceeded
	}
	s.topics[topic] = partitions
	return nil
}

// DeleteTopic removes a topic from the broker registry.
func (s *Service) DeleteTopic(topic string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[topic]; !exists {
		return false
	}
	delete(s.topics, topic)
	return true
}

// Produce appends a record to the specified topic partition and broadcasts it to the EventBus.
func (s *Service) Produce(ctx context.Context, topic string, partition int32, key, value []byte, headers []storage.Header) (*storage.Record, error) {
	if partition < 0 {
		return nil, fmt.Errorf("invalid partition %d: partition must be non-negative", partition)
	}
	if partition >= MaxPartitionsPerTopic {
		return nil, fmt.Errorf("%w: partition %d exceeds maximum limit of %d", ErrPartitionLimitExceeded, partition, MaxPartitionsPerTopic)
	}

	if c := s.Chaos(); c != nil && c.Enabled() {
		shouldDrop, shouldDuplicate, delay := c.EvaluateProduce(ctx)
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		if shouldDrop {
			return nil, ErrChaosDrop
		}
		if shouldDuplicate {
			s.EnsureTopic(topic, partition+1)
			rec1, err1 := s.storage.Append(ctx, topic, partition, key, value, headers)
			if err1 != nil {
				return nil, fmt.Errorf("storage append failed for topic %s: %w", topic, err1)
			}
			if s.eventBus != nil && (ctx == nil || ctx.Err() == nil) {
				s.eventBus.Publish(*rec1)
			}
			rec2, err2 := s.storage.Append(ctx, topic, partition, key, value, headers)
			if err2 != nil {
				return rec1, nil
			}
			if s.eventBus != nil && (ctx == nil || ctx.Err() == nil) {
				s.eventBus.Publish(*rec2)
			}
			return rec2, nil
		}
	}

	s.EnsureTopic(topic, partition+1)

	rec, err := s.storage.Append(ctx, topic, partition, key, value, headers)
	if err != nil {
		return nil, fmt.Errorf("storage append failed for topic %s: %w", topic, err)
	}

	if s.eventBus != nil && (ctx == nil || ctx.Err() == nil) {
		s.eventBus.Publish(*rec)
	}

	return rec, nil
}

// Fetch retrieves records starting from startOffset up to maxRecords.
func (s *Service) Fetch(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int) ([]storage.Record, error) {
	if partition < 0 {
		return nil, fmt.Errorf("invalid partition %d: partition must be non-negative", partition)
	}
	if partition >= MaxPartitionsPerTopic {
		return nil, fmt.Errorf("%w: partition %d exceeds maximum limit of %d", ErrPartitionLimitExceeded, partition, MaxPartitionsPerTopic)
	}

	if c := s.Chaos(); c != nil && c.Enabled() {
		shouldDrop, delay := c.EvaluateFetch(ctx)
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		if shouldDrop {
			return nil, ErrChaosDrop
		}
	}

	return s.storage.ReadFrom(ctx, topic, partition, startOffset, maxRecords)
}

// CommitOffset updates the committed offset for a consumer group.
func (s *Service) CommitOffset(ctx context.Context, groupID, topic string, partition int32, offset int64) error {
	if partition < 0 {
		return fmt.Errorf("invalid partition %d: partition must be non-negative", partition)
	}
	if partition >= MaxPartitionsPerTopic {
		return fmt.Errorf("%w: partition %d exceeds maximum limit of %d", ErrPartitionLimitExceeded, partition, MaxPartitionsPerTopic)
	}

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
	if s.offsetStore != nil {
		_ = s.offsetStore.Save(s.groupOffsets)
	}
	return nil
}

// GetOffset retrieves the committed offset for a consumer group. Returns -1 if not committed.
func (s *Service) GetOffset(ctx context.Context, groupID, topic string, partition int32) (int64, error) {
	if partition < 0 {
		return -1, fmt.Errorf("invalid partition %d: partition must be non-negative", partition)
	}
	if partition >= MaxPartitionsPerTopic {
		return -1, fmt.Errorf("%w: partition %d exceeds maximum limit of %d", ErrPartitionLimitExceeded, partition, MaxPartitionsPerTopic)
	}

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

// ResetAllOffsets resets committed consumer group offsets to 0 for a given topic across all consumer groups.
// If topic is empty, it resets offsets across all topics.
func (s *Service) ResetAllOffsets(ctx context.Context, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topicMap := range s.groupOffsets {
		if topic == "" {
			for _, partMap := range topicMap {
				for p := range partMap {
					partMap[p] = 0
				}
			}
		} else if partMap, exists := topicMap[topic]; exists {
			for p := range partMap {
				partMap[p] = 0
			}
		}
	}
	if s.offsetStore != nil {
		_ = s.offsetStore.Save(s.groupOffsets)
	}
	return nil
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
	if s.eventBus != nil && (ctx == nil || ctx.Err() == nil) {
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

	if s.offsetStore != nil {
		_ = s.offsetStore.Close()
	}

	if s.eventBus != nil {
		s.eventBus.Close()
	}
	if s.storage != nil {
		_ = s.storage.Sync()
		return s.storage.Close()
	}
	return nil
}
