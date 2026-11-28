package http

import (
	"encoding/json"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// TopicSummary represents topic metadata and watermarks for the web dashboard.
type TopicSummary struct {
	Name               string             `json:"name"`
	Partitions         int32              `json:"partitions"`
	TotalMessages      int64              `json:"total_messages"`
	PartitionsMetadata []PartitionSummary `json:"partitions_metadata,omitempty"`
}

// PartitionSummary holds watermarks for a single partition.
type PartitionSummary struct {
	ID         int32 `json:"id"`
	Earliest   int64 `json:"earliest"`
	Latest     int64 `json:"latest"`
	TotalCount int64 `json:"total_count"`
}

// ListTopicsResponse wraps list of topics.
type ListTopicsResponse struct {
	Topics []TopicSummary `json:"topics"`
}

// BrokerStatsResponse wraps telemetry metrics for the dashboard.
type BrokerStatsResponse struct {
	Status        string `json:"status"`
	Version       string `json:"version"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	TotalMessages int64  `json:"total_messages"`
	TopicsCount   int    `json:"topics_count"`
	MemoryBytes   uint64 `json:"memory_bytes"`
	NumGoroutine  int    `json:"num_goroutine"`
	Timestamp     string `json:"timestamp"`
}

func (h *Handler) handleListTopics(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	ctx := r.Context()

	topicMap, err := h.broker.ListTopics(ctx)
	if err != nil {
		http.Error(w, "failed to list topics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	summaries := make([]TopicSummary, 0, len(topicMap))
	for name, parts := range topicMap {
		meta, err := h.broker.GetTopicMetadata(ctx, name)
		var totalMsgs int64
		var partSummaries []PartitionSummary
		if err == nil && meta != nil {
			partSummaries = make([]PartitionSummary, len(meta.Partitions))
			for i, p := range meta.Partitions {
				totalMsgs += p.TotalCount
				partSummaries[i] = PartitionSummary{
					ID:         p.ID,
					Earliest:   p.Earliest,
					Latest:     p.Latest,
					TotalCount: p.TotalCount,
				}
			}
		}

		summaries = append(summaries, TopicSummary{
			Name:               name,
			Partitions:         parts,
			TotalMessages:      totalMsgs,
			PartitionsMetadata: partSummaries,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ListTopicsResponse{Topics: summaries})
}

func (h *Handler) handleBrokerStats(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	ctx := r.Context()

	topicMap, _ := h.broker.ListTopics(ctx)
	var totalMsgs int64
	for name := range topicMap {
		if meta, err := h.broker.GetTopicMetadata(ctx, name); err == nil && meta != nil {
			for _, p := range meta.Partitions {
				totalMsgs += p.TotalCount
			}
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	uptime := int64(time.Since(h.startTime).Seconds())
	if uptime < 0 {
		uptime = 0
	}

	stats := BrokerStatsResponse{
		Status:        "UP",
		Version:       "0.9.0",
		UptimeSeconds: uptime,
		TotalMessages: totalMsgs,
		TopicsCount:   len(topicMap),
		MemoryBytes:   m.Alloc,
		NumGoroutine:  runtime.NumGoroutine(),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}

// DLQReplayRequest represents payload to replay a DLQ message.
type DLQReplayRequest struct {
	DLQTopic    string            `json:"dlq_topic"`    // e.g. "orders.dlq"
	TargetTopic string            `json:"target_topic"` // e.g. "orders" (inferred if omitted)
	Key         string            `json:"key"`
	Value       any               `json:"value"`
	Partition   int32             `json:"partition"`
	Headers     map[string]string `json:"headers,omitempty"`
}

// DLQReplayResponse returns confirmation of message replay.
type DLQReplayResponse struct {
	Status      string `json:"status"`
	SourceTopic string `json:"source_topic"`
	TargetTopic string `json:"target_topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Timestamp   string `json:"timestamp"`
}

func (h *Handler) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	const maxReplayBodySize = 16 * 1024 * 1024
	body, err := io.ReadAll(io.LimitReader(r.Body, maxReplayBodySize+1))
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(body) > maxReplayBodySize {
		http.Error(w, "payload too large (max 16 MB)", http.StatusRequestEntityTooLarge)
		return
	}

	var req DLQReplayRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	targetTopic := strings.TrimSpace(req.TargetTopic)
	dlqTopic := strings.TrimSpace(req.DLQTopic)

	if targetTopic == "" && dlqTopic != "" {
		if strings.HasSuffix(dlqTopic, ".dlq") {
			targetTopic = strings.TrimSuffix(dlqTopic, ".dlq")
		} else {
			targetTopic = dlqTopic
		}
	}

	if targetTopic == "" {
		http.Error(w, "target_topic or dlq_topic is required", http.StatusBadRequest)
		return
	}

	if req.Value == nil {
		http.Error(w, "value cannot be empty", http.StatusBadRequest)
		return
	}

	var valueBytes []byte
	switch val := req.Value.(type) {
	case string:
		valueBytes = []byte(val)
	case []byte:
		valueBytes = val
	default:
		raw, err := json.Marshal(val)
		if err != nil {
			http.Error(w, "failed to serialize value: "+err.Error(), http.StatusBadRequest)
			return
		}
		valueBytes = raw
	}

	headers := make([]storage.Header, 0, len(req.Headers)+1)
	if dlqTopic != "" {
		headers = append(headers, storage.Header{Key: "x-replayed-from", Value: []byte(dlqTopic)})
	}
	for k, v := range req.Headers {
		headers = append(headers, storage.Header{Key: k, Value: []byte(v)})
	}

	rec, err := h.broker.Replay(r.Context(), targetTopic, req.Partition, []byte(req.Key), valueBytes, headers)
	if err != nil {
		http.Error(w, "failed to replay message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(DLQReplayResponse{
		Status:      "replayed",
		SourceTopic: dlqTopic,
		TargetTopic: targetTopic,
		Partition:   rec.Partition,
		Offset:      rec.Offset,
		Timestamp:   rec.Timestamp.Format(time.RFC3339),
	})
}
