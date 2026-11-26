package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/decoder"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// ProduceRequest represents standard JSON payload for HTTP ingress.
type ProduceRequest struct {
	Key       string            `json:"key,omitempty"`
	Value     any               `json:"value"`
	Partition int32             `json:"partition"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// ProduceResponse represents confirmation of persistent message storage.
type ProduceResponse struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
}

// Handler routes HTTP ingress requests to the broker service.
type Handler struct {
	broker    *broker.Service
	eventBus  *broker.EventBus
	logger    *slog.Logger
	startTime time.Time
	decoder   decoder.PayloadDecoder
}

// NewHandler instantiates an HTTP Handler.
func NewHandler(svc *broker.Service, bus *broker.EventBus, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		broker:    svc,
		eventBus:  bus,
		logger:    logger,
		startTime: time.Now().UTC(),
		decoder:   decoder.NewUniversalDecoder(nil),
	}
}

// SetDecoder configures the payload decoder used for SSE stream and inspections.
func (h *Handler) SetDecoder(d decoder.PayloadDecoder) {
	if d != nil {
		h.decoder = d
	}
}

// Decoder returns the active payload decoder.
func (h *Handler) Decoder() decoder.PayloadDecoder {
	return h.decoder
}

// RegisterRoutes binds HTTP REST, SSE, and Web UI endpoints to the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.handleHealthz)
	mux.HandleFunc("OPTIONS /", h.handleCORS)
	mux.HandleFunc("GET /{$}", h.handleServeUI)
	mux.HandleFunc("GET /ui", h.handleServeUI)
	mux.HandleFunc("GET /static/{file}", h.handleServeStatic)
	mux.HandleFunc("GET /api/v1/topics", h.handleListTopics)
	mux.HandleFunc("GET /api/v1/stats", h.handleBrokerStats)
	mux.HandleFunc("POST /api/v1/dlq/replay", h.handleDLQReplay)
	mux.HandleFunc("POST /api/v1/topics/{topic}", h.handleProduce)
	mux.HandleFunc("GET /api/v1/topics/{topic}/sse", h.handleSSEStream)
}

func (h *Handler) handleHealthz(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":    "UP",
		"version":   "0.7.0",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *Handler) handleCORS(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
}

func (h *Handler) handleProduce(w http.ResponseWriter, r *http.Request) {
	h.setCORS(w)
	topic := r.PathValue("topic")
	if strings.TrimSpace(topic) == "" {
		http.Error(w, "topic name cannot be empty", http.StatusBadRequest)
		return
	}

	const maxProduceBodySize = 16 * 1024 * 1024
	body, err := io.ReadAll(io.LimitReader(r.Body, maxProduceBodySize+1)) // 16 MB limit + 1 byte guard
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(body) > maxProduceBodySize {
		http.Error(w, "payload too large (max 16 MB)", http.StatusRequestEntityTooLarge)
		return
	}

	if len(body) == 0 {
		http.Error(w, "body cannot be empty", http.StatusBadRequest)
		return
	}

	var keyBytes []byte
	var valueBytes []byte
	var partition int32 = 0
	var headers []storage.Header

	// Check if JSON payload
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		var structured ProduceRequest
		if err := json.Unmarshal(body, &structured); err == nil && structured.Value != nil {
			if structured.Key != "" {
				keyBytes = []byte(structured.Key)
			}
			partition = structured.Partition

			// If structured value is string, use as bytes; otherwise marshal back to JSON bytes
			switch v := structured.Value.(type) {
			case string:
				valueBytes = []byte(v)
			default:
				valueBytes, _ = json.Marshal(v)
			}

			if len(structured.Headers) > 0 {
				headers = make([]storage.Header, 0, len(structured.Headers))
				for k, val := range structured.Headers {
					headers = append(headers, storage.Header{Key: k, Value: []byte(val)})
				}
			}
		} else {
			// Validate that body is valid JSON syntax
			var rawJSON any
			if err := json.Unmarshal(body, &rawJSON); err != nil {
				http.Error(w, "invalid json payload: "+err.Error(), http.StatusBadRequest)
				return
			}
			valueBytes = body
		}
	} else {
		// Raw payload body
		valueBytes = body
	}

	// Produce to broker
	rec, err := h.broker.Produce(r.Context(), topic, partition, keyBytes, valueBytes, headers)
	if err != nil {
		if errors.Is(err, broker.ErrPartitionLimitExceeded) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, fmt.Sprintf("produce failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(ProduceResponse{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Timestamp: rec.Timestamp.Format(time.RFC3339),
	})
}

func (h *Handler) handleSSEStream(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if strings.TrimSpace(topic) == "" {
		http.Error(w, "topic name cannot be empty", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	if h.eventBus == nil {
		http.Error(w, "event streaming unavailable", http.StatusServiceUnavailable)
		return
	}

	h.setCORS(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	subCh, unsub := h.eventBus.Subscribe()
	defer unsub()

	// Initial handshake flush
	flusher.Flush()

	h.logger.DebugContext(r.Context(), "http sse client connected", slog.String("topic", topic))

	for {
		select {
		case <-r.Context().Done():
			h.logger.DebugContext(r.Context(), "http sse client disconnected", slog.String("topic", topic))
			return
		case rec, ok := <-subCh:
			if !ok {
				return
			}
			if topic == "*" || topic == "_all" || rec.Topic == topic {
				eventData := map[string]any{
					"topic":     rec.Topic,
					"partition": rec.Partition,
					"offset":    rec.Offset,
					"key":       string(rec.Key),
					"value":     string(rec.Value),
					"timestamp": rec.Timestamp.Format(time.RFC3339),
				}
				if h.decoder != nil {
					dec := h.decoder.Decode(rec.Value)
					eventData["decoded"] = map[string]any{
						"kind":         dec.Kind,
						"schema_id":    dec.SchemaID,
						"message_name": dec.MessageName,
						"formatted":    dec.Formatted,
						"is_binary":    dec.IsBinary,
						"raw_bytes":    dec.RawBytes,
					}
				}

				payload, err := json.Marshal(eventData)
				if err == nil {
					fmt.Fprintf(w, "event: message\ndata: %s\n\n", payload)
					flusher.Flush()
				}
			}
		}
	}
}
