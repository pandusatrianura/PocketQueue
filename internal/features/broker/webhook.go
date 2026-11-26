package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

const (
	defaultWebhookTimeout = 3 * time.Second
	maxWebhookRetries     = 2
	webhookQueueCapacity  = 1000
)

// WebhookDLQDispatcher routes dead-letter messages to an external HTTP webhook endpoint.
type WebhookDLQDispatcher struct {
	targetURL string
	bus       *EventBus
	client    *http.Client
	logger    *slog.Logger
	queue     chan storage.Record

	ctx       context.Context
	cancel    context.CancelFunc
	unsub     func()
	ingressWg sync.WaitGroup
	workerWg  sync.WaitGroup
	closeOnce sync.Once
}

// NewWebhookDLQDispatcher creates a new dispatcher instance.
func NewWebhookDLQDispatcher(targetURL string, bus *EventBus, client *http.Client, logger *slog.Logger) *WebhookDLQDispatcher {
	if client == nil {
		client = &http.Client{Timeout: defaultWebhookTimeout}
	}
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WebhookDLQDispatcher{
		targetURL: targetURL,
		bus:       bus,
		client:    client,
		logger:    logger,
		queue:     make(chan storage.Record, webhookQueueCapacity),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start initiates the listener and background dispatch workers.
func (d *WebhookDLQDispatcher) Start() {
	if d.bus == nil || d.targetURL == "" {
		return
	}

	subCh, unsub := d.bus.Subscribe()
	d.unsub = unsub

	// Ingress goroutine: listens to EventBus and buffers DLQ records
	d.ingressWg.Add(1)
	go func() {
		defer d.ingressWg.Done()
		for rec := range subCh {
			if rec.IsDLQ || strings.HasSuffix(rec.Topic, ".dlq") {
				select {
				case d.queue <- rec:
				default:
					d.logger.Warn("webhook dlq queue full, dropping message",
						slog.String("topic", rec.Topic),
						slog.Int64("offset", rec.Offset),
					)
				}
			}
		}
	}()

	// Worker goroutines
	numWorkers := 2
	for i := 0; i < numWorkers; i++ {
		d.workerWg.Add(1)
		go func(workerID int) {
			defer d.workerWg.Done()
			for rec := range d.queue {
				d.dispatch(rec)
			}
		}(i)
	}

	d.logger.Info("webhook dlq dispatcher started", slog.String("target_url", d.targetURL))
}

func (d *WebhookDLQDispatcher) dispatch(rec storage.Record) {
	start := time.Now()
	var origTopic, reason string
	headerMap := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		headerMap[h.Key] = string(h.Value)
		if h.Key == "x-dlq-original-topic" {
			origTopic = string(h.Value)
		} else if h.Key == "x-dlq-reason" {
			reason = string(h.Value)
		}
	}

	payload := map[string]any{
		"topic":     rec.Topic,
		"partition": rec.Partition,
		"offset":    rec.Offset,
		"key":       string(rec.Key),
		"value":     string(rec.Value),
		"timestamp": rec.Timestamp.Format(time.RFC3339),
		"is_dlq":    true,
		"headers":   headerMap,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		d.logger.Error("failed to marshal dlq webhook payload", slog.Any("error", err))
		return
	}

	attempts := 0
	for attempts <= maxWebhookRetries {
		attempts++

		reqCtx, cancel := context.WithTimeout(context.Background(), defaultWebhookTimeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, d.targetURL, bytes.NewReader(bodyBytes))
		if err != nil {
			cancel()
			d.logger.Error("failed to build webhook request", slog.Any("error", err))
			return
		}

		req.Header.Set("Content-Type", "application/json")
		if origTopic != "" {
			req.Header.Set("x-dlq-original-topic", origTopic)
		}
		if reason != "" {
			req.Header.Set("x-dlq-reason", reason)
		}

		resp, err := d.client.Do(req)
		cancel()

		if err == nil && resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				d.logger.Info("webhook dlq dispatched successfully",
					slog.String("target_url", d.targetURL),
					slog.String("topic", rec.Topic),
					slog.Int64("offset", rec.Offset),
					slog.Int("attempts", attempts),
					slog.Duration("duration_ms", time.Since(start)),
				)
				return
			}
		}

		if attempts <= maxWebhookRetries {
			time.Sleep(time.Duration(attempts*50) * time.Millisecond)
		}
	}

	d.logger.Warn("webhook dlq dispatch failed after all retries",
		slog.String("target_url", d.targetURL),
		slog.String("topic", rec.Topic),
		slog.Int64("offset", rec.Offset),
		slog.Int("attempts", attempts),
	)
}

// Close gracefully stops the dispatcher and flushes active messages.
func (d *WebhookDLQDispatcher) Close() error {
	d.closeOnce.Do(func() {
		// 1. Unsubscribe from EventBus (closes subCh)
		if d.unsub != nil {
			d.unsub()
		}
		// 2. Wait for ingress goroutine to finish draining subCh into queue
		d.ingressWg.Wait()

		// 3. Close queue so workers process remaining buffered items and terminate
		close(d.queue)

		// 4. Wait for all workers to finish draining
		d.workerWg.Wait()
		d.cancel()
		d.logger.Info("webhook dlq dispatcher closed")
	})
	return nil
}
