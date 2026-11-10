package broker

import (
	"sync"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// Subscriber represents a consumer of real-time broker events.
type Subscriber chan storage.Record

// EventBus provides a non-blocking broadcast bus for broker events (e.g. for TUI).
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[Subscriber]struct{}
	bufSize     int
	closed      bool
}

// NewEventBus creates a new EventBus with the specified channel buffer size per subscriber.
func NewEventBus(bufSize int) *EventBus {
	if bufSize <= 0 {
		bufSize = 1000
	}
	return &EventBus{
		subscribers: make(map[Subscriber]struct{}),
		bufSize:     bufSize,
	}
}

// Subscribe returns a channel that receives real-time records and an unsubscribe function.
func (eb *EventBus) Subscribe() (<-chan storage.Record, func()) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	ch := make(Subscriber, eb.bufSize)
	if !eb.closed {
		eb.subscribers[ch] = struct{}{}
	}

	unsubscribe := func() {
		eb.mu.Lock()
		defer eb.mu.Unlock()
		if _, ok := eb.subscribers[ch]; ok {
			delete(eb.subscribers, ch)
			close(ch)
		}
	}

	return ch, unsubscribe
}

// Publish broadcasts a record to all active subscribers. Non-blocking: drops message if buffer is full.
func (eb *EventBus) Publish(rec storage.Record) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if eb.closed {
		return
	}

	for ch := range eb.subscribers {
		select {
		case ch <- rec:
		default:
			// Non-blocking drop to prevent slow UI subscribers from stalling high-throughput producers
		}
	}
}

// Close shuts down the EventBus and closes all subscriber channels.
func (eb *EventBus) Close() {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.closed {
		return
	}
	eb.closed = true

	for ch := range eb.subscribers {
		close(ch)
	}
	eb.subscribers = nil
}

