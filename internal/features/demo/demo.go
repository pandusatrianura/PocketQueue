package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

type orderEvent struct {
	OrderID   string   `json:"order_id"`
	Customer  string   `json:"customer"`
	Amount    float64  `json:"amount"`
	Currency  string   `json:"currency"`
	Items     []string `json:"items"`
	Status    string   `json:"status"`
	CreatedAt string   `json:"created_at"`
}

type paymentFailedEvent struct {
	TxnID      string  `json:"transaction_id"`
	OrderID    string  `json:"order_id"`
	Amount     float64 `json:"amount"`
	ErrorCode  string  `json:"error_code"`
	Reason     string  `json:"reason"`
	RetryCount int     `json:"retry_count"`
	FailedAt   string  `json:"failed_at"`
}

type userSignupEvent struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	Tier      string `json:"tier"`
	Country   string `json:"country"`
	Timestamp string `json:"timestamp"`
}

// Generator produces simulated real-world domain events to demonstrate PocketQueue in real time.
type Generator struct {
	broker *broker.Service
	r      *rand.Rand
}

// NewGenerator creates a new demo event generator.
func NewGenerator(b *broker.Service) *Generator {
	return &Generator{
		broker: b,
		r:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Start begins producing realistic events at the specified interval until ctx is cancelled.
func (g *Generator) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 800 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	customers := []string{"alice@example.com", "bob@corp.io", "charlie@acme.dev", "diana@fintech.co"}
	items := []string{"MacBook Pro 16", "Mechanical Keyboard", "Dell 4K Monitor", "USB-C Hub", "Sony WH-1000XM5"}
	countries := []string{"US", "DE", "SG", "JP", "ID", "GB"}

	orderCounter := 1000

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			orderCounter++
			eventChoice := g.r.Intn(10)

			switch {
			case eventChoice < 5:
				// 50% chance: order.created
				orderID := fmt.Sprintf("ord-%d", orderCounter)
				cust := customers[g.r.Intn(len(customers))]
				item := items[g.r.Intn(len(items))]
				amt := float64(g.r.Intn(1500)+50) + 0.99

				payload, _ := json.Marshal(orderEvent{
					OrderID:   orderID,
					Customer:  cust,
					Amount:    amt,
					Currency:  "USD",
					Items:     []string{item},
					Status:    "confirmed",
					CreatedAt: time.Now().UTC().Format(time.RFC3339),
				})

				headers := []storage.Header{
					{Key: "event-type", Value: []byte("order.created")},
					{Key: "trace-id", Value: []byte(fmt.Sprintf("trace-%d", g.r.Int63()))},
				}
				_, _ = g.broker.Produce(ctx, "orders", 0, []byte(orderID), payload, headers)

			case eventChoice < 8:
				// 30% chance: user.registered
				uid := fmt.Sprintf("usr-%d", g.r.Intn(9000)+1000)
				payload, _ := json.Marshal(userSignupEvent{
					UserID:    uid,
					Username:  fmt.Sprintf("dev_%d", g.r.Intn(500)),
					Email:     customers[g.r.Intn(len(customers))],
					Tier:      "pro",
					Country:   countries[g.r.Intn(len(countries))],
					Timestamp: time.Now().UTC().Format(time.RFC3339),
				})
				headers := []storage.Header{
					{Key: "event-type", Value: []byte("user.registered")},
				}
				_, _ = g.broker.Produce(ctx, "users.auth", 0, []byte(uid), payload, headers)

			default:
				// 20% chance: payment.failed -> sent to DLQ
				txnID := fmt.Sprintf("txn-%d", g.r.Int63()%100000)
				orderID := fmt.Sprintf("ord-%d", orderCounter)
				payload, _ := json.Marshal(paymentFailedEvent{
					TxnID:      txnID,
					OrderID:    orderID,
					Amount:     float64(g.r.Intn(300) + 10),
					ErrorCode:  "CARD_DECLINED",
					Reason:     "Insufficient funds or processor timeout",
					RetryCount: 3,
					FailedAt:   time.Now().UTC().Format(time.RFC3339),
				})
				_, _ = g.broker.SendToDLQ(ctx, "payments", 0, []byte(txnID), payload, nil, "Processor Card Declined")
			}
		}
	}
}

