package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// TrafficReplayer reads a .pqrec file and reproduces the recorded event stream into broker.Service.
type TrafficReplayer struct {
	filePath     string
	brokerSvc    *Service
	speed        float64
	resetOffsets bool
}

// NewTrafficReplayer creates a new replayer instance.
func NewTrafficReplayer(filePath string, brokerSvc *Service, speed float64, resetOffsets bool) *TrafficReplayer {
	if speed <= 0 {
		speed = 1.0
	}
	return &TrafficReplayer{
		filePath:     filePath,
		brokerSvc:    brokerSvc,
		speed:        speed,
		resetOffsets: resetOffsets,
	}
}

// Replay reads the file and produces each event into the broker, respecting relative time deltas.
func (r *TrafficReplayer) Replay(ctx context.Context) (int, error) {
	f, err := os.Open(r.filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open replay file %s: %w", r.filePath, err)
	}
	defer f.Close()

	if r.resetOffsets {
		_ = r.brokerSvc.ResetAllOffsets(ctx, "")
	}

	scanner := bufio.NewScanner(f)
	replayed := 0
	var prevTimestamp int64 = 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return replayed, ctx.Err()
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var item RecordItem
		if err := json.Unmarshal(line, &item); err != nil {
			// Skip malformed/corrupted trailing lines gracefully
			continue
		}

		// Calculate sleep delta
		if prevTimestamp > 0 && item.Timestamp > prevTimestamp && r.speed > 0 {
			deltaMs := float64(item.Timestamp-prevTimestamp) / r.speed
			if deltaMs > 0 {
				sleepDuration := time.Duration(deltaMs) * time.Millisecond
				// Cap max sleep to 5s to avoid freezing on sparse logs
				if sleepDuration > 5*time.Second {
					sleepDuration = 5 * time.Second
				}
				select {
				case <-ctx.Done():
					return replayed, ctx.Err()
				case <-time.After(sleepDuration):
				}
			}
		}
		prevTimestamp = item.Timestamp

		var headers []storage.Header
		if len(item.Headers) > 0 {
			for k, v := range item.Headers {
				headers = append(headers, storage.Header{
					Key:   k,
					Value: []byte(v),
				})
			}
		}

		_, err := r.brokerSvc.Produce(ctx, item.Topic, item.Partition, []byte(item.Key), []byte(item.Payload), headers)
		if err != nil {
			return replayed, fmt.Errorf("replay produce failed on record %d: %w", replayed, err)
		}

		replayed++
	}

	if err := scanner.Err(); err != nil {
		return replayed, fmt.Errorf("scanner error during replay: %w", err)
	}

	return replayed, nil
}
