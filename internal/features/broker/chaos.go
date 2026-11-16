package broker

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// ErrChaosDrop is returned when chaos engine simulates a network drop or timeout.
var ErrChaosDrop = errors.New("chaos: simulated network packet drop")

// ChaosConfig holds probabilistic fault injection parameters.
type ChaosConfig struct {
	LatencyMs     int     // Added delay in milliseconds
	LatencyRate   float64 // 0.0 to 1.0 (fraction of requests affected by latency)
	DropRate      float64 // 0.0 to 1.0 (fraction of produce/fetch dropped)
	DuplicateRate float64 // 0.0 to 1.0 (fraction of produce duplicated)
}

// ChaosEngine manages active chaos rules and probabilistic evaluations.
type ChaosEngine struct {
	mu     sync.RWMutex
	cfg    ChaosConfig
	rnd    *rand.Rand
	active bool
}

// NewChaosEngine instantiates a ChaosEngine with given configuration.
func NewChaosEngine(cfg ChaosConfig) *ChaosEngine {
	e := &ChaosEngine{
		cfg:    cfg,
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
		active: (cfg.LatencyMs > 0 && cfg.LatencyRate > 0) || cfg.DropRate > 0 || cfg.DuplicateRate > 0,
	}
	return e
}

// Enabled returns true if any chaos rule is configured and active.
func (e *ChaosEngine) Enabled() bool {
	if e == nil {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.active
}

// UpdateConfig updates the active chaos rules at runtime.
func (e *ChaosEngine) UpdateConfig(cfg ChaosConfig) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg
	e.active = (cfg.LatencyMs > 0 && cfg.LatencyRate > 0) || cfg.DropRate > 0 || cfg.DuplicateRate > 0
}

// Config returns a copy of current chaos configuration.
func (e *ChaosEngine) Config() ChaosConfig {
	if e == nil {
		return ChaosConfig{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cfg
}

// EvaluateProduce evaluates chaos decisions for an incoming Produce call.
func (e *ChaosEngine) EvaluateProduce(ctx context.Context) (shouldDrop bool, shouldDuplicate bool, delay time.Duration) {
	if e == nil || !e.Enabled() {
		return false, false, 0
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Check drop
	if e.cfg.DropRate > 0 && e.rnd.Float64() < e.cfg.DropRate {
		shouldDrop = true
		return
	}

	// Check duplicate
	if e.cfg.DuplicateRate > 0 && e.rnd.Float64() < e.cfg.DuplicateRate {
		shouldDuplicate = true
	}

	// Check latency
	if e.cfg.LatencyMs > 0 && e.cfg.LatencyRate > 0 && e.rnd.Float64() < e.cfg.LatencyRate {
		delay = time.Duration(e.cfg.LatencyMs) * time.Millisecond
	}

	return
}

// EvaluateFetch evaluates chaos decisions for an incoming Fetch call.
func (e *ChaosEngine) EvaluateFetch(ctx context.Context) (shouldDrop bool, delay time.Duration) {
	if e == nil || !e.Enabled() {
		return false, 0
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.cfg.DropRate > 0 && e.rnd.Float64() < e.cfg.DropRate {
		shouldDrop = true
		return
	}

	if e.cfg.LatencyMs > 0 && e.cfg.LatencyRate > 0 && e.rnd.Float64() < e.cfg.LatencyRate {
		delay = time.Duration(e.cfg.LatencyMs) * time.Millisecond
	}

	return
}
