package main

import (
	"log/slog"
	"testing"
)

func TestParseSlogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"unknown", slog.LevelInfo},
	}

	for _, tc := range tests {
		lvl := parseSlogLevel(tc.input)
		if lvl != tc.expected {
			t.Errorf("parseSlogLevel(%q) = %v; want %v", tc.input, lvl, tc.expected)
		}
	}
}

func TestConfig_ChaosFields(t *testing.T) {
	cfg := &Config{
		ChaosLatency:       100,
		ChaosLatencyRate:   0.5,
		ChaosDropRate:      0.1,
		ChaosDuplicateRate: 0.05,
		RetentionBytes:     50 * 1024 * 1024,
		DrainTimeout:       500 * 1000 * 1000,
	}

	if cfg.ChaosLatency != 100 || cfg.ChaosLatencyRate != 0.5 || cfg.ChaosDropRate != 0.1 || cfg.ChaosDuplicateRate != 0.05 {
		t.Errorf("unexpected chaos config values: %+v", cfg)
	}
	if cfg.RetentionBytes != 50*1024*1024 {
		t.Errorf("unexpected retention bytes: %d", cfg.RetentionBytes)
	}
}
