package main

import (
	"flag"
	"log/slog"
	"time"
)

// Config holds runtime configuration options for PocketQueue.
type Config struct {
	Host        string
	Port        int
	RedisPort   int
	HTTPPort    int
	DLQWebhookURL string
	DataDir     string
	Headless    bool
	MaxMessages int
	LogLevel    string
	JSONLog     bool
	WithDemo    bool
	ExportPath  string

	// Retention & pruning options
	RetentionBytes int64
	RetentionAge   time.Duration

	// Connection draining options
	DrainTimeout time.Duration

	// Traffic recording & replay options
	RecordPath      string
	ReplayPath      string
	ReplaySpeed     float64
	PreserveOffsets bool

	// Chaos & Fault injection options
	ChaosLatency       int
	ChaosLatencyRate   float64
	ChaosDropRate      float64
	ChaosDuplicateRate float64

	// Subcommand mode
	Command      string
	ProduceTopic string
	ProduceKey   string
	ProduceValue string
	TargetBroker string
}

func parseFlags() *Config {
	cfg := &Config{}
	var kafkaPort int

	flag.StringVar(&cfg.Host, "host", "127.0.0.1", "Broker listening host")
	flag.IntVar(&cfg.Port, "port", 9092, "Kafka wire protocol listening port (0 to disable)")
	flag.IntVar(&kafkaPort, "kafka-port", 0, "Kafka wire protocol listening port (alias for -port)")
	flag.IntVar(&cfg.RedisPort, "redis-port", 6379, "Redis Streams protocol listening port (0 to disable)")
	flag.IntVar(&cfg.HTTPPort, "http-port", 0, "HTTP REST & SSE protocol bridge port (0 to disable)")
	flag.StringVar(&cfg.DLQWebhookURL, "dlq-webhook-url", "", "HTTP POST endpoint URL to forward Dead-Letter Queue (.dlq) messages")
	flag.StringVar(&cfg.DataDir, "data-dir", "", "Directory for append-only WAL persistence (empty for pure in-memory)")
	flag.BoolVar(&cfg.Headless, "headless", false, "Run in headless daemon mode (disable TUI)")
	flag.IntVar(&cfg.MaxMessages, "max-messages", 5000, "In-memory ring buffer capacity per partition")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.BoolVar(&cfg.JSONLog, "json-log", false, "Format logs as structured JSON")
	flag.BoolVar(&cfg.WithDemo, "demo", false, "Emit simulated live traffic for testing / demonstration")
	flag.StringVar(&cfg.ExportPath, "export", "", "Dump topic events to JSON file upon shutdown")

	flag.Int64Var(&cfg.RetentionBytes, "retention-bytes", 100*1024*1024, "WAL maximum retention bytes per partition before rolling FIFO prune (default: 104857600 / 100MB)")
	flag.DurationVar(&cfg.RetentionAge, "retention-age", 0, "WAL retention age duration (e.g. 24h, default 0 for disabled)")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", 500*time.Millisecond, "Grace period duration to drain in-flight connections on shutdown (default 500ms)")

	flag.StringVar(&cfg.RecordPath, "record", "", "File path to record live traffic session (.pqrec)")
	flag.StringVar(&cfg.ReplayPath, "replay", "", "File path of .pqrec session to replay")
	flag.Float64Var(&cfg.ReplaySpeed, "replay-speed", 1.0, "Replay speed multiplier (e.g. 0.5, 1.0, 2.0)")
	flag.BoolVar(&cfg.PreserveOffsets, "preserve-offsets", false, "Preserve consumer group offsets on replay (default resets to 0)")

	flag.IntVar(&cfg.ChaosLatency, "chaos-latency", 0, "Simulated network latency in milliseconds")
	flag.Float64Var(&cfg.ChaosLatencyRate, "chaos-latency-rate", 1.0, "Fraction of requests affected by latency (0.0 to 1.0)")
	flag.Float64Var(&cfg.ChaosDropRate, "chaos-drop-rate", 0.0, "Fraction of requests dropped to simulate packet loss (0.0 to 1.0)")
	flag.Float64Var(&cfg.ChaosDuplicateRate, "chaos-duplicate-rate", 0.0, "Fraction of produce requests duplicated (0.0 to 1.0)")

	flag.StringVar(&cfg.ProduceTopic, "topic", "", "Topic name for CLI producer")
	flag.StringVar(&cfg.ProduceKey, "key", "", "Message key for CLI producer")
	flag.StringVar(&cfg.ProduceValue, "value", "", "Message payload/value for CLI producer")
	flag.StringVar(&cfg.TargetBroker, "broker", "127.0.0.1:9092", "Target broker address for CLI producer")

	flag.Parse()

	if kafkaPort != 0 {
		cfg.Port = kafkaPort
	}

	return cfg
}

func parseSlogLevel(lvl string) slog.Level {
	switch lvl {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
