package main

import (
	"flag"
	"log/slog"
)

// Config holds runtime configuration options for PocketQueue.
type Config struct {
	Host        string
	Port        int
	RedisPort   int
	DataDir     string
	Headless    bool
	MaxMessages int
	LogLevel    string
	JSONLog     bool
	WithDemo    bool
	ExportPath  string

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
	flag.StringVar(&cfg.DataDir, "data-dir", "", "Directory for append-only WAL persistence (empty for pure in-memory)")
	flag.BoolVar(&cfg.Headless, "headless", false, "Run in headless daemon mode (disable TUI)")
	flag.IntVar(&cfg.MaxMessages, "max-messages", 5000, "In-memory ring buffer capacity per partition")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.BoolVar(&cfg.JSONLog, "json-log", false, "Format logs as structured JSON")
	flag.BoolVar(&cfg.WithDemo, "demo", false, "Emit simulated live traffic for testing / demonstration")
	flag.StringVar(&cfg.ExportPath, "export", "", "Dump topic events to JSON file upon shutdown")

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
