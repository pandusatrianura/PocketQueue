package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/decoder"
	"github.com/pandusatrianura/pocketqueue/internal/features/demo"
	protocolhttp "github.com/pandusatrianura/pocketqueue/internal/features/protocol/http"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/mqtt"
	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/redis"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
	"github.com/pandusatrianura/pocketqueue/internal/features/tui"
	"github.com/pandusatrianura/pocketqueue/internal/platform/logger"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "version" || os.Args[1] == "--version" || os.Args[1] == "-v") {
		fmt.Println("PocketQueue v0.10.0")
		os.Exit(0)
	}

	if len(os.Args) > 1 && os.Args[1] == "ping" {
		if err := runPingCommand(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "❌ Healthcheck Failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	if len(os.Args) > 1 && os.Args[1] == "test" {
		if err := runTestCommand(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "❌ Assertion Failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	bootStart := time.Now()
	cfg := parseFlags()

	// If user passed -topic, run in CLI producer mode
	if cfg.ProduceTopic != "" {
		if cfg.ProduceValue == "" {
			fmt.Fprintln(os.Stderr, "Error: -value is required when publishing to -topic")
			os.Exit(1)
		}
		if err := PublishCLI(cfg.TargetBroker, cfg.ProduceTopic, cfg.ProduceKey, cfg.ProduceValue); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Initialize Logger
	slogLevel := parseSlogLevel(cfg.LogLevel)
	isTerminal := isatty.IsTerminal(os.Stdout.Fd()) && !cfg.Headless

	if isTerminal {
		// In TUI mode, route logs to pocketqueue.log to avoid corrupting terminal rendering
		logFile, err := os.OpenFile("pocketqueue.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			defer logFile.Close()
			logger.Setup(logFile, slogLevel, cfg.JSONLog)
		} else {
			logger.Setup(os.Stderr, slogLevel, cfg.JSONLog)
		}
	} else {
		logger.Setup(os.Stdout, slogLevel, cfg.JSONLog)
	}

	// Root context for lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Storage Engine (WAL vs In-Memory)
	var engine storage.Engine
	var err error
	if cfg.DataDir != "" {
		engine, err = storage.NewWALEngineWithLimit(cfg.DataDir, cfg.RetentionBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing WAL storage in %s: %v\n", cfg.DataDir, err)
			os.Exit(1)
		}
	} else {
		engine = storage.NewMemoryEngine(cfg.MaxMessages)
	}

	bus := broker.NewEventBus(1000)
	brokerSvc := broker.NewService(engine, bus, 1)
	defer brokerSvc.Close()

	if cfg.DataDir != "" {
		offsetStore, err := broker.NewFileOffsetStore(cfg.DataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing offset store in %s: %v\n", cfg.DataDir, err)
			os.Exit(1)
		}
		if err := brokerSvc.SetOffsetStore(offsetStore); err != nil {
			fmt.Fprintf(os.Stderr, "Error restoring offsets from %s: %v\n", cfg.DataDir, err)
		}
	}

	// If chaos flags are set, configure chaos engine
	if cfg.ChaosLatency > 0 || cfg.ChaosDropRate > 0 || cfg.ChaosDuplicateRate > 0 {
		chaosCfg := broker.ChaosConfig{
			LatencyMs:     cfg.ChaosLatency,
			LatencyRate:   cfg.ChaosLatencyRate,
			DropRate:      cfg.ChaosDropRate,
			DuplicateRate: cfg.ChaosDuplicateRate,
		}
		brokerSvc.SetChaos(broker.NewChaosEngine(chaosCfg))
	}

	// If --record is specified, record all live traffic to .pqrec file
	var recorder *broker.TrafficRecorder
	if cfg.RecordPath != "" {
		var recErr error
		recorder, recErr = broker.NewTrafficRecorder(cfg.RecordPath)
		if recErr != nil {
			fmt.Fprintf(os.Stderr, "Error initializing traffic recorder: %v\n", recErr)
			os.Exit(1)
		}
		defer recorder.Close()

		subCh, unsub := bus.Subscribe()
		defer unsub()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case rec, ok := <-subCh:
					if !ok {
						return
					}
					_ = recorder.Record(rec)
				}
			}
		}()
	}

	// If --replay is specified, reproduce traffic session into broker
	if cfg.ReplayPath != "" {
		go func() {
			time.Sleep(100 * time.Millisecond) // brief pause for servers to bind
			replayer := broker.NewTrafficReplayer(cfg.ReplayPath, brokerSvc, cfg.ReplaySpeed, !cfg.PreserveOffsets)
			count, err := replayer.Replay(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "Warning: traffic replay encountered error: %v (replayed %d msgs)\n", err, count)
			}
		}()
	}

	// If --demo is requested, start background demo event generator
	if cfg.WithDemo {
		gen := demo.NewGenerator(brokerSvc)
		go gen.Start(ctx, 800*time.Millisecond)
	}

	// 1. Initialize Universal Payload Decoder
	reg := decoder.NewSchemaRegistry()
	if cfg.ProtoDescPath != "" {
		if err := reg.LoadDescriptorSet(cfg.ProtoDescPath); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to load protobuf descriptor set from %s: %v\n", cfg.ProtoDescPath, err)
		} else {
			descs := reg.Descriptors()
			slog.Info("loaded protobuf descriptor set", "path", cfg.ProtoDescPath, "messages_count", len(descs))
		}
	}
	payloadDecoder := decoder.NewUniversalDecoder(reg)

	// 2. Start Kafka Protocol Server (port 9092)
	var kafkaServer *kafka.Server
	if cfg.Port > 0 {
		listenAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
		handler := kafka.NewHandler(brokerSvc, cfg.Host, int32(cfg.Port), nil)
		kafkaServer = kafka.NewServer(listenAddr, handler, nil)
		if err := kafkaServer.Start(); err != nil {
			if errors.Is(err, kafka.ErrPortInUse) {
				fmt.Fprintf(os.Stderr, "\n❌ Kafka port %d is already in use by another process (e.g. Docker Kafka/local service).\n   👉 Use --port <alt> to specify a different port (e.g. --port 9094)\n\n", cfg.Port)
			} else {
				fmt.Fprintf(os.Stderr, "Error starting Kafka protocol server: %v\n", err)
			}
			os.Exit(1)
		}
		defer kafkaServer.Close()
	}

	// 3. Start Redis Streams Protocol Server (port 6379)
	var redisServer *redis.Server
	if cfg.RedisPort > 0 {
		redisAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort)
		redisServer = redis.NewServer(redisAddr, brokerSvc, nil)
		if err := redisServer.Start(); err != nil {
			if errors.Is(err, redis.ErrPortInUse) {
				fmt.Fprintf(os.Stderr, "\n❌ Redis port %d is already in use by another process (e.g. Docker Redis/local service).\n   👉 Use --redis-port <alt> to specify a different port (e.g. --redis-port 6380)\n\n", cfg.RedisPort)
			} else {
				fmt.Fprintf(os.Stderr, "Error starting Redis Streams protocol server: %v\n", err)
			}
			os.Exit(1)
		}
		defer redisServer.Close()
	}

	// 4. Start HTTP REST & SSE Protocol Server (port 8080)
	var httpServer *protocolhttp.Server
	if cfg.HTTPPort > 0 {
		httpAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.HTTPPort)
		httpServer = protocolhttp.NewServer(httpAddr, brokerSvc, bus, nil)
		httpServer.SetDecoder(payloadDecoder)
		if err := httpServer.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Error starting HTTP protocol bridge: %v\n", err)
			os.Exit(1)
		}
		defer httpServer.Close()
	}

	// 5. Start MQTT v3.1.1 Protocol Server (port 1883)
	var mqttServer *mqtt.Server
	if cfg.MQTTPort > 0 {
		mqttAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.MQTTPort)
		mqttServer = mqtt.NewServer(mqttAddr, brokerSvc, bus, nil)
		if err := mqttServer.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Error starting MQTT protocol server: %v\n", err)
			os.Exit(1)
		}
		defer mqttServer.Close()
	}

	// 5. Start Webhook DLQ Dispatcher if configured
	var dlqDispatcher *broker.WebhookDLQDispatcher
	if cfg.DLQWebhookURL != "" {
		dlqDispatcher = broker.NewWebhookDLQDispatcher(cfg.DLQWebhookURL, bus, nil, nil)
		dlqDispatcher.Start()
		defer dlqDispatcher.Close()
	}

	// Setup OS interrupt trap
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	bootDuration := time.Since(bootStart)

	if isTerminal {
		// Run Charm Bubbletea TUI
		appModel := tui.NewAppModel(brokerSvc, bus, cfg.MaxMessages)
		appModel.SetDecoder(payloadDecoder)
		p := tea.NewProgram(appModel, tea.WithAltScreen())

		go func() {
			<-sigChan
			cancel()
			p.Quit()
		}()

		if _, err := p.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Error running TUI: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Headless mode
		endpoints := ""
		if kafkaServer != nil {
			endpoints += fmt.Sprintf("Kafka:%s ", kafkaServer.Addr())
		}
		if redisServer != nil {
			endpoints += fmt.Sprintf("Redis:%s ", redisServer.Addr())
		}
		if mqttServer != nil {
			endpoints += fmt.Sprintf("MQTT:%s ", mqttServer.Addr())
		}
		if httpServer != nil {
			endpoints += fmt.Sprintf("HTTP:%s (UI: http://%s) ", httpServer.Addr(), httpServer.Addr())
		}
		if dlqDispatcher != nil {
			endpoints += fmt.Sprintf("DLQ-Webhook:%s ", cfg.DLQWebhookURL)
		}
		if cfg.ProtoDescPath != "" {
			endpoints += fmt.Sprintf("Schemas:%s ", cfg.ProtoDescPath)
		}
		storageMode := "In-Memory"
		if cfg.DataDir != "" {
			storageMode = fmt.Sprintf("WAL(%s)", cfg.DataDir)
		}
		demoNote := ""
		if cfg.WithDemo {
			demoNote = " [Demo Active]"
		}

		chaosNote := ""
		if brokerSvc.Chaos() != nil && brokerSvc.Chaos().Enabled() {
			c := brokerSvc.Chaos().Config()
			chaosNote = fmt.Sprintf(" [⚡ Chaos Active: latency=%dms (rate=%.2f), drop=%.2f, dup=%.2f]", c.LatencyMs, c.LatencyRate, c.DropRate, c.DuplicateRate)
		}

		fmt.Printf("⚡ PocketQueue running (%s, Storage: %s)%s%s (booted in %v, Press Ctrl+C to stop)\n",
			endpoints, storageMode, demoNote, chaosNote, bootDuration)
		<-sigChan
		cancel()
		fmt.Printf("\nShutting down PocketQueue gracefully (draining up to %v)...\n", cfg.DrainTimeout)

		var drainWg sync.WaitGroup
		if kafkaServer != nil {
			drainWg.Add(1)
			go func() {
				defer drainWg.Done()
				_ = kafkaServer.CloseWithTimeout(cfg.DrainTimeout)
			}()
		}
		if redisServer != nil {
			drainWg.Add(1)
			go func() {
				defer drainWg.Done()
				_ = redisServer.CloseWithTimeout(cfg.DrainTimeout)
			}()
		}
		if httpServer != nil {
			drainWg.Add(1)
			go func() {
				defer drainWg.Done()
				_ = httpServer.CloseWithTimeout(cfg.DrainTimeout)
			}()
		}
		if mqttServer != nil {
			drainWg.Add(1)
			go func() {
				defer drainWg.Done()
				_ = mqttServer.Close()
			}()
		}
		if dlqDispatcher != nil {
			drainWg.Add(1)
			go func() {
				defer drainWg.Done()
				_ = dlqDispatcher.Close()
			}()
		}
		drainWg.Wait()
	}

	if cfg.ExportPath != "" {
		if err := exportBrokerData(brokerSvc, cfg.ExportPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error exporting data to %s: %v\n", cfg.ExportPath, err)
		} else {
			fmt.Printf("✅ Exported messages to %s\n", cfg.ExportPath)
		}
	}
}

type exportItem struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Timestamp string            `json:"timestamp"`
	Headers   map[string]string `json:"headers,omitempty"`
}

func exportBrokerData(b *broker.Service, filename string) error {
	ctx := context.Background()
	topics, err := b.ListTopics(ctx)
	if err != nil {
		return err
	}
	var allRecords []exportItem
	for topic, numParts := range topics {
		for p := int32(0); p < numParts; p++ {
			records, err := b.Fetch(ctx, topic, p, 0, 10000)
			if err != nil {
				continue
			}
			for _, r := range records {
				headers := make(map[string]string, len(r.Headers))
				for _, h := range r.Headers {
					headers[h.Key] = string(h.Value)
				}
				allRecords = append(allRecords, exportItem{
					Topic:     r.Topic,
					Partition: r.Partition,
					Offset:    r.Offset,
					Key:       string(r.Key),
					Value:     string(r.Value),
					Timestamp: r.Timestamp.Format(time.RFC3339),
					Headers:   headers,
				})
			}
		}
	}
	data, err := json.MarshalIndent(allRecords, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}
