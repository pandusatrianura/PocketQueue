package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/testrunner"
)

func runTestCommand(args []string) error {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)

	var brokerAddr string
	var topic string
	var topicAlias string
	var expectKey string
	var expectPayload string
	var timeout time.Duration
	var pollInterval time.Duration

	fs.StringVar(&brokerAddr, "broker", "127.0.0.1:9092", "Target PocketQueue/Kafka broker address")
	fs.StringVar(&topic, "expect-topic", "", "Topic name expected to receive message (required)")
	fs.StringVar(&topicAlias, "topic", "", "Topic name alias for --expect-topic")
	fs.StringVar(&expectKey, "expect-key", "", "Message key expected to match")
	fs.StringVar(&expectPayload, "expect-payload", "", "Substring expected inside message payload")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "Maximum time to wait before failing assertion")
	fs.DurationVar(&pollInterval, "poll", 50*time.Millisecond, "Polling interval between fetch checks")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if topic == "" && topicAlias != "" {
		topic = topicAlias
	}
	if topic == "" {
		return fmt.Errorf("--expect-topic is required")
	}

	spec := testrunner.AssertionSpec{
		BrokerAddr:         brokerAddr,
		Topic:              topic,
		Partition:          0,
		ExpectedKey:        expectKey,
		ExpectedPayloadSub: expectPayload,
		Timeout:            timeout,
		PollInterval:       pollInterval,
	}

	fmt.Printf("🔍 Testing assertion on %s (topic: %s, timeout: %v)...\n", brokerAddr, topic, timeout)

	res, err := testrunner.RunAssertion(context.Background(), spec)
	if err != nil {
		return err
	}

	fmt.Printf("✅ Assertion Passed! (matched in %v)\n", res.Latency.Round(time.Millisecond))
	fmt.Printf("   Topic:     %s (partition %d, offset %d)\n", res.Topic, res.Partition, res.Offset)
	if res.Key != "" {
		fmt.Printf("   Key:       %s\n", res.Key)
	}
	fmt.Printf("   Payload:   %s\n", res.Payload)
	return nil
}
