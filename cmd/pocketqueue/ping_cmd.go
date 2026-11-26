package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/kafka"
)

func runPingCommand(args []string) error {
	fs := flag.NewFlagSet("ping", flag.ContinueOnError)

	var brokerAddr string
	var timeout time.Duration

	fs.StringVar(&brokerAddr, "broker", "127.0.0.1:9092", "Target PocketQueue/Kafka broker address")
	fs.DurationVar(&timeout, "timeout", 2*time.Second, "Maximum time to wait for ping response")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	start := time.Now()
	conn, err := net.DialTimeout("tcp", brokerAddr, timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", brokerAddr, err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(timeout))

	// Construct Kafka ApiVersions (Key 18) request
	reqBuf := new(bytes.Buffer)
	enc := kafka.NewEncoder(reqBuf)
	enc.PutInt16(kafka.ApiKeyApiVersions)
	enc.PutInt16(0)   // version 0
	enc.PutInt32(999) // correlation ID
	enc.PutString("pocketqueue-ping")

	payload := reqBuf.Bytes()
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(payload)))

	if _, err := conn.Write(sizeBuf[:]); err != nil {
		return fmt.Errorf("write size header: %w", err)
	}
	if _, err := conn.Write(payload); err != nil {
		return fmt.Errorf("write ping payload: %w", err)
	}

	// Read response size
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return fmt.Errorf("read response size: %w", err)
	}
	respSize := binary.BigEndian.Uint32(sizeBuf[:])
	if respSize == 0 || respSize > 1024*1024 {
		return fmt.Errorf("invalid response size: %d", respSize)
	}

	respPayload := make([]byte, respSize)
	if _, err := io.ReadFull(conn, respPayload); err != nil {
		return fmt.Errorf("read response payload: %w", err)
	}

	dec := kafka.NewDecoder(respPayload)
	corrID, err := dec.GetInt32()
	if err != nil || corrID != 999 {
		return fmt.Errorf("correlation ID mismatch or error: %w", err)
	}
	errCode, err := dec.GetInt16()
	if err != nil || errCode != 0 {
		return fmt.Errorf("broker reported error code: %d", errCode)
	}

	rtt := time.Since(start).Round(time.Microsecond)
	fmt.Printf("✅ Broker %s is healthy (RTT: %v)\n", brokerAddr, rtt)
	return nil
}
