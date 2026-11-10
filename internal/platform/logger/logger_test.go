package logger_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/platform/logger"
)

func TestCorrelationIDInContext(t *testing.T) {
	ctx := context.Background()
	if got := logger.CorrelationIDFromContext(ctx); got != "" {
		t.Fatalf("expected empty correlation ID, got %q", got)
	}

	ctxWithID := logger.WithCorrelationID(ctx, "req-12345")
	if got := logger.CorrelationIDFromContext(ctxWithID); got != "req-12345" {
		t.Fatalf("expected req-12345, got %q", got)
	}
}

func TestLoggerSetupAndOperation(t *testing.T) {
	var buf bytes.Buffer
	l := logger.Setup(&buf, slog.LevelDebug, false)
	ctx := logger.WithCorrelationID(context.Background(), "test-corr-id")

	// Test successful operation
	opSuccess := logger.LogOperation(ctx, l, "test_success")
	var noErr error
	opSuccess(&noErr)

	output := buf.String()
	if !strings.Contains(output, "operation=test_success") {
		t.Errorf("expected log to contain operation=test_success, got: %s", output)
	}
	if !strings.Contains(output, "correlation_id=test-corr-id") {
		t.Errorf("expected log to contain correlation_id=test-corr-id, got: %s", output)
	}

	// Test failed operation
	buf.Reset()
	opFail := logger.LogOperation(ctx, l, "test_failure")
	errTest := errors.New("something broke")
	opFail(&errTest)

	outputFail := buf.String()
	if !strings.Contains(outputFail, "operation_failed") {
		t.Errorf("expected log to contain operation_failed, got: %s", outputFail)
	}
	if !strings.Contains(outputFail, "something broke") {
		t.Errorf("expected log to contain 'something broke', got: %s", outputFail)
	}
}

