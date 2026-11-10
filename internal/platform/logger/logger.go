package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"time"
)

type contextKey string

const (
	correlationIDKey contextKey = "correlation_id"
)

// WithCorrelationID stores a correlation ID in context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

// CorrelationIDFromContext retrieves the correlation ID from context if present.
func CorrelationIDFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(correlationIDKey).(string); ok {
		return val
	}
	return ""
}

// ContextHandler wraps an slog.Handler to automatically attach correlation_id to log records.
type ContextHandler struct {
	slog.Handler
}

// Handle enriches log records with correlation ID from context.
func (h ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if cid := CorrelationIDFromContext(ctx); cid != "" {
		r.AddAttrs(slog.String("correlation_id", cid))
	}
	return h.Handler.Handle(ctx, r)
}

// Setup initializes the global structured logger.
func Setup(out io.Writer, level slog.Level, isJSON bool) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}
	opts := &slog.HandlerOptions{
		Level: level,
	}

	var inner slog.Handler
	if isJSON {
		inner = slog.NewJSONHandler(out, opts)
	} else {
		inner = slog.NewTextHandler(out, opts)
	}

	handler := ContextHandler{Handler: inner}
	l := slog.New(handler)
	slog.SetDefault(l)
	return l
}

// LogOperation logs the lifecycle (start and completion with duration) of an operation.
func LogOperation(ctx context.Context, l *slog.Logger, op string) func(err *error) {
	if l == nil {
		l = slog.Default()
	}
	start := time.Now()
	l.DebugContext(ctx, "operation_started", slog.String("operation", op))

	return func(err *error) {
		durationMs := time.Since(start).Milliseconds()
		if err != nil && *err != nil {
			l.ErrorContext(ctx, "operation_failed",
				slog.String("operation", op),
				slog.Int64("duration_ms", durationMs),
				slog.Any("error", *err),
			)
			return
		}
		l.DebugContext(ctx, "operation_completed",
			slog.String("operation", op),
			slog.Int64("duration_ms", durationMs),
		)
	}
}

