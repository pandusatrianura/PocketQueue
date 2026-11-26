package http

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
)

// Server encapsulates the HTTP REST and SSE streaming server.
type Server struct {
	addr     string
	boundURL string
	handler  *Handler
	server   *http.Server
	listener net.Listener
	logger   *slog.Logger

	mu     sync.Mutex
	closed bool
}

// NewServer initializes an HTTP Server instance.
func NewServer(addr string, svc *broker.Service, bus *broker.EventBus, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	handler := NewHandler(svc, bus, logger)
	return &Server{
		addr:    addr,
		handler: handler,
		logger:  logger,
	}
}

// Start binds to the configured network address and begins listening.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("http listen on %s: %w", s.addr, err)
	}

	mux := http.NewServeMux()
	s.handler.RegisterRoutes(mux)

	s.mu.Lock()
	s.listener = ln
	s.addr = ln.Addr().String()
	s.boundURL = "http://" + s.addr
	s.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // 0 required for SSE streaming
		IdleTimeout:  60 * time.Second,
	}
	s.mu.Unlock()

	s.logger.Info("http protocol bridge listening", slog.String("addr", s.addr))

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server serve error", slog.Any("error", err))
		}
	}()

	return nil
}

// URL returns the base HTTP URL of the bound server (e.g. "http://127.0.0.1:8080").
func (s *Server) URL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.boundURL
}

// Addr returns the bound TCP address string.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// CloseWithTimeout drains active HTTP connections within the specified timeout.
func (s *Server) CloseWithTimeout(timeout time.Duration) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	srv := s.server
	s.mu.Unlock()

	if srv == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := srv.Shutdown(ctx)
	s.logger.Info("http protocol bridge closed")
	return err
}

// Close stops the server using a default 500ms drain timeout.
func (s *Server) Close() error {
	return s.CloseWithTimeout(500 * time.Millisecond)
}
