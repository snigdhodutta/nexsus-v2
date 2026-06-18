// Package nexusws provides a high-performance WebSocket framework with NATS backplane.
// Target: Go 1.25+
package nexusws

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/snigdhodutta/nexsus-v2/nexusws/internal/bridge"
	"github.com/snigdhodutta/nexsus-v2/nexusws/internal/codec"
	"github.com/snigdhodutta/nexsus-v2/nexusws/internal/conn"
	"github.com/snigdhodutta/nexsus-v2/nexusws/internal/router"
)

const (
	// Version is the current version of NexusWS
	Version = "0.1.0"
)

// httpServerWrapper wraps an http.Server for graceful shutdown.
type httpServerWrapper struct {
	server *http.Server
}

func newHTTPServer(addr string, handler http.Handler) *httpServerWrapper {
	return &httpServerWrapper{
		server: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

func (h *httpServerWrapper) ListenAndServe() error {
	return h.server.ListenAndServe()
}

func (h *httpServerWrapper) Shutdown(ctx context.Context) error {
	return h.server.Shutdown(ctx)
}

// Server is the main NexusWS server instance.
type Server struct {
	config     ServerConfig
	router     *router.RouterImpl
	bridge     *bridge.NATSBridge
	connMgr    *conn.ConnectionManager
	httpServer *httpServerWrapper
	shutdownCh chan struct{}
	wg         sync.WaitGroup
	logger     *log.Logger
	started    bool
	mu         sync.Mutex
}

// NewServer creates a new NexusWS server with the given configuration.
func NewServer(cfg ServerConfig) (*Server, error) {
	// Set defaults
	if cfg.Codec == nil {
		cfg.Codec = codec.NewRawCodec()
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 15 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 1024 * 1024 // 1MB
	}

	// Setup logger
	logger := log.New(os.Stdout, "[NexusWS] ", log.LstdFlags|log.Lshortfile)

	// Create NATS bridge
	b, err := bridge.NewNATSBridge(cfg.NATSURL, cfg.Codec)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS bridge: %w", err)
	}

	// Create router
	r := router.NewRouter(b)

	// Create connection manager
	cm := conn.NewConnectionManager(cfg)

	// Create HTTP server wrapper
	httpSrv := newHTTPServer(cfg.Addr, nil)

	s := &Server{
		config:     cfg,
		router:     r,
		bridge:     b,
		connMgr:    cm,
		httpServer: httpSrv,
		shutdownCh: make(chan struct{}),
		logger:     logger,
	}

	// Set up message handlers
	cm.SetOnMessage(s.handleWebSocketMessage)
	cm.SetOnClose(s.handleConnectionClose)
	b.SetOnMessage(s.handleNATSMessage)

	// Set up HTTP handler
	mux := http.NewServeMux()
	mux.HandleFunc(cfg.Path, s.handleWebSocketUpgrade)
	httpSrv.server.Handler = mux

	return s, nil
}

// Start starts the server and blocks until shutdown.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	s.started = true
	s.mu.Unlock()

	s.logger.Printf("starting NexusWS server version=%s addr=%s path=%s nats_url=%s",
		Version, s.config.Addr, s.config.Path, s.config.NATSURL)

	// Start HTTP server in goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		s.logger.Println("context cancelled, shutting down")
	case sig := <-sigCh:
		s.logger.Printf("received signal, shutting down: %v", sig)
	case <-s.shutdownCh:
		s.logger.Println("shutdown requested")
	}

	return s.Shutdown(context.Background())
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Println("shutting down NexusWS server")

	// Close all connections
	s.connMgr.CloseAll(ctx, uint16(1000), "server shutdown")

	// Shutdown HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Printf("HTTP server shutdown error: %v", err)
	}

	// Close NATS bridge
	if err := s.bridge.Close(); err != nil {
		s.logger.Printf("NATS bridge shutdown error: %v", err)
	}

	// Wait for goroutines to finish
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Println("server shutdown complete")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Router returns the server's router for registering handlers.
func (s *Server) Router() Router {
	return s.router
}

// Publish publishes a message to all subscribers of the subject.
func (s *Server) Publish(ctx context.Context, subject string, data any) error {
	payload, err := s.config.Codec.Encode(data)
	if err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}

	msg := &Message{
		Type:      FrameTypeEvent,
		Subject:   subject,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	return s.bridge.Publish(ctx, subject, msg)
}

// Request performs a request-reply RPC call.
func (s *Server) Request(ctx context.Context, subject string, data any, replyV any) error {
	payload, err := s.config.Codec.Encode(data)
	if err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}

	responsePayload, err := s.bridge.Request(ctx, subject, payload, DefaultRequestTimeout)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	if replyV != nil {
		if err := s.config.Codec.Decode(responsePayload, replyV); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

// handleWebSocketUpgrade handles WebSocket upgrade requests.
func (s *Server) handleWebSocketUpgrade(w http.ResponseWriter, r *http.Request) {
	// Check origin if configured
	if len(s.config.AllowedOrigins) > 0 {
		origin := r.Header.Get("Origin")
		allowed := false
		for _, o := range s.config.AllowedOrigins {
			if o == origin || o == "*" {
				allowed = true
				break
			}
		}
		if !allowed {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
	}

	opts := &websocket.AcceptOptions{
		Subprotocols:   []string{"nexusws.binary"},
		OriginPatterns: s.config.AllowedOrigins,
		CompressionMode: websocket.CompressionDisabled, // Disable for performance
	}

	if s.config.EnableCompression {
		opts.CompressionMode = websocket.CompressionContextTakeover
	}

	_, err := s.connMgr.Accept(w, r, opts)
	if err != nil {
		s.logger.Error("WebSocket accept error", "error", err)
		return
	}

	s.logger.Debug("WebSocket connection accepted", "remote", r.RemoteAddr)
}

// handleWebSocketMessage handles incoming WebSocket messages.
func (s *Server) handleWebSocketMessage(ctx context.Context, c Connection, msg *Message) error {
	// Route locally first
	if err := s.router.Route(ctx, c, msg); err != nil {
		s.logger.Warn("local routing error", "error", err)
	}

	// Bridge to NATS
	if err := s.bridge.HandleIncomingMessage(ctx, msg); err != nil {
		s.logger.Warn("NATS bridge error", "error", err)
		return err
	}

	return nil
}

// handleNATSMessage handles messages from NATS.
func (s *Server) handleNATSMessage(ctx context.Context, msg *Message) error {
	// Broadcast to all connected clients
	return s.connMgr.Broadcast(ctx, msg.Subject, msg)
}

// handleConnectionClose handles connection close events.
func (s *Server) handleConnectionClose(c Connection) {
	s.logger.Debug("connection closed", "id", c.ID(), "remote", c.RemoteAddr())
}

// ConnManager returns the connection manager (for advanced use cases).
func (s *Server) ConnManager() *conn.ConnectionManager {
	return s.connMgr
}

// Bridge returns the NATS bridge (for advanced use cases).
func (s *Server) Bridge() *bridge.NATSBridge {
	return s.bridge
}

// Logger returns the server's logger.
func (s *Server) Logger() *log.Logger {
	return s.logger
}

// ConnectionCount returns the number of active connections.
func (s *Server) ConnectionCount() int64 {
	return s.connMgr.Count()
}

// DefaultRequestTimeout is the default timeout for request-reply operations.
const DefaultRequestTimeout = 30 * time.Second
