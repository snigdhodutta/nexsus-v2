// Package nexusws provides a high-performance WebSocket framework with NATS backplane.
// Target: Go 1.25+
package nexusws

import (
	"context"
	"time"
)

// FrameType defines the type of message frame for multiplexing.
type FrameType uint8

const (
	FrameTypeEvent          FrameType = 0x01 // Fire-and-forget pub/sub
	FrameTypeActionRequest  FrameType = 0x02 // RPC request (expects response)
	FrameTypeActionResponse FrameType = 0x03 // RPC response
	FrameTypeProducerAck    FrameType = 0x04 // Producer/consumer acknowledgment
	FrameTypePing           FrameType = 0x05 // Application-level ping
	FrameTypePong           FrameType = 0x06 // Application-level pong
)

// Codec defines the interface for payload serialization/deserialization.
// Implementations must be thread-safe and zero-allocation where possible.
type Codec interface {
	// Encode serializes data into bytes. Returns error if encoding fails.
	Encode(v any) ([]byte, error)
	// Deserialize unmarshals bytes into v. Returns error if decoding fails.
	Decode(data []byte, v any) error
}

// Message represents a unified message structure for all workflow patterns.
type Message struct {
	// Type is the frame type (event, action request/response, etc.)
	Type FrameType
	// Subject is the NATS subject / WS topic (e.g., "chat.message")
	Subject string
	// CorrelationID is used for RPC request-reply correlation (0 for events)
	CorrelationID uint64
	// Payload is the raw serialized data
	Payload []byte
	// Reply is the subject to send responses to (for Request/Reply pattern)
	Reply string
	// Timestamp is when the message was created
	Timestamp time.Time
}

// Connection represents an active WebSocket connection.
type Connection interface {
	// ID returns the unique connection identifier
	ID() string
	// RemoteAddr returns the remote address
	RemoteAddr() string
	// Send sends a message to this connection
	Send(ctx context.Context, msg *Message) error
	// Close closes the connection with optional reason
	Close(code uint16, reason string) error
	// Context returns the connection's context
	Context() context.Context
	// IsClosed returns true if the connection is closed
	IsClosed() bool
}

// HandlerFunc is the signature for message handlers.
type HandlerFunc func(ctx context.Context, conn Connection, msg *Message) error

// Router defines the interface for message routing.
type Router interface {
	// HandleEvent registers a handler for event messages on a subject pattern.
	// Supports wildcard patterns like "chat.*" or "user.>".
	HandleEvent(subject string, fn HandlerFunc) Router
	
	// HandleAction registers a handler for action (RPC) requests.
	// The handler must return a response via the provided reply mechanism.
	HandleAction(subject string, fn HandlerFunc) Router
	
	// HandleProducer registers a handler for producer/consumer queued messages.
	// Messages are distributed among competing consumers.
	HandleProducer(subject string, fn HandlerFunc) Router
	
	// RemoveHandler removes all handlers for a subject pattern.
	RemoveHandler(subject string) Router
}

// ServerConfig holds configuration for the NexusWS server.
type ServerConfig struct {
	// Addr is the HTTP listen address (e.g., ":8080")
	Addr string
	// Path is the WebSocket endpoint path (e.g., "/ws")
	Path string
	// NATSURL is the NATS server URL (e.g., "nats://localhost:4222")
	NATSURL string
	// Codec is the codec for message serialization (defaults to RawCodec)
	Codec Codec
	// PingInterval is the interval for application-level pings
	PingInterval time.Duration
	// WriteTimeout is the timeout for write operations
	WriteTimeout time.Duration
	// ReadTimeout is the timeout for read operations
	ReadTimeout time.Duration
	// MaxMessageSize is the maximum allowed message size in bytes
	MaxMessageSize int64
	// EnableCompression enables per-message deflate compression
	EnableCompression bool
	// AllowedOrigins is a list of allowed WebSocket origins
	AllowedOrigins []string
}

// Server is the main NexusWS server instance.
type Server struct {
	config     ServerConfig
	router     Router
	bridge     *NATSBridge
	connMgr    *ConnectionManager
	httpServer *httpServerWrapper
	shutdownCh chan struct{}
}

// NewServer creates a new NexusWS server with the given configuration.
func NewServer(cfg ServerConfig) (*Server, error) {
	// Implementation in subsequent files
	return nil, nil
}

// Start starts the server and blocks until shutdown.
func (s *Server) Start(ctx context.Context) error {
	// Implementation in subsequent files
	return nil
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	// Implementation in subsequent files
	return nil
}

// Router returns the server's router for registering handlers.
func (s *Server) Router() Router {
	return s.router
}

// Publish publishes a message to all subscribers of the subject.
func (s *Server) Publish(ctx context.Context, subject string, data any) error {
	// Implementation in subsequent files
	return nil
}

// Request performs a request-reply RPC call.
func (s *Server) Request(ctx context.Context, subject string, data any, replyV any) error {
	// Implementation in subsequent files
	return nil
}

// ClientConfig holds configuration for a NexusWS client.
type ClientConfig struct {
	// URL is the WebSocket server URL (e.g., "ws://localhost:8080/ws")
	URL string
	// Codec is the codec for message serialization
	Codec Codec
	// ReconnectAttempts is the number of reconnection attempts (-1 for infinite)
	ReconnectAttempts int
	// ReconnectDelay is the delay between reconnection attempts
	ReconnectDelay time.Duration
	// PingInterval is the interval for application-level pings
	PingInterval time.Duration
}

// Client is a WebSocket client for connecting to a NexusWS server.
type Client struct {
	config ClientConfig
	conn   Connection
	msgCh  chan *Message
	errCh  chan error
	doneCh chan struct{}
}

// NewClient creates a new NexusWS client.
func NewClient(cfg ClientConfig) (*Client, error) {
	// Implementation in subsequent files
	return nil, nil
}

// Connect establishes a connection to the server.
func (c *Client) Connect(ctx context.Context) error {
	// Implementation in subsequent files
	return nil
}

// Subscribe subscribes to events on a subject pattern.
func (c *Client) Subscribe(subject string, handler func(*Message)) error {
	// Implementation in subsequent files
	return nil
}

// Request performs a request-reply RPC call.
func (c *Client) Request(ctx context.Context, subject string, data any, replyV any) error {
	// Implementation in subsequent files
	return nil
}

// Publish publishes a message to the server.
func (c *Client) Publish(ctx context.Context, subject string, data any) error {
	// Implementation in subsequent files
	return nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	// Implementation in subsequent files
	return nil
}

// Messages returns a channel of incoming messages for range iteration (Go 1.25+).
func (c *Client) Messages() <-chan *Message {
	return c.msgCh
}
