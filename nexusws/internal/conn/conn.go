// Package conn provides WebSocket connection management.
package conn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/snigdhodutta/nexsus-v2/nexusws/internal/codec"
	"github.com/snigdhodutta/nexsus-v2/nexusws/internal/pool"
	"github.com/snigdhodutta/nexsus-v2/nexusws/pkg"
)

const (
	// DefaultWriteTimeout is the default timeout for write operations
	DefaultWriteTimeout = 5 * time.Second
	// DefaultReadTimeout is the default timeout for read operations  
	DefaultReadTimeout = 30 * time.Second
	// DefaultPingInterval is the default interval for application-level pings
	DefaultPingInterval = 15 * time.Second
)

// wsConn wraps a websocket.Conn with additional metadata.
type wsConn struct {
	id           string
	conn         *websocket.Conn
	remoteAddr   string
	closed       atomic.Bool
	closeCh      chan struct{}
	mu           sync.Mutex
	writeMu      sync.Mutex // Separate mutex for writes to allow concurrent reads
	ctx          context.Context
	cancelFn     context.CancelFunc
	lastActivity time.Time
}

// ConnectionManager manages all active WebSocket connections.
type ConnectionManager struct {
	conns        sync.Map // map[string]*wsConn
	connCount    atomic.Int64
	codec        nexusws.Codec
	encoder      *codec.FrameEncoder
	decoder      *codec.FrameDecoder
	maxMsgSize   int64
	writeTimeout time.Duration
	readTimeout  time.Duration
	pingInterval time.Duration
	onMessage    func(ctx context.Context, conn nexusws.Connection, msg *nexusws.Message) error
	onClose      func(conn nexusws.Connection)
}

// NewConnectionManager creates a new connection manager.
func NewConnectionManager(cfg nexusws.ServerConfig) *ConnectionManager {
	cm := &ConnectionManager{
		codec:        cfg.Codec,
		encoder:      codec.NewFrameEncoder(),
		decoder:      codec.NewFrameDecoder(),
		maxMsgSize:   cfg.MaxMessageSize,
		writeTimeout: cfg.WriteTimeout,
		readTimeout:  cfg.ReadTimeout,
		pingInterval: cfg.PingInterval,
	}

	if cm.codec == nil {
		cm.codec = codec.NewRawCodec()
	}

	if cm.maxMsgSize == 0 {
		cm.maxMsgSize = 1024 * 1024 // 1MB default
	}

	if cm.writeTimeout == 0 {
		cm.writeTimeout = DefaultWriteTimeout
	}

	if cm.readTimeout == 0 {
		cm.readTimeout = DefaultReadTimeout
	}

	if cm.pingInterval == 0 {
		cm.pingInterval = DefaultPingInterval
	}

	return cm
}

// SetOnMessage sets the message handler callback.
func (cm *ConnectionManager) SetOnMessage(fn func(ctx context.Context, conn nexusws.Connection, msg *nexusws.Message) error) {
	cm.onMessage = fn
}

// SetOnClose sets the close handler callback.
func (cm *ConnectionManager) SetOnClose(fn func(conn nexusws.Connection)) {
	cm.onClose = fn
}

// Accept accepts a WebSocket connection and starts handling it.
func (cm *ConnectionManager) Accept(w http.ResponseWriter, r *http.Request, opts *websocket.AcceptOptions) (nexusws.Connection, error) {
	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		return nil, err
	}

	// Generate unique connection ID
	connID := generateConnID()
	remoteAddr := r.RemoteAddr
	if remoteAddr == "" {
		remoteAddr = conn.LocalAddr().String()
	}

	ctx, cancel := context.WithCancel(context.Background())
	wsConn := &wsConn{
		id:         connID,
		conn:       conn,
		remoteAddr: remoteAddr,
		closeCh:    make(chan struct{}),
		ctx:        ctx,
		cancelFn:   cancel,
	}

	// Store connection
	cm.conns.Store(connID, wsConn)
	cm.connCount.Add(1)

	// Start background tasks
	go wsConn.handlePings(cm.pingInterval)
	go wsConn.handleReads(cm)

	return wsConn, nil
}

// Get retrieves a connection by ID.
func (cm *ConnectionManager) Get(id string) (nexusws.Connection, bool) {
	val, ok := cm.conns.Load(id)
	if !ok {
		return nil, false
	}
	return val.(nexusws.Connection), true
}

// Remove removes a connection from the manager.
func (cm *ConnectionManager) Remove(id string) {
	cm.conns.Delete(id)
	cm.connCount.Add(-1)
}

// Count returns the number of active connections.
func (cm *ConnectionManager) Count() int64 {
	return cm.connCount.Load()
}

// Broadcast sends a message to all connections subscribed to a subject.
func (cm *ConnectionManager) Broadcast(ctx context.Context, subject string, msg *nexusws.Message) error {
	var wg sync.WaitGroup
	var firstErr error
	var firstErrMu sync.Mutex

	cm.conns.Range(func(key, value any) bool {
		wsConn := value.(*wsConn)
		if wsConn.IsClosed() {
			return true
		}

		wg.Add(1)
		go func(c *wsConn) {
			defer wg.Done()
			if err := c.Send(ctx, msg); err != nil {
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrMu.Unlock()
			}
		}(wsConn)

		return true
	})

	wg.Wait()
	return firstErr
}

// CloseAll closes all connections gracefully.
func (cm *ConnectionManager) CloseAll(ctx context.Context, code uint16, reason string) {
	cm.conns.Range(func(key, value any) bool {
		wsConn := value.(*wsConn)
		wsConn.Close(code, reason)
		return true
	})
}

// ID returns the connection ID.
func (c *wsConn) ID() string {
	return c.id
}

// RemoteAddr returns the remote address.
func (c *wsConn) RemoteAddr() string {
	return c.remoteAddr
}

// Context returns the connection's context.
func (c *wsConn) Context() context.Context {
	return c.ctx
}

// IsClosed returns true if the connection is closed.
func (c *wsConn) IsClosed() bool {
	return c.closed.Load()
}

// Send sends a message to the connection.
func (c *wsConn) Send(ctx context.Context, msg *nexusws.Message) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// Set write deadline
	writeCtx, cancel := context.WithTimeout(ctx, DefaultWriteTimeout)
	defer cancel()

	// Encode message
	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	frame, err := codec.NewFrameEncoder().Encode(msg)
	if err != nil {
		return err
	}

	buf.Write(frame)

	// Write binary message
	err = c.conn.Write(writeCtx, websocket.MessageBinary, buf.Bytes())
	if err != nil {
		c.closeWithError(websocket.StatusInternalError, "write failed")
		return err
	}

	c.updateActivity()
	return nil
}

// Close closes the connection.
func (c *wsConn) Close(code uint16, reason string) error {
	if c.closed.Swap(true) {
		return nil // Already closed
	}

	c.cancelFn()
	
	// Close channel safely (only once)
	select {
	case <-c.closeCh:
		// Already closed
	default:
		close(c.closeCh)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// Send close frame
	_ = c.conn.CloseNow()

	return nil
}

func (c *wsConn) closeWithError(code websocket.StatusCode, reason string) {
	if c.closed.Swap(true) {
		return // Already closed
	}
	
	c.cancelFn()
	
	// Close channel safely (only once)
	select {
	case <-c.closeCh:
		// Already closed
	default:
		close(c.closeCh)
	}
	
	_ = c.conn.Close(code, reason)
}

func (c *wsConn) updateActivity() {
	c.lastActivity = time.Now()
}

func (c *wsConn) handlePings(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.closed.Load() {
				return
			}

			c.writeMu.Lock()
			pingCtx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
			err := c.conn.Ping(pingCtx)
			cancel()
			c.writeMu.Unlock()

			if err != nil {
				c.closeWithError(websocket.StatusPolicyViolation, "ping failed")
				return
			}
		}
	}
}

func (c *wsConn) handleReads(cm *ConnectionManager) {
	defer func() {
		cm.Remove(c.id)
		if cm.onClose != nil {
			cm.onClose(c)
		}
		// Don't call Close here again - closeWithError already handles it
	}()

	for {
		if c.closed.Load() {
			return
		}

		// Set read deadline
		readCtx, cancel := context.WithTimeout(c.ctx, cm.readTimeout)
		
		msgType, reader, err := c.conn.Reader(readCtx)
		cancel()

		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				c.closeWithError(websocket.StatusNormalClosure, "normal closure")
				return
			}
			c.closeWithError(websocket.StatusInternalError, "read failed")
			return
		}

		if msgType != websocket.MessageBinary {
			// Skip non-binary messages (could log warning)
			continue
		}

		// Read message using pool buffer
		bufPtr := pool.GetByteSlice()
		defer pool.PutByteSlice(bufPtr)

		// Read with size limit
		lr := io.LimitReader(reader, cm.maxMsgSize)
		*bufPtr, err = io.ReadAll(lr)
		if err != nil {
			c.closeWithError(websocket.StatusMessageTooBig, "message too large")
			return
		}

		// Decode frame
		msg, _, err := cm.decoder.Decode(*bufPtr)
		if err != nil {
			c.closeWithError(websocket.StatusInvalidFramePayloadData, "decode failed")
			return
		}

		c.updateActivity()

		// Handle message
		if cm.onMessage != nil {
			if err := cm.onMessage(c.ctx, c, msg); err != nil {
				// Log error but continue processing
				// In production, use slog here
			}
		}
	}
}

// Errors
var (
	ErrConnectionClosed = fmt.Errorf("connection closed")
)

// generateConnID generates a unique connection ID.
func generateConnID() string {
	// Use nanotime + counter for uniqueness without allocation
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), atomic.AddUint64(&connCounter, 1))
}

var connCounter atomic.Uint64
