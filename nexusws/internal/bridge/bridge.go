// Package bridge provides NATS integration for NexusWS.
package bridge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	codecpkg "github.com/snigdhodutta/nexsus-v2/nexusws/internal/codec"
	"github.com/snigdhodutta/nexsus-v2/nexusws/internal/pool"
	nexusws "github.com/snigdhodutta/nexsus-v2/nexusws/pkg"
)

const (
	// DefaultNATSTimeout is the default timeout for NATS operations
	DefaultNATSTimeout = 10 * time.Second
	// DefaultRequestTimeout is the default timeout for request-reply patterns
	DefaultRequestTimeout = 30 * time.Second
	// ReplyPrefix is the prefix for reply subjects
	ReplyPrefix = "_nexus.reply."
)

// NATSBridge bridges WebSocket messages to/from NATS.
type NATSBridge struct {
	conn           *nats.Conn
	codec          nexusws.Codec
	encoder        *codecpkg.FrameEncoder
	decoder        *codecpkg.FrameDecoder
	subscriptions  sync.Map // map[string]*nats.Subscription
	requestMap     sync.Map // map[uint64]chan *nexusws.Message
	correlationSeq atomic.Uint64
	replyPrefix    string
	ctx            context.Context
	cancelFn       context.CancelFunc
	onMessage      func(ctx context.Context, msg *nexusws.Message) error
	mu             sync.RWMutex
}

// NewNATSBridge creates a new NATS bridge.
func NewNATSBridge(url string, c nexusws.Codec) (*NATSBridge, error) {
	if c == nil {
		c = codecpkg.NewRawCodec()
	}

	nc, err := nats.Connect(url,
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1), // Infinite reconnects
		nats.Timeout(5*time.Second),
		nats.PingInterval(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	bridge := &NATSBridge{
		conn:        nc,
		codec:       c,
		encoder:     codecpkg.NewFrameEncoder(),
		decoder:     codecpkg.NewFrameDecoder(),
		replyPrefix: ReplyPrefix,
		ctx:         ctx,
		cancelFn:    cancel,
	}

	return bridge, nil
}

// SetOnMessage sets the message handler callback.
func (b *NATSBridge) SetOnMessage(fn func(ctx context.Context, msg *nexusws.Message) error) {
	b.onMessage = fn
}

// Publish publishes a message to NATS.
func (b *NATSBridge) Publish(ctx context.Context, subject string, msg *nexusws.Message) error {
	// Encode payload
	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	frame, err := b.encoder.Encode(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	buf.Write(frame)

	// Publish to NATS
	if err := b.conn.Publish(subject, buf.Bytes()); err != nil {
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	return nil
}

// PublishWithReply publishes a message with a reply subject for request-reply.
func (b *NATSBridge) PublishWithReply(ctx context.Context, subject string, msg *nexusws.Message) (<-chan *nexusws.Message, error) {
	// Generate correlation ID
	correlationID := b.correlationSeq.Add(1)
	msg.CorrelationID = correlationID

	// Create reply subject
	replySubject := fmt.Sprintf("%s%d.%d", b.replyPrefix, time.Now().UnixNano(), correlationID)
	msg.Reply = replySubject

	// Create response channel
	responseCh := make(chan *nexusws.Message, 1)
	b.requestMap.Store(correlationID, responseCh)

	// Subscribe to reply subject
	var sub *nats.Subscription
	var err error
	sub, err = b.conn.Subscribe(replySubject, func(m *nats.Msg) {
		// Decode response
		respMsg, _, err := b.decoder.Decode(m.Data)
		if err != nil {
			// Log error in production
			close(responseCh)
			b.requestMap.Delete(correlationID)
			return
		}

		// Send to channel
		select {
		case responseCh <- respMsg:
		default:
			// Channel full or closed
		}

		// Cleanup
		close(responseCh)
		b.requestMap.Delete(correlationID)
	})
	if err != nil {
		b.requestMap.Delete(correlationID)
		close(responseCh)
		return nil, fmt.Errorf("failed to subscribe to reply subject: %w", err)
	}

	// Unsubscribe after response is received (handled in defer)
	defer func() {
		_ = sub.Unsubscribe()
	}()

	// Publish request
	if err := b.Publish(ctx, subject, msg); err != nil {
		b.requestMap.Delete(correlationID)
		close(responseCh)
		return nil, err
	}

	return responseCh, nil
}

// Subscribe subscribes to a NATS subject and forwards messages to the handler.
func (b *NATSBridge) Subscribe(ctx context.Context, subject string, handler func(*nexusws.Message)) error {
	sub, err := b.conn.Subscribe(subject, func(m *nats.Msg) {
		// Decode message
		msg, _, err := b.decoder.Decode(m.Data)
		if err != nil {
			// Log error in production
			return
		}

		// Call handler
		if handler != nil {
			handler(msg)
		}
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS subject: %w", err)
	}

	// Store subscription
	b.subscriptions.Store(subject, sub)

	return nil
}

// SubscribeQueue subscribes to a NATS subject with a queue group for load balancing.
func (b *NATSBridge) SubscribeQueue(ctx context.Context, subject, queue string, handler func(*nexusws.Message)) error {
	sub, err := b.conn.QueueSubscribe(subject, queue, func(m *nats.Msg) {
		// Decode message
		msg, _, err := b.decoder.Decode(m.Data)
		if err != nil {
			// Log error in production
			return
		}

		// Call handler
		if handler != nil {
			handler(msg)
		}
	})
	if err != nil {
		return fmt.Errorf("failed to queue subscribe to NATS subject: %w", err)
	}

	// Store subscription
	b.subscriptions.Store(subject+":"+queue, sub)

	return nil
}

// Request performs a request-reply RPC call.
func (b *NATSBridge) Request(ctx context.Context, subject string, data []byte, timeout time.Duration) ([]byte, error) {
	if timeout == 0 {
		timeout = DefaultRequestTimeout
	}

	// Create request message
	msg := &nexusws.Message{
		Type:      nexusws.FrameTypeActionRequest,
		Subject:   subject,
		Payload:   data,
		Timestamp: time.Now(),
	}

	// Publish with reply
	responseCh, err := b.PublishWithReply(ctx, subject, msg)
	if err != nil {
		return nil, err
	}

	// Wait for response
	select {
	case resp, ok := <-responseCh:
		if !ok {
			return nil, fmt.Errorf("response channel closed")
		}
		if resp.Type == nexusws.FrameTypeActionResponse {
			return resp.Payload, nil
		}
		return nil, fmt.Errorf("unexpected response type: %v", resp.Type)
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(timeout):
		return nil, fmt.Errorf("request timeout")
	}
}

// HandleIncomingMessage handles an incoming WebSocket message by publishing to NATS.
func (b *NATSBridge) HandleIncomingMessage(ctx context.Context, msg *nexusws.Message) error {
	switch msg.Type {
	case nexusws.FrameTypeEvent:
		// Fire-and-forget pub/sub
		return b.Publish(ctx, msg.Subject, msg)

	case nexusws.FrameTypeActionRequest:
		// Check if we have a local handler first
		if b.onMessage != nil {
			// Create response channel
			responseCh := make(chan *nexusws.Message, 1)
			b.requestMap.Store(msg.CorrelationID, responseCh)

			// Process locally
			go func() {
				defer close(responseCh)

				respMsg := &nexusws.Message{
					Type:          nexusws.FrameTypeActionResponse,
					Subject:       msg.Subject,
					CorrelationID: msg.CorrelationID,
					Reply:         msg.Reply,
					Timestamp:     time.Now(),
				}

				// Call handler
				if err := b.onMessage(ctx, msg); err != nil {
					respMsg.Payload = []byte(err.Error())
				}

				// Send response
				responseCh <- respMsg
			}()

			return nil
		}

		// Forward to NATS for remote handling
		return b.Publish(ctx, msg.Subject, msg)

	case nexusws.FrameTypeActionResponse:
		// Route to pending request
		if val, ok := b.requestMap.Load(msg.CorrelationID); ok {
			ch, ok := val.(chan *nexusws.Message)
			if ok {
				select {
				case ch <- msg:
				default:
				}
			}
		}
		return nil

	case nexusws.FrameTypeProducerAck:
		// Acknowledge producer message
		return nil

	default:
		return fmt.Errorf("unknown frame type: %v", msg.Type)
	}
}

// HandleOutgoingMessage handles a message from NATS to be sent to WebSocket clients.
func (b *NATSBridge) HandleOutgoingMessage(data []byte) (*nexusws.Message, error) {
	msg, _, err := b.decoder.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode NATS message: %w", err)
	}
	return msg, nil
}

// Close closes the NATS connection.
func (b *NATSBridge) Close() error {
	b.cancelFn()

	// Unsubscribe all subscriptions
	b.subscriptions.Range(func(key, value any) bool {
		sub := value.(*nats.Subscription)
		_ = sub.Unsubscribe()
		b.subscriptions.Delete(key)
		return true
	})

	// Clean up request map
	b.requestMap.Range(func(key, value any) bool {
		ch, ok := value.(chan *nexusws.Message)
		if ok {
			close(ch)
		}
		b.requestMap.Delete(key)
		return true
	})

	if b.conn != nil {
		b.conn.Drain()
	}

	return nil
}

// IsConnected returns true if the NATS connection is active.
func (b *NATSBridge) IsConnected() bool {
	return b.conn != nil && b.conn.IsConnected()
}

// Conn returns the underlying NATS connection.
func (b *NATSBridge) Conn() *nats.Conn {
	return b.conn
}
