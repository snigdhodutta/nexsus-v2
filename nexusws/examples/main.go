// Package main demonstrates how to use NexusWS in a production application.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/snigdhodutta/nexsus-v2/nexusws"
)

// Example message types for our chat application.
type ChatMessage struct {
	Channel   string `json:"channel"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
}

type UserStatus struct {
	UserID    string `json:"user_id"`
	Status    string `json:"status"` // "online", "offline", "away"
	Timestamp int64  `json:"timestamp"`
}

type RPCRequest struct {
	Action string `json:"action"`
	Data   any    `json:"data"`
}

type RPCResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func main() {
	// Setup logging
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

	logger.Println("starting NexusWS example server")

	// Create server configuration
	cfg := pkg.ServerConfig{
		Addr:              ":8080",
		Path:              "/ws",
		NATSURL:           "nats://localhost:4222",
		PingInterval:      15 * time.Second,
		WriteTimeout:      5 * time.Second,
		ReadTimeout:       30 * time.Second,
		MaxMessageSize:    1024 * 1024,   // 1MB
		EnableCompression: false,         // Disable for better performance
		AllowedOrigins:    []string{"*"}, // Configure appropriately for production
	}

	// Create server
	server, err := nexusws.NewServer(cfg)
	if err != nil {
		logger.Printf("failed to create server: %v", err)
		os.Exit(1)
	}

	// Register event handler for chat messages (@event pattern)
	server.Router().HandleEvent("chat.message", func(ctx context.Context, conn nexusws.Connection, msg *nexusws.Message) error {
		var chatMsg ChatMessage
		if err := json.Unmarshal(msg.Payload, &chatMsg); err != nil {
			logger.Printf("failed to decode chat message: %v", err)
			return nil // Don't fail on bad messages
		}

		logger.Printf("received chat message: channel=%s username=%s content=%s",
			chatMsg.Channel, chatMsg.Username, chatMsg.Content)

		// Message is automatically broadcast to all subscribers via NATS
		return nil
	})

	// Register action handler for user status updates (@action pattern - RPC)
	server.Router().HandleAction("user.status", func(ctx context.Context, conn nexusws.Connection, msg *nexusws.Message) error {
		var status UserStatus
		if err := json.Unmarshal(msg.Payload, &status); err != nil {
			logger.Printf("failed to decode status update: %v", err)
			return nil
		}

		logger.Printf("received status update: user_id=%s status=%s", status.UserID, status.Status)

		// Process the status update and send response
		response := RPCResponse{
			Success: true,
			Message: fmt.Sprintf("User %s status updated to %s", status.UserID, status.Status),
			Data: map[string]any{
				"user_id":    status.UserID,
				"new_status": status.Status,
				"updated_at": time.Now().Unix(),
			},
		}

		// Encode response
		responsePayload, err := json.Marshal(response)
		if err != nil {
			return err
		}

		// Send response back to the requester
		respMsg := &nexusws.Message{
			Type:          nexusws.FrameTypeActionResponse,
			Subject:       msg.Subject,
			CorrelationID: msg.CorrelationID,
			Reply:         msg.Reply,
			Payload:       responsePayload,
			Timestamp:     time.Now(),
		}

		return conn.Send(ctx, respMsg)
	})

	// Register producer/consumer handler for work distribution
	server.Router().HandleProducer("work.tasks", func(ctx context.Context, conn nexusws.Connection, msg *nexusws.Message) error {
		var task map[string]any
		if err := json.Unmarshal(msg.Payload, &task); err != nil {
			return err
		}

		logger.Printf("processing task: %v", task)

		// Simulate work
		time.Sleep(100 * time.Millisecond)

		// Send acknowledgment
		ackMsg := &pkg.Message{
			Type:      pkg.FrameTypeProducerAck,
			Subject:   msg.Subject,
			Payload:   []byte(`{"processed":true}`),
			Timestamp: time.Now(),
		}

		return conn.Send(ctx, ackMsg)
	})

	// Add HTTP health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":         "healthy",
			"connections":    server.ConnectionCount(),
			"nats_connected": server.Bridge().IsConnected(),
		})
	})

	// Start server in goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		if err := server.Start(ctx); err != nil {
			errCh <- err
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		logger.Printf("server error: %v", err)
		os.Exit(1)
	case sig := <-sigCh:
		logger.Printf("received shutdown signal: %v", sig)
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("shutdown error: %v", err)
		os.Exit(1)
	}

	logger.Println("server shutdown complete")
}
