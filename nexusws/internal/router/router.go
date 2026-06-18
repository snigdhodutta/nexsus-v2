// Package router provides message routing for NexusWS.
package router

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/snigdhodutta/nexsus-v2/nexusws/pkg"
)

// route represents a registered route handler.
type route struct {
	subject   string
	pattern   *regexp.Regexp
	handler   nexusws.HandlerFunc
	routeType RouteType
}

// RouteType defines the type of route.
type RouteType int

const (
	RouteTypeEvent RouteType = iota
	RouteTypeAction
	RouteTypeProducer
)

// RouterImpl implements the Router interface with pattern matching.
type RouterImpl struct {
	mu           sync.RWMutex
	routes       []*route
	subscriptions map[string][]*route // Cache for exact matches
	wildcards     []*route            // Wildcard patterns
	bridge        Bridge
}

// Bridge is an interface for NATS bridge operations.
type Bridge interface {
	Publish(ctx context.Context, subject string, msg *nexusws.Message) error
	Subscribe(ctx context.Context, subject string, handler func(*nexusws.Message)) error
	SubscribeQueue(ctx context.Context, subject, queue string, handler func(*nexusws.Message)) error
	Request(ctx context.Context, subject string, data []byte, timeout time.Duration) ([]byte, error)
}

// NewRouter creates a new router instance.
func NewRouter(bridge Bridge) *RouterImpl {
	return &RouterImpl{
		subscriptions: make(map[string][]*route),
		wildcards:     make([]*route, 0),
		bridge:        bridge,
	}
}

// HandleEvent registers a handler for event messages on a subject pattern.
// Supports wildcard patterns like "chat.*" or "user.>".
func (r *RouterImpl) HandleEvent(subject string, fn nexusws.HandlerFunc) nexusws.Router {
	r.mu.Lock()
	defer r.mu.Unlock()

	rt := &route{
		subject:   subject,
		pattern:   compilePattern(subject),
		handler:   fn,
		routeType: RouteTypeEvent,
	}

	r.routes = append(r.routes, rt)

	// If exact match (no wildcards), cache it
	if !strings.ContainsAny(subject, "*>") {
		r.subscriptions[subject] = append(r.subscriptions[subject], rt)
	} else {
		r.wildcards = append(r.wildcards, rt)
	}

	// Subscribe to NATS subject
	if r.bridge != nil {
		_ = r.bridge.Subscribe(context.Background(), subject, func(msg *nexusws.Message) {
			// Will be handled by message routing
		})
	}

	return r
}

// HandleAction registers a handler for action (RPC) requests.
func (r *RouterImpl) HandleAction(subject string, fn nexusws.HandlerFunc) nexusws.Router {
	r.mu.Lock()
	defer r.mu.Unlock()

	rt := &route{
		subject:   subject,
		pattern:   compilePattern(subject),
		handler:   fn,
		routeType: RouteTypeAction,
	}

	r.routes = append(r.routes, rt)

	// Subscribe to NATS subject for actions
	if r.bridge != nil {
		_ = r.bridge.Subscribe(context.Background(), subject, func(msg *nexusws.Message) {
			// Will be handled by message routing
		})
	}

	return r
}

// HandleProducer registers a handler for producer/consumer queued messages.
func (r *RouterImpl) HandleProducer(subject string, fn nexusws.HandlerFunc) nexusws.Router {
	r.mu.Lock()
	defer r.mu.Unlock()

	rt := &route{
		subject:   subject,
		pattern:   compilePattern(subject),
		handler:   fn,
		routeType: RouteTypeProducer,
	}

	r.routes = append(r.routes, rt)

	// Queue subscribe for load balancing
	if r.bridge != nil {
		queueGroup := fmt.Sprintf("nexus_producer_%s", subject)
		_ = r.bridge.SubscribeQueue(context.Background(), subject, queueGroup, func(msg *nexusws.Message) {
			// Will be handled by message routing
		})
	}

	return r
}

// RemoveHandler removes all handlers for a subject pattern.
func (r *RouterImpl) RemoveHandler(subject string) nexusws.Router {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Filter out routes matching the subject
	newRoutes := make([]*route, 0, len(r.routes))
	for _, rt := range r.routes {
		if rt.subject != subject {
			newRoutes = append(newRoutes, rt)
		}
	}
	r.routes = newRoutes

	// Clear from subscriptions
	delete(r.subscriptions, subject)

	// Clear from wildcards
	newWildcards := make([]*route, 0, len(r.wildcards))
	for _, rt := range r.wildcards {
		if rt.subject != subject {
			newWildcards = append(newWildcards, rt)
		}
	}
	r.wildcards = newWildcards

	return r
}

// Route routes a message to the appropriate handler.
func (r *RouterImpl) Route(ctx context.Context, conn nexusws.Connection, msg *nexusws.Message) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// First check exact matches
	if routes, ok := r.subscriptions[msg.Subject]; ok {
		for _, rt := range routes {
			if err := rt.handler(ctx, conn, msg); err != nil {
				return err
			}
		}
		return nil
	}

	// Check wildcard patterns
	for _, rt := range r.wildcards {
		if rt.pattern.MatchString(msg.Subject) {
			if err := rt.handler(ctx, conn, msg); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetHandlers returns all handlers for a subject (for debugging).
func (r *RouterImpl) GetHandlers(subject string) []RouteType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var types []RouteType

	if routes, ok := r.subscriptions[subject]; ok {
		for _, rt := range routes {
			types = append(types, rt.routeType)
		}
	}

	for _, rt := range r.wildcards {
		if rt.pattern.MatchString(subject) {
			types = append(types, rt.routeType)
		}
	}

	return types
}

// compilePattern converts a NATS-style subject pattern to a regex.
// NATS wildcards: * (matches one token), > (matches one or more tokens)
func compilePattern(pattern string) *regexp.Regexp {
	// Escape special regex characters except * and >
	escaped := strings.Map(func(r rune) rune {
		switch r {
		case '.', '*', '>':
			return r
		default:
			if strings.ContainsRune(".^$+?{}[]\\|()", r) {
				return '\\'
			}
			return r
		}
	}, pattern)

	// Convert NATS wildcards to regex
	// * matches exactly one token (no dots)
	escaped = strings.ReplaceAll(escaped, "*", "[^.]+")
	// > matches one or more tokens (including dots)
	escaped = strings.ReplaceAll(escaped, ">", ".+")

	// Anchor the pattern
	regex := "^" + escaped + "$"

	return regexp.MustCompile(regex)
}

// matchSubject checks if a subject matches a pattern.
func matchSubject(pattern, subject string) bool {
	if pattern == subject {
		return true
	}

	patternParts := strings.Split(pattern, ".")
	subjectParts := strings.Split(subject, ".")

	return matchParts(patternParts, subjectParts)
}

func matchParts(patternParts, subjectParts []string) bool {
	if len(patternParts) == 0 {
		return len(subjectParts) == 0
	}

	if patternParts[0] == ">" {
		// > matches one or more tokens
		return len(subjectParts) >= 1
	}

	if len(subjectParts) == 0 {
		return false
	}

	if patternParts[0] == "*" {
		// * matches exactly one token
		return matchParts(patternParts[1:], subjectParts[1:])
	}

	if patternParts[0] != subjectParts[0] {
		return false
	}

	return matchParts(patternParts[1:], subjectParts[1:])
}
