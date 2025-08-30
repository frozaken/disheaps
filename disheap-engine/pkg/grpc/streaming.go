package grpc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PopStream implements the streaming pop operation
func (s *DisheapService) PopStream(stream disheapv1.Disheap_PopStreamServer) error {
	ctx := stream.Context()

	s.logger.Debug("PopStream connection established",
		zap.String("component", "grpc_service"))

	// Create a stream handler
	handler := &streamHandler{
		service: s,
		stream:  stream,
		ctx:     ctx,
		logger:  s.logger.With(zap.String("component", "pop_stream")),
		done:    make(chan struct{}),
	}

	// Handle the stream
	return handler.handle()
}

// streamHandler manages a single PopStream connection
type streamHandler struct {
	service *DisheapService
	stream  disheapv1.Disheap_PopStreamServer
	ctx     context.Context
	logger  *zap.Logger

	// Stream state
	mu          sync.RWMutex
	topic       string
	maxMessages uint32
	timeout     time.Duration
	active      bool
	done        chan struct{}

	// Flow control
	credits        uint32
	totalDelivered uint64
	totalRequested uint64
}

// handle processes the bidirectional stream
func (h *streamHandler) handle() error {
	defer close(h.done)

	h.logger.Debug("Starting PopStream handler")

	// Start goroutines for receiving and sending
	errCh := make(chan error, 2)

	// Receive loop
	go func() {
		errCh <- h.receiveLoop()
	}()

	// Send loop
	go func() {
		errCh <- h.sendLoop()
	}()

	// Wait for first error or context cancellation
	select {
	case err := <-errCh:
		if err != nil && err != io.EOF {
			// Check if this is a normal disconnection
			if isNormalDisconnection(err) {
				h.logger.Debug("PopStream client disconnected normally")
				return nil
			}
			h.logger.Error("PopStream error", zap.Error(err))
			return err
		}
	case <-h.ctx.Done():
		h.logger.Debug("PopStream context cancelled")
		return h.ctx.Err()
	}

	h.logger.Debug("PopStream completed")
	return nil
}

// receiveLoop handles incoming PopOpen messages
func (h *streamHandler) receiveLoop() error {
	for {
		req, err := h.stream.Recv()
		if err != nil {
			if err == io.EOF {
				h.logger.Debug("PopStream client closed connection")
				return nil
			}

			// Check if this is a context cancellation (normal disconnect)
			if st, ok := status.FromError(err); ok {
				switch st.Code() {
				case codes.Canceled:
					h.logger.Debug("PopStream client disconnected (context canceled)")
					return nil
				case codes.DeadlineExceeded:
					h.logger.Debug("PopStream client disconnected (deadline exceeded)")
					return nil
				case codes.Unavailable:
					h.logger.Debug("PopStream client disconnected (unavailable)")
					return nil
				}
			}

			// Log actual errors
			h.logger.Error("Failed to receive PopOpen", zap.Error(err))
			return err
		}

		if err := h.handlePopOpen(req); err != nil {
			return err
		}
	}
}

// sendLoop handles outgoing PopItem messages
func (h *streamHandler) sendLoop() error {
	ticker := time.NewTicker(100 * time.Millisecond) // Check for messages every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case <-h.done:
			return nil
		case <-ticker.C:
			if err := h.tryDeliverMessages(); err != nil {
				return err
			}
		}
	}
}

// handlePopOpen processes a PopOpen message
func (h *streamHandler) handlePopOpen(req *disheapv1.PopOpen) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.logger.Debug("Received PopOpen", zap.Any("request", req))

	// Handle different types of PopOpen messages
	switch req.Kind.(type) {
	case *disheapv1.PopOpen_FlowControl:
		// Handle flow control
		fc := req.GetFlowControl()
		if fc != nil {
			h.credits += fc.Credits
			h.totalRequested += uint64(fc.Credits)
			h.logger.Debug("Flow control updated", zap.Uint32("credits", fc.Credits))
		}

	case *disheapv1.PopOpen_Filter:
		// Handle filter setup
		filter := req.GetFilter()
		if filter != nil {
			h.topic = filter.Topic
			h.active = true
			h.logger.Debug("Filter configured", zap.String("topic", filter.Topic))

			// Check if topic exists
			if h.topic != "" {
				_, err := h.service.getTopicConfig(h.topic)
				if err != nil {
					return err
				}
			}
		}

	default:
		h.logger.Warn("Unknown PopOpen message type")
	}

	return nil
}

// tryDeliverMessages attempts to deliver messages to the client
func (h *streamHandler) tryDeliverMessages() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if stream is active and has credits
	if !h.active || h.credits == 0 || h.topic == "" {
		return nil
	}

	// Check if we've reached the message limit
	if h.maxMessages > 0 && h.totalDelivered >= uint64(h.maxMessages) {
		h.logger.Debug("Message limit reached",
			zap.Uint64("delivered", h.totalDelivered),
			zap.Uint32("limit", h.maxMessages))
		return nil
	}

	// Get topic configuration to verify topic exists
	_, err := h.service.getTopicConfig(h.topic)
	if err != nil {
		h.logger.Error("Failed to get topic config", zap.Error(err))
		return err
	}

	// For now, we'll create test messages since the coordinator integration is not complete
	// TODO: Integrate with actual coordinator/partition manager

	// Try to pop a message from the topic
	// For now, we'll try to get one message at a time
	messagesDelivered := uint32(0)
	maxToDeliver := h.credits
	if h.maxMessages > 0 && uint64(maxToDeliver) > (uint64(h.maxMessages)-h.totalDelivered) {
		maxToDeliver = uint32(uint64(h.maxMessages) - h.totalDelivered)
	}

	for messagesDelivered < maxToDeliver {
		// Try to pop a message (this is a simplified implementation)
		// In a real implementation, this would interface with the partition manager
		// For now, we'll create a mock message to test the flow

		// TODO: Replace with actual message retrieval from partitions
		// This is a temporary implementation to test the streaming flow
		messageID := fmt.Sprintf("msg_%d_%d", time.Now().UnixNano(), messagesDelivered)
		payload := fmt.Sprintf("Test message %d for topic %s", messagesDelivered+1, h.topic)
		priority := int64(100)
		leaseToken := fmt.Sprintf("lease_%d_%d", time.Now().UnixNano(), messagesDelivered)
		deadline := time.Now().Add(5 * time.Minute)
		attempts := uint32(1)

		// Deliver the message
		if err := h.deliverMessage(messageID, payload, priority, leaseToken, deadline, attempts); err != nil {
			h.logger.Error("Failed to deliver message", zap.Error(err))
			return err
		}

		messagesDelivered++

		// For testing, only deliver one message per call to avoid flooding
		break
	}

	if messagesDelivered == 0 {
		h.logger.Debug("No messages available for delivery",
			zap.String("topic", h.topic),
			zap.Uint32("credits", h.credits))
	} else {
		h.logger.Debug("Messages delivered",
			zap.String("topic", h.topic),
			zap.Uint32("delivered", messagesDelivered),
			zap.Uint32("remaining_credits", h.credits))
	}

	return nil
}

// deliverMessage sends a message to the client
func (h *streamHandler) deliverMessage(messageID, payload string, priority int64, leaseToken string, deadline time.Time, attempts uint32) error {
	item := &disheapv1.PopItem{
		MessageId:     messageID,
		Topic:         h.topic,
		PartitionId:   0, // TODO: Set correct partition ID
		Payload:       []byte(payload),
		Priority:      priority,
		EnqueuedTime:  timestamppb.New(time.Now().Add(-1 * time.Minute)), // Mock enqueue time
		LeaseToken:    leaseToken,
		LeaseDeadline: timestamppb.New(deadline),
		Attempts:      attempts,
		MaxRetries:    3,                          // TODO: Get from topic config
		ProducerId:    stringPtr("test-producer"), // TODO: Get actual producer ID
		Epoch:         uint64Ptr(1),               // TODO: Get actual epoch
		Sequence:      uint64Ptr(1),               // TODO: Get actual sequence
	}

	if err := h.stream.Send(item); err != nil {
		// Check if this is a normal disconnection
		if isNormalDisconnection(err) {
			h.logger.Debug("Client disconnected during message send",
				zap.String("message_id", messageID))
			return err // Return error to stop the stream, but don't log as error
		}

		h.logger.Error("Failed to send PopItem",
			zap.String("message_id", messageID),
			zap.Error(err))
		return err
	}

	// Update flow control and stats
	h.credits--
	h.totalDelivered++
	h.service.stats.IncrementPops()

	h.logger.Debug("Message delivered",
		zap.String("message_id", messageID),
		zap.String("lease_token", leaseToken),
		zap.Uint32("remaining_credits", h.credits))

	return nil
}

// StreamStats returns statistics about active streams
type StreamStats struct {
	ActiveStreams     uint32 `json:"active_streams"`
	TotalConnected    uint64 `json:"total_connected"`
	TotalDisconnected uint64 `json:"total_disconnected"`
	MessagesDelivered uint64 `json:"messages_delivered"`
}

// StreamManager manages all active PopStream connections
type StreamManager struct {
	mu      sync.RWMutex
	streams map[string]*streamHandler // connection ID -> handler
	stats   StreamStats
	logger  *zap.Logger
}

// NewStreamManager creates a new stream manager
func NewStreamManager(logger *zap.Logger) *StreamManager {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &StreamManager{
		streams: make(map[string]*streamHandler),
		logger:  logger,
	}
}

// RegisterStream registers a new stream handler
func (sm *StreamManager) RegisterStream(id string, handler *streamHandler) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.streams[id] = handler
	sm.stats.ActiveStreams++
	sm.stats.TotalConnected++

	sm.logger.Debug("Stream registered",
		zap.String("component", "stream_manager"),
		zap.String("stream_id", id),
		zap.Uint32("active_streams", sm.stats.ActiveStreams))
}

// UnregisterStream removes a stream handler
func (sm *StreamManager) UnregisterStream(id string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.streams[id]; exists {
		delete(sm.streams, id)
		if sm.stats.ActiveStreams > 0 {
			sm.stats.ActiveStreams--
		}
		sm.stats.TotalDisconnected++

		sm.logger.Debug("Stream unregistered",
			zap.String("component", "stream_manager"),
			zap.String("stream_id", id),
			zap.Uint32("active_streams", sm.stats.ActiveStreams))
	}
}

// GetStats returns stream manager statistics
func (sm *StreamManager) GetStats() StreamStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.stats
}

// BroadcastHealthCheck sends health checks to all active streams
func (sm *StreamManager) BroadcastHealthCheck(ctx context.Context) {
	sm.mu.RLock()
	streams := make([]*streamHandler, 0, len(sm.streams))
	for _, handler := range sm.streams {
		streams = append(streams, handler)
	}
	sm.mu.RUnlock()

	for _, _ = range streams {
		select {
		case <-ctx.Done():
			return
		default:
			// Could send health check message or perform other maintenance
		}
	}
}

// Helper functions for creating pointers
func stringPtr(s string) *string {
	return &s
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}

// isNormalDisconnection checks if an error represents a normal client disconnection
func isNormalDisconnection(err error) bool {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Canceled:
			return true
		case codes.DeadlineExceeded:
			return true
		case codes.Unavailable:
			return true
		}
	}
	return false
}
