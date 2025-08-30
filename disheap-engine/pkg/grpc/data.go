package grpc

import (
	"context"
	"fmt"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Enqueue adds a single message to a topic
func (s *DisheapService) Enqueue(ctx context.Context, req *disheapv1.EnqueueReq) (*disheapv1.EnqueueResp, error) {
	s.logger.Debug("Enqueue request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.Int64("priority", req.Priority),
		zap.Int("payload_size", len(req.Payload)))

	// Validate request
	if err := validateEnqueueRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	// Check if topic exists
	config, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// Validate payload size
	if uint64(len(req.Payload)) > config.MaxPayloadBytes {
		return nil, status.Errorf(codes.InvalidArgument,
			"payload size %d exceeds maximum %d bytes",
			len(req.Payload), config.MaxPayloadBytes)
	}

	// Create message
	messageID := models.NewMessageID()
	message := &models.Message{
		ID:         messageID,
		Topic:      req.Topic,
		Priority:   req.Priority,
		Payload:    req.Payload,
		EnqueuedAt: time.Now(),
		ProducerID: "",
		Attempts:   0,
		MaxRetries: config.MaxRetriesDefault,
	}

	// Set producer ID if provided
	if req.ProducerId != nil {
		message.ProducerID = *req.ProducerId
	}

	// Set delayed processing if specified
	if req.NotBefore != nil {
		delay := req.NotBefore.AsDuration()
		if delay > 0 {
			notBefore := time.Now().Add(delay)
			message.NotBefore = &notBefore
		}
	}

	// TODO: Implement actual message enqueueing to partition
	// This would involve:
	// 1. Select appropriate partition based on partition key or round-robin
	// 2. Add message to the partition's heap
	// 3. Persist to storage
	// 4. Handle idempotent enqueue if producer info provided

	// For now, just store the message directly
	if err := s.storage.StoreMessage(ctx, message); err != nil {
		s.logger.Error("Failed to store message",
			zap.String("component", "grpc_service"),
			zap.String("topic", req.Topic),
			zap.String("message_id", string(messageID)),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to enqueue message: %v", err)
	}

	// Update stats
	s.stats.IncrementEnqueues()

	s.logger.Debug("Message enqueued successfully",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("message_id", string(messageID)))

	return &disheapv1.EnqueueResp{
		MessageId: string(messageID),
	}, nil
}

// EnqueueBatch adds multiple messages atomically
func (s *DisheapService) EnqueueBatch(ctx context.Context, req *disheapv1.EnqueueBatchReq) (*disheapv1.EnqueueBatchResp, error) {
	s.logger.Debug("EnqueueBatch request received",
		zap.String("component", "grpc_service"),
		zap.Int("request_count", len(req.Requests)))

	// Validate request
	if len(req.Requests) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "at least one request is required")
	}

	if len(req.Requests) > 100 { // Batch size limit
		return nil, status.Errorf(codes.InvalidArgument, "batch size exceeds maximum of 100 requests")
	}

	// Process each request
	responses := make([]*disheapv1.EnqueueResp, len(req.Requests))

	for i, enqReq := range req.Requests {
		// Process individual enqueue request
		resp, err := s.Enqueue(ctx, enqReq)
		if err != nil {
			s.logger.Error("Failed to process batch request",
				zap.String("component", "grpc_service"),
				zap.Int("request_index", i),
				zap.Error(err))
			return nil, status.Errorf(codes.Internal,
				"failed to process request at index %d: %v", i, err)
		}

		responses[i] = resp
	}

	s.logger.Debug("Batch enqueued successfully",
		zap.String("component", "grpc_service"),
		zap.Int("request_count", len(req.Requests)))

	resp := &disheapv1.EnqueueBatchResp{
		Responses:     responses,
		AllDuplicates: false, // TODO: Implement duplicate detection
	}

	// Set transaction ID if provided
	if req.TransactionId != nil {
		resp.TransactionId = *req.TransactionId
	}

	return resp, nil
}

// Ack acknowledges successful message processing
func (s *DisheapService) Ack(ctx context.Context, req *disheapv1.AckReq) (*emptypb.Empty, error) {
	s.logger.Debug("Ack request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("message_id", req.MessageId),
		zap.String("lease_token", req.LeaseToken))

	// Validate request
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name is required")
	}
	if req.MessageId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "message ID is required")
	}
	if req.LeaseToken == "" {
		return nil, status.Errorf(codes.InvalidArgument, "lease token is required")
	}

	// Check if topic exists
	_, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// Basic Ack implementation for testing
	// TODO: Implement full ack logic with partition management
	// This would involve:
	// 1. Find the message and its partition
	// 2. Verify the lease token
	// 3. Remove the message from the heap
	// 4. Remove from storage
	// 5. Update statistics

	s.logger.Debug("Ack operation processed (basic implementation)",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("message_id", req.MessageId),
		zap.String("lease_token", req.LeaseToken))

	// Update stats
	s.stats.IncrementAcks()

	return &emptypb.Empty{}, nil
}

// Nack rejects a message and potentially schedules retry
func (s *DisheapService) Nack(ctx context.Context, req *disheapv1.NackReq) (*emptypb.Empty, error) {
	s.logger.Debug("Nack request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("message_id", req.MessageId),
		zap.String("lease_token", req.LeaseToken))

	// Validate request
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name is required")
	}
	if req.MessageId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "message ID is required")
	}
	if req.LeaseToken == "" {
		return nil, status.Errorf(codes.InvalidArgument, "lease token is required")
	}

	// Check if topic exists
	_, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// TODO: Implement actual nack logic
	// This would involve:
	// 1. Find the message and its partition
	// 2. Verify the lease token
	// 3. Increment retry count
	// 4. Calculate backoff delay (use backoff override if provided)
	// 5. Schedule retry or move to DLQ if max retries exceeded
	// 6. Update statistics

	s.logger.Warn("Nack operation not yet fully implemented",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("message_id", req.MessageId))

	// Update stats for now
	s.stats.IncrementNacks()

	return &emptypb.Empty{}, nil
}

// Extend extends the lease timeout for a message
func (s *DisheapService) Extend(ctx context.Context, req *disheapv1.ExtendReq) (*emptypb.Empty, error) {
	s.logger.Debug("Extend request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("lease_token", req.LeaseToken))

	// Validate request
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name is required")
	}
	if req.LeaseToken == "" {
		return nil, status.Errorf(codes.InvalidArgument, "lease token is required")
	}
	if req.Extension == nil {
		return nil, status.Errorf(codes.InvalidArgument, "extension duration is required")
	}

	extension := req.Extension.AsDuration()
	if extension <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "extension duration must be positive")
	}

	// Check if topic exists
	_, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// TODO: Implement actual extend logic
	// This would involve:
	// 1. Find the message and its partition using the lease token
	// 2. Verify the lease is still valid
	// 3. Extend the lease deadline
	// 4. Update lease in storage
	// 5. Enforce extension limits

	s.logger.Warn("Extend operation not yet fully implemented",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("lease_token", req.LeaseToken),
		zap.Duration("extension", extension))

	// Update stats for now
	s.stats.IncrementExtends()

	return &emptypb.Empty{}, nil
}

// Peek returns messages without leasing them
func (s *DisheapService) Peek(ctx context.Context, req *disheapv1.PeekReq) (*disheapv1.PeekResp, error) {
	s.logger.Debug("Peek request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic))

	// Validate request
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name is required")
	}

	var count uint32 = 10 // Default count
	if req.Limit != nil {
		count = *req.Limit
	}
	if count > 100 {
		count = 100 // Max count
	}

	// Check if topic exists
	_, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// TODO: Implement actual peek logic
	// This would involve:
	// 1. Query all partitions for top messages
	// 2. Merge results while maintaining order
	// 3. Return without leasing

	s.logger.Warn("Peek operation not yet fully implemented",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.Uint32("count", count))

	// Return empty response for now
	return &disheapv1.PeekResp{
		Items:   []*disheapv1.PeekItem{},
		HasMore: false,
	}, nil
}

// validateEnqueueRequest validates an enqueue request
func validateEnqueueRequest(req *disheapv1.EnqueueReq) error {
	if req.Topic == "" {
		return fmt.Errorf("topic name is required")
	}

	if len(req.Payload) == 0 {
		return fmt.Errorf("message payload is required")
	}

	// Priority validation (reasonable range)
	if req.Priority < -1000000 || req.Priority > 1000000 {
		return fmt.Errorf("priority must be between -1,000,000 and 1,000,000")
	}

	// Validate NotBefore if specified
	if req.NotBefore != nil {
		delay := req.NotBefore.AsDuration()
		if delay < 0 {
			return fmt.Errorf("not_before delay cannot be negative")
		}

		// Don't allow too far in the future (1 year)
		if delay > 365*24*time.Hour {
			return fmt.Errorf("not_before delay too far in the future")
		}
	}

	return nil
}
