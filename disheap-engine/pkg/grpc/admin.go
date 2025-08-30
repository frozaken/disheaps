package grpc

import (
	"context"
	"fmt"
	"strings"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// MakeHeap creates a new message heap (topic)
func (s *DisheapService) MakeHeap(ctx context.Context, req *disheapv1.MakeHeapReq) (*disheapv1.MakeHeapResp, error) {
	s.logger.Info("MakeHeap request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("mode", req.Mode.String()),
		zap.Uint32("partitions", req.Partitions),
		zap.Uint32("replication_factor", req.ReplicationFactor))

	// Validate request
	if err := validateMakeHeapRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	// Check if topic already exists
	s.topicsMu.RLock()
	_, exists := s.topics[req.Topic]
	s.topicsMu.RUnlock()

	if exists {
		return nil, status.Errorf(codes.AlreadyExists, "topic %s already exists", req.Topic)
	}

	// Create topic configuration
	config := &models.TopicConfig{
		Name:                     req.Topic,
		Mode:                     protoToHeapMode(req.Mode),
		Partitions:               req.Partitions,
		ReplicationFactor:        req.ReplicationFactor,
		TopKBound:                req.TopKBound,
		RetentionTime:            req.RetentionTime.AsDuration(),
		VisibilityTimeoutDefault: req.VisibilityTimeoutDefault.AsDuration(),
		MaxRetriesDefault:        req.MaxRetriesDefault,
		MaxPayloadBytes:          req.MaxPayloadBytes,
		CompressionEnabled:       req.CompressionEnabled,
		DLQPolicy:                *protoDLQPolicyToModel(nil), // TODO: Fix proto definition
		CreatedAt:                time.Now(),
		UpdatedAt:                time.Now(),
	}

	// Validate the configuration
	if err := config.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid configuration: %v", err)
	}

	// Store the topic configuration through Raft consensus
	if err := s.storeTopicConfigViaRaft(config); err != nil {
		s.logger.Error("Failed to store topic config via Raft",
			zap.String("component", "grpc_service"),
			zap.String("topic", req.Topic),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	// Update stats
	s.stats.mu.Lock()
	s.stats.TotalTopics++
	s.stats.TotalPartitions += uint64(req.Partitions)
	s.stats.mu.Unlock()

	s.logger.Info("Topic created successfully",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.Uint32("partitions", req.Partitions))

	return &disheapv1.MakeHeapResp{
		Topic:  req.Topic,
		HeapId: fmt.Sprintf("%s-%d", req.Topic, time.Now().Unix()),
	}, nil
}

// DeleteHeap deletes an existing message heap (topic)
func (s *DisheapService) DeleteHeap(ctx context.Context, req *disheapv1.DeleteHeapReq) (*emptypb.Empty, error) {
	s.logger.Info("DeleteHeap request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.Bool("force", req.Force))

	// Validate request
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name is required")
	}

	// Check if topic exists
	config, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// For safety, require force=true for deletion
	if !req.Force {
		return nil, status.Errorf(codes.FailedPrecondition,
			"deletion requires force=true to prevent accidental data loss")
	}

	// TODO: Check if topic has active messages/consumers
	// For now, we'll allow deletion if force=true

	// Delete the topic configuration
	if err := s.deleteTopicConfig(req.Topic); err != nil {
		s.logger.Error("Failed to delete topic config",
			zap.String("component", "grpc_service"),
			zap.String("topic", req.Topic),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to delete topic: %v", err)
	}

	// Update stats
	s.stats.mu.Lock()
	if s.stats.TotalTopics > 0 {
		s.stats.TotalTopics--
	}
	if s.stats.TotalPartitions >= uint64(config.Partitions) {
		s.stats.TotalPartitions -= uint64(config.Partitions)
	}
	s.stats.mu.Unlock()

	s.logger.Info("Topic deleted successfully",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic))

	return &emptypb.Empty{}, nil
}

// UpdateHeapConfig updates configuration for an existing heap
func (s *DisheapService) UpdateHeapConfig(ctx context.Context, req *disheapv1.UpdateHeapConfigReq) (*emptypb.Empty, error) {
	s.logger.Info("UpdateHeapConfig request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic))

	// Validate request
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name is required")
	}

	// Get existing topic configuration
	config, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// Create a copy to modify
	updatedConfig := *config
	updatedConfig.UpdatedAt = time.Now()

	// Apply updates (only if specified in request)
	if req.RetentionTime != nil {
		updatedConfig.RetentionTime = req.RetentionTime.AsDuration()
	}
	if req.VisibilityTimeoutDefault != nil {
		updatedConfig.VisibilityTimeoutDefault = req.VisibilityTimeoutDefault.AsDuration()
	}
	if req.MaxRetriesDefault != nil {
		updatedConfig.MaxRetriesDefault = *req.MaxRetriesDefault
	}
	if req.MaxPayloadBytes != nil {
		updatedConfig.MaxPayloadBytes = *req.MaxPayloadBytes
	}
	if req.CompressionEnabled != nil {
		updatedConfig.CompressionEnabled = *req.CompressionEnabled
	}
	if req.DlqPolicy != nil {
		updatedConfig.DLQPolicy = *protoDLQPolicyToModel(req.DlqPolicy)
	}

	// Validate the updated configuration
	if err := updatedConfig.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid updated configuration: %v", err)
	}

	// Store the updated configuration
	if err := s.storeTopicConfig(&updatedConfig); err != nil {
		s.logger.Error("Failed to update topic config",
			zap.String("component", "grpc_service"),
			zap.String("topic", req.Topic),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update topic: %v", err)
	}

	s.logger.Info("Topic configuration updated successfully",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic))

	return &emptypb.Empty{}, nil
}

// ListHeaps returns a list of all heaps with pagination
func (s *DisheapService) ListHeaps(ctx context.Context, req *disheapv1.ListHeapsReq) (*disheapv1.ListHeapsResp, error) {
	s.logger.Debug("ListHeaps request received",
		zap.String("component", "grpc_service"),
		zap.Uint32("page_size", req.PageSize),
		zap.String("page_token", req.PageToken))

	// Set default page size if not specified
	pageSize := req.PageSize
	if pageSize == 0 {
		pageSize = 50 // Default page size
	}
	if pageSize > 1000 {
		pageSize = 1000 // Max page size
	}

	// Get all topics
	s.topicsMu.RLock()
	allTopics := make([]*models.TopicConfig, 0, len(s.topics))
	for _, config := range s.topics {
		allTopics = append(allTopics, config)
	}
	s.topicsMu.RUnlock()

	// Simple pagination (in production, this would be more sophisticated)
	startIdx := 0
	if req.PageToken != "" {
		// Parse the token to get start index (simplified implementation)
		if parsed, err := parseInt(req.PageToken); err == nil && parsed >= 0 {
			startIdx = parsed
		}
	}

	// Calculate end index
	endIdx := startIdx + int(pageSize)
	if endIdx > len(allTopics) {
		endIdx = len(allTopics)
	}

	// Get page of topics
	var pageTopics []*models.TopicConfig
	if startIdx < len(allTopics) {
		pageTopics = allTopics[startIdx:endIdx]
	}

	// Convert to proto format
	heaps := make([]*disheapv1.HeapInfo, len(pageTopics))
	for i, config := range pageTopics {
		heaps[i] = topicConfigToHeapInfo(config)
	}

	// Calculate next page token
	var nextPageToken string
	if endIdx < len(allTopics) {
		nextPageToken = fmt.Sprintf("%d", endIdx)
	}

	return &disheapv1.ListHeapsResp{
		Heaps:         heaps,
		NextPageToken: nextPageToken,
	}, nil
}

// Stats returns statistics about the service
func (s *DisheapService) Stats(ctx context.Context, req *disheapv1.StatsReq) (*disheapv1.StatsResp, error) {
	s.logger.Debug("Stats request received",
		zap.String("component", "grpc_service"))

	// Get service stats
	totalEnqueues, totalPops, totalAcks, totalNacks, _, _, _, _ := s.stats.GetStats()

	// Get storage stats
	storageStats, err := s.storage.Stats(ctx)
	if err != nil {
		s.logger.Error("Failed to get storage stats",
			zap.String("component", "grpc_service"),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get storage stats: %v", err)
	}

	globalStats := &disheapv1.HeapStats{
		TotalMessages:    storageStats.TotalMessages,
		InflightMessages: storageStats.TotalLeases, // Approximate
		DlqMessages:      0,                        // TODO: Get from DLQ manager
		TotalEnqueues:    totalEnqueues,
		TotalPops:        totalPops,
		TotalAcks:        totalAcks,
		TotalNacks:       totalNacks,
		TotalRetries:     0, // TODO: Track retries
		TotalTimeouts:    0, // TODO: Track timeouts
		PartitionStats:   make(map[uint32]*disheapv1.PartitionStats),
	}

	return &disheapv1.StatsResp{
		GlobalStats: globalStats,
		TopicStats:  make(map[string]*disheapv1.HeapStats), // TODO: Per-topic stats
	}, nil
}

// Purge removes all messages from a topic
func (s *DisheapService) Purge(ctx context.Context, req *disheapv1.PurgeReq) (*emptypb.Empty, error) {
	s.logger.Info("Purge request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic))

	// Validate request
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name is required")
	}

	// Check if topic exists
	_, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// TODO: Implement purge logic
	// This would involve:
	// 1. Clear all partitions for the topic
	// 2. Remove all messages from storage
	// 3. Reset partition statistics

	s.logger.Warn("Purge operation not yet fully implemented",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic))

	return nil, status.Errorf(codes.Unimplemented, "purge operation not yet implemented")
}

// MoveToDLQ moves messages to dead letter queue
func (s *DisheapService) MoveToDLQ(ctx context.Context, req *disheapv1.MoveToDlqReq) (*emptypb.Empty, error) {
	s.logger.Info("MoveToDLQ request received",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("message_id", req.MessageId))

	// Validate request
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic name is required")
	}
	if req.MessageId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "message ID is required")
	}

	// Check if topic exists
	_, err := s.getTopicConfig(req.Topic)
	if err != nil {
		return nil, err
	}

	// TODO: Implement move to DLQ logic
	// This would involve:
	// 1. Find the messages in their partitions
	// 2. Move them to the DLQ heap
	// 3. Update statistics

	s.logger.Warn("MoveToDLQ operation not yet fully implemented",
		zap.String("component", "grpc_service"),
		zap.String("topic", req.Topic),
		zap.String("message_id", req.MessageId))

	return nil, status.Errorf(codes.Unimplemented, "MoveToDLQ operation not yet implemented")
}

// validateMakeHeapRequest validates the MakeHeap request
func validateMakeHeapRequest(req *disheapv1.MakeHeapReq) error {
	if req.Topic == "" {
		return fmt.Errorf("topic name is required")
	}

	if strings.Contains(req.Topic, " ") || strings.Contains(req.Topic, "\t") {
		return fmt.Errorf("topic name cannot contain spaces")
	}

	if req.Partitions == 0 {
		return fmt.Errorf("partitions must be greater than 0")
	}

	if req.ReplicationFactor == 0 {
		return fmt.Errorf("replication factor must be greater than 0")
	}

	if req.ReplicationFactor > req.Partitions {
		return fmt.Errorf("replication factor cannot be greater than partitions")
	}

	if req.TopKBound == 0 {
		return fmt.Errorf("top K bound must be greater than 0")
	}

	if req.RetentionTime == nil {
		return fmt.Errorf("retention time is required")
	}

	if req.RetentionTime.AsDuration() <= 0 {
		return fmt.Errorf("retention time must be positive")
	}

	if req.VisibilityTimeoutDefault == nil {
		return fmt.Errorf("visibility timeout default is required")
	}

	if req.VisibilityTimeoutDefault.AsDuration() <= 0 {
		return fmt.Errorf("visibility timeout default must be positive")
	}

	if req.MaxPayloadBytes == 0 {
		return fmt.Errorf("max payload bytes must be greater than 0")
	}

	return nil
}

// Helper function to parse integer from string (simplified)
func parseInt(s string) (int, error) {
	var result int
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid integer")
		}
		result = result*10 + int(r-'0')
	}
	return result, nil
}
