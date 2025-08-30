package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"github.com/disheap/disheap/disheap-engine/pkg/models"
	disheap_raft "github.com/disheap/disheap/disheap-engine/pkg/raft"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ServiceDependencies holds all the dependencies needed by the DisheapService
type ServiceDependencies struct {
	Storage  storage.Storage
	RaftNode *disheap_raft.Node
	Logger   *zap.Logger
}

// DisheapService implements the Disheap gRPC service
type DisheapService struct {
	disheapv1.UnimplementedDisheapServer

	storage  storage.Storage
	raftNode *disheap_raft.Node
	logger   *zap.Logger

	// Topic configurations
	topicsMu sync.RWMutex
	topics   map[string]*models.TopicConfig

	// Metrics and stats
	stats ServiceStats
}

// ServiceStats holds service-level statistics
type ServiceStats struct {
	mu sync.RWMutex

	TotalEnqueues   uint64
	TotalPops       uint64
	TotalAcks       uint64
	TotalNacks      uint64
	TotalExtends    uint64
	TotalTopics     uint64
	TotalPartitions uint64
	StartTime       time.Time
}

// NewDisheapService creates a new Disheap gRPC service
func NewDisheapService(deps ServiceDependencies) *DisheapService {
	if deps.Logger == nil {
		deps.Logger = zap.NewNop()
	}

	service := &DisheapService{
		storage:  deps.Storage,
		raftNode: deps.RaftNode,
		logger:   deps.Logger,
		topics:   make(map[string]*models.TopicConfig),
		stats: ServiceStats{
			StartTime: time.Now(),
		},
	}

	// Topics will be loaded from storage on-demand via getTopicConfig

	return service
}

// HealthCheck performs a health check of the service
func (s *DisheapService) HealthCheck(ctx context.Context) error {
	// Check storage health
	if s.storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	// Test storage connectivity with a simple stats call
	_, err := s.storage.Stats(ctx)
	if err != nil {
		return fmt.Errorf("storage health check failed: %w", err)
	}

	return nil
}

// Helper method to get topic config
func (s *DisheapService) getTopicConfig(topic string) (*models.TopicConfig, error) {
	// First, check in-memory cache
	s.topicsMu.RLock()
	config, exists := s.topics[topic]
	s.topicsMu.RUnlock()

	if exists {
		return config, nil
	}

	// If not in memory, try loading from storage (for replicated topics)
	ctx := context.Background()
	storageConfig, err := s.storage.GetTopicConfig(ctx, topic)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "topic %s not found", topic)
	}

	// Cache the loaded config in memory for future access
	s.topicsMu.Lock()
	s.topics[topic] = storageConfig
	s.topicsMu.Unlock()

	s.logger.Debug("Loaded topic config from storage",
		zap.String("topic", topic),
		zap.Uint32("partitions", storageConfig.Partitions))

	return storageConfig, nil
}

// Helper method to store topic config
func (s *DisheapService) storeTopicConfig(config *models.TopicConfig) error {
	s.topicsMu.Lock()
	s.topics[config.Name] = config
	s.topicsMu.Unlock()

	// Persist to storage
	if err := s.storage.StoreTopicConfig(context.Background(), config); err != nil {
		return fmt.Errorf("failed to persist topic config: %w", err)
	}

	return nil
}

// Helper method to delete topic config
func (s *DisheapService) deleteTopicConfig(topic string) error {
	s.topicsMu.Lock()
	delete(s.topics, topic)
	s.topicsMu.Unlock()

	// Remove from storage
	if err := s.storage.DeleteTopicConfig(context.Background(), topic); err != nil {
		return fmt.Errorf("failed to delete topic config: %w", err)
	}

	return nil
}

// Helper method to convert models.HeapMode to proto Mode
func heapModeToProto(mode models.HeapMode) disheapv1.Mode {
	switch mode {
	case models.MinHeap:
		return disheapv1.Mode_MIN
	case models.MaxHeap:
		return disheapv1.Mode_MAX
	default:
		return disheapv1.Mode_MIN
	}
}

// Helper method to convert proto Mode to models.HeapMode
func protoToHeapMode(mode disheapv1.Mode) models.HeapMode {
	switch mode {
	case disheapv1.Mode_MIN:
		return models.MinHeap
	case disheapv1.Mode_MAX:
		return models.MaxHeap
	default:
		return models.MinHeap
	}
}

// Helper method to convert models.DLQPolicy to proto DLQPolicy
func dlqPolicyToProto(policy *models.DLQPolicy) interface{} {
	// TODO: Implement when DLQPolicy is properly defined in proto
	return nil
}

// Helper method to convert proto DLQPolicy to models.DLQPolicy
func protoDLQPolicyToModel(proto interface{}) *models.DLQPolicy {
	// TODO: Implement when DLQPolicy is properly defined in proto
	return &models.DLQPolicy{
		Type:          models.DLQPolicyNone,
		RetentionTime: 24 * time.Hour, // Default
	}
}

// Helper method to convert models.TopicConfig to proto HeapInfo
func topicConfigToHeapInfo(config *models.TopicConfig) *disheapv1.HeapInfo {
	return &disheapv1.HeapInfo{
		Topic:                    config.Name,
		Mode:                     heapModeToProto(config.Mode),
		Partitions:               config.Partitions,
		ReplicationFactor:        config.ReplicationFactor,
		TopKBound:                config.TopKBound,
		RetentionTime:            durationpb.New(config.RetentionTime),
		VisibilityTimeoutDefault: durationpb.New(config.VisibilityTimeoutDefault),
		MaxRetriesDefault:        config.MaxRetriesDefault,
		MaxPayloadBytes:          config.MaxPayloadBytes,
		CompressionEnabled:       config.CompressionEnabled,
		// DlqPolicy:                dlqPolicyToProto(&config.DLQPolicy), // TODO: Fix proto definition
		CreatedAt: timestamppb.New(config.CreatedAt),
		UpdatedAt: timestamppb.New(config.UpdatedAt),
	}
}

// Helper method to update service stats
func (s *ServiceStats) IncrementEnqueues() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalEnqueues++
}

func (s *ServiceStats) IncrementPops() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalPops++
}

func (s *ServiceStats) IncrementAcks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalAcks++
}

func (s *ServiceStats) IncrementNacks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalNacks++
}

func (s *ServiceStats) IncrementExtends() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalExtends++
}

func (s *ServiceStats) GetStats() (uint64, uint64, uint64, uint64, uint64, uint64, uint64, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TotalEnqueues, s.TotalPops, s.TotalAcks, s.TotalNacks,
		s.TotalExtends, s.TotalTopics, s.TotalPartitions, s.StartTime
}

// storeTopicConfigViaRaft stores topic configuration using Raft consensus
func (s *DisheapService) storeTopicConfigViaRaft(config *models.TopicConfig) error {
	if s.raftNode == nil {
		return fmt.Errorf("raft node not available")
	}

	// Create ConfigUpdateEntry for Raft
	configEntry := disheap_raft.ConfigUpdateEntry{
		ConfigType: "topic",
		ConfigID:   config.Name,
		OldConfig:  nil, // No old config for new topics
		NewConfig:  config,
	}

	// Create Raft log entry with encoded data
	logEntry, err := disheap_raft.NewLogEntry(
		disheap_raft.EntryConfigUpdate,
		config.Name,
		0, // Config updates don't belong to a specific partition
		configEntry,
	)
	if err != nil {
		return fmt.Errorf("failed to create log entry: %w", err)
	}

	// Apply through Raft consensus with 10 second timeout
	if err := s.raftNode.Apply(logEntry, 10*time.Second); err != nil {
		return fmt.Errorf("failed to apply config via raft: %w", err)
	}

	// Also store locally in memory for immediate access
	// The FSM will handle persistent storage
	s.topicsMu.Lock()
	s.topics[config.Name] = config
	s.topicsMu.Unlock()

	s.logger.Info("Topic configuration stored via Raft consensus",
		zap.String("topic", config.Name),
		zap.Uint32("partitions", config.Partitions),
		zap.Uint32("replication_factor", config.ReplicationFactor))

	return nil
}
