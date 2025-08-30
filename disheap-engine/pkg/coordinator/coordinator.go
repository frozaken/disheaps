package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/partition"
	"github.com/disheap/disheap/disheap-engine/pkg/spine"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
	"go.uber.org/zap"
)

// Coordinator orchestrates operations across multiple partitions
type Coordinator interface {
	// Multi-partition operations
	GlobalPop(ctx context.Context, topic string, count int) ([]*models.Message, error)
	GlobalPeek(ctx context.Context, topic string, count int) ([]*models.Message, error)
	GlobalStats(ctx context.Context, topic string) (*GlobalTopicStats, error)

	// Partition management
	AddPartition(topic string, partitionID models.PartitionID, config *models.TopicConfig) error
	RemovePartition(topic string, partitionID models.PartitionID) error
	GetPartition(topic string, partitionID models.PartitionID) (partition.Partition, error)
	ListPartitions(topic string) ([]models.PartitionInfo, error)

	// Topic management
	CreateTopic(config *models.TopicConfig) error
	DeleteTopic(topic string) error
	UpdateTopicConfig(topic string, config *models.TopicConfig) error
	GetTopicConfig(topic string) (*models.TopicConfig, error)
	ListTopics() ([]*models.TopicConfig, error)

	// Rebalancing
	TriggerRebalance(topic string) error
	GetRebalanceStatus(topic string) (*RebalanceStatus, error)

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	HealthCheck(ctx context.Context) error
	GetStats() *CoordinatorStats
}

// MultiPartitionCoordinator implements distributed coordination across partitions
type MultiPartitionCoordinator struct {
	mu     sync.RWMutex
	logger *zap.Logger
	config *CoordinatorConfig

	// Core components
	router     PartitionRouter
	spineIndex spine.SpineIndex
	storage    storage.Storage

	// Partition management
	partitions   map[string]map[models.PartitionID]partition.Partition // topic -> partition_id -> partition
	topicConfigs map[string]*models.TopicConfig                        // topic -> config

	// Rebalancing
	rebalancer *Rebalancer

	// State
	running bool

	// Statistics
	stats CoordinatorStats
}

// CoordinatorConfig holds configuration for the coordinator
type CoordinatorConfig struct {
	SpineIndexConfig      spine.SpineIndexConfig `json:"spine_index_config"`
	RebalanceConfig       RebalanceConfig        `json:"rebalance_config"`
	MaxPartitionsPerTopic int                    `json:"max_partitions_per_topic"`
	DefaultTopicConfig    models.TopicConfig     `json:"default_topic_config"`

	// Performance tuning
	GlobalOpTimeout      time.Duration `json:"global_op_timeout"`
	PartitionOpTimeout   time.Duration `json:"partition_op_timeout"`
	SpineCandidateUpdate time.Duration `json:"spine_candidate_update_interval"`
}

// DefaultCoordinatorConfig returns a default coordinator configuration
func DefaultCoordinatorConfig() *CoordinatorConfig {
	return &CoordinatorConfig{
		SpineIndexConfig: spine.SpineIndexConfig{
			TopK:   100,
			FanOut: 4,
			Mode:   models.MinHeap,
			MaxAge: 30 * time.Second,
		},
		RebalanceConfig:       DefaultRebalanceConfig(),
		MaxPartitionsPerTopic: 64,
		DefaultTopicConfig:    *models.NewTopicConfig("default", models.MinHeap, 1, 1),

		GlobalOpTimeout:      30 * time.Second,
		PartitionOpTimeout:   5 * time.Second,
		SpineCandidateUpdate: 1 * time.Second,
	}
}

// NewMultiPartitionCoordinator creates a new multi-partition coordinator
func NewMultiPartitionCoordinator(
	config *CoordinatorConfig,
	router PartitionRouter,
	storage storage.Storage,
	logger *zap.Logger,
) (Coordinator, error) {

	if config == nil {
		config = DefaultCoordinatorConfig()
	}

	if router == nil {
		router = NewRendezvousRouter(logger)
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	// Create spine index
	spineIndex, err := spine.NewSpineIndex(config.SpineIndexConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create spine index: %w", err)
	}

	// Create rebalancer
	rebalancer := NewRebalancer(config.RebalanceConfig, logger)

	coordinator := &MultiPartitionCoordinator{
		logger:       logger,
		config:       config,
		router:       router,
		spineIndex:   spineIndex,
		storage:      storage,
		partitions:   make(map[string]map[models.PartitionID]partition.Partition),
		topicConfigs: make(map[string]*models.TopicConfig),
		rebalancer:   rebalancer,
		running:      false,
		stats: CoordinatorStats{
			StartTime: time.Now(),
		},
	}

	return coordinator, nil
}

// GlobalPop performs a global pop operation across all partitions of a topic
func (mpc *MultiPartitionCoordinator) GlobalPop(ctx context.Context, topic string, count int) ([]*models.Message, error) {
	if !mpc.running {
		return nil, fmt.Errorf("coordinator is not running")
	}

	start := time.Now()
	defer func() {
		mpc.mu.Lock()
		mpc.stats.GlobalPops++
		mpc.stats.LastGlobalOpLatency = time.Since(start)
		mpc.mu.Unlock()
	}()

	// Use spine index to select the best candidates globally
	candidates, err := mpc.spineIndex.SelectBest(count)
	if err != nil {
		return nil, fmt.Errorf("failed to select candidates from spine index: %w", err)
	}

	// Group candidates by partition for efficient popping
	partitionMessages := make(map[models.PartitionID][]*models.Message)
	for _, msg := range candidates {
		partitionMessages[models.PartitionID(msg.PartitionID)] = append(
			partitionMessages[models.PartitionID(msg.PartitionID)], msg)
	}

	// Pop messages from each partition
	var poppedMessages []*models.Message
	var popErrors []error

	mpc.mu.RLock()
	topicPartitions, exists := mpc.partitions[topic]
	mpc.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	for partitionID, messages := range partitionMessages {
		partition, exists := topicPartitions[partitionID]
		if !exists {
			popErrors = append(popErrors, fmt.Errorf("partition %d not found for topic %s", partitionID, topic))
			continue
		}

		// Pop each message from the partition
		for _, msg := range messages {
			// Create pop context with timeout
			popCtx, cancel := context.WithTimeout(ctx, mpc.config.PartitionOpTimeout)

			poppedMsg, err := partition.Pop(popCtx, "coordinator", mpc.config.PartitionOpTimeout)
			cancel()

			if err != nil {
				popErrors = append(popErrors, fmt.Errorf("failed to pop from partition %d: %w", partitionID, err))
				continue
			}

			if poppedMsg != nil && poppedMsg.ID == msg.ID {
				poppedMessages = append(poppedMessages, poppedMsg)
			} else {
				// Spine index and partition heap are out of sync - this shouldn't happen
				// but we handle it gracefully
				mpc.logger.Warn("Spine index out of sync with partition",
					zap.String("component", "coordinator"),
					zap.String("topic", topic),
					zap.Uint32("partition_id", uint32(partitionID)),
					zap.String("expected_msg_id", string(msg.ID)),
					zap.String("actual_msg_id", func() string {
						if poppedMsg != nil {
							return string(poppedMsg.ID)
						}
						return "nil"
					}()))

				if poppedMsg != nil {
					poppedMessages = append(poppedMessages, poppedMsg)
				}
			}
		}
	}

	// Update spine index with new candidates after popping
	go mpc.updateSpineCandidates(topic)

	mpc.logger.Debug("Global pop completed",
		zap.String("component", "coordinator"),
		zap.String("topic", topic),
		zap.Int("requested", count),
		zap.Int("popped", len(poppedMessages)),
		zap.Int("errors", len(popErrors)),
		zap.Duration("latency", time.Since(start)))

	if len(popErrors) > 0 && len(poppedMessages) == 0 {
		return nil, fmt.Errorf("failed to pop any messages: %v", popErrors)
	}

	return poppedMessages, nil
}

// GlobalPeek performs a global peek operation across all partitions
func (mpc *MultiPartitionCoordinator) GlobalPeek(ctx context.Context, topic string, count int) ([]*models.Message, error) {
	if !mpc.running {
		return nil, fmt.Errorf("coordinator is not running")
	}

	// Check if topic exists
	mpc.mu.RLock()
	_, exists := mpc.partitions[topic]
	mpc.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	// Use spine index to peek at the best candidates globally
	candidates, err := mpc.spineIndex.SelectBest(count)
	if err != nil {
		// Handle "no available candidates" gracefully for peek operations
		var noAvailableCandidatesErr spine.ErrNoAvailableCandidates
		if errors.As(err, &noAvailableCandidatesErr) {
			return []*models.Message{}, nil // Return empty slice, not error for existing topics
		}
		return nil, fmt.Errorf("failed to select candidates from spine index: %w", err)
	}

	// Return the candidates (peek doesn't modify partition state)
	return candidates, nil
}

// GlobalStats aggregates statistics across all partitions of a topic
func (mpc *MultiPartitionCoordinator) GlobalStats(ctx context.Context, topic string) (*GlobalTopicStats, error) {
	mpc.mu.RLock()
	topicPartitions, exists := mpc.partitions[topic]
	mpc.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	stats := &GlobalTopicStats{
		Topic:          topic,
		PartitionCount: len(topicPartitions),
		CollectedAt:    time.Now(),
	}

	for _, partition := range topicPartitions {
		partStats := partition.Stats()
		stats.TotalMessages += partStats.MessageCount
		stats.TotalLeases += partStats.InflightCount
		// Note: TotalBytes not available in PartitionStats, using MessageCount as proxy

		// Collect per-partition stats
		stats.PartitionStats = append(stats.PartitionStats, partStats)

		if !partStats.IsHealthy {
			stats.UnhealthyPartitions++
		}
	}

	return stats, nil
}

// AddPartition adds a new partition to a topic
func (mpc *MultiPartitionCoordinator) AddPartition(topic string, partitionID models.PartitionID, config *models.TopicConfig) error {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()

	// Ensure topic exists in partition map
	if _, exists := mpc.partitions[topic]; !exists {
		mpc.partitions[topic] = make(map[models.PartitionID]partition.Partition)
	}

	// Check if partition already exists
	if _, exists := mpc.partitions[topic][partitionID]; exists {
		return fmt.Errorf("partition %d already exists for topic %s", partitionID, topic)
	}

	// Create local partition
	partitionConfig := &partition.PartitionConfig{
		PartitionID: partitionID,
		Topic:       topic,
		TopicConfig: config,
	}
	localPartition, err := partition.NewLocalPartition(partitionConfig, mpc.logger)
	if err != nil {
		return fmt.Errorf("failed to create partition: %w", err)
	}

	// Start the partition
	if err := localPartition.Start(context.Background()); err != nil {
		return fmt.Errorf("failed to start partition %d: %w", partitionID, err)
	}

	// Add to partition map
	mpc.partitions[topic][partitionID] = localPartition

	// Update router
	if err := mpc.router.AddPartition(topic, partitionID); err != nil {
		return fmt.Errorf("failed to add partition to router: %w", err)
	}

	// Add to spine index
	if err := mpc.spineIndex.AddPartition(uint32(partitionID)); err != nil {
		return fmt.Errorf("failed to add partition to spine index: %w", err)
	}

	// Store topic config if not already stored
	if _, exists := mpc.topicConfigs[topic]; !exists {
		mpc.topicConfigs[topic] = config
		if err := mpc.storage.StoreTopicConfig(context.Background(), config); err != nil {
			mpc.logger.Warn("Failed to persist topic config",
				zap.Error(err), zap.String("topic", topic))
		}
	}

	mpc.stats.PartitionsAdded++

	mpc.logger.Info("Added partition",
		zap.String("component", "coordinator"),
		zap.String("topic", topic),
		zap.Uint32("partition_id", uint32(partitionID)))

	// Update spine candidates
	go mpc.updateSpineCandidates(topic)

	return nil
}

// RemovePartition removes a partition from a topic
func (mpc *MultiPartitionCoordinator) RemovePartition(topic string, partitionID models.PartitionID) error {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()

	topicPartitions, exists := mpc.partitions[topic]
	if !exists {
		return fmt.Errorf("topic %s not found", topic)
	}

	partition, exists := topicPartitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found for topic %s", partitionID, topic)
	}

	// Stop the partition
	if err := partition.Stop(context.Background()); err != nil {
		mpc.logger.Warn("Error stopping partition",
			zap.Error(err),
			zap.String("topic", topic),
			zap.Uint32("partition_id", uint32(partitionID)))
	}

	// Remove from partition map
	delete(topicPartitions, partitionID)

	// Clean up topic if no partitions remain
	if len(topicPartitions) == 0 {
		delete(mpc.partitions, topic)
		delete(mpc.topicConfigs, topic)
	}

	// Update router
	if err := mpc.router.RemovePartition(topic, partitionID); err != nil {
		mpc.logger.Warn("Error removing partition from router",
			zap.Error(err), zap.String("topic", topic), zap.Uint32("partition_id", uint32(partitionID)))
	}

	// Remove from spine index
	if err := mpc.spineIndex.RemovePartition(uint32(partitionID)); err != nil {
		mpc.logger.Warn("Error removing partition from spine index",
			zap.Error(err), zap.Uint32("partition_id", uint32(partitionID)))
	}

	mpc.stats.PartitionsRemoved++

	mpc.logger.Info("Removed partition",
		zap.String("component", "coordinator"),
		zap.String("topic", topic),
		zap.Uint32("partition_id", uint32(partitionID)))

	return nil
}

// GetPartition returns a specific partition
func (mpc *MultiPartitionCoordinator) GetPartition(topic string, partitionID models.PartitionID) (partition.Partition, error) {
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()

	topicPartitions, exists := mpc.partitions[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	partition, exists := topicPartitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found for topic %s", partitionID, topic)
	}

	return partition, nil
}

// ListPartitions returns information about all partitions for a topic
func (mpc *MultiPartitionCoordinator) ListPartitions(topic string) ([]models.PartitionInfo, error) {
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()

	topicPartitions, exists := mpc.partitions[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	var partitionInfos []models.PartitionInfo
	for partitionID, partition := range topicPartitions {
		stats := partition.Stats()

		partitionInfo := models.PartitionInfo{
			ID:            models.PartitionID(partitionID),
			Topic:         topic,
			LeaderNode:    "local",           // TODO: Get actual leader node from raft
			ReplicaNodes:  []string{"local"}, // TODO: Get actual replica nodes
			IsHealthy:     stats.IsHealthy,
			MessageCount:  stats.MessageCount,
			InflightCount: stats.InflightCount,
			LastUpdated:   time.Now(),
		}

		partitionInfos = append(partitionInfos, partitionInfo)
	}

	return partitionInfos, nil
}

// updateSpineCandidates updates the spine index with fresh candidates from all partitions
func (mpc *MultiPartitionCoordinator) updateSpineCandidates(topic string) {
	mpc.mu.RLock()
	topicPartitions, exists := mpc.partitions[topic]
	if !exists {
		mpc.mu.RUnlock()
		return
	}

	// Get fan-out configuration
	fanOut := mpc.config.SpineIndexConfig.FanOut
	mpc.mu.RUnlock()

	// Update candidates for each partition
	for partitionID, partition := range topicPartitions {
		// Get top candidates from this partition
		candidates, err := partition.Peek(context.Background(), fanOut)
		if err != nil {
			mpc.logger.Warn("Failed to get candidates from partition",
				zap.Error(err),
				zap.String("topic", topic),
				zap.Uint32("partition_id", uint32(partitionID)))
			continue
		}

		// Update spine index
		if err := mpc.spineIndex.UpdateCandidates(uint32(partitionID), candidates); err != nil {
			mpc.logger.Warn("Failed to update spine index candidates",
				zap.Error(err),
				zap.Uint32("partition_id", uint32(partitionID)))
		}
	}
}

// Remaining methods will be implemented in the next part...
// (CreateTopic, DeleteTopic, UpdateTopicConfig, GetTopicConfig, ListTopics,
//  TriggerRebalance, GetRebalanceStatus, Start, Stop, HealthCheck, GetStats)

// GlobalTopicStats represents aggregated statistics for a topic across all partitions
type GlobalTopicStats struct {
	Topic               string                  `json:"topic"`
	PartitionCount      int                     `json:"partition_count"`
	UnhealthyPartitions int                     `json:"unhealthy_partitions"`
	TotalMessages       uint64                  `json:"total_messages"`
	TotalLeases         uint64                  `json:"total_leases"`
	TotalBytes          uint64                  `json:"total_bytes"`
	PartitionStats      []models.PartitionStats `json:"partition_stats"`
	CollectedAt         time.Time               `json:"collected_at"`
}

// CoordinatorStats holds statistics about the coordinator
type CoordinatorStats struct {
	StartTime           time.Time     `json:"start_time"`
	GlobalPops          uint64        `json:"global_pops"`
	PartitionsAdded     uint64        `json:"partitions_added"`
	PartitionsRemoved   uint64        `json:"partitions_removed"`
	RebalanceOperations uint64        `json:"rebalance_operations"`
	LastGlobalOpLatency time.Duration `json:"last_global_op_latency"`

	// Runtime stats
	TopicCount     int           `json:"topic_count"`
	PartitionCount int           `json:"partition_count"`
	Uptime         time.Duration `json:"uptime"`
}
