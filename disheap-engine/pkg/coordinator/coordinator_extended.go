package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/partition"
	"go.uber.org/zap"
)

// Extended methods for MultiPartitionCoordinator

// CreateTopic creates a new topic with the specified configuration
func (mpc *MultiPartitionCoordinator) CreateTopic(config *models.TopicConfig) error {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()

	if _, exists := mpc.topicConfigs[config.Name]; exists {
		return fmt.Errorf("topic %s already exists", config.Name)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid topic configuration: %w", err)
	}

	// Store topic configuration
	mpc.topicConfigs[config.Name] = config

	// Persist configuration
	if err := mpc.storage.StoreTopicConfig(context.Background(), config); err != nil {
		return fmt.Errorf("failed to persist topic configuration: %w", err)
	}

	// Initialize partition map for this topic
	mpc.partitions[config.Name] = make(map[models.PartitionID]partition.Partition)

	// Create initial partitions
	for i := uint32(0); i < config.Partitions; i++ {
		partitionID := models.PartitionID(i)
		if err := mpc.addPartitionUnsafe(config.Name, partitionID, config); err != nil {
			// Cleanup on failure
			delete(mpc.topicConfigs, config.Name)
			delete(mpc.partitions, config.Name)
			return fmt.Errorf("failed to create partition %d: %w", partitionID, err)
		}
	}

	mpc.stats.PartitionsAdded += uint64(config.Partitions)

	mpc.logger.Info("Created topic",
		zap.String("component", "coordinator"),
		zap.String("topic", config.Name),
		zap.Uint32("partitions", config.Partitions))

	return nil
}

// DeleteTopic deletes a topic and all its partitions
func (mpc *MultiPartitionCoordinator) DeleteTopic(topic string) error {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()

	topicPartitions, exists := mpc.partitions[topic]
	if !exists {
		return fmt.Errorf("topic %s not found", topic)
	}

	// Stop all partitions
	for partitionID, partition := range topicPartitions {
		if err := partition.Stop(context.Background()); err != nil {
			mpc.logger.Warn("Error stopping partition during topic deletion",
				zap.Error(err),
				zap.String("topic", topic),
				zap.Uint32("partition_id", uint32(partitionID)))
		}

		// Remove from spine index
		if err := mpc.spineIndex.RemovePartition(uint32(partitionID)); err != nil {
			mpc.logger.Warn("Error removing partition from spine index",
				zap.Error(err), zap.Uint32("partition_id", uint32(partitionID)))
		}
	}

	// Clean up routing
	if err := mpc.router.UpdatePartitions(topic, []models.PartitionID{}); err != nil {
		mpc.logger.Warn("Error clearing topic from router", zap.Error(err), zap.String("topic", topic))
	}

	// Remove from maps
	delete(mpc.partitions, topic)
	delete(mpc.topicConfigs, topic)

	// Delete from storage
	if err := mpc.storage.DeleteTopicConfig(context.Background(), topic); err != nil {
		mpc.logger.Warn("Error deleting topic config from storage",
			zap.Error(err), zap.String("topic", topic))
	}

	mpc.stats.PartitionsRemoved += uint64(len(topicPartitions))

	mpc.logger.Info("Deleted topic",
		zap.String("component", "coordinator"),
		zap.String("topic", topic),
		zap.Int("partitions", len(topicPartitions)))

	return nil
}

// UpdateTopicConfig updates the configuration for an existing topic
func (mpc *MultiPartitionCoordinator) UpdateTopicConfig(topic string, config *models.TopicConfig) error {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()

	currentConfig, exists := mpc.topicConfigs[topic]
	if !exists {
		return fmt.Errorf("topic %s not found", topic)
	}

	// Validate new configuration
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid topic configuration: %w", err)
	}

	// Check if partition count is changing (requires rebalancing)
	if config.Partitions != currentConfig.Partitions {
		// TODO: Trigger rebalancing when partition count changes
		mpc.logger.Warn("Partition count change requires rebalancing",
			zap.String("topic", topic),
			zap.Uint32("old_partitions", currentConfig.Partitions),
			zap.Uint32("new_partitions", config.Partitions))
	}

	// Update configuration
	mpc.topicConfigs[topic] = config

	// Persist configuration
	if err := mpc.storage.StoreTopicConfig(context.Background(), config); err != nil {
		return fmt.Errorf("failed to persist updated topic configuration: %w", err)
	}

	mpc.logger.Info("Updated topic configuration",
		zap.String("component", "coordinator"),
		zap.String("topic", topic))

	return nil
}

// GetTopicConfig returns the configuration for a topic
func (mpc *MultiPartitionCoordinator) GetTopicConfig(topic string) (*models.TopicConfig, error) {
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()

	config, exists := mpc.topicConfigs[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	// Return a copy to prevent external modification
	configCopy := *config
	return &configCopy, nil
}

// ListTopics returns configurations for all topics
func (mpc *MultiPartitionCoordinator) ListTopics() ([]*models.TopicConfig, error) {
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()

	configs := make([]*models.TopicConfig, 0, len(mpc.topicConfigs))
	for _, config := range mpc.topicConfigs {
		configCopy := *config
		configs = append(configs, &configCopy)
	}

	return configs, nil
}

// TriggerRebalance initiates a rebalancing operation for a topic
func (mpc *MultiPartitionCoordinator) TriggerRebalance(topic string) error {
	mpc.mu.RLock()
	topicPartitions, exists := mpc.partitions[topic]
	if !exists {
		mpc.mu.RUnlock()
		return fmt.Errorf("topic %s not found", topic)
	}

	// Get current partitions
	currentPartitions := make([]models.PartitionID, 0, len(topicPartitions))
	for partitionID := range topicPartitions {
		currentPartitions = append(currentPartitions, partitionID)
	}

	// Get target partitions (for now, same as current - this would be different in real rebalancing)
	targetPartitions := currentPartitions
	mpc.mu.RUnlock()

	// Start rebalancing
	ctx := context.Background() // TODO: Use proper context
	if err := mpc.rebalancer.TriggerRebalance(ctx, topic, currentPartitions, targetPartitions); err != nil {
		return fmt.Errorf("failed to trigger rebalance: %w", err)
	}

	mpc.mu.Lock()
	mpc.stats.RebalanceOperations++
	mpc.mu.Unlock()

	return nil
}

// GetRebalanceStatus returns the status of a rebalancing operation
func (mpc *MultiPartitionCoordinator) GetRebalanceStatus(topic string) (*RebalanceStatus, error) {
	status, err := mpc.rebalancer.GetRebalanceStatus(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get rebalance status: %w", err)
	}

	return status, nil
}

// Start starts the coordinator and all its components
func (mpc *MultiPartitionCoordinator) Start(ctx context.Context) error {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()

	if mpc.running {
		return fmt.Errorf("coordinator is already running")
	}

	// Load topic configurations from storage
	if err := mpc.loadTopicConfigurations(); err != nil {
		return fmt.Errorf("failed to load topic configurations: %w", err)
	}

	// Start all partitions
	for topic, topicPartitions := range mpc.partitions {
		for partitionID, partition := range topicPartitions {
			if err := partition.Start(ctx); err != nil {
				mpc.logger.Error("Failed to start partition",
					zap.Error(err),
					zap.String("topic", topic),
					zap.Uint32("partition_id", uint32(partitionID)))
				return fmt.Errorf("failed to start partition %d for topic %s: %w", partitionID, topic, err)
			}
		}
	}

	mpc.running = true

	// Start background tasks
	go mpc.startBackgroundTasks(ctx)

	mpc.logger.Info("Started coordinator",
		zap.String("component", "coordinator"),
		zap.Int("topics", len(mpc.topicConfigs)))

	return nil
}

// Stop stops the coordinator and all its components
func (mpc *MultiPartitionCoordinator) Stop(ctx context.Context) error {
	mpc.mu.Lock()
	defer mpc.mu.Unlock()

	if !mpc.running {
		return nil // Already stopped
	}

	mpc.running = false

	// Stop all partitions
	for topic, topicPartitions := range mpc.partitions {
		for partitionID, partition := range topicPartitions {
			if err := partition.Stop(context.Background()); err != nil {
				mpc.logger.Error("Failed to stop partition",
					zap.Error(err),
					zap.String("topic", topic),
					zap.Uint32("partition_id", uint32(partitionID)))
			}
		}
	}

	// Close spine index
	if err := mpc.spineIndex.Close(); err != nil {
		mpc.logger.Error("Failed to close spine index", zap.Error(err))
	}

	mpc.logger.Info("Stopped coordinator", zap.String("component", "coordinator"))

	return nil
}

// HealthCheck performs a health check on the coordinator and its components
func (mpc *MultiPartitionCoordinator) HealthCheck(ctx context.Context) error {
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()

	if !mpc.running {
		return fmt.Errorf("coordinator is not running")
	}

	// Check partition health
	unhealthyCount := 0
	totalCount := 0

	for topic, topicPartitions := range mpc.partitions {
		for partitionID, partition := range topicPartitions {
			totalCount++
			if !partition.IsHealthy() {
				unhealthyCount++
				mpc.logger.Warn("Unhealthy partition detected",
					zap.String("topic", topic),
					zap.Uint32("partition_id", uint32(partitionID)))
			}
		}
	}

	if unhealthyCount > 0 {
		return fmt.Errorf("%d of %d partitions are unhealthy", unhealthyCount, totalCount)
	}

	return nil
}

// GetStats returns coordinator statistics
func (mpc *MultiPartitionCoordinator) GetStats() *CoordinatorStats {
	mpc.mu.RLock()
	defer mpc.mu.RUnlock()

	stats := mpc.stats

	// Calculate current partition count
	totalPartitions := 0
	for _, topicPartitions := range mpc.partitions {
		totalPartitions += len(topicPartitions)
	}

	// Add runtime statistics
	stats.TopicCount = len(mpc.topicConfigs)
	stats.PartitionCount = totalPartitions
	stats.Uptime = time.Since(stats.StartTime)

	return &stats
}

// Helper methods

// addPartitionUnsafe adds a partition without acquiring the lock (internal helper)
func (mpc *MultiPartitionCoordinator) addPartitionUnsafe(topic string, partitionID models.PartitionID, config *models.TopicConfig) error {
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

	return nil
}

// loadTopicConfigurations loads topic configurations from storage
func (mpc *MultiPartitionCoordinator) loadTopicConfigurations() error {
	// TODO: Implement loading from storage
	// For now, we start with an empty configuration set
	mpc.logger.Debug("Loading topic configurations from storage", zap.String("component", "coordinator"))
	return nil
}

// startBackgroundTasks starts background maintenance tasks
func (mpc *MultiPartitionCoordinator) startBackgroundTasks(ctx context.Context) {
	ticker := time.NewTicker(mpc.config.SpineCandidateUpdate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mpc.updateAllSpineCandidates()
		}
	}
}

// updateAllSpineCandidates updates spine candidates for all topics
func (mpc *MultiPartitionCoordinator) updateAllSpineCandidates() {
	mpc.mu.RLock()
	topics := make([]string, 0, len(mpc.partitions))
	for topic := range mpc.partitions {
		topics = append(topics, topic)
	}
	mpc.mu.RUnlock()

	for _, topic := range topics {
		mpc.updateSpineCandidates(topic)
	}
}

// Extended CoordinatorStats with runtime information
type ExtendedCoordinatorStats struct {
	CoordinatorStats
	TopicCount     int           `json:"topic_count"`
	PartitionCount int           `json:"partition_count"`
	Uptime         time.Duration `json:"uptime"`
}
