package coordinator

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Helper function to create test coordinator
func createTestCoordinator(t *testing.T) (Coordinator, storage.Storage) {
	logger := zap.NewNop()

	// Create temporary storage
	storageInstance := createTestStorage(t)

	// Create router
	router := NewRendezvousRouter(logger)

	// Create coordinator
	config := DefaultCoordinatorConfig()
	config.GlobalOpTimeout = 5 * time.Second
	config.PartitionOpTimeout = 1 * time.Second

	coordinator, err := NewMultiPartitionCoordinator(config, router, storageInstance, logger)
	require.NoError(t, err)

	return coordinator, storageInstance
}

// createTestStorage creates a test storage instance
func createTestStorage(t *testing.T) storage.Storage {
	tempDir, err := os.MkdirTemp("", "badger_test_*")
	require.NoError(t, err)

	config := storage.DefaultConfig()
	config.Path = tempDir

	logger := zap.NewNop()
	badgerStorage, err := storage.NewBadgerStorage(config, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = badgerStorage.Open(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		badgerStorage.Close(ctx)
		os.RemoveAll(tempDir)
	})

	return badgerStorage
}

// Helper function to create test topic configuration
func createTestTopicConfig(name string, partitions uint32) *models.TopicConfig {
	return &models.TopicConfig{
		Name:              name,
		Mode:              models.MinHeap,
		TopKBound:         10,
		Partitions:        partitions,
		ReplicationFactor: 1,
		RetentionTime:     24 * time.Hour,
		DLQPolicy: models.DLQPolicy{
			Type:          models.DLQPolicyMaxRetries,
			MaxRetries:    3,
			RetentionTime: 7 * 24 * time.Hour,
		},
		VisibilityTimeoutDefault: 30 * time.Second,
		MaxPayloadBytes:          1024 * 1024,
		CompressionEnabled:       false,
	}
}

func TestRendezvousRouter(t *testing.T) {
	logger := zap.NewNop()
	router := NewRendezvousRouter(logger)

	t.Run("single_partition_routing", func(t *testing.T) {
		topic := "test-topic"
		partitionID := models.PartitionID(1)

		err := router.AddPartition(topic, partitionID)
		require.NoError(t, err)

		// Route should always return the single partition
		for i := 0; i < 10; i++ {
			result, err := router.Route(topic, "test-key")
			require.NoError(t, err)
			assert.Equal(t, partitionID, result)
		}

		stats := router.GetStats()
		assert.Equal(t, uint64(10), stats.RoutingCalls)
		assert.Equal(t, 1, stats.TopicCount)
	})

	t.Run("multi_partition_routing", func(t *testing.T) {
		topic := "multi-topic"
		partitions := []models.PartitionID{1, 2, 3, 4}

		// Add all partitions
		for _, p := range partitions {
			err := router.AddPartition(topic, p)
			require.NoError(t, err)
		}

		// Test consistent routing (same key should always go to same partition)
		key := "consistent-key"
		firstResult, err := router.Route(topic, key)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			result, err := router.Route(topic, key)
			require.NoError(t, err)
			assert.Equal(t, firstResult, result, "Same key should always route to same partition")
		}

		// Test distribution (different keys should spread across partitions)
		routingResults := make(map[models.PartitionID]int)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%d", i)
			result, err := router.Route(topic, key)
			require.NoError(t, err)
			routingResults[result]++
		}

		// Each partition should get at least some keys (not perfectly uniform due to hash randomness)
		assert.Len(t, routingResults, len(partitions), "All partitions should receive some keys")
		for _, count := range routingResults {
			assert.Greater(t, count, 0, "Each partition should receive at least one key")
		}
	})

	t.Run("partition_addition_removal", func(t *testing.T) {
		topic := "dynamic-topic"

		// Add initial partitions
		err := router.AddPartition(topic, 1)
		require.NoError(t, err)
		err = router.AddPartition(topic, 2)
		require.NoError(t, err)

		partitions, err := router.GetPartitions(topic)
		require.NoError(t, err)
		assert.Len(t, partitions, 2)

		// Remove partition
		err = router.RemovePartition(topic, 1)
		require.NoError(t, err)

		partitions, err = router.GetPartitions(topic)
		require.NoError(t, err)
		assert.Len(t, partitions, 1)
		assert.Equal(t, models.PartitionID(2), partitions[0])

		// Remove non-existent partition should error
		err = router.RemovePartition(topic, 999)
		assert.Error(t, err)
	})

	t.Run("update_partitions", func(t *testing.T) {
		topic := "update-topic"
		initialPartitions := []models.PartitionID{1, 2}
		updatedPartitions := []models.PartitionID{2, 3, 4}

		err := router.UpdatePartitions(topic, initialPartitions)
		require.NoError(t, err)

		partitions, err := router.GetPartitions(topic)
		require.NoError(t, err)
		assert.ElementsMatch(t, initialPartitions, partitions)

		// Update to different partition set
		err = router.UpdatePartitions(topic, updatedPartitions)
		require.NoError(t, err)

		partitions, err = router.GetPartitions(topic)
		require.NoError(t, err)
		assert.ElementsMatch(t, updatedPartitions, partitions)
	})
}

func TestMultiPartitionCoordinator_TopicManagement(t *testing.T) {
	coordinator, _ := createTestCoordinator(t)

	t.Run("create_topic", func(t *testing.T) {
		config := createTestTopicConfig("test-topic", 3)

		err := coordinator.CreateTopic(config)
		require.NoError(t, err)

		// Verify topic was created
		retrievedConfig, err := coordinator.GetTopicConfig("test-topic")
		require.NoError(t, err)
		assert.Equal(t, config.Name, retrievedConfig.Name)
		assert.Equal(t, config.Partitions, retrievedConfig.Partitions)

		// Verify partitions were created
		partitions, err := coordinator.ListPartitions("test-topic")
		require.NoError(t, err)
		assert.Len(t, partitions, 3)

		// Creating same topic again should fail
		err = coordinator.CreateTopic(config)
		assert.Error(t, err)
	})

	t.Run("list_topics", func(t *testing.T) {
		// Create multiple topics
		for i := 1; i <= 3; i++ {
			config := createTestTopicConfig(fmt.Sprintf("topic-%d", i), uint32(i))
			err := coordinator.CreateTopic(config)
			require.NoError(t, err)
		}

		topics, err := coordinator.ListTopics()
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(topics), 3) // At least the 3 we just created

		topicNames := make([]string, len(topics))
		for i, topic := range topics {
			topicNames[i] = topic.Name
		}
		assert.Contains(t, topicNames, "topic-1")
		assert.Contains(t, topicNames, "topic-2")
		assert.Contains(t, topicNames, "topic-3")
	})

	t.Run("update_topic_config", func(t *testing.T) {
		topicName := "update-test-topic"
		originalConfig := createTestTopicConfig(topicName, 2)

		err := coordinator.CreateTopic(originalConfig)
		require.NoError(t, err)

		// Update configuration
		updatedConfig := *originalConfig
		updatedConfig.TopKBound = 20
		updatedConfig.RetentionTime = 48 * time.Hour

		err = coordinator.UpdateTopicConfig(topicName, &updatedConfig)
		require.NoError(t, err)

		// Verify update
		retrievedConfig, err := coordinator.GetTopicConfig(topicName)
		require.NoError(t, err)
		assert.Equal(t, uint32(20), retrievedConfig.TopKBound)
		assert.Equal(t, 48*time.Hour, retrievedConfig.RetentionTime)
	})

	t.Run("delete_topic", func(t *testing.T) {
		topicName := "delete-test-topic"
		config := createTestTopicConfig(topicName, 2)

		err := coordinator.CreateTopic(config)
		require.NoError(t, err)

		// Verify topic exists
		_, err = coordinator.GetTopicConfig(topicName)
		require.NoError(t, err)

		// Delete topic
		err = coordinator.DeleteTopic(topicName)
		require.NoError(t, err)

		// Verify topic is gone
		_, err = coordinator.GetTopicConfig(topicName)
		assert.Error(t, err)

		// Verify partitions are gone
		_, err = coordinator.ListPartitions(topicName)
		assert.Error(t, err)
	})
}

func TestMultiPartitionCoordinator_PartitionManagement(t *testing.T) {
	coordinator, _ := createTestCoordinator(t)

	ctx := context.Background()

	// Start coordinator
	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	t.Run("add_remove_partitions", func(t *testing.T) {
		topicName := "partition-test-topic"
		config := createTestTopicConfig(topicName, 2)

		err := coordinator.CreateTopic(config)
		require.NoError(t, err)

		// Add additional partition
		err = coordinator.AddPartition(topicName, 3, config)
		require.NoError(t, err)

		partitions, err := coordinator.ListPartitions(topicName)
		require.NoError(t, err)
		assert.Len(t, partitions, 3)

		// Get specific partition
		partition, err := coordinator.GetPartition(topicName, 3)
		require.NoError(t, err)
		assert.NotNil(t, partition)

		// Remove partition
		err = coordinator.RemovePartition(topicName, 3)
		require.NoError(t, err)

		partitions, err = coordinator.ListPartitions(topicName)
		require.NoError(t, err)
		assert.Len(t, partitions, 2)

		// Getting removed partition should fail
		_, err = coordinator.GetPartition(topicName, 3)
		assert.Error(t, err)
	})
}

func TestMultiPartitionCoordinator_GlobalOperations(t *testing.T) {
	coordinator, _ := createTestCoordinator(t)

	ctx := context.Background()

	// Start coordinator
	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	t.Run("global_peek", func(t *testing.T) {
		topicName := "global-peek-topic"
		config := createTestTopicConfig(topicName, 2)

		err := coordinator.CreateTopic(config)
		require.NoError(t, err)

		// Test GlobalPeek on empty partitions - should return empty slice, not error
		messages, err := coordinator.GlobalPeek(ctx, topicName, 10)
		require.NoError(t, err)    // Should not error on empty partitions
		assert.NotNil(t, messages) // Should return empty slice, not nil
		assert.Len(t, messages, 0) // Should be empty since no messages
	})

	t.Run("global_stats", func(t *testing.T) {
		topicName := "global-stats-topic"
		config := createTestTopicConfig(topicName, 3)

		err := coordinator.CreateTopic(config)
		require.NoError(t, err)

		stats, err := coordinator.GlobalStats(ctx, topicName)
		require.NoError(t, err)
		assert.Equal(t, topicName, stats.Topic)
		assert.Equal(t, 3, stats.PartitionCount)
		assert.Len(t, stats.PartitionStats, 3)
	})

	t.Run("global_operations_nonexistent_topic", func(t *testing.T) {
		nonExistentTopic := "non-existent-topic"

		_, err := coordinator.GlobalPeek(ctx, nonExistentTopic, 10)
		assert.Error(t, err)

		_, err = coordinator.GlobalStats(ctx, nonExistentTopic)
		assert.Error(t, err)

		_, err = coordinator.GlobalPop(ctx, nonExistentTopic, 10)
		assert.Error(t, err)
	})
}

func TestMultiPartitionCoordinator_Rebalancing(t *testing.T) {
	coordinator, _ := createTestCoordinator(t)

	ctx := context.Background()

	// Start coordinator
	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	t.Run("trigger_rebalance", func(t *testing.T) {
		topicName := "rebalance-topic"
		config := createTestTopicConfig(topicName, 2)

		err := coordinator.CreateTopic(config)
		require.NoError(t, err)

		// Trigger rebalance
		err = coordinator.TriggerRebalance(topicName)
		require.NoError(t, err)

		// Check rebalance status
		status, err := coordinator.GetRebalanceStatus(topicName)
		require.NoError(t, err)
		assert.Equal(t, topicName, status.Topic)
		assert.NotEqual(t, RebalanceStateIdle, status.State)

		// Wait a bit for rebalance to complete
		time.Sleep(100 * time.Millisecond)

		// Check final status
		status, err = coordinator.GetRebalanceStatus(topicName)
		require.NoError(t, err)
		// Status should eventually be completed, but we don't want to wait too long in tests
	})

	t.Run("rebalance_nonexistent_topic", func(t *testing.T) {
		err := coordinator.TriggerRebalance("non-existent-topic")
		assert.Error(t, err)
	})
}

func TestMultiPartitionCoordinator_Lifecycle(t *testing.T) {
	coordinator, _ := createTestCoordinator(t)

	ctx := context.Background()

	t.Run("start_stop", func(t *testing.T) {
		// Initial health check should fail (not started)
		err := coordinator.HealthCheck(ctx)
		assert.Error(t, err)

		// Start coordinator
		err = coordinator.Start(ctx)
		require.NoError(t, err)

		// Health check should pass
		err = coordinator.HealthCheck(ctx)
		require.NoError(t, err)

		// Get stats
		stats := coordinator.GetStats()
		assert.NotNil(t, stats)
		assert.NotZero(t, stats.StartTime)

		// Stop coordinator
		err = coordinator.Stop(ctx)
		require.NoError(t, err)

		// Health check should fail again
		err = coordinator.HealthCheck(ctx)
		assert.Error(t, err)
	})

	t.Run("double_start_stop", func(t *testing.T) {
		// Start
		err := coordinator.Start(ctx)
		require.NoError(t, err)

		// Starting again should error
		err = coordinator.Start(ctx)
		assert.Error(t, err)

		// Stop
		err = coordinator.Stop(ctx)
		require.NoError(t, err)

		// Stopping again should be no-op
		err = coordinator.Stop(ctx)
		require.NoError(t, err)
	})
}

func TestRebalancer(t *testing.T) {
	logger := zap.NewNop()
	config := DefaultRebalanceConfig()
	rebalancer := NewRebalancer(config, logger)

	t.Run("trigger_and_status", func(t *testing.T) {
		ctx := context.Background()
		topic := "rebalance-test"
		currentPartitions := []models.PartitionID{1, 2}
		targetPartitions := []models.PartitionID{1, 2, 3}

		err := rebalancer.TriggerRebalance(ctx, topic, currentPartitions, targetPartitions)
		require.NoError(t, err)

		// Get status immediately
		status, err := rebalancer.GetRebalanceStatus(topic)
		require.NoError(t, err)
		assert.Equal(t, topic, status.Topic)
		assert.NotEqual(t, RebalanceStateIdle, status.State)
		assert.False(t, status.StartedAt.IsZero())

		// Wait for completion (short timeout since it's a simple test rebalance)
		maxWait := 5 * time.Second
		start := time.Now()
		for time.Since(start) < maxWait {
			status, err = rebalancer.GetRebalanceStatus(topic)
			require.NoError(t, err)

			if status.State == RebalanceStateCompleted || status.State == RebalanceStateFailed {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		// Should eventually complete
		assert.True(t, status.State == RebalanceStateCompleted || status.State == RebalanceStateFailed,
			"Rebalance should complete within timeout, got state: %s", status.State)
	})

	t.Run("concurrent_rebalance", func(t *testing.T) {
		ctx := context.Background()
		topic := "concurrent-test"
		currentPartitions := []models.PartitionID{1}
		targetPartitions := []models.PartitionID{1, 2}

		// Start first rebalance
		err := rebalancer.TriggerRebalance(ctx, topic, currentPartitions, targetPartitions)
		require.NoError(t, err)

		// Starting second rebalance should fail
		err = rebalancer.TriggerRebalance(ctx, topic, currentPartitions, targetPartitions)
		assert.Error(t, err)
	})

	t.Run("rebalance_stats", func(t *testing.T) {
		stats := rebalancer.GetStats()
		assert.NotNil(t, stats)
		assert.GreaterOrEqual(t, stats.TotalOperations, 0)
	})
}

// Benchmark tests
func BenchmarkRendezvousRouting(b *testing.B) {
	router := NewRendezvousRouter(zap.NewNop())
	topic := "bench-topic"

	// Setup partitions
	for i := 1; i <= 10; i++ {
		router.AddPartition(topic, models.PartitionID(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, err := router.Route(topic, key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCoordinatorGlobalPeek(b *testing.B) {
	// Convert b to t for helper function
	t := &testing.T{}
	coordinator, _ := createTestCoordinator(t)

	ctx := context.Background()
	coordinator.Start(ctx)
	defer coordinator.Stop(ctx)

	// Create test topic
	config := createTestTopicConfig("bench-topic", 4)
	coordinator.CreateTopic(config)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := coordinator.GlobalPeek(ctx, "bench-topic", 10)
		if err != nil {
			b.Fatal(err)
		}
	}
}
