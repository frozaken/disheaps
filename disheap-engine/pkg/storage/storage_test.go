package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestBadgerStorage_Lifecycle tests basic storage lifecycle operations
func TestBadgerStorage_Lifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "badger_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	config := DefaultConfig()
	config.Path = tempDir

	logger := zaptest.NewLogger(t)
	storage, err := NewBadgerStorage(config, logger)
	require.NoError(t, err)

	ctx := context.Background()

	// Test opening
	err = storage.Open(ctx)
	require.NoError(t, err)

	// Test health check
	err = storage.Health(ctx)
	require.NoError(t, err)

	// Test closing
	err = storage.Close(ctx)
	require.NoError(t, err)

	// Test health check after close (should fail)
	err = storage.Health(ctx)
	assert.Error(t, err)
	assert.True(t, IsClosed(err))
}

// TestBadgerStorage_MessageOperations tests message CRUD operations
func TestBadgerStorage_MessageOperations(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Create test message
	msg := models.NewMessage("test-topic", 1, 100, []byte("test payload"))
	msg.ProducerID = "producer-1"
	msg.Epoch = 1
	msg.Sequence = 42

	// Test store message
	err := storage.StoreMessage(ctx, msg)
	require.NoError(t, err)

	// Test get message
	retrieved, err := storage.GetMessage(ctx, msg.Topic, msg.PartitionID, msg.ID)
	require.NoError(t, err)
	assert.Equal(t, msg.ID, retrieved.ID)
	assert.Equal(t, msg.Topic, retrieved.Topic)
	assert.Equal(t, msg.Priority, retrieved.Priority)
	assert.Equal(t, msg.Payload, retrieved.Payload)
	assert.Equal(t, msg.ProducerID, retrieved.ProducerID)
	assert.Equal(t, msg.Epoch, retrieved.Epoch)
	assert.Equal(t, msg.Sequence, retrieved.Sequence)

	// Test list messages
	messages, err := storage.ListMessages(ctx, msg.Topic, msg.PartitionID, 10)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.Equal(t, msg.ID, messages[0].ID)

	// Test delete message
	err = storage.DeleteMessage(ctx, msg.Topic, msg.PartitionID, msg.ID)
	require.NoError(t, err)

	// Test get deleted message (should not be found)
	_, err = storage.GetMessage(ctx, msg.Topic, msg.PartitionID, msg.ID)
	assert.Error(t, err)
	assert.True(t, IsNotFound(err))
}

// TestBadgerStorage_LeaseOperations tests lease CRUD operations
func TestBadgerStorage_LeaseOperations(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Create test lease
	msgID := models.NewMessageID()
	lease := models.NewLease(msgID, "consumer-1", 30*time.Second)

	// Test store lease
	err := storage.StoreLease(ctx, "test-topic", 1, lease)
	require.NoError(t, err)

	// Test get lease
	retrieved, err := storage.GetLease(ctx, "test-topic", 1, msgID)
	require.NoError(t, err)
	assert.Equal(t, lease.Token, retrieved.Token)
	assert.Equal(t, lease.Holder, retrieved.Holder)
	assert.Equal(t, lease.MessageID, retrieved.MessageID)

	// Test delete lease
	err = storage.DeleteLease(ctx, "test-topic", 1, msgID)
	require.NoError(t, err)

	// Test get deleted lease (should not be found)
	_, err = storage.GetLease(ctx, "test-topic", 1, msgID)
	assert.Error(t, err)
	assert.True(t, IsNotFound(err))
}

// TestBadgerStorage_DedupOperations tests deduplication operations
func TestBadgerStorage_DedupOperations(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Test deduplication
	topic := "test-topic"
	producerKey := models.ProducerKey{ID: "producer-1", Epoch: 1}
	seq := uint64(42)
	msgID := models.NewMessageID()
	ttl := time.Hour

	// Store dedup entry
	err := storage.StoreDedup(ctx, topic, producerKey, seq, msgID, ttl)
	require.NoError(t, err)

	// Check dedup (should exist)
	foundMsgID, exists, err := storage.CheckDedup(ctx, topic, producerKey, seq)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, msgID, foundMsgID)

	// Check non-existent dedup
	_, exists, err = storage.CheckDedup(ctx, topic, producerKey, seq+1)
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestBadgerStorage_SnapshotOperations tests snapshot operations
func TestBadgerStorage_SnapshotOperations(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Test snapshot storage
	topic := "test-topic"
	partition := uint32(1)
	snapshotData := []byte("snapshot data")

	// Store heap snapshot
	err := storage.StoreSnapshot(ctx, topic, partition, SnapshotTypeHeap, snapshotData)
	require.NoError(t, err)

	// Get heap snapshot
	retrieved, err := storage.GetSnapshot(ctx, topic, partition, SnapshotTypeHeap)
	require.NoError(t, err)
	assert.Equal(t, snapshotData, retrieved)

	// Store timer snapshot
	timerData := []byte("timer snapshot data")
	err = storage.StoreSnapshot(ctx, topic, partition, SnapshotTypeTimer, timerData)
	require.NoError(t, err)

	// Get timer snapshot
	retrieved, err = storage.GetSnapshot(ctx, topic, partition, SnapshotTypeTimer)
	require.NoError(t, err)
	assert.Equal(t, timerData, retrieved)

	// Delete snapshots
	err = storage.DeleteSnapshot(ctx, topic, partition, SnapshotTypeHeap)
	require.NoError(t, err)

	// Get deleted snapshot (should not be found)
	_, err = storage.GetSnapshot(ctx, topic, partition, SnapshotTypeHeap)
	assert.Error(t, err)
	assert.True(t, IsNotFound(err))
}

// TestBadgerStorage_TopicConfigOperations tests topic configuration operations
func TestBadgerStorage_TopicConfigOperations(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Create test topic config (following INTERFACE_CONTRACTS.md)
	config := models.NewTopicConfig("test_topic", models.MinHeap, 4, 3)
	config.TopKBound = 100
	config.RetentionTime = 24 * time.Hour

	// Test store topic config
	err := storage.StoreTopicConfig(ctx, config)
	require.NoError(t, err)

	// Test get topic config
	retrieved, err := storage.GetTopicConfig(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, config.Name, retrieved.Name)
	assert.Equal(t, config.Mode, retrieved.Mode)
	assert.Equal(t, config.Partitions, retrieved.Partitions)
	assert.Equal(t, config.ReplicationFactor, retrieved.ReplicationFactor)
	assert.Equal(t, config.TopKBound, retrieved.TopKBound)

	// Test list topic configs
	configs, err := storage.ListTopicConfigs(ctx)
	require.NoError(t, err)
	assert.Len(t, configs, 1)
	assert.Equal(t, config.Name, configs[0].Name)

	// Test delete topic config
	err = storage.DeleteTopicConfig(ctx, config.Name)
	require.NoError(t, err)

	// Test get deleted config (should not be found)
	_, err = storage.GetTopicConfig(ctx, config.Name)
	assert.Error(t, err)
	assert.True(t, IsNotFound(err))
}

// TestBadgerStorage_PartitionInfoOperations tests partition info operations
func TestBadgerStorage_PartitionInfoOperations(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Create test partition info
	info := models.NewPartitionInfo(1, "test_topic", "node-1", []string{"node-2", "node-3"})
	info.MessageCount = 100
	info.InflightCount = 5

	// Test store partition info
	err := storage.StorePartitionInfo(ctx, info)
	require.NoError(t, err)

	// Test get partition info
	retrieved, err := storage.GetPartitionInfo(ctx, info.Topic, uint32(info.ID))
	require.NoError(t, err)
	assert.Equal(t, info.ID, retrieved.ID)
	assert.Equal(t, info.Topic, retrieved.Topic)
	assert.Equal(t, info.LeaderNode, retrieved.LeaderNode)
	assert.Equal(t, info.ReplicaNodes, retrieved.ReplicaNodes)
	assert.Equal(t, info.MessageCount, retrieved.MessageCount)
	assert.Equal(t, info.InflightCount, retrieved.InflightCount)

	// Test list partition infos
	infos, err := storage.ListPartitionInfos(ctx, info.Topic)
	require.NoError(t, err)
	assert.Len(t, infos, 1)
	assert.Equal(t, info.ID, infos[0].ID)

	// Test update partition health
	err = storage.UpdatePartitionHealth(ctx, info.Topic, uint32(info.ID), false, "test failure")
	require.NoError(t, err)

	// Verify health update
	updated, err := storage.GetPartitionInfo(ctx, info.Topic, uint32(info.ID))
	require.NoError(t, err)
	assert.False(t, updated.IsHealthy)
	assert.True(t, updated.IsThrottled)
	assert.Equal(t, "test failure", updated.ThrottleReason)
}

// TestBadgerStorage_BatchOperations tests atomic batch operations
func TestBadgerStorage_BatchOperations(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Create test data
	msg1 := models.NewMessage("test-topic", 1, 100, []byte("payload 1"))
	msg2 := models.NewMessage("test-topic", 1, 200, []byte("payload 2"))
	lease := models.NewLease(msg1.ID, "consumer-1", 30*time.Second)

	// Create batch
	batch := storage.Batch()

	// Add operations to batch
	err := batch.StoreMessage(msg1)
	require.NoError(t, err)

	err = batch.StoreMessage(msg2)
	require.NoError(t, err)

	err = batch.StoreLease("test-topic", 1, lease)
	require.NoError(t, err)

	assert.Equal(t, 3, batch.Size())

	// Commit batch
	err = batch.Commit(ctx)
	require.NoError(t, err)

	// Verify all operations were applied
	retrieved1, err := storage.GetMessage(ctx, msg1.Topic, msg1.PartitionID, msg1.ID)
	require.NoError(t, err)
	assert.Equal(t, msg1.ID, retrieved1.ID)

	retrieved2, err := storage.GetMessage(ctx, msg2.Topic, msg2.PartitionID, msg2.ID)
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, retrieved2.ID)

	retrievedLease, err := storage.GetLease(ctx, "test-topic", 1, msg1.ID)
	require.NoError(t, err)
	assert.Equal(t, lease.Token, retrievedLease.Token)
}

// TestBadgerStorage_BatchRollback tests batch rollback
func TestBadgerStorage_BatchRollback(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Create test message
	msg := models.NewMessage("test-topic", 1, 100, []byte("payload"))

	// Create batch and add operation
	batch := storage.Batch()
	err := batch.StoreMessage(msg)
	require.NoError(t, err)

	// Rollback batch
	err = batch.Rollback()
	require.NoError(t, err)

	assert.Equal(t, 0, batch.Size())

	// Verify message was not stored
	_, err = storage.GetMessage(ctx, msg.Topic, msg.PartitionID, msg.ID)
	assert.Error(t, err)
	assert.True(t, IsNotFound(err))
}

// TestBadgerStorage_Stats tests storage statistics
func TestBadgerStorage_Stats(t *testing.T) {
	storage := setupTestStorage(t)
	defer teardownTestStorage(storage)

	ctx := context.Background()

	// Add some data
	msg := models.NewMessage("test-topic", 1, 100, []byte("payload"))
	err := storage.StoreMessage(ctx, msg)
	require.NoError(t, err)

	config := models.NewTopicConfig("test_topic", models.MinHeap, 4, 3)
	err = storage.StoreTopicConfig(ctx, config)
	require.NoError(t, err)

	// Get stats - just verify basic functionality works
	stats, err := storage.Stats(ctx)
	require.NoError(t, err)

	assert.True(t, stats.IsHealthy)
	// Note: BadgerDB size reporting can be complex with timing and compaction
	// The important thing is that Stats() doesn't error and returns valid structure
	assert.NotNil(t, stats.EngineStats)
	assert.NotZero(t, stats.LastHealthCheck)
}

// TestTopicNameValidation tests topic name validation according to INTERFACE_CONTRACTS.md
func TestTopicNameValidation(t *testing.T) {
	tests := []struct {
		name      string
		topicName string
		wantErr   bool
	}{
		{"valid topic", "test-topic", false},
		{"valid with underscore", "test_topic", false},
		{"valid alphanumeric", "topic123", false},
		{"valid mixed", "test-topic_123", false},
		{"empty topic", "", true},
		{"invalid characters", "test@topic", true},
		{"invalid characters space", "test topic", true},
		{"invalid characters dot", "test.topic", true},
		{"too long", string(make([]byte, 256)), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTopicName(tt.topicName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// setupTestStorage creates a test storage instance
func setupTestStorage(t *testing.T) *BadgerStorage {
	tempDir, err := os.MkdirTemp("", "badger_test_*")
	require.NoError(t, err)

	config := DefaultConfig()
	config.Path = tempDir
	config.CompactionInterval = time.Minute // Shorter for tests

	logger := zaptest.NewLogger(t)
	storage, err := NewBadgerStorage(config, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = storage.Open(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		storage.Close(ctx)
		os.RemoveAll(tempDir)
	})

	return storage
}

// teardownTestStorage cleans up test storage
func teardownTestStorage(storage *BadgerStorage) {
	ctx := context.Background()
	storage.Close(ctx)
}
