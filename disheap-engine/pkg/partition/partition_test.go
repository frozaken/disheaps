package partition

import (
	"context"
	"testing"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// MockStorage implements storage.Storage for testing
type MockStorage struct {
	messages map[string]*models.Message
	leases   map[string]*models.Lease
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		messages: make(map[string]*models.Message),
		leases:   make(map[string]*models.Lease),
	}
}

func (m *MockStorage) StoreMessage(ctx context.Context, msg *models.Message) error {
	key := string(msg.ID)
	m.messages[key] = msg
	return nil
}

func (m *MockStorage) GetMessage(ctx context.Context, topic string, partition uint32, msgID models.MessageID) (*models.Message, error) {
	key := string(msgID)
	msg, exists := m.messages[key]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return msg, nil
}

func (m *MockStorage) DeleteMessage(ctx context.Context, topic string, partition uint32, msgID models.MessageID) error {
	key := string(msgID)
	delete(m.messages, key)
	return nil
}

func (m *MockStorage) StoreLease(ctx context.Context, topic string, partition uint32, lease *models.Lease) error {
	key := string(lease.MessageID)
	m.leases[key] = lease
	return nil
}

func (m *MockStorage) GetLease(ctx context.Context, topic string, partition uint32, msgID models.MessageID) (*models.Lease, error) {
	key := string(msgID)
	lease, exists := m.leases[key]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return lease, nil
}

func (m *MockStorage) DeleteLease(ctx context.Context, topic string, partition uint32, msgID models.MessageID) error {
	key := string(msgID)
	delete(m.leases, key)
	return nil
}

// Implement other required storage methods as no-ops for testing
func (m *MockStorage) ListMessages(ctx context.Context, topic string, partition uint32, limit int) ([]*models.Message, error) {
	return nil, nil
}
func (m *MockStorage) ListExpiredLeases(ctx context.Context, before time.Time, limit int) ([]*models.Lease, error) {
	return nil, nil
}
func (m *MockStorage) StoreDedup(ctx context.Context, topic string, producerKey models.ProducerKey, seq uint64, msgID models.MessageID, ttl time.Duration) error {
	return nil
}
func (m *MockStorage) CheckDedup(ctx context.Context, topic string, producerKey models.ProducerKey, seq uint64) (models.MessageID, bool, error) {
	return "", false, nil
}
func (m *MockStorage) CleanupExpiredDedup(ctx context.Context, before time.Time) (int, error) {
	return 0, nil
}
func (m *MockStorage) StoreSnapshot(ctx context.Context, topic string, partition uint32, snapshotType storage.SnapshotType, data []byte) error {
	return nil
}
func (m *MockStorage) GetSnapshot(ctx context.Context, topic string, partition uint32, snapshotType storage.SnapshotType) ([]byte, error) {
	return nil, storage.ErrNotFound
}
func (m *MockStorage) DeleteSnapshot(ctx context.Context, topic string, partition uint32, snapshotType storage.SnapshotType) error {
	return nil
}
func (m *MockStorage) StoreTopicConfig(ctx context.Context, config *models.TopicConfig) error {
	return nil
}
func (m *MockStorage) GetTopicConfig(ctx context.Context, topic string) (*models.TopicConfig, error) {
	return nil, storage.ErrNotFound
}
func (m *MockStorage) DeleteTopicConfig(ctx context.Context, topic string) error {
	return nil
}
func (m *MockStorage) ListTopicConfigs(ctx context.Context) ([]*models.TopicConfig, error) {
	return nil, nil
}
func (m *MockStorage) StorePartitionInfo(ctx context.Context, info *models.PartitionInfo) error {
	return nil
}
func (m *MockStorage) GetPartitionInfo(ctx context.Context, topic string, partition uint32) (*models.PartitionInfo, error) {
	return nil, storage.ErrNotFound
}
func (m *MockStorage) ListPartitionInfos(ctx context.Context, topic string) ([]*models.PartitionInfo, error) {
	return nil, nil
}
func (m *MockStorage) Batch() storage.StorageBatch {
	return nil
}
func (m *MockStorage) Open(ctx context.Context) error {
	return nil
}
func (m *MockStorage) Close(ctx context.Context) error {
	return nil
}
func (m *MockStorage) Health(ctx context.Context) error {
	return nil
}
func (m *MockStorage) Compact(ctx context.Context) error {
	return nil
}
func (m *MockStorage) Stats(ctx context.Context) (storage.StorageStats, error) {
	return storage.StorageStats{}, nil
}

// setupTestPartition creates a partition for testing
func setupTestPartition(t *testing.T) (*LocalPartition, *MockStorage) {
	logger := zaptest.NewLogger(t)
	mockStorage := NewMockStorage()

	topicConfig := models.NewTopicConfig("test-topic", models.MinHeap, 4, 3)
	topicConfig.VisibilityTimeoutDefault = 30 * time.Second
	topicConfig.MaxPayloadBytes = 1024 * 1024

	config := &PartitionConfig{
		PartitionID:         1,
		Topic:               "test-topic",
		TopicConfig:         topicConfig,
		Storage:             mockStorage,
		CheckInterval:       10 * time.Millisecond,
		DefaultLeaseTimeout: 30 * time.Second,
	}

	partition, err := NewLocalPartition(config, logger)
	require.NoError(t, err)

	// Start the partition
	ctx := context.Background()
	err = partition.Start(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		partition.Stop(context.Background())
	})

	return partition, mockStorage
}

// TestPartition_Lifecycle tests partition lifecycle operations
func TestPartition_Lifecycle(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStorage := NewMockStorage()

	topicConfig := models.NewTopicConfig("test-topic", models.MinHeap, 4, 3)
	config := &PartitionConfig{
		PartitionID: 1,
		Topic:       "test-topic",
		TopicConfig: topicConfig,
		Storage:     mockStorage,
	}

	partition, err := NewLocalPartition(config, logger)
	require.NoError(t, err)

	// Test initial state
	assert.False(t, partition.isRunning)
	assert.True(t, partition.IsHealthy())

	// Start partition
	ctx := context.Background()
	err = partition.Start(ctx)
	require.NoError(t, err)
	assert.True(t, partition.isRunning)

	// Test double start
	err = partition.Start(ctx)
	assert.Error(t, err)

	// Stop partition
	err = partition.Stop(ctx)
	require.NoError(t, err)
	assert.False(t, partition.isRunning)

	// Test double stop
	err = partition.Stop(ctx)
	assert.NoError(t, err) // Should not error
}

// TestPartition_EnqueuePop tests basic enqueue and pop operations
func TestPartition_EnqueuePop(t *testing.T) {
	partition, _ := setupTestPartition(t)
	ctx := context.Background()

	// Create test messages with different priorities
	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg2 := models.NewMessage("test-topic", 1, 5, []byte("payload2")) // Higher priority
	msg3 := models.NewMessage("test-topic", 1, 15, []byte("payload3"))

	// Enqueue messages
	err := partition.Enqueue(ctx, msg1)
	require.NoError(t, err)

	err = partition.Enqueue(ctx, msg2)
	require.NoError(t, err)

	err = partition.Enqueue(ctx, msg3)
	require.NoError(t, err)

	// Pop messages - should come out in priority order (min-heap)
	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)
	assert.Equal(t, msg2.ID, poppedMsg.ID) // Priority 5 (highest in min-heap)
	assert.NotNil(t, poppedMsg.Lease)
	assert.Equal(t, "consumer-1", poppedMsg.Lease.Holder)

	poppedMsg, err = partition.Pop(ctx, "consumer-2", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)
	assert.Equal(t, msg1.ID, poppedMsg.ID) // Priority 10

	poppedMsg, err = partition.Pop(ctx, "consumer-3", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)
	assert.Equal(t, msg3.ID, poppedMsg.ID) // Priority 15

	// No more messages
	poppedMsg, err = partition.Pop(ctx, "consumer-4", 30*time.Second)
	require.NoError(t, err)
	assert.Nil(t, poppedMsg)
}

// TestPartition_AckMessage tests message acknowledgment
func TestPartition_AckMessage(t *testing.T) {
	partition, mockStorage := setupTestPartition(t)
	ctx := context.Background()

	// Enqueue a message
	msg := models.NewMessage("test-topic", 1, 10, []byte("payload"))
	err := partition.Enqueue(ctx, msg)
	require.NoError(t, err)

	// Pop the message
	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)

	// Ack the message
	err = partition.Ack(ctx, poppedMsg.ID, poppedMsg.Lease.Token)
	require.NoError(t, err)

	// Message should be deleted from storage
	_, exists := mockStorage.messages[string(msg.ID)]
	assert.False(t, exists)

	// Lease should be deleted from storage
	_, exists = mockStorage.leases[string(msg.ID)]
	assert.False(t, exists)

	// Test acking with invalid token
	err = partition.Ack(ctx, poppedMsg.ID, "invalid-token")
	assert.Error(t, err)
}

// TestPartition_NackMessage tests message negative acknowledgment
func TestPartition_NackMessage(t *testing.T) {
	partition, mockStorage := setupTestPartition(t)
	ctx := context.Background()

	// Enqueue a message
	msg := models.NewMessage("test-topic", 1, 10, []byte("payload"))
	err := partition.Enqueue(ctx, msg)
	require.NoError(t, err)

	// Pop the message
	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)

	// Nack the message (immediate retry)
	backoff := time.Duration(0)
	err = partition.Nack(ctx, poppedMsg.ID, poppedMsg.Lease.Token, &backoff)
	require.NoError(t, err)

	// Message should be back in the heap
	// Wait a moment for background processing
	time.Sleep(10 * time.Millisecond)

	// Should be able to pop again
	repoppedMsg, err := partition.Pop(ctx, "consumer-2", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, repoppedMsg)
	assert.Equal(t, msg.ID, repoppedMsg.ID)

	// Retry count should be incremented
	storedMsg, exists := mockStorage.messages[string(msg.ID)]
	require.True(t, exists)
	assert.Equal(t, int32(1), storedMsg.RetryCount)
}

// TestPartition_NackWithDelay tests nack with backoff delay
func TestPartition_NackWithDelay(t *testing.T) {
	partition, _ := setupTestPartition(t)
	ctx := context.Background()

	// Enqueue a message
	msg := models.NewMessage("test-topic", 1, 10, []byte("payload"))
	err := partition.Enqueue(ctx, msg)
	require.NoError(t, err)

	// Pop the message
	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)

	// Nack the message with delay
	backoff := 100 * time.Millisecond
	err = partition.Nack(ctx, poppedMsg.ID, poppedMsg.Lease.Token, &backoff)
	require.NoError(t, err)

	// Should not be immediately available
	repoppedMsg, err := partition.Pop(ctx, "consumer-2", 30*time.Second)
	require.NoError(t, err)
	assert.Nil(t, repoppedMsg)

	// Wait for backoff to expire
	time.Sleep(150 * time.Millisecond)

	// Should now be available
	repoppedMsg, err = partition.Pop(ctx, "consumer-3", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, repoppedMsg)
	assert.Equal(t, msg.ID, repoppedMsg.ID)
}

// TestPartition_ExtendLease tests lease extension
func TestPartition_ExtendLease(t *testing.T) {
	partition, _ := setupTestPartition(t)
	ctx := context.Background()

	// Enqueue a message
	msg := models.NewMessage("test-topic", 1, 10, []byte("payload"))
	err := partition.Enqueue(ctx, msg)
	require.NoError(t, err)

	// Pop the message
	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)

	// Extend the lease
	extension := 30 * time.Second
	err = partition.ExtendLease(ctx, poppedMsg.Lease.Token, extension)
	require.NoError(t, err)

	// Test extending with invalid token
	err = partition.ExtendLease(ctx, "invalid-token", extension)
	assert.Error(t, err)

	// Verify lease was extended in registry
	// (This would require accessing the lease registry directly or through stats)
}

// TestPartition_DelayedMessage tests delayed message processing
func TestPartition_DelayedMessage(t *testing.T) {
	partition, _ := setupTestPartition(t)
	ctx := context.Background()

	// Create delayed message
	futureTime := time.Now().Add(50 * time.Millisecond)
	msg := models.NewMessage("test-topic", 1, 10, []byte("payload"))
	msg.NotBefore = &futureTime

	// Enqueue delayed message
	err := partition.Enqueue(ctx, msg)
	require.NoError(t, err)

	// Should not be immediately available
	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	assert.Nil(t, poppedMsg)

	// Wait for delay to expire plus processing time
	time.Sleep(100 * time.Millisecond)

	// Should now be available
	poppedMsg, err = partition.Pop(ctx, "consumer-2", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)
	assert.Equal(t, msg.ID, poppedMsg.ID)
}

// TestPartition_Peek tests peeking at messages
func TestPartition_Peek(t *testing.T) {
	partition, _ := setupTestPartition(t)
	ctx := context.Background()

	// Enqueue messages with different priorities
	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg2 := models.NewMessage("test-topic", 1, 5, []byte("payload2"))
	msg3 := models.NewMessage("test-topic", 1, 15, []byte("payload3"))

	err := partition.Enqueue(ctx, msg1)
	require.NoError(t, err)
	err = partition.Enqueue(ctx, msg2)
	require.NoError(t, err)
	err = partition.Enqueue(ctx, msg3)
	require.NoError(t, err)

	// Peek at messages
	peekedMessages, err := partition.Peek(ctx, 2)
	require.NoError(t, err)
	require.Len(t, peekedMessages, 2)

	// Should be in priority order
	assert.Equal(t, msg2.ID, peekedMessages[0].ID) // Priority 5
	assert.Equal(t, msg1.ID, peekedMessages[1].ID) // Priority 10

	// Messages should still be in the heap
	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, poppedMsg.ID)
}

// TestPartition_Stats tests partition statistics
func TestPartition_Stats(t *testing.T) {
	partition, _ := setupTestPartition(t)
	ctx := context.Background()

	// Initial stats
	stats := partition.Stats()
	assert.Equal(t, "test-topic", stats.Topic)
	assert.Equal(t, models.PartitionID(1), stats.PartitionID)
	assert.True(t, stats.IsHealthy)
	assert.False(t, stats.IsThrottled)

	// Enqueue a message
	msg := models.NewMessage("test-topic", 1, 10, []byte("payload"))
	err := partition.Enqueue(ctx, msg)
	require.NoError(t, err)

	// Stats should reflect the message
	stats = partition.Stats()
	assert.Equal(t, uint64(1), stats.MessageCount)

	// Pop the message
	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)

	// Stats should show inflight message
	stats = partition.Stats()
	assert.Equal(t, uint64(0), stats.MessageCount) // No longer in heap
	assert.Equal(t, uint64(1), stats.InflightCount)

	// Ack the message
	err = partition.Ack(ctx, poppedMsg.ID, poppedMsg.Lease.Token)
	require.NoError(t, err)

	// Stats should show acked message
	stats = partition.Stats()
	assert.Equal(t, uint64(0), stats.MessageCount)
	assert.Equal(t, uint64(0), stats.InflightCount)
	assert.Equal(t, uint64(1), stats.AckedCount)
}

// TestPartition_ThrottleUnthrottle tests partition throttling
func TestPartition_ThrottleUnthrottle(t *testing.T) {
	partition, _ := setupTestPartition(t)
	ctx := context.Background()

	// Initially not throttled
	assert.False(t, partition.isThrottled)

	// Throttle partition
	partition.Throttle("test throttling")
	assert.True(t, partition.isThrottled)
	assert.Equal(t, "test throttling", partition.throttleReason)

	// Operations should fail when throttled
	msg := models.NewMessage("test-topic", 1, 10, []byte("payload"))
	err := partition.Enqueue(ctx, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "throttled")

	_, err = partition.Pop(ctx, "consumer-1", 30*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "throttled")

	// Unthrottle partition
	partition.Unthrottle()
	assert.False(t, partition.isThrottled)
	assert.Equal(t, "", partition.throttleReason)

	// Operations should work again
	err = partition.Enqueue(ctx, msg)
	require.NoError(t, err)

	poppedMsg, err := partition.Pop(ctx, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, poppedMsg)
}

// TestPartition_GetCandidates tests spine index candidate selection
func TestPartition_GetCandidates(t *testing.T) {
	partition, _ := setupTestPartition(t)
	ctx := context.Background()

	// Enqueue messages with different priorities
	messages := []*models.Message{
		models.NewMessage("test-topic", 1, 30, []byte("payload1")),
		models.NewMessage("test-topic", 1, 10, []byte("payload2")),
		models.NewMessage("test-topic", 1, 20, []byte("payload3")),
		models.NewMessage("test-topic", 1, 5, []byte("payload4")),
	}

	for _, msg := range messages {
		err := partition.Enqueue(ctx, msg)
		require.NoError(t, err)
	}

	// Get top 3 candidates
	candidates := partition.GetCandidates(3)
	require.Len(t, candidates, 3)

	// Should be in priority order (min-heap)
	assert.Equal(t, int64(5), candidates[0].Priority)
	assert.Equal(t, int64(10), candidates[1].Priority)
	assert.Equal(t, int64(20), candidates[2].Priority)

	// Get more candidates than available
	candidates = partition.GetCandidates(10)
	require.Len(t, candidates, 4)

	// Get zero candidates
	candidates = partition.GetCandidates(0)
	assert.Nil(t, candidates)
}
