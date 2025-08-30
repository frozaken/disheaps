package idempotent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
)

// MockStorage implements storage.Storage for testing deduplication
type MockStorage struct {
	mu      sync.RWMutex
	dedup   map[string]models.MessageID // key -> message_id
	ttls    map[string]time.Time        // key -> expiry time
	cleaned int
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		dedup: make(map[string]models.MessageID),
		ttls:  make(map[string]time.Time),
	}
}

func (m *MockStorage) buildDedupKey(topic string, producerKey models.ProducerKey, seq uint64) string {
	return fmt.Sprintf("dedup:%s:%s:%d:%d", topic, producerKey.ID, producerKey.Epoch, seq)
}

func (m *MockStorage) StoreDedup(ctx context.Context, topic string, producerKey models.ProducerKey, seq uint64, msgID models.MessageID, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.buildDedupKey(topic, producerKey, seq)
	m.dedup[key] = msgID
	m.ttls[key] = time.Now().Add(ttl)
	return nil
}

func (m *MockStorage) CheckDedup(ctx context.Context, topic string, producerKey models.ProducerKey, seq uint64) (models.MessageID, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.buildDedupKey(topic, producerKey, seq)
	msgID, exists := m.dedup[key]
	if !exists {
		return "", false, nil
	}

	// Check if expired
	if expiry, hasExpiry := m.ttls[key]; hasExpiry && time.Now().After(expiry) {
		return "", false, nil
	}

	return msgID, true, nil
}

func (m *MockStorage) CleanupExpiredDedup(ctx context.Context, before time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cleaned := 0
	for key, expiry := range m.ttls {
		if before.After(expiry) {
			delete(m.dedup, key)
			delete(m.ttls, key)
			cleaned++
		}
	}

	m.cleaned += cleaned
	return cleaned, nil
}

func (m *MockStorage) Health(ctx context.Context) error {
	return nil
}

func (m *MockStorage) GetCleaned() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cleaned
}

// Implement minimal storage.Storage interface methods (not used in dedup tests)
func (m *MockStorage) StoreMessage(ctx context.Context, message *models.Message) error { return nil }
func (m *MockStorage) GetMessage(ctx context.Context, topic string, partition uint32, msgID models.MessageID) (*models.Message, error) {
	return nil, nil
}
func (m *MockStorage) DeleteMessage(ctx context.Context, topic string, partition uint32, msgID models.MessageID) error {
	return nil
}
func (m *MockStorage) ListMessages(ctx context.Context, topic string, partition uint32, limit int) ([]*models.Message, error) {
	return nil, nil
}
func (m *MockStorage) StoreLease(ctx context.Context, topic string, partition uint32, lease *models.Lease) error {
	return nil
}
func (m *MockStorage) GetLease(ctx context.Context, topic string, partition uint32, msgID models.MessageID) (*models.Lease, error) {
	return nil, nil
}
func (m *MockStorage) DeleteLease(ctx context.Context, topic string, partition uint32, msgID models.MessageID) error {
	return nil
}
func (m *MockStorage) ListExpiredLeases(ctx context.Context, before time.Time, limit int) ([]*models.Lease, error) {
	return nil, nil
}
func (m *MockStorage) StoreSnapshot(ctx context.Context, topic string, partition uint32, snapshotType storage.SnapshotType, data []byte) error {
	return nil
}
func (m *MockStorage) GetSnapshot(ctx context.Context, topic string, partition uint32, snapshotType storage.SnapshotType) ([]byte, error) {
	return nil, nil
}
func (m *MockStorage) DeleteSnapshot(ctx context.Context, topic string, partition uint32, snapshotType storage.SnapshotType) error {
	return nil
}
func (m *MockStorage) StoreTopicConfig(ctx context.Context, config *models.TopicConfig) error {
	return nil
}
func (m *MockStorage) GetTopicConfig(ctx context.Context, topic string) (*models.TopicConfig, error) {
	return nil, nil
}
func (m *MockStorage) DeleteTopicConfig(ctx context.Context, topic string) error { return nil }
func (m *MockStorage) ListTopicConfigs(ctx context.Context) ([]*models.TopicConfig, error) {
	return nil, nil
}
func (m *MockStorage) StorePartitionInfo(ctx context.Context, info *models.PartitionInfo) error {
	return nil
}
func (m *MockStorage) GetPartitionInfo(ctx context.Context, topic string, partition uint32) (*models.PartitionInfo, error) {
	return nil, nil
}
func (m *MockStorage) ListPartitionInfos(ctx context.Context, topic string) ([]*models.PartitionInfo, error) {
	return nil, nil
}
func (m *MockStorage) Batch() storage.StorageBatch       { return nil }
func (m *MockStorage) Open(ctx context.Context) error    { return nil }
func (m *MockStorage) Close(ctx context.Context) error   { return nil }
func (m *MockStorage) Compact(ctx context.Context) error { return nil }
func (m *MockStorage) Stats(ctx context.Context) (storage.StorageStats, error) {
	return storage.StorageStats{}, nil
}

// Helper functions
func createTestDeduplicator(t *testing.T) (*Deduplicator, *MockStorage) {
	config := DefaultDeduplicationConfig()
	config.CleanupInterval = 100 * time.Millisecond // Fast cleanup for testing

	mockStorage := NewMockStorage()
	logger := zaptest.NewLogger(t)

	dedup := NewDeduplicator(config, mockStorage, logger)
	return dedup, mockStorage
}

func createTestMessageID(id string) models.MessageID {
	return models.MessageID(fmt.Sprintf("msg_%s_%d", id, time.Now().UnixNano()))
}

// Test cases
func TestDeduplicator_Creation(t *testing.T) {
	dedup, _ := createTestDeduplicator(t)
	require.NotNil(t, dedup)
	assert.NotNil(t, dedup.config)
	assert.NotNil(t, dedup.cache)
	assert.Equal(t, 24*time.Hour, dedup.config.DefaultTTL)
}

func TestDeduplicator_CheckAndStore_NewEntry(t *testing.T) {
	dedup, mockStorage := createTestDeduplicator(t)
	ctx := context.Background()

	topic := "test-topic"
	producerID := "producer-1"
	epoch := uint64(1)
	sequence := uint64(1)
	messageID := createTestMessageID("1")
	ttl := 1 * time.Hour

	// First call should store the entry
	resultID, isDuplicate, err := dedup.CheckAndStore(ctx, topic, producerID, epoch, sequence, messageID, ttl)
	require.NoError(t, err)
	assert.False(t, isDuplicate)
	assert.Equal(t, messageID, resultID)

	// Verify it was stored in mock storage
	producerKey := models.ProducerKey{ID: producerID, Epoch: epoch}
	storedID, exists, err := mockStorage.CheckDedup(ctx, topic, producerKey, sequence)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, messageID, storedID)
}

func TestDeduplicator_CheckAndStore_DuplicateEntry(t *testing.T) {
	dedup, _ := createTestDeduplicator(t)
	ctx := context.Background()

	topic := "test-topic"
	producerID := "producer-1"
	epoch := uint64(1)
	sequence := uint64(1)
	originalID := createTestMessageID("original")
	duplicateID := createTestMessageID("duplicate")
	ttl := 1 * time.Hour

	// First call stores the entry
	_, isDuplicate, err := dedup.CheckAndStore(ctx, topic, producerID, epoch, sequence, originalID, ttl)
	require.NoError(t, err)
	assert.False(t, isDuplicate)

	// Second call should detect duplicate and return original ID
	resultID, isDuplicate, err := dedup.CheckAndStore(ctx, topic, producerID, epoch, sequence, duplicateID, ttl)
	require.NoError(t, err)
	assert.True(t, isDuplicate)
	assert.Equal(t, originalID, resultID)
}

func TestDeduplicator_CacheHit(t *testing.T) {
	dedup, _ := createTestDeduplicator(t)
	ctx := context.Background()

	topic := "test-topic"
	producerID := "producer-1"
	epoch := uint64(1)
	sequence := uint64(1)
	messageID := createTestMessageID("1")

	// Store in cache first
	dedup.CheckAndStore(ctx, topic, producerID, epoch, sequence, messageID, time.Hour)

	// Second check should hit cache
	resultID, isDuplicate, err := dedup.CheckDuplicate(ctx, topic, producerID, epoch, sequence)
	require.NoError(t, err)
	assert.True(t, isDuplicate)
	assert.Equal(t, messageID, resultID)

	// Verify cache stats
	stats := dedup.GetStats()
	assert.Greater(t, stats.CacheHits, uint64(0))
}

func TestDeduplicator_TTLBounds(t *testing.T) {
	config := DefaultDeduplicationConfig()
	config.MinTTL = 1 * time.Minute
	config.MaxTTL = 2 * time.Hour

	mockStorage := NewMockStorage()
	logger := zaptest.NewLogger(t)
	dedup := NewDeduplicator(config, mockStorage, logger)

	ctx := context.Background()
	topic := "test-topic"
	producerID := "producer-1"
	epoch := uint64(1)
	messageID := createTestMessageID("1")

	// Test minimum TTL enforcement
	_, _, err := dedup.CheckAndStore(ctx, topic, producerID, epoch, 1, messageID, 30*time.Second)
	require.NoError(t, err) // Should not error, but TTL should be clamped to minimum

	// Test maximum TTL enforcement
	_, _, err = dedup.CheckAndStore(ctx, topic, producerID, epoch, 2, messageID, 10*time.Hour)
	require.NoError(t, err) // Should not error, but TTL should be clamped to maximum
}

func TestDeduplicator_DifferentEpochs(t *testing.T) {
	dedup, _ := createTestDeduplicator(t)
	ctx := context.Background()

	topic := "test-topic"
	producerID := "producer-1"
	sequence := uint64(1)
	messageID1 := createTestMessageID("epoch1")
	messageID2 := createTestMessageID("epoch2")

	// Store with epoch 1
	_, isDuplicate, err := dedup.CheckAndStore(ctx, topic, producerID, 1, sequence, messageID1, time.Hour)
	require.NoError(t, err)
	assert.False(t, isDuplicate)

	// Store same sequence with epoch 2 - should not be duplicate
	_, isDuplicate, err = dedup.CheckAndStore(ctx, topic, producerID, 2, sequence, messageID2, time.Hour)
	require.NoError(t, err)
	assert.False(t, isDuplicate)

	// Verify both can be retrieved
	id1, exists, err := dedup.CheckDuplicate(ctx, topic, producerID, 1, sequence)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, messageID1, id1)

	id2, exists, err := dedup.CheckDuplicate(ctx, topic, producerID, 2, sequence)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, messageID2, id2)
}

func TestDeduplicator_Lifecycle(t *testing.T) {
	dedup, _ := createTestDeduplicator(t)
	ctx := context.Background()

	// Test start
	err := dedup.Start(ctx)
	require.NoError(t, err)

	// Test health check
	assert.True(t, dedup.IsHealthy())

	// Test stop
	err = dedup.Stop(ctx)
	require.NoError(t, err)
}

func TestDeduplicationCache_BasicOperations(t *testing.T) {
	cache := NewDeduplicationCache(10, 1000) // 10MB, 1000 window size

	topic := "test-topic"
	producerKey := models.ProducerKey{ID: "producer-1", Epoch: 1}
	sequence := uint64(1)
	messageID := createTestMessageID("1")

	// Initially empty
	_, exists := cache.Check(topic, producerKey, sequence)
	assert.False(t, exists)

	// Store entry
	cache.Store(topic, producerKey, sequence, messageID)

	// Should now exist
	retrievedID, exists := cache.Check(topic, producerKey, sequence)
	assert.True(t, exists)
	assert.Equal(t, messageID, retrievedID)

	// Check stats
	stats := cache.GetStats()
	assert.Equal(t, uint64(1), stats.Size)
	assert.Greater(t, stats.Hits, uint64(0))
	assert.Greater(t, stats.Misses, uint64(0))
}

func TestDeduplicationCache_SlidingWindow(t *testing.T) {
	cache := NewDeduplicationCache(10, 3) // Small window for testing

	topic := "test-topic"
	producerKey := models.ProducerKey{ID: "producer-1", Epoch: 1}

	// Fill window
	for i := uint64(1); i <= 5; i++ {
		messageID := createTestMessageID(fmt.Sprintf("%d", i))
		cache.Store(topic, producerKey, i, messageID)
	}

	// Early sequences should be evicted due to window size
	_, exists := cache.Check(topic, producerKey, 1)
	assert.False(t, exists, "sequence 1 should be evicted")

	_, exists = cache.Check(topic, producerKey, 2)
	assert.False(t, exists, "sequence 2 should be evicted")

	// Recent sequences should still exist
	_, exists = cache.Check(topic, producerKey, 5)
	assert.True(t, exists, "sequence 5 should exist")
}

func TestDeduplicationCache_ProducerWindows(t *testing.T) {
	cache := NewDeduplicationCache(10, 1000)

	topic := "test-topic"
	producerKey1 := models.ProducerKey{ID: "producer-1", Epoch: 1}
	producerKey2 := models.ProducerKey{ID: "producer-2", Epoch: 1}

	// Store sequences for different producers
	cache.Store(topic, producerKey1, 1, createTestMessageID("p1-1"))
	cache.Store(topic, producerKey1, 5, createTestMessageID("p1-5"))
	cache.Store(topic, producerKey2, 3, createTestMessageID("p2-3"))

	windows := cache.GetProducerWindows()
	assert.Len(t, windows, 2)

	// Find producer-1 window
	var p1Window *ProducerWindowInfo
	for _, window := range windows {
		if window.ProducerID == "producer-1" {
			p1Window = window
			break
		}
	}

	require.NotNil(t, p1Window)
	assert.Equal(t, uint64(1), p1Window.MinSequence)
	assert.Equal(t, uint64(5), p1Window.MaxSequence)
	assert.Equal(t, 2, p1Window.SequenceCount)
}

func TestProducerStateManager_BasicOperations(t *testing.T) {
	logger := zaptest.NewLogger(t)
	psm := NewProducerStateManager(logger)

	producerID := "producer-1"
	epoch := uint64(1)
	topic := "test-topic"
	sequence := uint64(1)

	// Initially no producers
	assert.Equal(t, 0, psm.GetActiveProducers())

	// Register activity
	psm.RegisterActivity(producerID, epoch, topic, sequence, false)
	assert.Equal(t, 1, psm.GetActiveProducers())

	// Get producer state
	state, exists := psm.GetProducerState(producerID)
	require.True(t, exists)
	assert.Equal(t, producerID, state.ID)
	assert.Equal(t, epoch, state.Epoch)
	assert.Equal(t, uint64(1), state.MessageCount)
	assert.Equal(t, uint64(0), state.DuplicateCount)
	assert.True(t, state.Topics[topic])

	// Register duplicate activity
	psm.RegisterActivity(producerID, epoch, topic, sequence, true)
	state, _ = psm.GetProducerState(producerID)
	assert.Equal(t, uint64(2), state.MessageCount)
	assert.Equal(t, uint64(1), state.DuplicateCount)
}

func TestProducerStateManager_EpochChange(t *testing.T) {
	logger := zaptest.NewLogger(t)
	psm := NewProducerStateManager(logger)

	producerID := "producer-1"
	topic := "test-topic"

	// Register with epoch 1
	psm.RegisterActivity(producerID, 1, topic, 5, false)
	state, _ := psm.GetProducerState(producerID)
	assert.Equal(t, uint64(5), state.LastSequence[topic])

	// Register with epoch 2 (should reset sequences)
	psm.RegisterActivity(producerID, 2, topic, 1, false)
	state, _ = psm.GetProducerState(producerID)
	assert.Equal(t, uint64(2), state.Epoch)
	assert.Equal(t, uint64(1), state.LastSequence[topic])
}

func TestProducerStateManager_SequenceOrdering(t *testing.T) {
	logger := zaptest.NewLogger(t)
	psm := NewProducerStateManager(logger)

	producerID := "producer-1"
	epoch := uint64(1)
	topic := "test-topic"

	// Initially any sequence is in order
	assert.True(t, psm.IsSequenceInOrder(producerID, epoch, topic, 5))

	// Register sequence 5
	psm.RegisterActivity(producerID, epoch, topic, 5, false)

	// Sequence 6 should be in order
	assert.True(t, psm.IsSequenceInOrder(producerID, epoch, topic, 6))

	// Sequence 3 should not be in order
	assert.False(t, psm.IsSequenceInOrder(producerID, epoch, topic, 3))

	// Expected next sequence should be 6
	assert.Equal(t, uint64(6), psm.GetExpectedNextSequence(producerID, epoch, topic))
}

func TestCleanupManager_BasicOperations(t *testing.T) {
	config := DefaultCleanupConfig()
	config.Interval = 50 * time.Millisecond // Fast for testing

	mockStorage := NewMockStorage()
	logger := zaptest.NewLogger(t)

	cm := NewCleanupManager(config, mockStorage, logger)
	ctx := context.Background()

	// Test immediate cleanup
	result, err := cm.RunCleanupNow(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Success)

	// Test stats
	stats := cm.GetStats()
	assert.Greater(t, stats.TotalCleanupRuns, uint64(0))

	// Test health
	assert.True(t, cm.IsHealthy())
}

func TestCleanupManager_Lifecycle(t *testing.T) {
	config := DefaultCleanupConfig()
	mockStorage := NewMockStorage()
	logger := zaptest.NewLogger(t)

	cm := NewCleanupManager(config, mockStorage, logger)
	ctx := context.Background()

	// Test start
	err := cm.Start(ctx)
	require.NoError(t, err)

	// Wait a bit for background operations
	time.Sleep(10 * time.Millisecond)

	// Test stop
	err = cm.Stop(ctx)
	require.NoError(t, err)
}

func TestIntegration_DeduplicationFlow(t *testing.T) {
	// Create components
	dedup, _ := createTestDeduplicator(t)
	logger := zaptest.NewLogger(t)
	psm := NewProducerStateManager(logger)

	ctx := context.Background()

	// Start components
	err := dedup.Start(ctx)
	require.NoError(t, err)
	defer dedup.Stop(ctx)

	err = psm.Start(ctx)
	require.NoError(t, err)
	defer psm.Stop(ctx)

	topic := "integration-topic"
	producerID := "integration-producer"
	epoch := uint64(1)

	// Simulate producer sending messages
	for i := uint64(1); i <= 10; i++ {
		messageID := createTestMessageID(fmt.Sprintf("msg_%d", i))

		// Check and store
		resultID, isDuplicate, err := dedup.CheckAndStore(ctx, topic, producerID, epoch, i, messageID, time.Hour)
		require.NoError(t, err)
		assert.False(t, isDuplicate)
		assert.Equal(t, messageID, resultID)

		// Register with producer state manager
		psm.RegisterActivity(producerID, epoch, topic, i, isDuplicate)
	}

	// Verify producer state
	state, exists := psm.GetProducerState(producerID)
	require.True(t, exists)
	assert.Equal(t, uint64(10), state.MessageCount)
	assert.Equal(t, uint64(0), state.DuplicateCount)
	assert.Equal(t, uint64(10), state.LastSequence[topic])

	// Test duplicate detection
	duplicateID := createTestMessageID("duplicate")
	resultID, isDuplicate, err := dedup.CheckAndStore(ctx, topic, producerID, epoch, 5, duplicateID, time.Hour)
	require.NoError(t, err)
	assert.True(t, isDuplicate)
	assert.NotEqual(t, duplicateID, resultID) // Should return original ID

	// Register duplicate
	psm.RegisterActivity(producerID, epoch, topic, 5, isDuplicate)
	state, _ = psm.GetProducerState(producerID)
	assert.Equal(t, uint64(11), state.MessageCount)
	assert.Equal(t, uint64(1), state.DuplicateCount)
}

func TestConcurrency_DeduplicationSafety(t *testing.T) {
	dedup, _ := createTestDeduplicator(t)
	ctx := context.Background()

	topic := "concurrency-topic"
	producerID := "concurrent-producer"
	epoch := uint64(1)
	numGoroutines := 10
	numSequences := 100

	var wg sync.WaitGroup
	duplicateCount := int64(0)

	// Launch multiple goroutines trying to store the same sequences
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for seq := uint64(1); seq <= uint64(numSequences); seq++ {
				messageID := createTestMessageID(fmt.Sprintf("g%d_seq%d", goroutineID, seq))

				_, isDuplicate, err := dedup.CheckAndStore(ctx, topic, producerID, epoch, seq, messageID, time.Hour)
				require.NoError(t, err)

				if isDuplicate {
					duplicateCount++
				}
			}
		}(g)
	}

	wg.Wait()

	// Most should be duplicates (only first goroutine to store each sequence should succeed)
	expectedDuplicates := int64(numGoroutines-1) * int64(numSequences)
	assert.Equal(t, expectedDuplicates, duplicateCount)
}

// Benchmarks
func BenchmarkDeduplicator_CheckAndStore(b *testing.B) {
	dedup, _ := createTestDeduplicator(&testing.T{})
	ctx := context.Background()

	topic := "bench-topic"
	producerID := "bench-producer"
	epoch := uint64(1)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		messageID := createTestMessageID(fmt.Sprintf("bench_%d", i))
		_, _, err := dedup.CheckAndStore(ctx, topic, producerID, epoch, uint64(i), messageID, time.Hour)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeduplicationCache_Operations(b *testing.B) {
	cache := NewDeduplicationCache(100, 10000) // 100MB, 10K window

	topic := "bench-topic"
	producerKey := models.ProducerKey{ID: "bench-producer", Epoch: 1}

	// Pre-populate cache
	for i := uint64(1); i <= 1000; i++ {
		messageID := createTestMessageID(fmt.Sprintf("prepop_%d", i))
		cache.Store(topic, producerKey, i, messageID)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sequence := uint64(i%1000 + 1) // Mix of hits and misses
		cache.Check(topic, producerKey, sequence)
	}
}
