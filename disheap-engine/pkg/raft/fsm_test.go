package raft

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
)

// MockPartition implements the partition.Partition interface for testing
type MockPartition struct {
	messages     map[string]*models.Message
	leases       map[string]string
	ackCount     int
	nackCount    int
	extendCount  int
	enqueueCount int
}

func NewMockPartition() *MockPartition {
	return &MockPartition{
		messages: make(map[string]*models.Message),
		leases:   make(map[string]string),
	}
}

func (m *MockPartition) Enqueue(ctx context.Context, message *models.Message) error {
	m.messages[message.ID.String()] = message
	m.enqueueCount++
	return nil
}

func (m *MockPartition) Pop(ctx context.Context, consumer string, timeout time.Duration) (*models.Message, error) {
	// Return first available message for testing
	for id, message := range m.messages {
		m.leases[id] = consumer
		delete(m.messages, id)
		return message, nil
	}
	return nil, nil
}

func (m *MockPartition) Peek(ctx context.Context, count int) ([]*models.Message, error) {
	var messages []*models.Message
	for _, message := range m.messages {
		messages = append(messages, message)
		if len(messages) >= count {
			break
		}
	}
	return messages, nil
}

func (m *MockPartition) Ack(ctx context.Context, messageID models.MessageID, leaseToken models.LeaseToken) error {
	delete(m.leases, messageID.String())
	m.ackCount++
	return nil
}

func (m *MockPartition) Nack(ctx context.Context, messageID models.MessageID, leaseToken models.LeaseToken, backoff *time.Duration) error {
	delete(m.leases, messageID.String())
	m.nackCount++
	return nil
}

func (m *MockPartition) ExtendLease(ctx context.Context, leaseToken models.LeaseToken, extension time.Duration) error {
	m.extendCount++
	return nil
}

func (m *MockPartition) Stats() models.PartitionStats {
	return models.PartitionStats{
		MessageCount:  uint64(len(m.messages)),
		InflightCount: uint64(len(m.leases)),
	}
}

func (m *MockPartition) GetCandidates(count int) []*models.Message {
	messages, _ := m.Peek(context.Background(), count)
	return messages
}

func (m *MockPartition) Start(ctx context.Context) error {
	return nil
}

func (m *MockPartition) Stop(ctx context.Context) error {
	return nil
}

func (m *MockPartition) IsHealthy() bool {
	return true
}

func (m *MockPartition) Throttle(reason string) {
	// Test implementation - no-op
}

func (m *MockPartition) Unthrottle() {
	// Test implementation - no-op
}

// Test helper functions
func createTestFSM(t *testing.T) *HeapFSM {
	logger := zaptest.NewLogger(t)
	storage := createTestStorage(t)
	return NewHeapFSM(storage, logger)
}

func createTestStorage(t *testing.T) storage.Storage {
	// Create a mock storage implementation
	return &MockStorage{}
}

func createTestMessage(id string, priority int) *models.Message {
	return &models.Message{
		ID:         models.MessageID(id),
		Priority:   int64(priority),
		Payload:    []byte("test payload"),
		EnqueuedAt: time.Now(),
	}
}

func createTestLogEntry(entryType EntryType, topic string, partition uint32, data interface{}) *raft.Log {
	logEntry, err := NewLogEntry(entryType, topic, partition, data)
	if err != nil {
		panic(fmt.Sprintf("failed to create log entry: %v", err))
	}
	serialized, err := logEntry.Serialize()
	if err != nil {
		panic(fmt.Sprintf("failed to serialize log entry: %v", err))
	}

	return &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  serialized,
	}
}

// MockStorage implements the storage.Storage interface for testing
type MockStorage struct {
	topics   map[string]*models.TopicConfig
	messages map[string]map[uint32][]*models.Message
}

func (m *MockStorage) StoreMessage(ctx context.Context, message *models.Message) error {
	// For testing, we'll store under a default topic/partition
	topic := "test-topic"
	partition := uint32(0)

	if m.messages == nil {
		m.messages = make(map[string]map[uint32][]*models.Message)
	}
	if m.messages[topic] == nil {
		m.messages[topic] = make(map[uint32][]*models.Message)
	}
	m.messages[topic][partition] = append(m.messages[topic][partition], message)
	return nil
}

func (m *MockStorage) GetMessage(ctx context.Context, topic string, partition uint32, messageID models.MessageID) (*models.Message, error) {
	return nil, nil
}

func (m *MockStorage) DeleteMessage(ctx context.Context, topic string, partition uint32, messageID models.MessageID) error {
	return nil
}

func (m *MockStorage) ListMessages(ctx context.Context, topic string, partition uint32, limit int) ([]*models.Message, error) {
	if m.messages == nil || m.messages[topic] == nil {
		return nil, nil
	}
	messages := m.messages[topic][partition]
	if limit > len(messages) {
		limit = len(messages)
	}
	return messages[:limit], nil
}

func (m *MockStorage) StoreTopicConfig(ctx context.Context, config *models.TopicConfig) error {
	if m.topics == nil {
		m.topics = make(map[string]*models.TopicConfig)
	}
	m.topics[config.Name] = config
	return nil
}

func (m *MockStorage) GetTopicConfig(ctx context.Context, topic string) (*models.TopicConfig, error) {
	if m.topics == nil || m.topics[topic] == nil {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	return m.topics[topic], nil
}

func (m *MockStorage) DeleteTopicConfig(ctx context.Context, topic string) error {
	if m.topics != nil {
		delete(m.topics, topic)
	}
	return nil
}

func (m *MockStorage) ListTopicConfigs(ctx context.Context) ([]*models.TopicConfig, error) {
	var configs []*models.TopicConfig
	for _, config := range m.topics {
		configs = append(configs, config)
	}
	return configs, nil
}

func (m *MockStorage) GetPartitionInfo(ctx context.Context, topic string, partition uint32) (*models.PartitionInfo, error) {
	return &models.PartitionInfo{
		ID:           models.PartitionID(partition),
		Topic:        topic,
		MessageCount: 0,
	}, nil
}

func (m *MockStorage) Close(ctx context.Context) error {
	return nil
}

// Additional required methods for storage.Storage interface
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
	return nil, nil
}

func (m *MockStorage) DeleteSnapshot(ctx context.Context, topic string, partition uint32, snapshotType storage.SnapshotType) error {
	return nil
}

func (m *MockStorage) StorePartitionInfo(ctx context.Context, info *models.PartitionInfo) error {
	return nil
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

func (m *MockStorage) Health(ctx context.Context) error {
	return nil
}

func (m *MockStorage) Compact(ctx context.Context) error {
	return nil
}

func (m *MockStorage) Stats(ctx context.Context) (storage.StorageStats, error) {
	return storage.StorageStats{}, nil
}

// Test cases
func TestHeapFSM_Creation(t *testing.T) {
	fsm := createTestFSM(t)
	require.NotNil(t, fsm)

	stats := fsm.GetStats()
	assert.Equal(t, uint64(0), stats.AppliedEntries)
	assert.Equal(t, uint64(0), stats.AppliedBatches)
	assert.Equal(t, uint64(0), stats.FailedOperations)
}

func TestHeapFSM_ApplyEnqueue(t *testing.T) {
	fsm := createTestFSM(t)

	// Add a mock partition
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Create enqueue entry
	message := createTestMessage("msg-1", 100)
	enqueueEntry := EnqueueEntry{
		Message:     message,
		ProducerID:  "producer-1",
		ProducerSeq: 1,
	}

	log := createTestLogEntry(EntryEnqueue, "test-topic", 0, enqueueEntry)

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)

	// Verify partition received the message
	assert.Equal(t, 1, partition.enqueueCount)
	assert.Contains(t, partition.messages, message.ID.String())

	// Check FSM stats
	stats := fsm.GetStats()
	assert.Equal(t, uint64(1), stats.AppliedEntries)
	assert.Equal(t, uint64(0), stats.FailedOperations)
}

func TestHeapFSM_ApplyAck(t *testing.T) {
	fsm := createTestFSM(t)

	// Add a mock partition
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Create ack entry
	ackEntry := AckEntry{
		MessageID:  "msg-1",
		LeaseToken: "lease-123",
		Consumer:   "consumer-1",
	}

	log := createTestLogEntry(EntryAck, "test-topic", 0, ackEntry)

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)

	// Verify partition received the ack
	assert.Equal(t, 1, partition.ackCount)
}

func TestHeapFSM_ApplyNack(t *testing.T) {
	fsm := createTestFSM(t)

	// Add a mock partition
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Create nack entry
	nackEntry := NackEntry{
		MessageID:    "msg-1",
		LeaseToken:   "lease-123",
		Consumer:     "consumer-1",
		Reason:       "processing failed",
		BackoffLevel: 1,
	}

	log := createTestLogEntry(EntryNack, "test-topic", 0, nackEntry)

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)

	// Verify partition received the nack
	assert.Equal(t, 1, partition.nackCount)
}

func TestHeapFSM_ApplyLeaseExtend(t *testing.T) {
	fsm := createTestFSM(t)

	// Add a mock partition
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Create lease extend entry
	extendEntry := LeaseExtendEntry{
		MessageID:  "msg-1",
		LeaseToken: "lease-123",
		Consumer:   "consumer-1",
		Extension:  30 * time.Second,
	}

	log := createTestLogEntry(EntryLeaseExtend, "test-topic", 0, extendEntry)

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)

	// Verify partition received the extension
	assert.Equal(t, 1, partition.extendCount)
}

func TestHeapFSM_ApplyBatchPrepare(t *testing.T) {
	fsm := createTestFSM(t)

	// Create batch prepare entry
	messages := []*models.Message{
		createTestMessage("msg-1", 100),
		createTestMessage("msg-2", 200),
	}

	prepareEntry := BatchPrepareEntry{
		TransactionID: "txn-123",
		Messages:      messages,
		ProducerID:    "producer-1",
		ProducerSeq:   1,
		PreparedAt:    time.Now(),
	}

	log := createTestLogEntry(EntryBatchPrepare, "test-topic", 0, prepareEntry)

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)

	// Verify transaction is pending
	stats := fsm.GetStats()
	assert.Equal(t, uint64(1), stats.PendingTxns)
}

func TestHeapFSM_ApplyBatchCommit(t *testing.T) {
	fsm := createTestFSM(t)

	// Add a mock partition
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// First, prepare a batch
	messages := []*models.Message{
		createTestMessage("msg-1", 100),
		createTestMessage("msg-2", 200),
	}

	prepareEntry := BatchPrepareEntry{
		TransactionID: "txn-123",
		Messages:      messages,
		ProducerID:    "producer-1",
		ProducerSeq:   1,
		PreparedAt:    time.Now(),
	}

	prepareLog := createTestLogEntry(EntryBatchPrepare, "test-topic", 0, prepareEntry)
	fsm.Apply(prepareLog)

	// Now commit the batch
	commitEntry := BatchCommitEntry{
		TransactionID: "txn-123",
		CommittedAt:   time.Now(),
	}

	commitLog := createTestLogEntry(EntryBatchCommit, "test-topic", 0, commitEntry)

	// Apply the commit
	result := fsm.Apply(commitLog)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)

	// Verify messages were enqueued
	assert.Equal(t, 2, partition.enqueueCount)

	// Verify transaction is no longer pending
	stats := fsm.GetStats()
	assert.Equal(t, uint64(0), stats.PendingTxns)
	assert.Equal(t, uint64(1), stats.AppliedBatches)
}

func TestHeapFSM_ApplyBatchAbort(t *testing.T) {
	fsm := createTestFSM(t)

	// First, prepare a batch
	messages := []*models.Message{
		createTestMessage("msg-1", 100),
		createTestMessage("msg-2", 200),
	}

	prepareEntry := BatchPrepareEntry{
		TransactionID: "txn-123",
		Messages:      messages,
		ProducerID:    "producer-1",
		ProducerSeq:   1,
		PreparedAt:    time.Now(),
	}

	prepareLog := createTestLogEntry(EntryBatchPrepare, "test-topic", 0, prepareEntry)
	fsm.Apply(prepareLog)

	// Now abort the batch
	abortEntry := BatchAbortEntry{
		TransactionID: "txn-123",
		Reason:        "timeout",
		AbortedAt:     time.Now(),
	}

	abortLog := createTestLogEntry(EntryBatchAbort, "test-topic", 0, abortEntry)

	// Apply the abort
	result := fsm.Apply(abortLog)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)

	// Verify transaction is no longer pending
	stats := fsm.GetStats()
	assert.Equal(t, uint64(0), stats.PendingTxns)
}

func TestHeapFSM_ApplyConfigUpdate(t *testing.T) {
	fsm := createTestFSM(t)

	// Create config update entry
	oldConfig := &models.TopicConfig{Name: "test-topic", Partitions: 2}
	newConfig := &models.TopicConfig{Name: "test-topic", Partitions: 4}

	configEntry := ConfigUpdateEntry{
		ConfigType: "topic",
		ConfigID:   "test-topic",
		OldConfig:  oldConfig,
		NewConfig:  newConfig,
	}

	log := createTestLogEntry(EntryConfigUpdate, "test-topic", 0, configEntry)

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)
}

func TestHeapFSM_ApplySnapshotMark(t *testing.T) {
	fsm := createTestFSM(t)

	// Create snapshot mark entry
	snapshotEntry := SnapshotMarkEntry{
		SnapshotID:   "snapshot-123",
		LastIndex:    100,
		LastTerm:     1,
		SnapshotPath: "/tmp/snapshot-123",
		CreatedAt:    time.Now(),
	}

	log := createTestLogEntry(EntrySnapshotMark, "test-topic", 0, snapshotEntry)

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.True(t, applyResult.Success)
	assert.NoError(t, applyResult.Error)
}

func TestHeapFSM_ApplyUnknownEntryType(t *testing.T) {
	fsm := createTestFSM(t)

	// Create log with unknown entry type
	logEntry := &LogEntry{
		Type:      EntryType(999), // Invalid type
		Topic:     "test-topic",
		Partition: 0,
		Data:      []byte("test"),
		Timestamp: time.Now(),
	}

	serialized, _ := logEntry.Serialize()
	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  serialized,
	}

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.False(t, applyResult.Success)
	assert.Error(t, applyResult.Error)

	// Check that failed operations counter increased
	stats := fsm.GetStats()
	assert.Equal(t, uint64(1), stats.FailedOperations)
}

func TestHeapFSM_ApplyMissingPartition(t *testing.T) {
	fsm := createTestFSM(t)

	// Create enqueue entry for non-existent partition
	message := createTestMessage("msg-1", 100)
	enqueueEntry := EnqueueEntry{
		Message:     message,
		ProducerID:  "producer-1",
		ProducerSeq: 1,
	}

	log := createTestLogEntry(EntryEnqueue, "missing-topic", 0, enqueueEntry)

	// Apply the entry
	result := fsm.Apply(log)
	applyResult, ok := result.(*FSMApplyResult)
	require.True(t, ok)
	assert.False(t, applyResult.Success)
	assert.Error(t, applyResult.Error)
	assert.Contains(t, applyResult.Error.Error(), "partition")
}

func TestHeapFSM_Snapshot(t *testing.T) {
	fsm := createTestFSM(t)

	// Add some state
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Apply some entries to create state
	message := createTestMessage("msg-1", 100)
	enqueueEntry := EnqueueEntry{
		Message:     message,
		ProducerID:  "producer-1",
		ProducerSeq: 1,
	}

	log := createTestLogEntry(EntryEnqueue, "test-topic", 0, enqueueEntry)
	fsm.Apply(log)

	// Create snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	heapSnapshot, ok := snapshot.(*HeapSnapshot)
	require.True(t, ok)
	assert.Equal(t, uint64(1), heapSnapshot.LastIndex)
	assert.Equal(t, uint64(1), heapSnapshot.LastTerm)
	assert.Equal(t, uint64(1), heapSnapshot.AppliedEntries)
}

func TestHeapFSM_SnapshotPersistAndRestore(t *testing.T) {
	fsm := createTestFSM(t)

	// Add some state
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Apply some entries
	message := createTestMessage("msg-1", 100)
	enqueueEntry := EnqueueEntry{
		Message:     message,
		ProducerID:  "producer-1",
		ProducerSeq: 1,
	}

	log := createTestLogEntry(EntryEnqueue, "test-topic", 0, enqueueEntry)
	fsm.Apply(log)

	// Create snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)

	// Persist snapshot to buffer
	var buf bytes.Buffer
	mockSink := &MockSnapshotSink{Buffer: &buf}

	heapSnapshot := snapshot.(*HeapSnapshot)
	err = heapSnapshot.Persist(mockSink)
	require.NoError(t, err)

	// Create new FSM and restore
	newFSM := createTestFSM(t)

	reader := io.NopCloser(bytes.NewReader(buf.Bytes()))
	err = newFSM.Restore(reader)
	require.NoError(t, err)

	// Verify restored state
	stats := newFSM.GetStats()
	assert.Equal(t, uint64(1), stats.AppliedEntries)
}

func TestHeapFSM_PartitionManagement(t *testing.T) {
	fsm := createTestFSM(t)

	// Add partition
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Verify partition exists
	retrievedPartition := fsm.getPartition("test-topic", 0)
	assert.NotNil(t, retrievedPartition)
	assert.Equal(t, partition, retrievedPartition)

	// Remove partition
	fsm.RemovePartition("test-topic", 0)

	// Verify partition removed
	retrievedPartition = fsm.getPartition("test-topic", 0)
	assert.Nil(t, retrievedPartition)
}

func TestHeapFSM_Stats(t *testing.T) {
	fsm := createTestFSM(t)

	// Initial stats
	stats := fsm.GetStats()
	assert.Equal(t, uint64(0), stats.AppliedEntries)
	assert.Equal(t, uint64(0), stats.AppliedBatches)
	assert.Equal(t, uint64(0), stats.FailedOperations)
	assert.Equal(t, uint64(0), stats.TopicCount)
	assert.Equal(t, uint64(0), stats.PartitionCount)

	// Add partition
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Check updated stats
	stats = fsm.GetStats()
	assert.Equal(t, uint64(1), stats.TopicCount)
	assert.Equal(t, uint64(1), stats.PartitionCount)
}

// MockSnapshotSink implements raft.SnapshotSink for testing
type MockSnapshotSink struct {
	*bytes.Buffer
}

func (m *MockSnapshotSink) ID() string {
	return "test-snapshot"
}

func (m *MockSnapshotSink) Cancel() error {
	return nil
}

func (m *MockSnapshotSink) Close() error {
	return nil
}

// Benchmarks
func BenchmarkHeapFSM_ApplyEnqueue(b *testing.B) {
	fsm := createTestFSM(&testing.T{})
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	message := createTestMessage("msg-1", 100)
	enqueueEntry := EnqueueEntry{
		Message:     message,
		ProducerID:  "producer-1",
		ProducerSeq: 1,
	}

	log := createTestLogEntry(EntryEnqueue, "test-topic", 0, enqueueEntry)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fsm.Apply(log)
	}
}

func BenchmarkHeapFSM_Snapshot(b *testing.B) {
	fsm := createTestFSM(&testing.T{})
	partition := NewMockPartition()
	fsm.AddPartition("test-topic", 0, partition)

	// Apply some entries to create state
	message := createTestMessage("msg-1", 100)
	enqueueEntry := EnqueueEntry{
		Message:     message,
		ProducerID:  "producer-1",
		ProducerSeq: 1,
	}

	log := createTestLogEntry(EntryEnqueue, "test-topic", 0, enqueueEntry)
	fsm.Apply(log)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		snapshot, _ := fsm.Snapshot()
		snapshot.Release()
	}
}
