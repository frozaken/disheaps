package txn

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// MockPartition implements partition.Partition interface for testing
type MockPartition struct {
	mock.Mock
	mu sync.Mutex
}

func NewMockPartition() *MockPartition {
	return &MockPartition{}
}

func (m *MockPartition) Enqueue(ctx context.Context, message *models.Message) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}

func (m *MockPartition) Pop(ctx context.Context, holder string, timeout time.Duration) (*models.Message, error) {
	args := m.Called(ctx, holder, timeout)
	return args.Get(0).(*models.Message), args.Error(1)
}

func (m *MockPartition) Peek(ctx context.Context, limit int) ([]*models.Message, error) {
	args := m.Called(ctx, limit)
	return args.Get(0).([]*models.Message), args.Error(1)
}

func (m *MockPartition) Ack(ctx context.Context, messageID models.MessageID, leaseToken models.LeaseToken) error {
	args := m.Called(ctx, messageID, leaseToken)
	return args.Error(0)
}

func (m *MockPartition) Nack(ctx context.Context, messageID models.MessageID, leaseToken models.LeaseToken, backoff *time.Duration) error {
	args := m.Called(ctx, messageID, leaseToken, backoff)
	return args.Error(0)
}

func (m *MockPartition) ExtendLease(ctx context.Context, token models.LeaseToken, extension time.Duration) error {
	args := m.Called(ctx, token, extension)
	return args.Error(0)
}

func (m *MockPartition) Stats() models.PartitionStats {
	args := m.Called()
	return args.Get(0).(models.PartitionStats)
}

func (m *MockPartition) GetCandidates(count int) []*models.Message {
	args := m.Called(count)
	return args.Get(0).([]*models.Message)
}

func (m *MockPartition) Throttle(reason string) {
	m.Called(reason)
}

func (m *MockPartition) Unthrottle() {
	m.Called()
}

func (m *MockPartition) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockPartition) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockPartition) IsHealthy() bool {
	args := m.Called()
	return args.Bool(0)
}

// MockParticipant implements the Participant interface for testing
type MockParticipant struct {
	mock.Mock
	mu sync.Mutex
}

func NewMockParticipant() *MockParticipant {
	return &MockParticipant{}
}

func (m *MockParticipant) Prepare(ctx context.Context, txnID string, ops []*models.BatchOperation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, txnID, ops)
	return args.Error(0)
}

func (m *MockParticipant) Commit(ctx context.Context, txnID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, txnID)
	return args.Error(0)
}

func (m *MockParticipant) Abort(ctx context.Context, txnID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called(ctx, txnID)
	return args.Error(0)
}

func createTestMessage(topic string, priority int) *models.Message {
	return &models.Message{
		ID:         models.NewMessageID(),
		Topic:      topic,
		Priority:   int64(priority),
		EnqueuedAt: time.Now(),
		Payload:    []byte(fmt.Sprintf("test message %d", priority)),
		ProducerID: "test-producer",
	}
}

func createTestBatchOperation(opType models.BatchOperationType, topic string, partition int) *models.BatchOperation {
	op := &models.BatchOperation{
		Type:      opType,
		Topic:     topic,
		Partition: partition,
	}

	switch opType {
	case models.BatchOperationEnqueue:
		op.Message = createTestMessage(topic, 10)
	case models.BatchOperationAck, models.BatchOperationNack:
		op.MessageID = models.NewMessageID()
		op.LeaseToken = models.LeaseToken("test-lease")
	}

	return op
}

func TestCoordinator_BasicLifecycle(t *testing.T) {
	config := DefaultCoordinatorConfig()
	logger := zaptest.NewLogger(t)

	coordinator := NewCoordinator(config, logger)
	ctx := context.Background()

	// Test start
	err := coordinator.Start(ctx)
	require.NoError(t, err)

	// Test stop
	err = coordinator.Stop(ctx)
	require.NoError(t, err)
}

func TestCoordinator_RegisterParticipants(t *testing.T) {
	config := DefaultCoordinatorConfig()
	logger := zaptest.NewLogger(t)
	coordinator := NewCoordinator(config, logger)

	participant1 := NewMockParticipant()
	participant2 := NewMockParticipant()

	pid1 := ParticipantID{Topic: "topic1", Partition: 0}
	pid2 := ParticipantID{Topic: "topic2", Partition: 1}

	// Test register
	coordinator.RegisterParticipant(pid1, participant1)
	coordinator.RegisterParticipant(pid2, participant2)

	// Verify participants are registered
	coordinator.mu.RLock()
	assert.Equal(t, participant1, coordinator.participants[pid1])
	assert.Equal(t, participant2, coordinator.participants[pid2])
	coordinator.mu.RUnlock()

	// Test unregister
	coordinator.UnregisterParticipant(pid1)

	coordinator.mu.RLock()
	_, exists := coordinator.participants[pid1]
	assert.False(t, exists)
	assert.Equal(t, participant2, coordinator.participants[pid2])
	coordinator.mu.RUnlock()
}

func TestCoordinator_SuccessfulTransaction(t *testing.T) {
	config := DefaultCoordinatorConfig()
	config.ParticipantTimeout = 5 * time.Second
	logger := zaptest.NewLogger(t)

	coordinator := NewCoordinator(config, logger)
	ctx := context.Background()

	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	// Setup mock participants
	participant1 := NewMockParticipant()
	participant2 := NewMockParticipant()

	pid1 := ParticipantID{Topic: "topic1", Partition: 0}
	pid2 := ParticipantID{Topic: "topic2", Partition: 1}

	coordinator.RegisterParticipant(pid1, participant1)
	coordinator.RegisterParticipant(pid2, participant2)

	// Prepare mock expectations
	operations := []*models.BatchOperation{
		createTestBatchOperation(models.BatchOperationEnqueue, "topic1", 0),
		createTestBatchOperation(models.BatchOperationEnqueue, "topic2", 1),
	}

	participant1.On("Prepare", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]*models.BatchOperation")).Return(nil)
	participant1.On("Commit", mock.Anything, mock.AnythingOfType("string")).Return(nil)

	participant2.On("Prepare", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]*models.BatchOperation")).Return(nil)
	participant2.On("Commit", mock.Anything, mock.AnythingOfType("string")).Return(nil)

	// Begin transaction
	txn, err := coordinator.BeginTransaction(ctx, operations)
	require.NoError(t, err)
	assert.NotNil(t, txn)
	assert.Equal(t, StateInit, txn.State)
	assert.Len(t, txn.Participants, 2)

	// Commit transaction
	err = coordinator.CommitTransaction(ctx, txn.ID)
	require.NoError(t, err)

	// Verify final state
	txn.mu.RLock()
	assert.Equal(t, StateCommitted, txn.State)
	assert.NotNil(t, txn.CommittedAt)
	txn.mu.RUnlock()

	// Verify mock expectations
	participant1.AssertExpectations(t)
	participant2.AssertExpectations(t)
}

func TestCoordinator_PrepareFailure(t *testing.T) {
	config := DefaultCoordinatorConfig()
	config.ParticipantTimeout = 5 * time.Second
	logger := zaptest.NewLogger(t)

	coordinator := NewCoordinator(config, logger)
	ctx := context.Background()

	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	// Setup mock participants
	participant1 := NewMockParticipant()
	participant2 := NewMockParticipant()

	pid1 := ParticipantID{Topic: "topic1", Partition: 0}
	pid2 := ParticipantID{Topic: "topic2", Partition: 1}

	coordinator.RegisterParticipant(pid1, participant1)
	coordinator.RegisterParticipant(pid2, participant2)

	// Prepare mock expectations - participant1 succeeds, participant2 fails
	operations := []*models.BatchOperation{
		createTestBatchOperation(models.BatchOperationEnqueue, "topic1", 0),
		createTestBatchOperation(models.BatchOperationEnqueue, "topic2", 1),
	}

	participant1.On("Prepare", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]*models.BatchOperation")).Return(nil)
	participant1.On("Abort", mock.Anything, mock.AnythingOfType("string")).Return(nil)

	participant2.On("Prepare", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]*models.BatchOperation")).Return(fmt.Errorf("prepare failed"))
	participant2.On("Abort", mock.Anything, mock.AnythingOfType("string")).Return(nil)

	// Begin transaction
	txn, err := coordinator.BeginTransaction(ctx, operations)
	require.NoError(t, err)

	// Commit transaction - should fail during prepare
	err = coordinator.CommitTransaction(ctx, txn.ID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prepare phase failed")

	// Verify final state
	txn.mu.RLock()
	assert.Equal(t, StateAborted, txn.State)
	assert.NotNil(t, txn.AbortedAt)
	txn.mu.RUnlock()

	// Verify mock expectations
	participant1.AssertExpectations(t)
	participant2.AssertExpectations(t)
}

func TestCoordinator_CommitFailure(t *testing.T) {
	config := DefaultCoordinatorConfig()
	config.ParticipantTimeout = 5 * time.Second
	logger := zaptest.NewLogger(t)

	coordinator := NewCoordinator(config, logger)
	ctx := context.Background()

	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	// Setup mock participants
	participant1 := NewMockParticipant()
	participant2 := NewMockParticipant()

	pid1 := ParticipantID{Topic: "topic1", Partition: 0}
	pid2 := ParticipantID{Topic: "topic2", Partition: 1}

	coordinator.RegisterParticipant(pid1, participant1)
	coordinator.RegisterParticipant(pid2, participant2)

	// Prepare mock expectations - both prepare succeed, participant2 commit fails
	operations := []*models.BatchOperation{
		createTestBatchOperation(models.BatchOperationEnqueue, "topic1", 0),
		createTestBatchOperation(models.BatchOperationEnqueue, "topic2", 1),
	}

	participant1.On("Prepare", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]*models.BatchOperation")).Return(nil)
	participant1.On("Commit", mock.Anything, mock.AnythingOfType("string")).Return(nil)

	participant2.On("Prepare", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]*models.BatchOperation")).Return(nil)
	participant2.On("Commit", mock.Anything, mock.AnythingOfType("string")).Return(fmt.Errorf("commit failed"))

	// Begin transaction
	txn, err := coordinator.BeginTransaction(ctx, operations)
	require.NoError(t, err)

	// Commit transaction - should succeed but with error
	err = coordinator.CommitTransaction(ctx, txn.ID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit phase had")

	// Verify final state - transaction is still committed despite commit error
	txn.mu.RLock()
	assert.Equal(t, StateCommitted, txn.State)
	assert.NotNil(t, txn.CommittedAt)
	assert.Error(t, txn.Error)
	txn.mu.RUnlock()

	// Verify mock expectations
	participant1.AssertExpectations(t)
	participant2.AssertExpectations(t)
}

func TestCoordinator_TransactionTimeout(t *testing.T) {
	config := DefaultCoordinatorConfig()
	config.DefaultTimeout = 100 * time.Millisecond
	logger := zaptest.NewLogger(t)

	coordinator := NewCoordinator(config, logger)
	ctx := context.Background()

	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	// Setup mock participant
	participant := NewMockParticipant()
	pid := ParticipantID{Topic: "topic1", Partition: 0}
	coordinator.RegisterParticipant(pid, participant)

	participant.On("Abort", mock.Anything, mock.AnythingOfType("string")).Return(nil)

	// Begin transaction
	operations := []*models.BatchOperation{
		createTestBatchOperation(models.BatchOperationEnqueue, "topic1", 0),
	}

	txn, err := coordinator.BeginTransaction(ctx, operations)
	require.NoError(t, err)

	// Wait for timeout
	time.Sleep(200 * time.Millisecond)

	// Verify transaction was aborted due to timeout
	txn.mu.RLock()
	assert.Equal(t, StateAborted, txn.State)
	txn.mu.RUnlock()

	// Verify abort was called
	participant.AssertExpectations(t)
}

func TestCoordinator_ConcurrentTransactionLimit(t *testing.T) {
	config := DefaultCoordinatorConfig()
	config.MaxConcurrentTxns = 2
	logger := zaptest.NewLogger(t)

	coordinator := NewCoordinator(config, logger)
	ctx := context.Background()

	// Setup mock participant
	participant := NewMockParticipant()
	pid := ParticipantID{Topic: "topic1", Partition: 0}
	coordinator.RegisterParticipant(pid, participant)

	operations := []*models.BatchOperation{
		createTestBatchOperation(models.BatchOperationEnqueue, "topic1", 0),
	}

	// Begin two transactions (should succeed)
	txn1, err := coordinator.BeginTransaction(ctx, operations)
	require.NoError(t, err)
	assert.NotNil(t, txn1)

	txn2, err := coordinator.BeginTransaction(ctx, operations)
	require.NoError(t, err)
	assert.NotNil(t, txn2)

	// Try to begin third transaction (should fail)
	txn3, err := coordinator.BeginTransaction(ctx, operations)
	require.Error(t, err)
	assert.Nil(t, txn3)
	assert.Contains(t, err.Error(), "maximum concurrent transactions reached")
}

func TestPartitionParticipant_BasicLifecycle(t *testing.T) {
	config := DefaultPartitionParticipantConfig()
	mockPartition := NewMockPartition()
	logger := zaptest.NewLogger(t)

	participant := NewPartitionParticipant(config, mockPartition, logger)
	ctx := context.Background()

	// Test start
	err := participant.Start(ctx)
	require.NoError(t, err)

	// Test stop
	err = participant.Stop(ctx)
	require.NoError(t, err)
}

func TestPartitionParticipant_SuccessfulTransaction(t *testing.T) {
	config := DefaultPartitionParticipantConfig()
	mockPartition := NewMockPartition()
	logger := zaptest.NewLogger(t)

	participant := NewPartitionParticipant(config, mockPartition, logger)
	ctx := context.Background()

	err := participant.Start(ctx)
	require.NoError(t, err)
	defer participant.Stop(ctx)

	// Setup mock expectations
	message := createTestMessage("topic1", 10)
	mockPartition.On("Enqueue", mock.Anything, message).Return(nil)

	operations := []*models.BatchOperation{
		{
			Type:      models.BatchOperationEnqueue,
			Topic:     "topic1",
			Partition: 0,
			Message:   message,
		},
	}

	txnID := "test-txn-123"

	// Test prepare phase
	err = participant.Prepare(ctx, txnID, operations)
	require.NoError(t, err)

	// Verify transaction state
	txn, err := participant.GetTransaction(txnID)
	require.NoError(t, err)
	assert.Equal(t, ParticipantPrepared, txn.State)
	assert.NotNil(t, txn.PreparedAt)

	// Test commit phase
	err = participant.Commit(ctx, txnID)
	require.NoError(t, err)

	// Verify final state
	txn, err = participant.GetTransaction(txnID)
	require.NoError(t, err)
	assert.Equal(t, ParticipantCommitted, txn.State)
	assert.NotNil(t, txn.CommittedAt)

	// Verify mock expectations
	mockPartition.AssertExpectations(t)
}

func TestPartitionParticipant_AbortTransaction(t *testing.T) {
	config := DefaultPartitionParticipantConfig()
	mockPartition := NewMockPartition()
	logger := zaptest.NewLogger(t)

	participant := NewPartitionParticipant(config, mockPartition, logger)
	ctx := context.Background()

	err := participant.Start(ctx)
	require.NoError(t, err)
	defer participant.Stop(ctx)

	operations := []*models.BatchOperation{
		createTestBatchOperation(models.BatchOperationEnqueue, "topic1", 0),
	}

	txnID := "test-txn-123"

	// Prepare transaction
	err = participant.Prepare(ctx, txnID, operations)
	require.NoError(t, err)

	// Abort transaction
	err = participant.Abort(ctx, txnID)
	require.NoError(t, err)

	// Verify final state
	txn, err := participant.GetTransaction(txnID)
	require.NoError(t, err)
	assert.Equal(t, ParticipantAborted, txn.State)
	assert.NotNil(t, txn.AbortedAt)
}

func TestPartitionParticipant_PreparedTimeout(t *testing.T) {
	config := DefaultPartitionParticipantConfig()
	config.PreparedTimeout = 100 * time.Millisecond
	mockPartition := NewMockPartition()
	logger := zaptest.NewLogger(t)

	participant := NewPartitionParticipant(config, mockPartition, logger)
	ctx := context.Background()

	err := participant.Start(ctx)
	require.NoError(t, err)
	defer participant.Stop(ctx)

	operations := []*models.BatchOperation{
		createTestBatchOperation(models.BatchOperationEnqueue, "topic1", 0),
	}

	txnID := "test-txn-123"

	// Prepare transaction
	err = participant.Prepare(ctx, txnID, operations)
	require.NoError(t, err)

	// Wait for timeout
	time.Sleep(200 * time.Millisecond)

	// Verify transaction was auto-aborted
	txn, err := participant.GetTransaction(txnID)
	require.NoError(t, err)
	assert.Equal(t, ParticipantAborted, txn.State)
}

func TestTimeoutManager_BasicOperations(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tm := NewTimeoutManager(logger)
	ctx := context.Background()

	err := tm.Start(ctx)
	require.NoError(t, err)
	defer tm.Stop(ctx)

	// Test scheduling timeout
	var called bool
	callback := func() { called = true }

	tm.ScheduleTimeout("test-id", time.Now().Add(50*time.Millisecond), callback)

	// Verify timeout is scheduled
	timeouts := tm.GetTimeouts()
	assert.Len(t, timeouts, 1)
	assert.Contains(t, timeouts, "test-id")

	// Wait for timeout to expire
	time.Sleep(100 * time.Millisecond)

	// Verify callback was called
	assert.True(t, called)

	// Verify timeout was removed
	timeouts = tm.GetTimeouts()
	assert.Len(t, timeouts, 0)
}

func TestTimeoutManager_CancelTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tm := NewTimeoutManager(logger)
	ctx := context.Background()

	err := tm.Start(ctx)
	require.NoError(t, err)
	defer tm.Stop(ctx)

	// Test scheduling and canceling timeout
	var called bool
	callback := func() { called = true }

	tm.ScheduleTimeout("test-id", time.Now().Add(50*time.Millisecond), callback)

	// Cancel timeout
	canceled := tm.CancelTimeout("test-id")
	assert.True(t, canceled)

	// Wait for original timeout period
	time.Sleep(100 * time.Millisecond)

	// Verify callback was not called
	assert.False(t, called)

	// Verify timeout was removed
	timeouts := tm.GetTimeouts()
	assert.Len(t, timeouts, 0)
}

func TestTimeoutManager_Stats(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tm := NewTimeoutManager(logger)

	// Test stats with no timeouts
	stats := tm.Stats()
	assert.Equal(t, 0, stats.ActiveTimeouts)
	assert.Nil(t, stats.NextTimeout)

	// Schedule some timeouts
	tm.ScheduleTimeout("test-1", time.Now().Add(1*time.Hour), func() {})
	tm.ScheduleTimeout("test-2", time.Now().Add(2*time.Hour), func() {})

	// Test stats with timeouts
	stats = tm.Stats()
	assert.Equal(t, 2, stats.ActiveTimeouts)
	assert.NotNil(t, stats.NextTimeout)
}

func TestIntegration_EndToEndTransaction(t *testing.T) {
	// Integration test that combines coordinator and participants
	config := DefaultCoordinatorConfig()
	config.ParticipantTimeout = 5 * time.Second
	logger := zaptest.NewLogger(t)

	coordinator := NewCoordinator(config, logger)
	ctx := context.Background()

	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	// Create partition participants
	partConfig := DefaultPartitionParticipantConfig()
	mockPartition1 := NewMockPartition()
	mockPartition2 := NewMockPartition()

	participant1 := NewPartitionParticipant(partConfig, mockPartition1, logger)
	participant2 := NewPartitionParticipant(partConfig, mockPartition2, logger)

	err = participant1.Start(ctx)
	require.NoError(t, err)
	defer participant1.Stop(ctx)

	err = participant2.Start(ctx)
	require.NoError(t, err)
	defer participant2.Stop(ctx)

	// Register participants
	pid1 := ParticipantID{Topic: "topic1", Partition: 0}
	pid2 := ParticipantID{Topic: "topic2", Partition: 1}

	coordinator.RegisterParticipant(pid1, participant1)
	coordinator.RegisterParticipant(pid2, participant2)

	// Setup mock expectations
	message1 := createTestMessage("topic1", 10)
	message2 := createTestMessage("topic2", 20)

	mockPartition1.On("Enqueue", mock.Anything, message1).Return(nil)
	mockPartition2.On("Enqueue", mock.Anything, message2).Return(nil)

	operations := []*models.BatchOperation{
		{
			Type:      models.BatchOperationEnqueue,
			Topic:     "topic1",
			Partition: 0,
			Message:   message1,
		},
		{
			Type:      models.BatchOperationEnqueue,
			Topic:     "topic2",
			Partition: 1,
			Message:   message2,
		},
	}

	// Execute transaction
	txn, err := coordinator.BeginTransaction(ctx, operations)
	require.NoError(t, err)

	err = coordinator.CommitTransaction(ctx, txn.ID)
	require.NoError(t, err)

	// Verify transaction state
	txn.mu.RLock()
	assert.Equal(t, StateCommitted, txn.State)
	txn.mu.RUnlock()

	// Verify participant states
	ptxn1, err := participant1.GetTransaction(txn.ID)
	require.NoError(t, err)
	assert.Equal(t, ParticipantCommitted, ptxn1.State)

	ptxn2, err := participant2.GetTransaction(txn.ID)
	require.NoError(t, err)
	assert.Equal(t, ParticipantCommitted, ptxn2.State)

	// Verify mock expectations
	mockPartition1.AssertExpectations(t)
	mockPartition2.AssertExpectations(t)
}

func TestIntegration_ConcurrentTransactions(t *testing.T) {
	config := DefaultCoordinatorConfig()
	config.MaxConcurrentTxns = 10
	logger := zaptest.NewLogger(t)

	coordinator := NewCoordinator(config, logger)
	ctx := context.Background()

	err := coordinator.Start(ctx)
	require.NoError(t, err)
	defer coordinator.Stop(ctx)

	// Create mock participant
	mockPartition := NewMockPartition()
	participant := NewPartitionParticipant(DefaultPartitionParticipantConfig(), mockPartition, logger)

	err = participant.Start(ctx)
	require.NoError(t, err)
	defer participant.Stop(ctx)

	pid := ParticipantID{Topic: "topic1", Partition: 0}
	coordinator.RegisterParticipant(pid, participant)

	// Setup mock expectations for multiple operations
	numTxns := 5
	for i := 0; i < numTxns; i++ {
		mockPartition.On("Enqueue", mock.Anything, mock.AnythingOfType("*models.Message")).Return(nil)
	}

	// Execute multiple concurrent transactions
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []error

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			operations := []*models.BatchOperation{
				{
					Type:      models.BatchOperationEnqueue,
					Topic:     "topic1",
					Partition: 0,
					Message:   createTestMessage("topic1", i),
				},
			}

			txn, err := coordinator.BeginTransaction(ctx, operations)
			if err != nil {
				mu.Lock()
				results = append(results, err)
				mu.Unlock()
				return
			}

			err = coordinator.CommitTransaction(ctx, txn.ID)
			mu.Lock()
			results = append(results, err)
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify all transactions succeeded
	assert.Len(t, results, numTxns)
	for i, err := range results {
		assert.NoError(t, err, "transaction %d failed", i)
	}

	// Verify mock expectations
	mockPartition.AssertExpectations(t)
}
