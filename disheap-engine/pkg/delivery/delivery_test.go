package delivery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDeliveryManager_LeaseMessage(t *testing.T) {
	tests := []struct {
		name        string
		messageID   models.MessageID
		holder      string
		timeout     time.Duration
		wantErr     bool
		expectedErr error
	}{
		{
			name:      "successful_lease",
			messageID: models.MessageID("msg-123"),
			holder:    "consumer-1",
			timeout:   30 * time.Second,
			wantErr:   false,
		},
		{
			name:      "default_timeout",
			messageID: models.MessageID("msg-124"),
			holder:    "consumer-2",
			timeout:   0, // Should use default
			wantErr:   false,
		},
		{
			name:        "already_leased",
			messageID:   models.MessageID("msg-125"),
			holder:      "consumer-1",
			timeout:     30 * time.Second,
			wantErr:     true,
			expectedErr: ErrAlreadyLeased,
		},
	}

	dm := NewDeliveryManager(nil, zap.NewNop())
	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pre-lease message for already_leased test
			if tt.name == "already_leased" {
				_, err := dm.LeaseMessage(ctx, tt.messageID, "existing-holder", 30*time.Second)
				require.NoError(t, err)
			}

			lease, err := dm.LeaseMessage(ctx, tt.messageID, tt.holder, tt.timeout)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedErr != nil {
					assert.ErrorIs(t, err, tt.expectedErr)
				}
				assert.Nil(t, lease)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, lease)
				assert.Equal(t, tt.holder, lease.Holder)
				assert.NotEmpty(t, lease.Token)
				assert.False(t, lease.GrantedAt.IsZero())
				assert.False(t, lease.Deadline.IsZero())
				assert.True(t, lease.Deadline.After(lease.GrantedAt))
			}
		})
	}
}

func TestDeliveryManager_AckMessage(t *testing.T) {
	dm := NewDeliveryManager(nil, zap.NewNop())
	ctx := context.Background()

	// Test successful ack
	t.Run("successful_ack", func(t *testing.T) {
		messageID := models.MessageID("msg-ack-success")
		lease, err := dm.LeaseMessage(ctx, messageID, "consumer-1", 30*time.Second)
		require.NoError(t, err)
		require.NotNil(t, lease)

		err = dm.AckMessage(ctx, messageID, lease.Token)
		assert.NoError(t, err)
		assert.False(t, dm.IsMessageLeased(messageID))
	})

	// Test message not found
	t.Run("message_not_found", func(t *testing.T) {
		err := dm.AckMessage(ctx, models.MessageID("non-existent"), models.LeaseToken("some-token"))
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMessageNotFound)
	})

	// Test invalid token
	t.Run("invalid_token", func(t *testing.T) {
		messageID := models.MessageID("msg-ack-invalid-token")
		lease, err := dm.LeaseMessage(ctx, messageID, "consumer-1", 30*time.Second)
		require.NoError(t, err)
		require.NotNil(t, lease)

		err = dm.AckMessage(ctx, messageID, models.LeaseToken("invalid-token"))
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidLease)
	})
}

func TestDeliveryManager_NackMessage(t *testing.T) {
	dm := NewDeliveryManager(nil, zap.NewNop())
	ctx := context.Background()
	messageID := models.MessageID("msg-nack-123")

	// First lease the message
	lease, err := dm.LeaseMessage(ctx, messageID, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, lease)

	err = dm.NackMessage(ctx, messageID, lease.Token)
	assert.NoError(t, err)

	// Verify lease is removed
	assert.False(t, dm.IsMessageLeased(messageID))
}

func TestDeliveryManager_ExtendLease(t *testing.T) {
	config := &DeliveryConfig{
		MaxLeaseExtensions:  2,
		MaxLeaseExtension:   1 * time.Minute,
		DefaultLeaseTimeout: 30 * time.Second,
	}
	dm := NewDeliveryManager(config, zap.NewNop())
	ctx := context.Background()
	messageID := models.MessageID("msg-extend-123")

	// First lease the message
	lease, err := dm.LeaseMessage(ctx, messageID, "consumer-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, lease)

	tests := []struct {
		name      string
		extension time.Duration
		wantErr   bool
	}{
		{
			name:      "first_extension",
			extension: 30 * time.Second,
			wantErr:   false,
		},
		{
			name:      "second_extension",
			extension: 30 * time.Second,
			wantErr:   false,
		},
		{
			name:      "too_many_extensions",
			extension: 30 * time.Second,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldDeadline := lease.Deadline

			extendedLease, err := dm.ExtendLease(ctx, messageID, lease.Token, tt.extension)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, extendedLease)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, extendedLease)
				assert.True(t, extendedLease.Deadline.After(oldDeadline))
				lease = extendedLease // Update for next iteration
			}
		})
	}
}

func TestDeliveryManager_LeaseExpiry(t *testing.T) {
	dm := NewDeliveryManager(nil, zap.NewNop())
	ctx := context.Background()
	messageID := models.MessageID("msg-expire-123")

	// Lease with very short timeout
	lease, err := dm.LeaseMessage(ctx, messageID, "consumer-1", 1*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, lease)

	// Wait for lease to expire
	time.Sleep(10 * time.Millisecond)

	// Try to ack expired lease
	err = dm.AckMessage(ctx, messageID, lease.Token)
	assert.ErrorIs(t, err, ErrLeaseExpired)

	// Verify lease is considered inactive
	assert.False(t, dm.IsMessageLeased(messageID))
}

func TestDeliveryManager_CleanExpiredLeases(t *testing.T) {
	dm := NewDeliveryManager(nil, zap.NewNop())
	ctx := context.Background()

	// Create multiple leases with short timeouts
	messageIDs := []models.MessageID{
		models.MessageID("msg-clean-1"),
		models.MessageID("msg-clean-2"),
		models.MessageID("msg-clean-3"),
	}

	for _, msgID := range messageIDs {
		_, err := dm.LeaseMessage(ctx, msgID, "consumer-1", 1*time.Millisecond)
		require.NoError(t, err)
	}

	// Verify leases are active
	stats := dm.Stats()
	assert.Equal(t, uint64(3), stats.ActiveLeases)

	// Wait for expiry
	time.Sleep(10 * time.Millisecond)

	// Clean expired leases
	expired := dm.CleanExpiredLeases(ctx)
	assert.Len(t, expired, 3)

	// Verify stats updated
	stats = dm.Stats()
	assert.Equal(t, uint64(0), stats.ActiveLeases)
	assert.Equal(t, uint64(3), stats.TimeoutCount)
}

func TestExponentialBackoff_Calculate(t *testing.T) {
	config := &BackoffConfig{
		BaseDelay:  1 * time.Second,
		MaxDelay:   1 * time.Minute,
		Multiplier: 2.0,
		Jitter:     NoJitter,
	}
	backoff := NewExponentialBackoff(config, zap.NewNop())

	tests := []struct {
		attempts uint32
		expected time.Duration
	}{
		{0, 0},
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 32 * time.Second},
		{7, 1 * time.Minute}, // Capped at max
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempts), func(t *testing.T) {
			result := backoff.Calculate(tt.attempts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExponentialBackoff_WithJitter(t *testing.T) {
	config := &BackoffConfig{
		BaseDelay:  1 * time.Second,
		MaxDelay:   1 * time.Minute,
		Multiplier: 2.0,
		Jitter:     FullJitter,
	}
	backoff := NewExponentialBackoff(config, zap.NewNop())

	// Test that jittered values are within expected range
	baseDelay := backoff.Calculate(3) // 4 seconds

	for i := 0; i < 100; i++ {
		jittered := backoff.WithJitter(3)
		assert.True(t, jittered >= 0)
		assert.True(t, jittered <= baseDelay)
	}
}

func TestLinearBackoff_Calculate(t *testing.T) {
	backoff := NewLinearBackoff(
		1*time.Second,  // base delay
		10*time.Second, // max delay
		1*time.Second,  // increment
		NoJitter,
		zap.NewNop(),
	)

	tests := []struct {
		attempts uint32
		expected time.Duration
	}{
		{0, 0},
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 3 * time.Second},
		{5, 5 * time.Second},
		{15, 10 * time.Second}, // Capped at max
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempts), func(t *testing.T) {
			result := backoff.Calculate(tt.attempts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBackoffManager_ShouldRetry(t *testing.T) {
	bm := NewBackoffManager(nil, zap.NewNop())

	tests := []struct {
		attempts   uint32
		maxRetries uint32
		expected   bool
	}{
		{0, 3, true},
		{1, 3, true},
		{2, 3, true},
		{3, 3, false},
		{4, 3, false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempts_%d_max_%d", tt.attempts, tt.maxRetries), func(t *testing.T) {
			result := bm.ShouldRetry(tt.attempts, tt.maxRetries)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDLQManager_MoveToDLQ(t *testing.T) {
	dm := NewDLQManager(nil, zap.NewNop())
	ctx := context.Background()
	topic := "test-topic"

	message := &models.Message{
		ID:       models.MessageID("msg-dlq-123"),
		Priority: 5,
		Payload:  []byte("test payload"),
		Attempts: 3,
	}

	err := dm.MoveToDLQ(ctx, topic, message, "max retries exceeded")
	assert.NoError(t, err)

	// Verify message is in DLQ
	messages, err := dm.PeekDLQ(ctx, topic, 10)
	assert.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.Equal(t, message.ID, messages[0].Message.ID)
	assert.Equal(t, "max retries exceeded", messages[0].FinalFailureReason)
}

func TestDLQManager_ReplayFromDLQ(t *testing.T) {
	dm := NewDLQManager(nil, zap.NewNop())
	ctx := context.Background()
	topic := "test-topic"

	// Add message to DLQ
	message := &models.Message{
		ID:       models.MessageID("msg-replay-123"),
		Priority: 5,
		Payload:  []byte("test payload"),
		Attempts: 3,
	}

	err := dm.MoveToDLQ(ctx, topic, message, "max retries exceeded")
	require.NoError(t, err)

	// Replay the message
	replayed, err := dm.ReplayFromDLQ(ctx, topic, []models.MessageID{message.ID})
	assert.NoError(t, err)
	assert.Len(t, replayed, 1)
	assert.Equal(t, message.ID, replayed[0].ID)
	assert.Equal(t, uint32(0), replayed[0].Attempts) // Should be reset

	// Verify message removed from DLQ
	messages, err := dm.PeekDLQ(ctx, topic, 10)
	assert.NoError(t, err)
	assert.Len(t, messages, 0)
}

func TestDLQManager_DeleteFromDLQ(t *testing.T) {
	dm := NewDLQManager(nil, zap.NewNop())
	ctx := context.Background()
	topic := "test-topic"

	// Add message to DLQ
	message := &models.Message{
		ID:       models.MessageID("msg-delete-123"),
		Priority: 5,
		Payload:  []byte("test payload"),
		Attempts: 3,
	}

	err := dm.MoveToDLQ(ctx, topic, message, "max retries exceeded")
	require.NoError(t, err)

	// Delete the message
	err = dm.DeleteFromDLQ(ctx, topic, []models.MessageID{message.ID})
	assert.NoError(t, err)

	// Verify message removed from DLQ
	messages, err := dm.PeekDLQ(ctx, topic, 10)
	assert.NoError(t, err)
	assert.Len(t, messages, 0)
}

func TestDLQHeap_Compact(t *testing.T) {
	config := &DLQConfig{
		MaxCapacity:        1000,
		RetentionTime:      1 * time.Millisecond, // Very short for testing
		EnableCompaction:   true,
		CompactionInterval: 1 * time.Hour,
	}
	dm := NewDLQManager(config, zap.NewNop())
	ctx := context.Background()
	topic := "test-topic"

	// Add message to DLQ
	message := &models.Message{
		ID:       models.MessageID("msg-compact-123"),
		Priority: 5,
		Payload:  []byte("test payload"),
		Attempts: 3,
	}

	err := dm.MoveToDLQ(ctx, topic, message, "max retries exceeded")
	require.NoError(t, err)

	// Wait for retention to expire
	time.Sleep(10 * time.Millisecond)

	// Run compaction
	heap := dm.GetOrCreateDLQ(topic)
	err = heap.Compact(ctx)
	assert.NoError(t, err)

	// Verify message was removed
	stats := heap.GetStats()
	assert.Equal(t, uint64(0), stats.MessageCount)
	assert.Equal(t, uint64(1), stats.TotalExpired)
}

func TestDLQManager_Stats(t *testing.T) {
	dm := NewDLQManager(nil, zap.NewNop())
	ctx := context.Background()
	topic := "test-topic"

	// Add multiple messages
	for i := 0; i < 5; i++ {
		message := &models.Message{
			ID:       models.MessageID(fmt.Sprintf("msg-stats-%d", i)),
			Priority: 5,
			Payload:  []byte("test payload"),
			Attempts: 3,
		}

		err := dm.MoveToDLQ(ctx, topic, message, "max retries exceeded")
		require.NoError(t, err)
	}

	// Get stats
	stats, err := dm.GetDLQStats(topic)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), stats.MessageCount)
	assert.Equal(t, uint64(5), stats.TotalMoved)
}

func TestDLQManager_CapacityLimit(t *testing.T) {
	config := &DLQConfig{
		MaxCapacity:        2, // Very small for testing
		RetentionTime:      1 * time.Hour,
		EnableCompaction:   true,
		CompactionInterval: 1 * time.Hour,
	}
	dm := NewDLQManager(config, zap.NewNop())
	ctx := context.Background()
	topic := "test-topic"

	// Add messages up to capacity
	for i := 0; i < 2; i++ {
		message := &models.Message{
			ID:       models.MessageID(fmt.Sprintf("msg-capacity-%d", i)),
			Priority: 5,
			Payload:  []byte("test payload"),
			Attempts: 3,
		}

		err := dm.MoveToDLQ(ctx, topic, message, "max retries exceeded")
		assert.NoError(t, err)
	}

	// Try to add one more (should fail)
	message := &models.Message{
		ID:       models.MessageID("msg-capacity-overflow"),
		Priority: 5,
		Payload:  []byte("test payload"),
		Attempts: 3,
	}

	err := dm.MoveToDLQ(ctx, topic, message, "max retries exceeded")
	assert.ErrorIs(t, err, ErrDLQFull)
}

func TestDeliveryManager_ConcurrentOperations(t *testing.T) {
	dm := NewDeliveryManager(nil, zap.NewNop())
	ctx := context.Background()

	// Test concurrent lease creation
	const numGoroutines = 10
	const numMessages = 100

	// Channel to collect results
	results := make(chan error, numGoroutines*numMessages)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < numMessages; j++ {
				messageID := models.MessageID(fmt.Sprintf("concurrent-%d-%d", goroutineID, j))
				_, err := dm.LeaseMessage(ctx, messageID, fmt.Sprintf("consumer-%d", goroutineID), 30*time.Second)
				results <- err
			}
		}(i)
	}

	// Collect all results
	successCount := 0
	for i := 0; i < numGoroutines*numMessages; i++ {
		err := <-results
		if err == nil {
			successCount++
		}
	}

	assert.Equal(t, numGoroutines*numMessages, successCount)

	// Verify stats
	stats := dm.Stats()
	assert.Equal(t, uint64(numGoroutines*numMessages), stats.ActiveLeases)
}
