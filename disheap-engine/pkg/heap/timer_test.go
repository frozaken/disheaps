package heap

import (
	"testing"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestBinaryTimerHeap_BasicOperations tests basic timer heap operations
func TestBinaryTimerHeap_BasicOperations(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	// Test empty timer heap
	assert.Equal(t, 0, timerHeap.Size())
	assert.Nil(t, timerHeap.NextReadyTime())
	assert.Empty(t, timerHeap.ProcessReady())

	// Create test messages
	baseTime := time.Now()
	futureTime := baseTime.Add(time.Hour)

	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg2 := models.NewMessage("test-topic", 1, 20, []byte("payload2"))
	msg3 := models.NewMessage("test-topic", 1, 30, []byte("payload3"))

	// Schedule messages for future processing
	err := timerHeap.Schedule(msg1, futureTime)
	require.NoError(t, err)
	assert.Equal(t, 1, timerHeap.Size())
	assert.True(t, timerHeap.Contains(msg1.ID))

	// Next ready time should be the scheduled time
	nextTime := timerHeap.NextReadyTime()
	require.NotNil(t, nextTime)
	assert.True(t, nextTime.Equal(futureTime))

	// Schedule more messages
	err = timerHeap.Schedule(msg2, futureTime.Add(30*time.Minute))
	require.NoError(t, err)
	err = timerHeap.Schedule(msg3, futureTime.Add(-10*time.Minute)) // Earlier
	require.NoError(t, err)

	assert.Equal(t, 3, timerHeap.Size())

	// Validate heap property
	err = timerHeap.Validate()
	assert.NoError(t, err)

	// Next ready time should now be the earliest (msg3)
	nextTime = timerHeap.NextReadyTime()
	require.NotNil(t, nextTime)
	assert.True(t, nextTime.Equal(futureTime.Add(-10*time.Minute)))

	// No messages should be ready yet (all in future)
	readyMessages := timerHeap.ProcessReady()
	assert.Empty(t, readyMessages)
	assert.Equal(t, 3, timerHeap.Size()) // Size shouldn't change
}

// TestBinaryTimerHeap_ProcessReady tests processing of ready messages
func TestBinaryTimerHeap_ProcessReady(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	now := time.Now()
	pastTime := now.Add(-time.Hour)  // Already ready
	futureTime := now.Add(time.Hour) // Not ready yet

	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg2 := models.NewMessage("test-topic", 1, 20, []byte("payload2"))
	msg3 := models.NewMessage("test-topic", 1, 30, []byte("payload3"))

	// Schedule one ready message and two future messages
	require.NoError(t, timerHeap.Schedule(msg1, pastTime))
	require.NoError(t, timerHeap.Schedule(msg2, futureTime))
	require.NoError(t, timerHeap.Schedule(msg3, futureTime.Add(time.Hour)))

	assert.Equal(t, 3, timerHeap.Size())

	// Process ready messages
	readyMessages := timerHeap.ProcessReady()
	require.Len(t, readyMessages, 1)
	assert.Equal(t, msg1.ID, readyMessages[0].ID)

	// Timer heap should now have 2 messages
	assert.Equal(t, 2, timerHeap.Size())
	assert.False(t, timerHeap.Contains(msg1.ID))
	assert.True(t, timerHeap.Contains(msg2.ID))
	assert.True(t, timerHeap.Contains(msg3.ID))

	// Validate heap after processing
	err := timerHeap.Validate()
	assert.NoError(t, err)
}

// TestBinaryTimerHeap_MultipleReadyMessages tests processing multiple ready messages
func TestBinaryTimerHeap_MultipleReadyMessages(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	now := time.Now()
	pastTime1 := now.Add(-2 * time.Hour)
	pastTime2 := now.Add(-time.Hour)
	futureTime := now.Add(time.Hour)

	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg2 := models.NewMessage("test-topic", 1, 20, []byte("payload2"))
	msg3 := models.NewMessage("test-topic", 1, 30, []byte("payload3"))
	msg4 := models.NewMessage("test-topic", 1, 40, []byte("payload4"))

	// Schedule multiple ready messages and one future message
	require.NoError(t, timerHeap.Schedule(msg1, pastTime1))
	require.NoError(t, timerHeap.Schedule(msg2, pastTime2))
	require.NoError(t, timerHeap.Schedule(msg3, now)) // Right now
	require.NoError(t, timerHeap.Schedule(msg4, futureTime))

	assert.Equal(t, 4, timerHeap.Size())

	// Process ready messages - should get 3 ready messages
	readyMessages := timerHeap.ProcessReady()
	assert.Len(t, readyMessages, 3)

	// Should be in chronological order (earliest first)
	readyIDs := make([]models.MessageID, len(readyMessages))
	for i, msg := range readyMessages {
		readyIDs[i] = msg.ID
	}

	// The order should be: msg1 (earliest), msg2, msg3
	expectedOrder := []models.MessageID{msg1.ID, msg2.ID, msg3.ID}
	assert.Equal(t, expectedOrder, readyIDs)

	// Only future message should remain
	assert.Equal(t, 1, timerHeap.Size())
	assert.True(t, timerHeap.Contains(msg4.ID))
}

// TestBinaryTimerHeap_Remove tests removing scheduled messages
func TestBinaryTimerHeap_Remove(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	futureTime := time.Now().Add(time.Hour)

	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg2 := models.NewMessage("test-topic", 1, 20, []byte("payload2"))
	msg3 := models.NewMessage("test-topic", 1, 30, []byte("payload3"))

	// Schedule all messages
	require.NoError(t, timerHeap.Schedule(msg1, futureTime))
	require.NoError(t, timerHeap.Schedule(msg2, futureTime.Add(time.Hour)))
	require.NoError(t, timerHeap.Schedule(msg3, futureTime.Add(2*time.Hour)))

	assert.Equal(t, 3, timerHeap.Size())

	// Remove non-existent message
	_, err := timerHeap.Remove("non-existent")
	assert.Error(t, err)

	// Remove middle message
	removedMsg, err := timerHeap.Remove(msg2.ID)
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, removedMsg.ID)

	assert.Equal(t, 2, timerHeap.Size())
	assert.False(t, timerHeap.Contains(msg2.ID))
	assert.True(t, timerHeap.Contains(msg1.ID))
	assert.True(t, timerHeap.Contains(msg3.ID))

	// Validate heap after removal
	err = timerHeap.Validate()
	assert.NoError(t, err)

	// Remove remaining messages
	_, err = timerHeap.Remove(msg1.ID)
	require.NoError(t, err)
	_, err = timerHeap.Remove(msg3.ID)
	require.NoError(t, err)

	assert.Equal(t, 0, timerHeap.Size())
	assert.Nil(t, timerHeap.NextReadyTime())
}

// TestBinaryTimerHeap_DuplicateScheduling tests duplicate message scheduling
func TestBinaryTimerHeap_DuplicateScheduling(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	futureTime := time.Now().Add(time.Hour)
	msg := models.NewMessage("test-topic", 1, 10, []byte("payload"))

	// Schedule message
	err := timerHeap.Schedule(msg, futureTime)
	require.NoError(t, err)

	// Try to schedule same message again
	err = timerHeap.Schedule(msg, futureTime.Add(time.Hour))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already scheduled")

	// Size should remain 1
	assert.Equal(t, 1, timerHeap.Size())
}

// TestBinaryTimerHeap_Clear tests clearing all scheduled messages
func TestBinaryTimerHeap_Clear(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	futureTime := time.Now().Add(time.Hour)

	// Schedule multiple messages
	for i := 0; i < 10; i++ {
		msg := models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
		err := timerHeap.Schedule(msg, futureTime.Add(time.Duration(i)*time.Minute))
		require.NoError(t, err)
	}

	assert.Equal(t, 10, timerHeap.Size())

	// Clear all messages
	timerHeap.Clear()

	assert.Equal(t, 0, timerHeap.Size())
	assert.Nil(t, timerHeap.NextReadyTime())
	assert.Empty(t, timerHeap.ProcessReady())
}

// TestBinaryTimerHeap_HeapProperty tests that heap property is maintained
func TestBinaryTimerHeap_HeapProperty(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	baseTime := time.Now().Add(time.Hour)

	// Schedule messages with various times (not in order)
	times := []time.Duration{
		30 * time.Minute, // msg0
		10 * time.Minute, // msg1 - earliest
		45 * time.Minute, // msg2
		5 * time.Minute,  // msg3 - very early
		60 * time.Minute, // msg4 - latest
	}

	for i, offset := range times {
		msg := models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
		err := timerHeap.Schedule(msg, baseTime.Add(offset))
		require.NoError(t, err)

		// Validate heap property after each insertion
		err = timerHeap.Validate()
		assert.NoError(t, err, "Heap property violated after inserting message %d", i)
	}

	// Root should have the earliest time (5 minutes)
	nextTime := timerHeap.NextReadyTime()
	require.NotNil(t, nextTime)
	assert.True(t, nextTime.Equal(baseTime.Add(5*time.Minute)))
}

// TestBinaryTimerHeap_Stats tests timer heap statistics
func TestBinaryTimerHeap_Stats(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	// Empty heap stats
	stats := timerHeap.Stats()
	assert.Equal(t, 0, stats.ScheduledMessages)
	assert.Equal(t, 0, stats.IndexSize)
	assert.Nil(t, stats.NextReadyTime)
	assert.Nil(t, stats.AverageDelay)

	// Add some messages
	baseTime := time.Now()
	futureTime := baseTime.Add(time.Hour)

	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg2 := models.NewMessage("test-topic", 1, 20, []byte("payload2"))

	require.NoError(t, timerHeap.Schedule(msg1, futureTime))
	require.NoError(t, timerHeap.Schedule(msg2, futureTime.Add(time.Hour)))

	// Stats after scheduling
	stats = timerHeap.Stats()
	assert.Equal(t, 2, stats.ScheduledMessages)
	assert.Equal(t, 2, stats.IndexSize)
	assert.NotNil(t, stats.NextReadyTime)
	assert.True(t, stats.NextReadyTime.Equal(futureTime))
	assert.NotNil(t, stats.AverageDelay)
	assert.Greater(t, *stats.AverageDelay, time.Duration(0))
}

// TestBinaryTimerHeap_NilMessage tests error handling for nil messages
func TestBinaryTimerHeap_NilMessage(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	err := timerHeap.Schedule(nil, time.Now().Add(time.Hour))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be nil")
}

// TestBinaryTimerHeap_EdgeCaseTimes tests edge cases with times
func TestBinaryTimerHeap_EdgeCaseTimes(t *testing.T) {
	logger := zaptest.NewLogger(t)
	timerHeap := NewBinaryTimerHeap(logger)

	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg2 := models.NewMessage("test-topic", 1, 20, []byte("payload2"))

	// Schedule message for exactly now
	now := time.Now()
	require.NoError(t, timerHeap.Schedule(msg1, now))

	// Schedule message for past time (should be immediately ready)
	pastTime := now.Add(-time.Hour)
	require.NoError(t, timerHeap.Schedule(msg2, pastTime))

	// Both messages should be ready
	readyMessages := timerHeap.ProcessReady()
	assert.Len(t, readyMessages, 2)

	// Should be ordered by scheduled time (past first, then now)
	assert.Equal(t, msg2.ID, readyMessages[0].ID) // Past time first
	assert.Equal(t, msg1.ID, readyMessages[1].ID) // Now second
}

// BenchmarkTimerHeap_Schedule benchmarks message scheduling
func BenchmarkTimerHeap_Schedule(b *testing.B) {
	logger := zaptest.NewLogger(b)
	timerHeap := NewBinaryTimerHeap(logger)

	baseTime := time.Now().Add(time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
		scheduleTime := baseTime.Add(time.Duration(i) * time.Second)
		_ = timerHeap.Schedule(msg, scheduleTime)
	}
}

// BenchmarkTimerHeap_ProcessReady benchmarks ready message processing
func BenchmarkTimerHeap_ProcessReady(b *testing.B) {
	logger := zaptest.NewLogger(b)
	timerHeap := NewBinaryTimerHeap(logger)

	// Pre-populate with ready messages
	pastTime := time.Now().Add(-time.Hour)
	for i := 0; i < 1000; i++ {
		msg := models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
		_ = timerHeap.Schedule(msg, pastTime.Add(time.Duration(i)*time.Second))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = timerHeap.ProcessReady()

		// Re-add a message to keep the heap populated
		if timerHeap.Size() == 0 {
			msg := models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
			_ = timerHeap.Schedule(msg, pastTime)
		}
	}
}
