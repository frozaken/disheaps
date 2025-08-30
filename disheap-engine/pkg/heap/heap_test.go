package heap

import (
	"testing"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBinaryHeap_BasicOperations tests basic heap operations
func TestBinaryHeap_BasicOperations(t *testing.T) {
	heap := NewBinaryHeap(models.MinHeap)

	// Test empty heap
	assert.True(t, heap.IsEmpty())
	assert.Equal(t, 0, heap.Size())

	// Test peek on empty heap
	_, err := heap.Peek()
	assert.Error(t, err)

	// Test extract on empty heap
	_, err = heap.Extract()
	assert.Error(t, err)

	// Create test messages
	baseTime := time.Now()
	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg1.EnqueuedAt = baseTime

	msg2 := models.NewMessage("test-topic", 1, 5, []byte("payload2"))
	msg2.EnqueuedAt = baseTime.Add(time.Second)

	msg3 := models.NewMessage("test-topic", 1, 15, []byte("payload3"))
	msg3.EnqueuedAt = baseTime.Add(2 * time.Second)

	// Test insert
	err = heap.Insert(msg1)
	require.NoError(t, err)
	assert.False(t, heap.IsEmpty())
	assert.Equal(t, 1, heap.Size())

	err = heap.Insert(msg2)
	require.NoError(t, err)
	err = heap.Insert(msg3)
	require.NoError(t, err)
	assert.Equal(t, 3, heap.Size())

	// Test heap property validation
	err = heap.Validate()
	assert.NoError(t, err)

	// Test duplicate insert
	err = heap.Insert(msg1)
	assert.Error(t, err)

	// Test peek (should return min priority = 5)
	topMsg, err := heap.Peek()
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, topMsg.ID)
	assert.Equal(t, int64(5), topMsg.Priority)
	assert.Equal(t, 3, heap.Size()) // Size shouldn't change

	// Test extract (should return min priority = 5)
	extractedMsg, err := heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, extractedMsg.ID)
	assert.Equal(t, int64(5), extractedMsg.Priority)
	assert.Equal(t, 2, heap.Size())

	// Validate heap after extraction
	err = heap.Validate()
	assert.NoError(t, err)

	// Next extraction should be priority 10
	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg1.ID, extractedMsg.ID)
	assert.Equal(t, int64(10), extractedMsg.Priority)

	// Final extraction should be priority 15
	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg3.ID, extractedMsg.ID)
	assert.Equal(t, int64(15), extractedMsg.Priority)

	// Heap should be empty now
	assert.True(t, heap.IsEmpty())
	assert.Equal(t, 0, heap.Size())
}

// TestBinaryHeap_MaxHeap tests max-heap behavior
func TestBinaryHeap_MaxHeap(t *testing.T) {
	heap := NewBinaryHeap(models.MaxHeap)

	baseTime := time.Now()
	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg1.EnqueuedAt = baseTime

	msg2 := models.NewMessage("test-topic", 1, 5, []byte("payload2"))
	msg2.EnqueuedAt = baseTime.Add(time.Second)

	msg3 := models.NewMessage("test-topic", 1, 15, []byte("payload3"))
	msg3.EnqueuedAt = baseTime.Add(2 * time.Second)

	// Insert all messages
	require.NoError(t, heap.Insert(msg1))
	require.NoError(t, heap.Insert(msg2))
	require.NoError(t, heap.Insert(msg3))

	// Validate heap property
	err := heap.Validate()
	assert.NoError(t, err)

	// Max-heap should return highest priority first (15)
	extractedMsg, err := heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg3.ID, extractedMsg.ID)
	assert.Equal(t, int64(15), extractedMsg.Priority)

	// Next should be priority 10
	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg1.ID, extractedMsg.ID)
	assert.Equal(t, int64(10), extractedMsg.Priority)

	// Last should be priority 5
	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, extractedMsg.ID)
	assert.Equal(t, int64(5), extractedMsg.Priority)
}

// TestBinaryHeap_SecondaryOrdering tests timestamp-based secondary ordering
func TestBinaryHeap_SecondaryOrdering(t *testing.T) {
	heap := NewBinaryHeap(models.MinHeap)

	baseTime := time.Now()

	// Create messages with same priority but different timestamps
	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg1.EnqueuedAt = baseTime.Add(2 * time.Second) // Later timestamp

	msg2 := models.NewMessage("test-topic", 1, 10, []byte("payload2"))
	msg2.EnqueuedAt = baseTime // Earlier timestamp

	msg3 := models.NewMessage("test-topic", 1, 10, []byte("payload3"))
	msg3.EnqueuedAt = baseTime.Add(time.Second) // Middle timestamp

	// Insert in different order
	require.NoError(t, heap.Insert(msg1))
	require.NoError(t, heap.Insert(msg2))
	require.NoError(t, heap.Insert(msg3))

	err := heap.Validate()
	assert.NoError(t, err)

	// Should extract in timestamp order (earlier first)
	extractedMsg, err := heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, extractedMsg.ID) // Earliest timestamp

	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg3.ID, extractedMsg.ID) // Middle timestamp

	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg1.ID, extractedMsg.ID) // Latest timestamp
}

// TestBinaryHeap_TertiaryOrdering tests message ID-based tertiary ordering
func TestBinaryHeap_TertiaryOrdering(t *testing.T) {
	heap := NewBinaryHeap(models.MinHeap)

	baseTime := time.Now()

	// Create messages with same priority and timestamp
	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg1.ID = "zzzz" // Lexicographically last
	msg1.EnqueuedAt = baseTime

	msg2 := models.NewMessage("test-topic", 1, 10, []byte("payload2"))
	msg2.ID = "aaaa" // Lexicographically first
	msg2.EnqueuedAt = baseTime

	msg3 := models.NewMessage("test-topic", 1, 10, []byte("payload3"))
	msg3.ID = "mmmm" // Lexicographically middle
	msg3.EnqueuedAt = baseTime

	// Insert in different order
	require.NoError(t, heap.Insert(msg1))
	require.NoError(t, heap.Insert(msg2))
	require.NoError(t, heap.Insert(msg3))

	err := heap.Validate()
	assert.NoError(t, err)

	// Should extract in lexicographic order (ID)
	extractedMsg, err := heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, models.MessageID("aaaa"), extractedMsg.ID)

	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, models.MessageID("mmmm"), extractedMsg.ID)

	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, models.MessageID("zzzz"), extractedMsg.ID)
}

// TestBinaryHeap_Remove tests message removal by ID
func TestBinaryHeap_Remove(t *testing.T) {
	heap := NewBinaryHeap(models.MinHeap)

	baseTime := time.Now()
	msg1 := models.NewMessage("test-topic", 1, 10, []byte("payload1"))
	msg1.EnqueuedAt = baseTime

	msg2 := models.NewMessage("test-topic", 1, 5, []byte("payload2"))
	msg2.EnqueuedAt = baseTime.Add(time.Second)

	msg3 := models.NewMessage("test-topic", 1, 15, []byte("payload3"))
	msg3.EnqueuedAt = baseTime.Add(2 * time.Second)

	// Insert all messages
	require.NoError(t, heap.Insert(msg1))
	require.NoError(t, heap.Insert(msg2))
	require.NoError(t, heap.Insert(msg3))
	assert.Equal(t, 3, heap.Size())

	// Remove non-existent message
	_, err := heap.Remove("non-existent")
	assert.Error(t, err)

	// Remove middle priority message
	removedMsg, err := heap.Remove(msg1.ID)
	require.NoError(t, err)
	assert.Equal(t, msg1.ID, removedMsg.ID)
	assert.Equal(t, 2, heap.Size())

	// Validate heap after removal
	err = heap.Validate()
	assert.NoError(t, err)

	// Extract should now give min priority (5), then max (15)
	extractedMsg, err := heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg2.ID, extractedMsg.ID)

	extractedMsg, err = heap.Extract()
	require.NoError(t, err)
	assert.Equal(t, msg3.ID, extractedMsg.ID)

	assert.True(t, heap.IsEmpty())
}

// TestBinaryHeap_TopN tests getting top N messages without removal
func TestBinaryHeap_TopN(t *testing.T) {
	heap := NewBinaryHeap(models.MinHeap)

	// Test TopN on empty heap
	topMessages := heap.TopN(5)
	assert.Nil(t, topMessages)

	baseTime := time.Now()
	messages := make([]*models.Message, 5)
	priorities := []int64{30, 10, 20, 5, 25}

	// Insert messages with various priorities
	for i, priority := range priorities {
		messages[i] = models.NewMessage("test-topic", 1, priority, []byte("payload"))
		messages[i].EnqueuedAt = baseTime.Add(time.Duration(i) * time.Second)
		require.NoError(t, heap.Insert(messages[i]))
	}

	// Test TopN with n > size
	topMessages = heap.TopN(10)
	require.Len(t, topMessages, 5)

	// Should be in priority order (min-heap: 5, 10, 20, 25, 30)
	expectedPriorities := []int64{5, 10, 20, 25, 30}
	for i, msg := range topMessages {
		assert.Equal(t, expectedPriorities[i], msg.Priority)
	}

	// Test TopN with n < size
	topMessages = heap.TopN(3)
	require.Len(t, topMessages, 3)

	// Should be top 3: 5, 10, 20
	for i, msg := range topMessages {
		assert.Equal(t, expectedPriorities[i], msg.Priority)
	}

	// Original heap should be unchanged
	assert.Equal(t, 5, heap.Size())
	peekedMsg, err := heap.Peek()
	require.NoError(t, err)
	assert.Equal(t, int64(5), peekedMsg.Priority) // Min priority should still be at top
}

// TestBinaryHeap_LargeOperations tests heap with many elements
func TestBinaryHeap_LargeOperations(t *testing.T) {
	heap := NewBinaryHeap(models.MinHeap)

	const numMessages = 1000
	messages := make([]*models.Message, numMessages)
	baseTime := time.Now()

	// Insert many messages
	for i := 0; i < numMessages; i++ {
		messages[i] = models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
		messages[i].EnqueuedAt = baseTime.Add(time.Duration(i) * time.Nanosecond)
		require.NoError(t, heap.Insert(messages[i]))
	}

	assert.Equal(t, numMessages, heap.Size())

	// Validate heap property
	err := heap.Validate()
	assert.NoError(t, err)

	// Extract all messages - should come out in priority order
	for i := 0; i < numMessages; i++ {
		extractedMsg, err := heap.Extract()
		require.NoError(t, err)
		assert.Equal(t, int64(i), extractedMsg.Priority, "Message %d should have priority %d", i, i)
	}

	assert.True(t, heap.IsEmpty())
}

// TestBinaryHeap_NilMessage tests error handling for nil messages
func TestBinaryHeap_NilMessage(t *testing.T) {
	heap := NewBinaryHeap(models.MinHeap)

	err := heap.Insert(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be nil")
}

// BenchmarkBinaryHeap_Insert benchmarks heap insertion performance
func BenchmarkBinaryHeap_Insert(b *testing.B) {
	heap := NewBinaryHeap(models.MinHeap)
	baseTime := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
		msg.EnqueuedAt = baseTime.Add(time.Duration(i) * time.Nanosecond)
		_ = heap.Insert(msg)
	}
}

// BenchmarkBinaryHeap_Extract benchmarks heap extraction performance
func BenchmarkBinaryHeap_Extract(b *testing.B) {
	heap := NewBinaryHeap(models.MinHeap)
	baseTime := time.Now()

	// Pre-populate heap
	for i := 0; i < b.N; i++ {
		msg := models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
		msg.EnqueuedAt = baseTime.Add(time.Duration(i) * time.Nanosecond)
		_ = heap.Insert(msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = heap.Extract()
	}
}

// BenchmarkBinaryHeap_TopN benchmarks TopN operation performance
func BenchmarkBinaryHeap_TopN(b *testing.B) {
	heap := NewBinaryHeap(models.MinHeap)
	baseTime := time.Now()

	// Pre-populate heap
	for i := 0; i < 10000; i++ {
		msg := models.NewMessage("test-topic", 1, int64(i), []byte("payload"))
		msg.EnqueuedAt = baseTime.Add(time.Duration(i) * time.Nanosecond)
		_ = heap.Insert(msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = heap.TopN(100)
	}
}
