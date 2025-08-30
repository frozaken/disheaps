package heap

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// TimerHeap manages delayed messages using a min-heap ordered by not_before timestamps
// Messages are held until their scheduled time, then promoted to the main heap
type TimerHeap interface {
	Schedule(msg *models.Message, notBefore time.Time) error
	ProcessReady() []*models.Message
	Size() int
	NextReadyTime() *time.Time
	Remove(msgID models.MessageID) (*models.Message, error)
	Contains(msgID models.MessageID) bool
	Clear()
}

// BinaryTimerHeap implements TimerHeap using a binary min-heap ordered by not_before time
type BinaryTimerHeap struct {
	mu      sync.RWMutex
	entries []*TimerEntry
	index   MessageIndex // For O(1) message lookup
	logger  *zap.Logger
}

// TimerEntry represents a scheduled message with its activation time
type TimerEntry struct {
	Message     *models.Message
	NotBefore   time.Time
	ScheduledAt time.Time // When this entry was added
}

// NewTimerEntry creates a new timer entry
func NewTimerEntry(msg *models.Message, notBefore time.Time) *TimerEntry {
	return &TimerEntry{
		Message:     msg,
		NotBefore:   notBefore,
		ScheduledAt: time.Now(),
	}
}

// NewBinaryTimerHeap creates a new timer heap
func NewBinaryTimerHeap(logger *zap.Logger) *BinaryTimerHeap {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &BinaryTimerHeap{
		entries: make([]*TimerEntry, 0),
		index:   NewMessageIndex(),
		logger:  logger.With(zap.String("component", "timer_heap")),
	}
}

// Schedule adds a message to be activated at the specified time
func (th *BinaryTimerHeap) Schedule(msg *models.Message, notBefore time.Time) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	th.mu.Lock()
	defer th.mu.Unlock()

	// Check if message is already scheduled
	if th.index.Contains(msg.ID) {
		return fmt.Errorf("message %s is already scheduled", msg.ID)
	}

	entry := NewTimerEntry(msg, notBefore)

	// Add to the end of the heap
	th.entries = append(th.entries, entry)
	position := len(th.entries) - 1

	// Update index
	th.index.Set(msg.ID, position)

	// Heapify up to maintain min-heap property (earliest time at root)
	th.heapifyUp(position)

	th.logger.Debug("Message scheduled for delayed processing",
		zap.String("message_id", string(msg.ID)),
		zap.Time("not_before", notBefore),
		zap.Duration("delay", time.Until(notBefore)),
	)

	return nil
}

// ProcessReady returns all messages that are ready to be processed (not_before <= now)
// and removes them from the timer heap
func (th *BinaryTimerHeap) ProcessReady() []*models.Message {
	th.mu.Lock()
	defer th.mu.Unlock()

	now := time.Now()
	var ready []*models.Message

	// Extract all ready messages from the top of the heap
	for len(th.entries) > 0 && !th.entries[0].NotBefore.After(now) {
		entry := th.entries[0]
		msg := entry.Message

		// Remove from index
		th.index.Remove(msg.ID)

		// Move last element to root
		lastIndex := len(th.entries) - 1
		if lastIndex == 0 {
			// Only one element
			th.entries = th.entries[:0]
		} else {
			// Move last element to root
			th.entries[0] = th.entries[lastIndex]
			th.entries = th.entries[:lastIndex]

			// Update index for moved element
			th.index.Set(th.entries[0].Message.ID, 0)

			// Heapify down from root
			th.heapifyDown(0)
		}

		ready = append(ready, msg)

		th.logger.Debug("Message promoted from timer heap",
			zap.String("message_id", string(msg.ID)),
			zap.Time("not_before", entry.NotBefore),
			zap.Duration("waited", time.Since(entry.ScheduledAt)),
		)
	}

	if len(ready) > 0 {
		th.logger.Info("Promoted messages from timer heap",
			zap.Int("count", len(ready)),
			zap.Int("remaining_scheduled", len(th.entries)),
		)
	}

	return ready
}

// Size returns the number of scheduled messages
func (th *BinaryTimerHeap) Size() int {
	th.mu.RLock()
	defer th.mu.RUnlock()
	return len(th.entries)
}

// NextReadyTime returns the time when the next message will be ready
// Returns nil if no messages are scheduled
func (th *BinaryTimerHeap) NextReadyTime() *time.Time {
	th.mu.RLock()
	defer th.mu.RUnlock()

	if len(th.entries) == 0 {
		return nil
	}

	// Root of min-heap has the earliest time
	nextTime := th.entries[0].NotBefore
	return &nextTime
}

// Remove removes a scheduled message by ID
func (th *BinaryTimerHeap) Remove(msgID models.MessageID) (*models.Message, error) {
	th.mu.Lock()
	defer th.mu.Unlock()

	position, exists := th.index.Get(msgID)
	if !exists {
		return nil, fmt.Errorf("message %s not found in timer heap", msgID)
	}

	entry := th.entries[position]
	msg := entry.Message

	// Remove from index
	th.index.Remove(msgID)

	lastIndex := len(th.entries) - 1
	if position == lastIndex {
		// Removing last element, just truncate
		th.entries = th.entries[:lastIndex]
	} else {
		// Move last element to removed position
		th.entries[position] = th.entries[lastIndex]
		th.entries = th.entries[:lastIndex]

		// Update index for moved element
		th.index.Set(th.entries[position].Message.ID, position)

		// Heapify both up and down from the position
		th.heapifyUp(position)
		th.heapifyDown(position)
	}

	th.logger.Debug("Message removed from timer heap",
		zap.String("message_id", string(msgID)),
		zap.Time("was_scheduled_for", entry.NotBefore),
	)

	return msg, nil
}

// Contains checks if a message is scheduled in the timer heap
func (th *BinaryTimerHeap) Contains(msgID models.MessageID) bool {
	th.mu.RLock()
	defer th.mu.RUnlock()
	return th.index.Contains(msgID)
}

// Clear removes all scheduled messages
func (th *BinaryTimerHeap) Clear() {
	th.mu.Lock()
	defer th.mu.Unlock()

	th.entries = th.entries[:0]
	th.index.Clear()

	th.logger.Info("Timer heap cleared")
}

// heapifyUp maintains min-heap property by moving element up
func (th *BinaryTimerHeap) heapifyUp(index int) {
	for index > 0 {
		parentIndex := (index - 1) / 2

		if th.entries[index].NotBefore.Before(th.entries[parentIndex].NotBefore) {
			th.swap(index, parentIndex)
			index = parentIndex
		} else {
			break
		}
	}
}

// heapifyDown maintains min-heap property by moving element down
func (th *BinaryTimerHeap) heapifyDown(index int) {
	for {
		leftChild := 2*index + 1
		rightChild := 2*index + 2
		earliest := index

		// Find the earliest time among parent and children
		if leftChild < len(th.entries) &&
			th.entries[leftChild].NotBefore.Before(th.entries[earliest].NotBefore) {
			earliest = leftChild
		}

		if rightChild < len(th.entries) &&
			th.entries[rightChild].NotBefore.Before(th.entries[earliest].NotBefore) {
			earliest = rightChild
		}

		if earliest != index {
			th.swap(index, earliest)
			index = earliest
		} else {
			break
		}
	}
}

// swap exchanges two elements in the heap and updates the index
func (th *BinaryTimerHeap) swap(i, j int) {
	// Swap entries
	th.entries[i], th.entries[j] = th.entries[j], th.entries[i]

	// Update index mappings
	th.index.Set(th.entries[i].Message.ID, i)
	th.index.Set(th.entries[j].Message.ID, j)
}

// GetScheduledMessages returns all scheduled messages (for debugging/testing)
func (th *BinaryTimerHeap) GetScheduledMessages() []*TimerEntry {
	th.mu.RLock()
	defer th.mu.RUnlock()

	// Return a sorted copy by NotBefore time
	result := make([]*TimerEntry, len(th.entries))
	copy(result, th.entries)

	sort.Slice(result, func(i, j int) bool {
		return result[i].NotBefore.Before(result[j].NotBefore)
	})

	return result
}

// Validate checks heap invariants (for testing/debugging)
func (th *BinaryTimerHeap) Validate() error {
	th.mu.RLock()
	defer th.mu.RUnlock()

	// Check heap property (min-heap: parent <= children)
	for i := 0; i < len(th.entries); i++ {
		leftChild := 2*i + 1
		rightChild := 2*i + 2

		if leftChild < len(th.entries) {
			if th.entries[leftChild].NotBefore.Before(th.entries[i].NotBefore) {
				return fmt.Errorf("heap property violated at index %d (left child %d)", i, leftChild)
			}
		}

		if rightChild < len(th.entries) {
			if th.entries[rightChild].NotBefore.Before(th.entries[i].NotBefore) {
				return fmt.Errorf("heap property violated at index %d (right child %d)", i, rightChild)
			}
		}
	}

	// Check index consistency
	if th.index.Size() != len(th.entries) {
		return fmt.Errorf("index size %d doesn't match entries size %d", th.index.Size(), len(th.entries))
	}

	for i, entry := range th.entries {
		indexPos, exists := th.index.Get(entry.Message.ID)
		if !exists {
			return fmt.Errorf("message %s at position %d not found in index", entry.Message.ID, i)
		}
		if indexPos != i {
			return fmt.Errorf("message %s index position %d doesn't match actual position %d",
				entry.Message.ID, indexPos, i)
		}
	}

	return nil
}

// Stats returns timer heap statistics
func (th *BinaryTimerHeap) Stats() TimerHeapStats {
	th.mu.RLock()
	defer th.mu.RUnlock()

	stats := TimerHeapStats{
		ScheduledMessages: len(th.entries),
		IndexSize:         th.index.Size(),
	}

	if len(th.entries) > 0 {
		stats.NextReadyTime = &th.entries[0].NotBefore

		// Calculate distribution of delay times
		now := time.Now()
		var totalDelay time.Duration
		for _, entry := range th.entries {
			delay := entry.NotBefore.Sub(now)
			if delay > 0 {
				totalDelay += delay
			}
		}
		if len(th.entries) > 0 {
			avgDelay := totalDelay / time.Duration(len(th.entries))
			stats.AverageDelay = &avgDelay
		}
	}

	return stats
}

// TimerHeapStats provides statistics about the timer heap
type TimerHeapStats struct {
	ScheduledMessages int            `json:"scheduled_messages"`
	IndexSize         int            `json:"index_size"`
	NextReadyTime     *time.Time     `json:"next_ready_time,omitempty"`
	AverageDelay      *time.Duration `json:"average_delay,omitempty"`
}
