package heap

import (
	"fmt"
	"sync"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
)

// MessageIndex provides O(1) lookup for message ID to heap position mapping
// This enables efficient Remove() operations on the heap
type MessageIndex interface {
	Set(msgID models.MessageID, position int)
	Get(msgID models.MessageID) (position int, exists bool)
	Remove(msgID models.MessageID)
	Contains(msgID models.MessageID) bool
	Size() int
	Clear()
}

// ConcurrentMessageIndex implements MessageIndex with thread-safety
// Note: The heap itself is not thread-safe, but the index can be safely
// accessed concurrently for read operations
type ConcurrentMessageIndex struct {
	mu    sync.RWMutex
	index map[models.MessageID]int
}

// NewMessageIndex creates a new message index
func NewMessageIndex() MessageIndex {
	return &ConcurrentMessageIndex{
		index: make(map[models.MessageID]int),
	}
}

// Set updates the position for a message ID
func (idx *ConcurrentMessageIndex) Set(msgID models.MessageID, position int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.index[msgID] = position
}

// Get retrieves the position for a message ID
func (idx *ConcurrentMessageIndex) Get(msgID models.MessageID) (int, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	position, exists := idx.index[msgID]
	return position, exists
}

// Remove deletes a message ID from the index
func (idx *ConcurrentMessageIndex) Remove(msgID models.MessageID) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.index, msgID)
}

// Contains checks if a message ID exists in the index
func (idx *ConcurrentMessageIndex) Contains(msgID models.MessageID) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, exists := idx.index[msgID]
	return exists
}

// Size returns the number of entries in the index
func (idx *ConcurrentMessageIndex) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.index)
}

// Clear removes all entries from the index
func (idx *ConcurrentMessageIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.index = make(map[models.MessageID]int)
}

// GetAll returns a copy of all message IDs and their positions
// This is useful for debugging and validation
func (idx *ConcurrentMessageIndex) GetAll() map[models.MessageID]int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make(map[models.MessageID]int, len(idx.index))
	for msgID, position := range idx.index {
		result[msgID] = position
	}
	return result
}

// Validate checks the consistency of the index
func (idx *ConcurrentMessageIndex) Validate(expectedSize int) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.index) != expectedSize {
		return fmt.Errorf("index size mismatch: expected %d, got %d", expectedSize, len(idx.index))
	}

	// Check for invalid positions
	for msgID, position := range idx.index {
		if position < 0 {
			return fmt.Errorf("invalid negative position %d for message %s", position, msgID)
		}
		if position >= expectedSize {
			return fmt.Errorf("position %d out of bounds for message %s (size %d)", position, msgID, expectedSize)
		}
	}

	return nil
}

// Stats returns statistics about the index
func (idx *ConcurrentMessageIndex) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return IndexStats{
		Size:     len(idx.index),
		Capacity: len(idx.index), // Go maps don't have separate capacity
	}
}

// IndexStats provides statistics about the message index
type IndexStats struct {
	Size     int `json:"size"`
	Capacity int `json:"capacity"`
}
