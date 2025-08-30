package heap

import (
	"fmt"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
)

// HeapEntry represents a single entry in the heap with pre-computed comparison key
type HeapEntry struct {
	Message       *models.Message
	ComparisonKey *MessageComparisonKey
}

// NewHeapEntry creates a new heap entry with computed comparison key
func NewHeapEntry(msg *models.Message) *HeapEntry {
	return &HeapEntry{
		Message:       msg,
		ComparisonKey: NewMessageComparisonKey(msg),
	}
}

// MessageComparisonKey provides pre-computed fields for efficient message comparison
// This avoids repeated field access during heap operations
type MessageComparisonKey struct {
	Priority   int64
	EnqueuedAt time.Time
	MessageID  string
}

// NewMessageComparisonKey creates a comparison key from a message
func NewMessageComparisonKey(msg *models.Message) *MessageComparisonKey {
	return &MessageComparisonKey{
		Priority:   msg.Priority,
		EnqueuedAt: msg.EnqueuedAt,
		MessageID:  string(msg.ID),
	}
}

// CompareTo compares this key to another according to heap ordering rules:
// 1. Primary: Priority (based on heap mode)
// 2. Secondary: EnqueuedAt timestamp (earlier first)
// 3. Tertiary: MessageID (lexicographic for determinism)
//
// Returns:
//
//	-1 if this key should come before other (has higher priority)
//	 0 if keys are equal in ordering
//	+1 if this key should come after other (has lower priority)
func (k *MessageComparisonKey) CompareTo(other *MessageComparisonKey, mode models.HeapMode) int {
	// Primary comparison: Priority
	if k.Priority != other.Priority {
		switch mode {
		case models.MinHeap:
			if k.Priority < other.Priority {
				return -1 // Lower priority values first in min-heap
			}
			return 1
		case models.MaxHeap:
			if k.Priority > other.Priority {
				return -1 // Higher priority values first in max-heap
			}
			return 1
		}
	}

	// Secondary comparison: EnqueuedAt timestamp (earlier first)
	if k.EnqueuedAt.Before(other.EnqueuedAt) {
		return -1
	}
	if k.EnqueuedAt.After(other.EnqueuedAt) {
		return 1
	}

	// Tertiary comparison: MessageID (lexicographic for determinism)
	if k.MessageID < other.MessageID {
		return -1
	}
	if k.MessageID > other.MessageID {
		return 1
	}

	return 0 // Equal
}

// String returns a string representation for debugging
func (k *MessageComparisonKey) String() string {
	return fmt.Sprintf("ComparisonKey{Priority:%d, EnqueuedAt:%v, MessageID:%s}",
		k.Priority, k.EnqueuedAt, k.MessageID)
}

// IsEqual checks if two comparison keys are equal
func (k *MessageComparisonKey) IsEqual(other *MessageComparisonKey) bool {
	return k.Priority == other.Priority &&
		k.EnqueuedAt.Equal(other.EnqueuedAt) &&
		k.MessageID == other.MessageID
}

// Clone creates a deep copy of the comparison key
func (k *MessageComparisonKey) Clone() *MessageComparisonKey {
	return &MessageComparisonKey{
		Priority:   k.Priority,
		EnqueuedAt: k.EnqueuedAt,
		MessageID:  k.MessageID,
	}
}
