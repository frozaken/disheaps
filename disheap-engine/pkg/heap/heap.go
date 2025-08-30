package heap

import (
	"fmt"
	"sort"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
)

// Heap represents a binary heap with message ordering
type Heap interface {
	Insert(msg *models.Message) error
	Extract() (*models.Message, error)
	Peek() (*models.Message, error)
	Remove(msgID models.MessageID) (*models.Message, error)
	Size() int
	IsEmpty() bool

	// For spine index candidate selection
	TopN(n int) []*models.Message
}

// BinaryHeap implements a binary heap for messages
// Supports both min-heap and max-heap modes based on topic configuration
type BinaryHeap struct {
	entries []*HeapEntry
	index   MessageIndex
	mode    models.HeapMode
}

// NewBinaryHeap creates a new binary heap instance
func NewBinaryHeap(mode models.HeapMode) *BinaryHeap {
	return &BinaryHeap{
		entries: make([]*HeapEntry, 0),
		index:   NewMessageIndex(),
		mode:    mode,
	}
}

// Insert adds a message to the heap
func (h *BinaryHeap) Insert(msg *models.Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	// Check if message already exists
	if h.index.Contains(msg.ID) {
		return fmt.Errorf("message %s already exists in heap", msg.ID)
	}

	entry := NewHeapEntry(msg)

	// Add to the end of the heap
	h.entries = append(h.entries, entry)
	position := len(h.entries) - 1

	// Update index
	h.index.Set(msg.ID, position)

	// Heapify up
	h.heapifyUp(position)

	return nil
}

// Extract removes and returns the top element (min or max based on mode)
func (h *BinaryHeap) Extract() (*models.Message, error) {
	if h.IsEmpty() {
		return nil, fmt.Errorf("heap is empty")
	}

	// Get the root element
	root := h.entries[0]
	msg := root.Message

	// Remove from index
	h.index.Remove(msg.ID)

	// Move last element to root
	lastIndex := len(h.entries) - 1
	if lastIndex == 0 {
		// Only one element, just remove it
		h.entries = h.entries[:0]
		return msg, nil
	}

	// Move last element to root
	h.entries[0] = h.entries[lastIndex]
	h.entries = h.entries[:lastIndex]

	// Update index for moved element
	h.index.Set(h.entries[0].Message.ID, 0)

	// Heapify down from root
	h.heapifyDown(0)

	return msg, nil
}

// Peek returns the top element without removing it
func (h *BinaryHeap) Peek() (*models.Message, error) {
	if h.IsEmpty() {
		return nil, fmt.Errorf("heap is empty")
	}

	return h.entries[0].Message, nil
}

// Remove removes a specific message by ID
func (h *BinaryHeap) Remove(msgID models.MessageID) (*models.Message, error) {
	position, exists := h.index.Get(msgID)
	if !exists {
		return nil, fmt.Errorf("message %s not found in heap", msgID)
	}

	msg := h.entries[position].Message

	// Remove from index
	h.index.Remove(msgID)

	lastIndex := len(h.entries) - 1
	if position == lastIndex {
		// Removing last element, just truncate
		h.entries = h.entries[:lastIndex]
		return msg, nil
	}

	// Move last element to removed position
	h.entries[position] = h.entries[lastIndex]
	h.entries = h.entries[:lastIndex]

	// Update index for moved element
	h.index.Set(h.entries[position].Message.ID, position)

	// Heapify both up and down from the position
	// This handles cases where the moved element needs to go either direction
	h.heapifyUp(position)
	h.heapifyDown(position)

	return msg, nil
}

// Size returns the number of elements in the heap
func (h *BinaryHeap) Size() int {
	return len(h.entries)
}

// IsEmpty returns true if the heap has no elements
func (h *BinaryHeap) IsEmpty() bool {
	return len(h.entries) == 0
}

// TopN returns the top N messages without removing them
// Used for spine index candidate selection
func (h *BinaryHeap) TopN(n int) []*models.Message {
	if n <= 0 || h.IsEmpty() {
		return nil
	}

	// If n >= heap size, return all messages sorted
	if n >= h.Size() {
		messages := make([]*models.Message, len(h.entries))
		for i, entry := range h.entries {
			messages[i] = entry.Message
		}

		// Sort according to heap ordering
		sort.Slice(messages, func(i, j int) bool {
			return h.compareMessages(messages[i], messages[j])
		})

		return messages
	}

	// For smaller n, we need to extract top n without modifying the original heap
	// Create a temporary heap for this purpose
	tempHeap := &BinaryHeap{
		entries: make([]*HeapEntry, len(h.entries)),
		index:   NewMessageIndex(),
		mode:    h.mode,
	}

	// Copy all entries
	for i, entry := range h.entries {
		tempHeap.entries[i] = &HeapEntry{
			Message:       entry.Message,
			ComparisonKey: entry.ComparisonKey,
		}
		tempHeap.index.Set(entry.Message.ID, i)
	}

	// Extract top n messages
	result := make([]*models.Message, 0, n)
	for i := 0; i < n && !tempHeap.IsEmpty(); i++ {
		msg, _ := tempHeap.Extract()
		result = append(result, msg)
	}

	return result
}

// heapifyUp maintains heap property by moving element up
func (h *BinaryHeap) heapifyUp(index int) {
	for index > 0 {
		parentIndex := (index - 1) / 2

		if h.compare(index, parentIndex) {
			h.swap(index, parentIndex)
			index = parentIndex
		} else {
			break
		}
	}
}

// heapifyDown maintains heap property by moving element down
func (h *BinaryHeap) heapifyDown(index int) {
	for {
		leftChild := 2*index + 1
		rightChild := 2*index + 2
		target := index

		// Find the target element (min or max based on heap mode)
		if leftChild < len(h.entries) && h.compare(leftChild, target) {
			target = leftChild
		}

		if rightChild < len(h.entries) && h.compare(rightChild, target) {
			target = rightChild
		}

		if target != index {
			h.swap(index, target)
			index = target
		} else {
			break
		}
	}
}

// compare returns true if entries[i] should be prioritized over entries[j]
// based on the heap mode (min-heap or max-heap)
func (h *BinaryHeap) compare(i, j int) bool {
	return h.compareMessages(h.entries[i].Message, h.entries[j].Message)
}

// compareMessages compares two messages according to ordering rules:
// 1. Primary: Priority (min/max based on heap mode)
// 2. Secondary: EnqueuedAt timestamp (earlier first)
// 3. Tertiary: MessageID (lexicographic for determinism)
func (h *BinaryHeap) compareMessages(msg1, msg2 *models.Message) bool {
	// Primary comparison: Priority
	if msg1.Priority != msg2.Priority {
		switch h.mode {
		case models.MinHeap:
			return msg1.Priority < msg2.Priority // Lower priority values first
		case models.MaxHeap:
			return msg1.Priority > msg2.Priority // Higher priority values first
		}
	}

	// Secondary comparison: EnqueuedAt timestamp (earlier first)
	if !msg1.EnqueuedAt.Equal(msg2.EnqueuedAt) {
		return msg1.EnqueuedAt.Before(msg2.EnqueuedAt)
	}

	// Tertiary comparison: MessageID (lexicographic for determinism)
	return string(msg1.ID) < string(msg2.ID)
}

// swap exchanges two elements in the heap and updates the index
func (h *BinaryHeap) swap(i, j int) {
	// Swap entries
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]

	// Update index mappings
	h.index.Set(h.entries[i].Message.ID, i)
	h.index.Set(h.entries[j].Message.ID, j)
}

// Validate checks heap invariants (for testing/debugging)
func (h *BinaryHeap) Validate() error {
	// Check heap property
	for i := 0; i < len(h.entries); i++ {
		leftChild := 2*i + 1
		rightChild := 2*i + 2

		if leftChild < len(h.entries) {
			if h.compare(leftChild, i) {
				return fmt.Errorf("heap property violated at index %d (left child %d)", i, leftChild)
			}
		}

		if rightChild < len(h.entries) {
			if h.compare(rightChild, i) {
				return fmt.Errorf("heap property violated at index %d (right child %d)", i, rightChild)
			}
		}
	}

	// Check index consistency
	if h.index.Size() != len(h.entries) {
		return fmt.Errorf("index size %d doesn't match entries size %d", h.index.Size(), len(h.entries))
	}

	for i, entry := range h.entries {
		indexPos, exists := h.index.Get(entry.Message.ID)
		if !exists {
			return fmt.Errorf("message %s at position %d not found in index", entry.Message.ID, i)
		}
		if indexPos != i {
			return fmt.Errorf("message %s index position %d doesn't match actual position %d", entry.Message.ID, indexPos, i)
		}
	}

	return nil
}
