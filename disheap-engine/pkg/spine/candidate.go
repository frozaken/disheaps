package spine

import (
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// Candidate represents a message candidate from a specific partition
type Candidate struct {
	Message     *models.Message `json:"message"`
	PartitionID uint32          `json:"partition_id"`
	Rank        int             `json:"rank"` // 0=root, 1=next best, etc.
	UpdatedAt   time.Time       `json:"updated_at"`
}

// PartitionCandidateSet manages the candidate messages from a single partition
type PartitionCandidateSet struct {
	mu          sync.RWMutex
	partitionID uint32
	fanOut      int             // Maximum number of candidates to maintain
	candidates  []*Candidate    // Ordered by priority (best first)
	mode        models.HeapMode // MIN or MAX heap
	logger      *zap.Logger

	// Statistics
	totalUpdates   uint64
	lastUpdateTime time.Time
	isThrottled    bool
	throttleReason string
}

// NewPartitionCandidateSet creates a new partition candidate set
func NewPartitionCandidateSet(partitionID uint32, fanOut int, mode models.HeapMode, logger *zap.Logger) *PartitionCandidateSet {
	if fanOut <= 0 {
		fanOut = 4 // Default fan-out
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	return &PartitionCandidateSet{
		partitionID:    partitionID,
		fanOut:         fanOut,
		candidates:     make([]*Candidate, 0, fanOut),
		mode:           mode,
		logger:         logger,
		lastUpdateTime: time.Now(),
	}
}

// UpdateCandidates updates the candidate set with new messages from the partition
func (pcs *PartitionCandidateSet) UpdateCandidates(messages []*models.Message) {
	pcs.mu.Lock()
	defer pcs.mu.Unlock()

	// Clear existing candidates
	pcs.candidates = pcs.candidates[:0]

	// Add new candidates up to fanOut limit
	now := time.Now()
	for i, msg := range messages {
		if i >= pcs.fanOut {
			break
		}

		candidate := &Candidate{
			Message:     msg,
			PartitionID: pcs.partitionID,
			Rank:        i,
			UpdatedAt:   now,
		}
		pcs.candidates = append(pcs.candidates, candidate)
	}

	pcs.totalUpdates++
	pcs.lastUpdateTime = now

	pcs.logger.Debug("Updated partition candidates",
		zap.String("component", "partition_candidate_set"),
		zap.Uint32("partition_id", pcs.partitionID),
		zap.Int("candidates_count", len(pcs.candidates)),
		zap.Int("input_messages", len(messages)))
}

// GetCandidates returns the current candidate set (thread-safe copy)
func (pcs *PartitionCandidateSet) GetCandidates() []*Candidate {
	pcs.mu.RLock()
	defer pcs.mu.RUnlock()

	// Return a copy to prevent external modifications
	candidates := make([]*Candidate, len(pcs.candidates))
	copy(candidates, pcs.candidates)
	return candidates
}

// GetBestCandidate returns the best (root) candidate from this partition
func (pcs *PartitionCandidateSet) GetBestCandidate() *Candidate {
	pcs.mu.RLock()
	defer pcs.mu.RUnlock()

	if len(pcs.candidates) == 0 {
		return nil
	}

	return pcs.candidates[0] // Root candidate
}

// RemoveBestCandidate removes and returns the best candidate, promoting the next one
func (pcs *PartitionCandidateSet) RemoveBestCandidate() *Candidate {
	pcs.mu.Lock()
	defer pcs.mu.Unlock()

	if len(pcs.candidates) == 0 {
		return nil
	}

	// Remove the first (best) candidate
	best := pcs.candidates[0]
	pcs.candidates = pcs.candidates[1:]

	// Update ranks for remaining candidates
	for i, candidate := range pcs.candidates {
		candidate.Rank = i
	}

	pcs.logger.Debug("Removed best candidate",
		zap.String("component", "partition_candidate_set"),
		zap.Uint32("partition_id", pcs.partitionID),
		zap.String("message_id", string(best.Message.ID)),
		zap.Int("remaining_candidates", len(pcs.candidates)))

	return best
}

// IsEmpty returns true if there are no candidates
func (pcs *PartitionCandidateSet) IsEmpty() bool {
	pcs.mu.RLock()
	defer pcs.mu.RUnlock()

	return len(pcs.candidates) == 0
}

// SetThrottled marks this partition as throttled (e.g., during rebalancing)
func (pcs *PartitionCandidateSet) SetThrottled(throttled bool, reason string) {
	pcs.mu.Lock()
	defer pcs.mu.Unlock()

	pcs.isThrottled = throttled
	pcs.throttleReason = reason

	if throttled {
		pcs.logger.Warn("Partition throttled",
			zap.String("component", "partition_candidate_set"),
			zap.Uint32("partition_id", pcs.partitionID),
			zap.String("reason", reason))
	} else {
		pcs.logger.Info("Partition unthrottled",
			zap.String("component", "partition_candidate_set"),
			zap.Uint32("partition_id", pcs.partitionID))
	}
}

// IsThrottled returns true if this partition is currently throttled
func (pcs *PartitionCandidateSet) IsThrottled() bool {
	pcs.mu.RLock()
	defer pcs.mu.RUnlock()

	return pcs.isThrottled
}

// GetStats returns statistics about this partition candidate set
func (pcs *PartitionCandidateSet) GetStats() PartitionCandidateStats {
	pcs.mu.RLock()
	defer pcs.mu.RUnlock()

	return PartitionCandidateStats{
		PartitionID:    pcs.partitionID,
		CandidateCount: len(pcs.candidates),
		FanOut:         pcs.fanOut,
		TotalUpdates:   pcs.totalUpdates,
		LastUpdateTime: pcs.lastUpdateTime,
		IsThrottled:    pcs.isThrottled,
		ThrottleReason: pcs.throttleReason,
	}
}

// PartitionCandidateStats holds statistics for a partition candidate set
type PartitionCandidateStats struct {
	PartitionID    uint32    `json:"partition_id"`
	CandidateCount int       `json:"candidate_count"`
	FanOut         int       `json:"fan_out"`
	TotalUpdates   uint64    `json:"total_updates"`
	LastUpdateTime time.Time `json:"last_update_time"`
	IsThrottled    bool      `json:"is_throttled"`
	ThrottleReason string    `json:"throttle_reason,omitempty"`
}

// CompareCandidates compares two candidates based on message priority and heap mode
func CompareCandidates(a, b *Candidate, mode models.HeapMode) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}

	// Compare using message priority, enqueued time, and ID for determinism
	msgA, msgB := a.Message, b.Message

	// Primary comparison: Priority
	var priorityResult int
	if mode == models.MinHeap {
		// For min heap, lower priority values come first
		if msgA.Priority < msgB.Priority {
			priorityResult = -1
		} else if msgA.Priority > msgB.Priority {
			priorityResult = 1
		} else {
			priorityResult = 0
		}
	} else { // MaxHeap
		// For max heap, higher priority values come first
		if msgA.Priority > msgB.Priority {
			priorityResult = -1
		} else if msgA.Priority < msgB.Priority {
			priorityResult = 1
		} else {
			priorityResult = 0
		}
	}

	if priorityResult != 0 {
		return priorityResult
	}

	// Secondary comparison: EnqueuedAt (earlier messages first)
	if msgA.EnqueuedAt.Before(msgB.EnqueuedAt) {
		return -1
	} else if msgA.EnqueuedAt.After(msgB.EnqueuedAt) {
		return 1
	}

	// Tertiary comparison: MessageID (lexicographic for determinism)
	if msgA.ID < msgB.ID {
		return -1
	} else if msgA.ID > msgB.ID {
		return 1
	}

	return 0
}

// ValidateCandidate performs basic validation on a candidate
func ValidateCandidate(candidate *Candidate) error {
	if candidate == nil {
		return ErrInvalidCandidate{Reason: "candidate is nil"}
	}

	if candidate.Message == nil {
		return ErrInvalidCandidate{Reason: "candidate message is nil"}
	}

	if candidate.Rank < 0 {
		return ErrInvalidCandidate{Reason: "candidate rank cannot be negative"}
	}

	return nil
}
