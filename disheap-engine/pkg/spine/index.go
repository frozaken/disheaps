package spine

import (
	"sort"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// SpineIndex defines the interface for hierarchical partition coordination
type SpineIndex interface {
	// UpdateCandidates updates the candidate set for a specific partition
	UpdateCandidates(partitionID uint32, candidates []*models.Message) error

	// SelectBest selects the best K messages across all partitions
	SelectBest(count int) ([]*models.Message, error)

	// GetBounds returns the current bounds (configured K vs actual available)
	GetBounds() (topK int, actualK int)

	// AddPartition adds a partition to the spine index
	AddPartition(partitionID uint32) error

	// RemovePartition removes a partition from the spine index
	RemovePartition(partitionID uint32) error

	// SetPartitionThrottled marks a partition as throttled/unthrottled
	SetPartitionThrottled(partitionID uint32, throttled bool, reason string) error

	// GetStats returns spine index statistics
	GetStats() SpineIndexStats

	// Close cleans up resources
	Close() error
}

// SpineIndexConfig holds configuration for the spine index
type SpineIndexConfig struct {
	TopK   int             `json:"top_k"`   // Target top-K bound
	FanOut int             `json:"fan_out"` // Candidates per partition (F)
	Mode   models.HeapMode `json:"mode"`    // MIN or MAX heap
	MaxAge time.Duration   `json:"max_age"` // Max age for candidate freshness
}

// HierarchicalSpineIndex implements the SpineIndex interface
type HierarchicalSpineIndex struct {
	mu     sync.RWMutex
	config SpineIndexConfig
	logger *zap.Logger

	// Partition candidate sets
	partitions map[uint32]*PartitionCandidateSet

	// Global coordinator heap (maintains best candidates across all partitions)
	coordinatorHeap []*Candidate

	// Statistics
	stats        SpineIndexStats
	lastSelectAt time.Time
	closed       bool
}

// NewSpineIndex creates a new hierarchical spine index
func NewSpineIndex(config SpineIndexConfig, logger *zap.Logger) (SpineIndex, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	spine := &HierarchicalSpineIndex{
		config:          config,
		logger:          logger,
		partitions:      make(map[uint32]*PartitionCandidateSet),
		coordinatorHeap: make([]*Candidate, 0),
		stats: SpineIndexStats{
			CreatedAt: time.Now(),
		},
	}

	logger.Info("Created spine index",
		zap.String("component", "spine_index"),
		zap.Int("top_k", config.TopK),
		zap.Int("fan_out", config.FanOut),
		zap.String("mode", config.Mode.String()))

	return spine, nil
}

// UpdateCandidates updates the candidate set for a specific partition
func (si *HierarchicalSpineIndex) UpdateCandidates(partitionID uint32, candidates []*models.Message) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.closed {
		return ErrSpineClosed
	}

	// Get or create partition candidate set
	pcs, exists := si.partitions[partitionID]
	if !exists {
		return ErrPartitionNotFound{PartitionID: partitionID}
	}

	// Update the partition's candidates
	pcs.UpdateCandidates(candidates)

	// Rebuild the coordinator heap with all partition candidates
	si.rebuildCoordinatorHeap()

	si.stats.TotalUpdates++
	si.stats.LastUpdateAt = time.Now()

	si.logger.Debug("Updated spine candidates",
		zap.String("component", "spine_index"),
		zap.Uint32("partition_id", partitionID),
		zap.Int("candidate_count", len(candidates)),
		zap.Int("coordinator_size", len(si.coordinatorHeap)))

	return nil
}

// SelectBest selects the best K messages across all partitions
func (si *HierarchicalSpineIndex) SelectBest(count int) ([]*models.Message, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if si.closed {
		return nil, ErrSpineClosed
	}

	if count <= 0 {
		return []*models.Message{}, nil
	}

	// Filter out throttled candidates
	available := si.getAvailableCandidates()
	if len(available) == 0 {
		return nil, ErrNoAvailableCandidates{Reason: "all partitions throttled or empty"}
	}

	// Select up to 'count' best candidates
	selectedCount := count
	if selectedCount > len(available) {
		selectedCount = len(available)
	}

	result := make([]*models.Message, selectedCount)
	for i := 0; i < selectedCount; i++ {
		result[i] = available[i].Message
	}

	si.lastSelectAt = time.Now()

	si.logger.Debug("Selected best candidates",
		zap.String("component", "spine_index"),
		zap.Int("requested", count),
		zap.Int("selected", selectedCount),
		zap.Int("available", len(available)))

	return result, nil
}

// GetBounds returns the current bounds
func (si *HierarchicalSpineIndex) GetBounds() (topK int, actualK int) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	available := si.getAvailableCandidates()
	return si.config.TopK, len(available)
}

// AddPartition adds a partition to the spine index
func (si *HierarchicalSpineIndex) AddPartition(partitionID uint32) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.closed {
		return ErrSpineClosed
	}

	if _, exists := si.partitions[partitionID]; exists {
		return nil // Already exists
	}

	pcs := NewPartitionCandidateSet(partitionID, si.config.FanOut, si.config.Mode, si.logger)
	si.partitions[partitionID] = pcs

	si.stats.PartitionCount++

	si.logger.Info("Added partition to spine index",
		zap.String("component", "spine_index"),
		zap.Uint32("partition_id", partitionID),
		zap.Int("total_partitions", len(si.partitions)))

	return nil
}

// RemovePartition removes a partition from the spine index
func (si *HierarchicalSpineIndex) RemovePartition(partitionID uint32) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.closed {
		return ErrSpineClosed
	}

	if _, exists := si.partitions[partitionID]; !exists {
		return ErrPartitionNotFound{PartitionID: partitionID}
	}

	delete(si.partitions, partitionID)
	si.rebuildCoordinatorHeap()

	si.stats.PartitionCount--

	si.logger.Info("Removed partition from spine index",
		zap.String("component", "spine_index"),
		zap.Uint32("partition_id", partitionID),
		zap.Int("remaining_partitions", len(si.partitions)))

	return nil
}

// SetPartitionThrottled marks a partition as throttled/unthrottled
func (si *HierarchicalSpineIndex) SetPartitionThrottled(partitionID uint32, throttled bool, reason string) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.closed {
		return ErrSpineClosed
	}

	pcs, exists := si.partitions[partitionID]
	if !exists {
		return ErrPartitionNotFound{PartitionID: partitionID}
	}

	pcs.SetThrottled(throttled, reason)

	return nil
}

// GetStats returns spine index statistics
func (si *HierarchicalSpineIndex) GetStats() SpineIndexStats {
	si.mu.RLock()
	defer si.mu.RUnlock()

	stats := si.stats
	stats.PartitionCount = len(si.partitions)
	stats.CoordinatorHeapSize = len(si.coordinatorHeap)
	stats.LastSelectAt = si.lastSelectAt

	// Collect partition stats
	stats.PartitionStats = make(map[uint32]PartitionCandidateStats)
	for id, pcs := range si.partitions {
		stats.PartitionStats[id] = pcs.GetStats()
	}

	// Calculate available vs throttled partitions
	stats.AvailablePartitions = 0
	stats.ThrottledPartitions = 0
	for _, pcs := range si.partitions {
		if pcs.IsThrottled() {
			stats.ThrottledPartitions++
		} else {
			stats.AvailablePartitions++
		}
	}

	return stats
}

// Close cleans up resources
func (si *HierarchicalSpineIndex) Close() error {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.closed {
		return nil
	}

	si.closed = true
	si.partitions = nil
	si.coordinatorHeap = nil

	si.logger.Info("Closed spine index",
		zap.String("component", "spine_index"))

	return nil
}

// rebuildCoordinatorHeap rebuilds the coordinator heap with all available candidates
func (si *HierarchicalSpineIndex) rebuildCoordinatorHeap() {
	si.coordinatorHeap = si.coordinatorHeap[:0]

	// Collect all candidates from all partitions
	for _, pcs := range si.partitions {
		candidates := pcs.GetCandidates()
		si.coordinatorHeap = append(si.coordinatorHeap, candidates...)
	}

	// Sort candidates by priority (best first)
	sort.Slice(si.coordinatorHeap, func(i, j int) bool {
		return CompareCandidates(si.coordinatorHeap[i], si.coordinatorHeap[j], si.config.Mode) < 0
	})

	// Limit to top-K bound if configured
	if si.config.TopK > 0 && len(si.coordinatorHeap) > si.config.TopK {
		si.coordinatorHeap = si.coordinatorHeap[:si.config.TopK]
	}
}

// getAvailableCandidates returns candidates from non-throttled partitions
func (si *HierarchicalSpineIndex) getAvailableCandidates() []*Candidate {
	available := make([]*Candidate, 0)

	for _, candidate := range si.coordinatorHeap {
		if pcs, exists := si.partitions[candidate.PartitionID]; exists {
			if !pcs.IsThrottled() {
				available = append(available, candidate)
			}
		}
	}

	return available
}

// validateConfig validates the spine index configuration
func validateConfig(config SpineIndexConfig) error {
	if config.TopK <= 0 {
		return ErrInvalidSpineConfig{Field: "TopK", Reason: "must be positive"}
	}

	if config.FanOut <= 0 {
		return ErrInvalidSpineConfig{Field: "FanOut", Reason: "must be positive"}
	}

	if config.FanOut > 10 {
		return ErrInvalidSpineConfig{Field: "FanOut", Reason: "fan-out too large (max 10)"}
	}

	if config.Mode != models.MinHeap && config.Mode != models.MaxHeap {
		return ErrInvalidSpineConfig{Field: "Mode", Reason: "must be MIN or MAX"}
	}

	if config.MaxAge <= 0 {
		config.MaxAge = 30 * time.Second // Default
	}

	return nil
}

// SpineIndexStats holds statistics for the spine index
type SpineIndexStats struct {
	CreatedAt           time.Time                          `json:"created_at"`
	PartitionCount      int                                `json:"partition_count"`
	AvailablePartitions int                                `json:"available_partitions"`
	ThrottledPartitions int                                `json:"throttled_partitions"`
	CoordinatorHeapSize int                                `json:"coordinator_heap_size"`
	TotalUpdates        uint64                             `json:"total_updates"`
	LastUpdateAt        time.Time                          `json:"last_update_at"`
	LastSelectAt        time.Time                          `json:"last_select_at"`
	PartitionStats      map[uint32]PartitionCandidateStats `json:"partition_stats"`
}
