package spine

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// SelectorStrategy defines how candidates are selected from the spine index
type SelectorStrategy int

const (
	// StrategyBestFirst selects candidates in strict priority order
	StrategyBestFirst SelectorStrategy = iota

	// StrategyRoundRobin rotates through partitions to ensure fairness
	StrategyRoundRobin

	// StrategyWeighted selects based on partition load and performance
	StrategyWeighted
)

// SelectionPolicy defines policies for candidate selection
type SelectionPolicy struct {
	Strategy         SelectorStrategy   `json:"strategy"`
	MaxStaleness     time.Duration      `json:"max_staleness"`
	MinFreshness     time.Duration      `json:"min_freshness"`
	FairnessWeight   float64            `json:"fairness_weight"`   // 0.0-1.0
	PartitionWeights map[uint32]float64 `json:"partition_weights"` // Custom partition weights
}

// DefaultSelectionPolicy returns a default selection policy
func DefaultSelectionPolicy() SelectionPolicy {
	return SelectionPolicy{
		Strategy:         StrategyBestFirst,
		MaxStaleness:     5 * time.Second,
		MinFreshness:     100 * time.Millisecond,
		FairnessWeight:   0.1,
		PartitionWeights: make(map[uint32]float64),
	}
}

// GlobalSelector implements sophisticated selection algorithms for the spine index
type GlobalSelector struct {
	mu     sync.RWMutex
	policy SelectionPolicy
	logger *zap.Logger

	// Selection state for round-robin and fairness
	lastPartitionIndex int
	partitionCounters  map[uint32]uint64 // Track selections per partition

	// Performance tracking
	selectionLatency    time.Duration
	totalSelections     uint64
	stalenessViolations uint64
}

// NewGlobalSelector creates a new global selector with the given policy
func NewGlobalSelector(policy SelectionPolicy, logger *zap.Logger) *GlobalSelector {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &GlobalSelector{
		policy:            policy,
		logger:            logger,
		partitionCounters: make(map[uint32]uint64),
	}
}

// SelectCandidates selects the best candidates based on the configured strategy
func (gs *GlobalSelector) SelectCandidates(ctx context.Context, candidates []*Candidate, count int) ([]*Candidate, error) {
	start := time.Now()
	defer func() {
		gs.mu.Lock()
		gs.selectionLatency = time.Since(start)
		gs.totalSelections++
		gs.mu.Unlock()
	}()

	if len(candidates) == 0 || count <= 0 {
		return []*Candidate{}, nil
	}

	// Filter candidates by freshness and staleness policies
	filtered := gs.filterByPolicy(candidates)

	// Apply selection strategy
	var selected []*Candidate
	switch gs.policy.Strategy {
	case StrategyBestFirst:
		selected = gs.selectBestFirst(filtered, count)
	case StrategyRoundRobin:
		selected = gs.selectRoundRobin(filtered, count)
	case StrategyWeighted:
		selected = gs.selectWeighted(filtered, count)
	default:
		selected = gs.selectBestFirst(filtered, count)
	}

	// Update selection counters
	gs.updateCounters(selected)

	gs.logger.Debug("Selected candidates",
		zap.String("component", "global_selector"),
		zap.String("strategy", gs.strategyString()),
		zap.Int("requested", count),
		zap.Int("available", len(candidates)),
		zap.Int("filtered", len(filtered)),
		zap.Int("selected", len(selected)),
		zap.Duration("latency", gs.selectionLatency))

	return selected, nil
}

// selectBestFirst selects candidates in strict priority order (default strategy)
func (gs *GlobalSelector) selectBestFirst(candidates []*Candidate, count int) []*Candidate {
	// Sort candidates by priority
	sorted := make([]*Candidate, len(candidates))
	copy(sorted, candidates)

	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		return CompareCandidates(a, b, models.MinHeap) < 0 // Assume MinHeap for now
	})

	// Select top 'count' candidates
	if count > len(sorted) {
		count = len(sorted)
	}

	return sorted[:count]
}

// selectRoundRobin rotates through partitions to ensure fairness
func (gs *GlobalSelector) selectRoundRobin(candidates []*Candidate, count int) []*Candidate {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if len(candidates) == 0 {
		return []*Candidate{}
	}

	// Group candidates by partition
	partitionCandidates := make(map[uint32][]*Candidate)
	for _, candidate := range candidates {
		partitionCandidates[candidate.PartitionID] = append(
			partitionCandidates[candidate.PartitionID], candidate)
	}

	// Sort candidates within each partition
	for partitionID := range partitionCandidates {
		sort.Slice(partitionCandidates[partitionID], func(i, j int) bool {
			a, b := partitionCandidates[partitionID][i], partitionCandidates[partitionID][j]
			return CompareCandidates(a, b, models.MinHeap) < 0
		})
	}

	// Round-robin selection
	selected := make([]*Candidate, 0, count)
	partitionIDs := make([]uint32, 0, len(partitionCandidates))
	for id := range partitionCandidates {
		partitionIDs = append(partitionIDs, id)
	}
	sort.Slice(partitionIDs, func(i, j int) bool {
		return partitionIDs[i] < partitionIDs[j]
	})

	// Start from the last used partition index
	partitionIndex := gs.lastPartitionIndex
	candidateIndex := 0

	for len(selected) < count {
		selectedFromRound := false

		// Try to select one candidate from each partition in this round
		for i := 0; i < len(partitionIDs) && len(selected) < count; i++ {
			currentPartitionIndex := (partitionIndex + i) % len(partitionIDs)
			partitionID := partitionIDs[currentPartitionIndex]

			if candidateIndex < len(partitionCandidates[partitionID]) {
				selected = append(selected, partitionCandidates[partitionID][candidateIndex])
				selectedFromRound = true
			}
		}

		if !selectedFromRound {
			break // No more candidates available
		}

		candidateIndex++
		partitionIndex = (partitionIndex + 1) % len(partitionIDs)
	}

	gs.lastPartitionIndex = partitionIndex
	return selected
}

// selectWeighted selects candidates based on partition weights and performance
func (gs *GlobalSelector) selectWeighted(candidates []*Candidate, count int) []*Candidate {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	// Group candidates by partition
	partitionCandidates := make(map[uint32][]*Candidate)
	for _, candidate := range candidates {
		partitionCandidates[candidate.PartitionID] = append(
			partitionCandidates[candidate.PartitionID], candidate)
	}

	// Calculate selection quotas based on weights
	totalWeight := 0.0
	for partitionID := range partitionCandidates {
		weight := gs.policy.PartitionWeights[partitionID]
		if weight == 0 {
			weight = 1.0 // Default weight
		}
		totalWeight += weight
	}

	// Allocate candidates proportionally
	selected := make([]*Candidate, 0, count)
	remaining := count

	for partitionID, candidates := range partitionCandidates {
		if remaining == 0 {
			break
		}

		// Sort partition candidates by priority
		sort.Slice(candidates, func(i, j int) bool {
			a, b := candidates[i], candidates[j]
			return CompareCandidates(a, b, models.MinHeap) < 0
		})

		// Calculate quota for this partition
		weight := gs.policy.PartitionWeights[partitionID]
		if weight == 0 {
			weight = 1.0
		}
		quota := int(float64(count) * (weight / totalWeight))
		if quota == 0 && len(candidates) > 0 {
			quota = 1 // Ensure at least one candidate per partition
		}

		// Select up to quota from this partition
		selectCount := quota
		if selectCount > len(candidates) {
			selectCount = len(candidates)
		}
		if selectCount > remaining {
			selectCount = remaining
		}

		selected = append(selected, candidates[:selectCount]...)
		remaining -= selectCount
	}

	// If we still have remaining quota, fill with best available
	if remaining > 0 {
		allRemaining := make([]*Candidate, 0)
		for _, partCands := range partitionCandidates {
			allRemaining = append(allRemaining, partCands...)
		}

		// Remove already selected candidates
		filtered := make([]*Candidate, 0)
		for _, candidate := range allRemaining {
			found := false
			for _, sel := range selected {
				if candidate.Message.ID == sel.Message.ID {
					found = true
					break
				}
			}
			if !found {
				filtered = append(filtered, candidate)
			}
		}

		// Sort and take remaining
		sort.Slice(filtered, func(i, j int) bool {
			a, b := filtered[i], filtered[j]
			return CompareCandidates(a, b, models.MinHeap) < 0
		})

		takeCount := remaining
		if takeCount > len(filtered) {
			takeCount = len(filtered)
		}
		selected = append(selected, filtered[:takeCount]...)
	}

	return selected
}

// filterByPolicy filters candidates based on staleness and freshness policies
func (gs *GlobalSelector) filterByPolicy(candidates []*Candidate) []*Candidate {
	now := time.Now()
	filtered := make([]*Candidate, 0, len(candidates))

	for _, candidate := range candidates {
		// Check staleness policy
		age := now.Sub(candidate.UpdatedAt)
		if gs.policy.MaxStaleness > 0 && age > gs.policy.MaxStaleness {
			gs.mu.Lock()
			gs.stalenessViolations++
			gs.mu.Unlock()

			gs.logger.Warn("Candidate exceeds staleness policy",
				zap.String("component", "global_selector"),
				zap.String("message_id", string(candidate.Message.ID)),
				zap.Uint32("partition_id", candidate.PartitionID),
				zap.Duration("age", age),
				zap.Duration("max_staleness", gs.policy.MaxStaleness))
			continue
		}

		// Check freshness policy (minimum age)
		if gs.policy.MinFreshness > 0 && age < gs.policy.MinFreshness {
			continue
		}

		filtered = append(filtered, candidate)
	}

	return filtered
}

// updateCounters updates selection counters for partition fairness tracking
func (gs *GlobalSelector) updateCounters(selected []*Candidate) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	for _, candidate := range selected {
		gs.partitionCounters[candidate.PartitionID]++
	}
}

// GetStats returns selector statistics
func (gs *GlobalSelector) GetStats() SelectorStats {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	return SelectorStats{
		Strategy:             gs.policy.Strategy,
		TotalSelections:      gs.totalSelections,
		LastSelectionLatency: gs.selectionLatency,
		StalenessViolations:  gs.stalenessViolations,
		PartitionCounters:    gs.copyPartitionCounters(),
	}
}

// copyPartitionCounters creates a copy of partition counters
func (gs *GlobalSelector) copyPartitionCounters() map[uint32]uint64 {
	counters := make(map[uint32]uint64)
	for id, count := range gs.partitionCounters {
		counters[id] = count
	}
	return counters
}

// strategyString returns a string representation of the selection strategy
func (gs *GlobalSelector) strategyString() string {
	switch gs.policy.Strategy {
	case StrategyBestFirst:
		return "best_first"
	case StrategyRoundRobin:
		return "round_robin"
	case StrategyWeighted:
		return "weighted"
	default:
		return "unknown"
	}
}

// SelectorStats holds statistics for the global selector
type SelectorStats struct {
	Strategy             SelectorStrategy  `json:"strategy"`
	TotalSelections      uint64            `json:"total_selections"`
	LastSelectionLatency time.Duration     `json:"last_selection_latency"`
	StalenessViolations  uint64            `json:"staleness_violations"`
	PartitionCounters    map[uint32]uint64 `json:"partition_counters"`
}
