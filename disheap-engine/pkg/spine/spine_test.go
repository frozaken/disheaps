package spine

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

// Helper function to create test messages
func createTestMessage(id string, priority int64, topic string) *models.Message {
	return &models.Message{
		ID:         models.MessageID(id),
		Topic:      topic,
		Priority:   priority,
		EnqueuedAt: time.Now(),
		Payload:    []byte("test payload"),
		Attempts:   0,
		MaxRetries: 3,
	}
}

// Helper function to create multiple test messages with sequential priorities
func createTestMessages(count int, topic string, startPriority int64) []*models.Message {
	messages := make([]*models.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = createTestMessage(
			fmt.Sprintf("msg-%d", i),
			startPriority+int64(i),
			topic,
		)
	}
	return messages
}

func TestPartitionCandidateSet(t *testing.T) {
	logger := zap.NewNop()

	t.Run("create_and_update", func(t *testing.T) {
		pcs := NewPartitionCandidateSet(1, 3, models.MinHeap, logger)
		assert.NotNil(t, pcs)
		assert.Equal(t, uint32(1), pcs.partitionID)
		assert.Equal(t, 3, pcs.fanOut)
		assert.True(t, pcs.IsEmpty())

		// Update with test messages
		messages := createTestMessages(5, "test", 100)
		pcs.UpdateCandidates(messages)

		assert.False(t, pcs.IsEmpty())
		candidates := pcs.GetCandidates()
		assert.Len(t, candidates, 3) // Limited by fanOut

		// Check that candidates are properly ranked
		for i, candidate := range candidates {
			assert.Equal(t, i, candidate.Rank)
			assert.Equal(t, uint32(1), candidate.PartitionID)
		}
	})

	t.Run("get_and_remove_best", func(t *testing.T) {
		pcs := NewPartitionCandidateSet(1, 4, models.MinHeap, logger)
		messages := createTestMessages(3, "test", 200)
		pcs.UpdateCandidates(messages)

		// Get best candidate
		best := pcs.GetBestCandidate()
		assert.NotNil(t, best)
		assert.Equal(t, int64(200), best.Message.Priority) // First message
		assert.Equal(t, 0, best.Rank)

		// Remove best candidate
		removed := pcs.RemoveBestCandidate()
		assert.Equal(t, best.Message.ID, removed.Message.ID)

		// Check that the next candidate became the best
		newBest := pcs.GetBestCandidate()
		assert.NotNil(t, newBest)
		assert.Equal(t, int64(201), newBest.Message.Priority) // Second message
		assert.Equal(t, 0, newBest.Rank)                      // Rank should be updated
	})

	t.Run("throttling", func(t *testing.T) {
		pcs := NewPartitionCandidateSet(1, 2, models.MinHeap, logger)

		assert.False(t, pcs.IsThrottled())

		pcs.SetThrottled(true, "test throttle")
		assert.True(t, pcs.IsThrottled())

		pcs.SetThrottled(false, "")
		assert.False(t, pcs.IsThrottled())
	})

	t.Run("stats", func(t *testing.T) {
		pcs := NewPartitionCandidateSet(2, 3, models.MinHeap, logger)
		messages := createTestMessages(2, "test", 300)
		pcs.UpdateCandidates(messages)

		stats := pcs.GetStats()
		assert.Equal(t, uint32(2), stats.PartitionID)
		assert.Equal(t, 2, stats.CandidateCount)
		assert.Equal(t, 3, stats.FanOut)
		assert.Equal(t, uint64(1), stats.TotalUpdates)
		assert.False(t, stats.IsThrottled)
	})
}

func TestHierarchicalSpineIndex(t *testing.T) {
	logger := zap.NewNop()

	t.Run("create_and_configure", func(t *testing.T) {
		config := SpineIndexConfig{
			TopK:   10,
			FanOut: 3,
			Mode:   models.MinHeap,
			MaxAge: 30 * time.Second,
		}

		spine, err := NewSpineIndex(config, logger)
		require.NoError(t, err)
		require.NotNil(t, spine)

		topK, actualK := spine.GetBounds()
		assert.Equal(t, 10, topK)
		assert.Equal(t, 0, actualK) // No partitions yet

		stats := spine.GetStats()
		assert.Equal(t, 0, stats.PartitionCount)
	})

	t.Run("add_remove_partitions", func(t *testing.T) {
		config := SpineIndexConfig{
			TopK:   10,
			FanOut: 2,
			Mode:   models.MinHeap,
		}

		spine, err := NewSpineIndex(config, logger)
		require.NoError(t, err)

		// Add partitions
		err = spine.AddPartition(1)
		assert.NoError(t, err)
		err = spine.AddPartition(2)
		assert.NoError(t, err)
		err = spine.AddPartition(1) // Duplicate should be fine
		assert.NoError(t, err)

		stats := spine.GetStats()
		assert.Equal(t, 2, stats.PartitionCount)

		// Remove partition
		err = spine.RemovePartition(1)
		assert.NoError(t, err)

		stats = spine.GetStats()
		assert.Equal(t, 1, stats.PartitionCount)

		// Remove non-existent partition should error
		err = spine.RemovePartition(99)
		assert.Error(t, err)
		assert.IsType(t, ErrPartitionNotFound{}, err)
	})

	t.Run("update_candidates_and_select", func(t *testing.T) {
		config := SpineIndexConfig{
			TopK:   8,
			FanOut: 3,
			Mode:   models.MinHeap,
		}

		spine, err := NewSpineIndex(config, logger)
		require.NoError(t, err)

		// Add partitions
		spine.AddPartition(1)
		spine.AddPartition(2)

		// Update candidates for partition 1 (priorities 100-104)
		messages1 := createTestMessages(5, "test", 100)
		err = spine.UpdateCandidates(1, messages1)
		assert.NoError(t, err)

		// Update candidates for partition 2 (priorities 200-204)
		messages2 := createTestMessages(5, "test", 200)
		err = spine.UpdateCandidates(2, messages2)
		assert.NoError(t, err)

		// Select best candidates
		selected, err := spine.SelectBest(4)
		assert.NoError(t, err)
		assert.Len(t, selected, 4)

		// Verify selection order (should be lowest priorities first for MinHeap)
		assert.Equal(t, int64(100), selected[0].Priority)
		assert.Equal(t, int64(101), selected[1].Priority)
		assert.Equal(t, int64(102), selected[2].Priority)
		assert.Equal(t, int64(200), selected[3].Priority) // Best from partition 2

		// Check bounds
		topK, actualK := spine.GetBounds()
		assert.Equal(t, 8, topK)
		assert.Equal(t, 6, actualK) // 3 from each partition (fanOut=3)
	})

	t.Run("throttled_partitions", func(t *testing.T) {
		config := SpineIndexConfig{
			TopK:   10,
			FanOut: 2,
			Mode:   models.MinHeap,
		}

		spine, err := NewSpineIndex(config, logger)
		require.NoError(t, err)

		spine.AddPartition(1)
		spine.AddPartition(2)

		// Add candidates
		spine.UpdateCandidates(1, createTestMessages(3, "test", 100))
		spine.UpdateCandidates(2, createTestMessages(3, "test", 200))

		// Select should return candidates from both partitions
		selected, err := spine.SelectBest(4)
		assert.NoError(t, err)
		assert.Len(t, selected, 4)

		// Throttle partition 1
		err = spine.SetPartitionThrottled(1, true, "maintenance")
		assert.NoError(t, err)

		// Now selection should only return candidates from partition 2
		selected, err = spine.SelectBest(4)
		assert.NoError(t, err)
		assert.Len(t, selected, 2) // Only from partition 2

		for _, msg := range selected {
			assert.True(t, msg.Priority >= 200) // All from partition 2
		}

		// Unthrottle partition 1
		err = spine.SetPartitionThrottled(1, false, "")
		assert.NoError(t, err)

		// Should now include candidates from both partitions again
		selected, err = spine.SelectBest(4)
		assert.NoError(t, err)
		assert.Len(t, selected, 4)
	})

	t.Run("bounded_staleness_guarantee", func(t *testing.T) {
		// Test the key bounded staleness property
		config := SpineIndexConfig{
			TopK:   6, // K = 6
			FanOut: 2, // F = 2 candidates per partition
			Mode:   models.MinHeap,
		}

		spine, err := NewSpineIndex(config, logger)
		require.NoError(t, err)

		// Add 3 partitions (P = 3)
		spine.AddPartition(1)
		spine.AddPartition(2)
		spine.AddPartition(3)

		// Each partition contributes F=2 candidates → total P*F=6 candidates
		// This should guarantee that SelectBest(K) returns a message within rank K

		// Partition 1: priorities [10, 20] (candidates), [30, 40, 50] (not candidates)
		messages1 := []*models.Message{
			createTestMessage("p1-best", 10, "test"),
			createTestMessage("p1-second", 20, "test"),
			createTestMessage("p1-third", 30, "test"),
			createTestMessage("p1-fourth", 40, "test"),
		}

		// Partition 2: priorities [15, 25] (candidates)
		messages2 := []*models.Message{
			createTestMessage("p2-best", 15, "test"),
			createTestMessage("p2-second", 25, "test"),
			createTestMessage("p2-third", 35, "test"),
		}

		// Partition 3: priorities [5, 35] (candidates)
		messages3 := []*models.Message{
			createTestMessage("p3-best", 5, "test"), // Global best
			createTestMessage("p3-second", 35, "test"),
		}

		spine.UpdateCandidates(1, messages1)
		spine.UpdateCandidates(2, messages2)
		spine.UpdateCandidates(3, messages3)

		// The spine index should contain exactly 6 candidates:
		// [5, 10, 15, 20, 25, 35] from the candidate sets

		// Select top K=6 candidates
		selected, err := spine.SelectBest(6)
		assert.NoError(t, err)
		assert.Len(t, selected, 6)

		// Verify bounded staleness: all selected messages should be within
		// the top-K globally across all available candidates
		expectedPriorities := []int64{5, 10, 15, 20, 25, 35}
		for i, msg := range selected {
			assert.Equal(t, expectedPriorities[i], msg.Priority,
				"Message %d should have priority %d but has %d", i, expectedPriorities[i], msg.Priority)
		}

		// Key property: The selected messages form the exact top-K across
		// all candidates exposed to the spine index (bounded staleness)
	})

	t.Run("close", func(t *testing.T) {
		config := SpineIndexConfig{
			TopK:   5,
			FanOut: 2,
			Mode:   models.MinHeap,
		}

		spine, err := NewSpineIndex(config, logger)
		require.NoError(t, err)

		spine.AddPartition(1)
		spine.UpdateCandidates(1, createTestMessages(2, "test", 100))

		// Close the index
		err = spine.Close()
		assert.NoError(t, err)

		// Operations after close should fail
		err = spine.AddPartition(2)
		assert.Error(t, err)
		assert.Equal(t, ErrSpineClosed, err)

		_, err = spine.SelectBest(1)
		assert.Error(t, err)
		assert.Equal(t, ErrSpineClosed, err)
	})
}

func TestGlobalSelector(t *testing.T) {
	logger := zap.NewNop()

	// Helper to create test candidates
	createCandidates := func(partitionID uint32, priorities []int64) []*Candidate {
		candidates := make([]*Candidate, len(priorities))
		for i, priority := range priorities {
			candidates[i] = &Candidate{
				Message:     createTestMessage(fmt.Sprintf("p%d-m%d", partitionID, i), priority, "test"),
				PartitionID: partitionID,
				Rank:        i,
				UpdatedAt:   time.Now(),
			}
		}
		return candidates
	}

	t.Run("best_first_strategy", func(t *testing.T) {
		policy := DefaultSelectionPolicy()
		policy.Strategy = StrategyBestFirst
		policy.MinFreshness = 0 // Disable freshness filter for testing
		policy.MaxStaleness = 0 // Disable staleness filter for testing

		selector := NewGlobalSelector(policy, logger)

		// Create candidates from multiple partitions
		candidates := append(
			createCandidates(1, []int64{10, 30}),
			createCandidates(2, []int64{5, 25})...,
		)
		candidates = append(candidates, createCandidates(3, []int64{15, 35})...)

		selected, err := selector.SelectCandidates(context.Background(), candidates, 4)
		assert.NoError(t, err)
		assert.Len(t, selected, 4)

		// Should select in priority order: 5, 10, 15, 25
		expectedPriorities := []int64{5, 10, 15, 25}
		for i, candidate := range selected {
			assert.Equal(t, expectedPriorities[i], candidate.Message.Priority)
		}
	})

	t.Run("round_robin_strategy", func(t *testing.T) {
		policy := DefaultSelectionPolicy()
		policy.Strategy = StrategyRoundRobin
		policy.MinFreshness = 0 // Disable freshness filter for testing
		policy.MaxStaleness = 0 // Disable staleness filter for testing

		selector := NewGlobalSelector(policy, logger)

		candidates := append(
			createCandidates(1, []int64{10, 20}),
			createCandidates(2, []int64{11, 21})...,
		)
		candidates = append(candidates, createCandidates(3, []int64{12, 22})...)

		selected, err := selector.SelectCandidates(context.Background(), candidates, 6)
		assert.NoError(t, err)
		assert.Len(t, selected, 6)

		// Should alternate between partitions
		partitions := make([]uint32, len(selected))
		for i, candidate := range selected {
			partitions[i] = candidate.PartitionID
		}

		// Round robin should touch each partition
		partitionCounts := make(map[uint32]int)
		for _, p := range partitions {
			partitionCounts[p]++
		}
		assert.Equal(t, 3, len(partitionCounts)) // All partitions represented
		assert.Equal(t, 2, partitionCounts[1])   // Each partition appears twice
		assert.Equal(t, 2, partitionCounts[2])
		assert.Equal(t, 2, partitionCounts[3])
	})

	t.Run("staleness_filtering", func(t *testing.T) {
		policy := DefaultSelectionPolicy()
		policy.MaxStaleness = 1 * time.Second
		policy.MinFreshness = 0 // Allow fresh candidates through

		selector := NewGlobalSelector(policy, logger)

		// Create candidates with different ages
		freshCandidates := createCandidates(1, []int64{10})
		staleCandidates := createCandidates(2, []int64{5}) // Better priority but will be stale

		// Make stale candidates old
		staleCandidates[0].UpdatedAt = time.Now().Add(-2 * time.Second)

		all := append(freshCandidates, staleCandidates...)

		selected, err := selector.SelectCandidates(context.Background(), all, 2)
		assert.NoError(t, err)
		assert.Len(t, selected, 1) // Only fresh candidate selected
		if len(selected) > 0 {
			assert.Equal(t, int64(10), selected[0].Message.Priority)
		}

		stats := selector.GetStats()
		assert.Equal(t, uint64(1), stats.StalenessViolations)
	})

	t.Run("weighted_strategy", func(t *testing.T) {
		policy := DefaultSelectionPolicy()
		policy.Strategy = StrategyWeighted
		policy.MinFreshness = 0 // Disable freshness filter for testing
		policy.MaxStaleness = 0 // Disable staleness filter for testing
		policy.PartitionWeights = map[uint32]float64{
			1: 3.0, // Partition 1 gets 3x weight
			2: 1.0, // Partition 2 gets 1x weight
		}

		selector := NewGlobalSelector(policy, logger)

		candidates := append(
			createCandidates(1, []int64{10, 20, 30, 40}),
			createCandidates(2, []int64{11, 21})...,
		)

		selected, err := selector.SelectCandidates(context.Background(), candidates, 4)
		assert.NoError(t, err)
		assert.Len(t, selected, 4)

		// Count selections per partition
		partitionCounts := make(map[uint32]int)
		for _, candidate := range selected {
			partitionCounts[candidate.PartitionID]++
		}

		// Partition 1 should get more selections due to higher weight
		assert.True(t, partitionCounts[1] >= partitionCounts[2])
	})
}

func TestValidation(t *testing.T) {
	t.Run("invalid_spine_config", func(t *testing.T) {
		// Invalid TopK
		config := SpineIndexConfig{TopK: 0, FanOut: 2, Mode: models.MinHeap}
		_, err := NewSpineIndex(config, nil)
		assert.Error(t, err)
		assert.IsType(t, ErrInvalidSpineConfig{}, err)

		// Invalid FanOut
		config = SpineIndexConfig{TopK: 5, FanOut: 0, Mode: models.MinHeap}
		_, err = NewSpineIndex(config, nil)
		assert.Error(t, err)
		assert.IsType(t, ErrInvalidSpineConfig{}, err)

		// Invalid Mode
		config = SpineIndexConfig{TopK: 5, FanOut: 2, Mode: models.HeapMode(99)} // Invalid mode value
		_, err = NewSpineIndex(config, nil)
		assert.Error(t, err)
		assert.IsType(t, ErrInvalidSpineConfig{}, err)
	})

	t.Run("candidate_validation", func(t *testing.T) {
		// Nil candidate
		err := ValidateCandidate(nil)
		assert.Error(t, err)
		assert.IsType(t, ErrInvalidCandidate{}, err)

		// Candidate with nil message
		candidate := &Candidate{Message: nil}
		err = ValidateCandidate(candidate)
		assert.Error(t, err)

		// Candidate with negative rank
		candidate = &Candidate{
			Message: createTestMessage("test", 10, "test"),
			Rank:    -1,
		}
		err = ValidateCandidate(candidate)
		assert.Error(t, err)

		// Valid candidate
		candidate = &Candidate{
			Message: createTestMessage("test", 10, "test"),
			Rank:    0,
		}
		err = ValidateCandidate(candidate)
		assert.NoError(t, err)
	})
}

// Benchmark tests
func BenchmarkSpineIndexSelect(b *testing.B) {
	config := SpineIndexConfig{
		TopK:   100,
		FanOut: 4,
		Mode:   models.MinHeap,
	}

	spine, _ := NewSpineIndex(config, zap.NewNop())

	// Add 10 partitions with candidates
	for p := uint32(1); p <= 10; p++ {
		spine.AddPartition(p)
		messages := createTestMessages(20, "test", int64(p*100))
		spine.UpdateCandidates(p, messages)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := spine.SelectBest(50)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGlobalSelectorBestFirst(b *testing.B) {
	policy := DefaultSelectionPolicy()
	policy.Strategy = StrategyBestFirst
	selector := NewGlobalSelector(policy, zap.NewNop())

	// Helper to create test candidates
	createCandidates := func(partitionID uint32, priorities []int64) []*Candidate {
		candidates := make([]*Candidate, len(priorities))
		for i, priority := range priorities {
			candidates[i] = &Candidate{
				Message:     createTestMessage(fmt.Sprintf("p%d-m%d", partitionID, i), priority, "test"),
				PartitionID: partitionID,
				Rank:        i,
				UpdatedAt:   time.Now(),
			}
		}
		return candidates
	}

	// Create 100 candidates across 10 partitions
	var candidates []*Candidate
	for p := uint32(1); p <= 10; p++ {
		candidates = append(candidates, createCandidates(p, []int64{
			int64(p * 10), int64(p*10 + 1), int64(p*10 + 2), int64(p*10 + 3),
		})...)
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := selector.SelectCandidates(ctx, candidates, 20)
		if err != nil {
			b.Fatal(err)
		}
	}
}
