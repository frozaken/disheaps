package idempotent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// ProducerState represents the state of a producer
type ProducerState struct {
	ID             string            `json:"id"`
	Epoch          uint64            `json:"epoch"`
	LastSequence   map[string]uint64 `json:"last_sequence"` // topic -> last_sequence
	FirstSeen      time.Time         `json:"first_seen"`
	LastActivity   time.Time         `json:"last_activity"`
	MessageCount   uint64            `json:"message_count"`
	DuplicateCount uint64            `json:"duplicate_count"`
	Topics         map[string]bool   `json:"topics"` // Set of topics this producer has used
}

// ProducerStateManager manages producer states and epochs
type ProducerStateManager struct {
	mu        sync.RWMutex
	producers map[string]*ProducerState // producer_id -> state
	logger    *zap.Logger

	// Configuration
	maxInactiveTime time.Duration // How long to keep inactive producers
	cleanupInterval time.Duration // How often to clean up inactive producers

	// Background cleanup
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewProducerStateManager creates a new producer state manager
func NewProducerStateManager(logger *zap.Logger) *ProducerStateManager {
	return &ProducerStateManager{
		producers:       make(map[string]*ProducerState),
		logger:          logger.Named("producer-state"),
		maxInactiveTime: 7 * 24 * time.Hour, // 7 days
		cleanupInterval: 1 * time.Hour,      // 1 hour
		stopCh:          make(chan struct{}),
	}
}

// Start begins background cleanup operations
func (psm *ProducerStateManager) Start(ctx context.Context) error {
	psm.logger.Info("starting producer state manager",
		zap.Duration("max_inactive_time", psm.maxInactiveTime),
		zap.Duration("cleanup_interval", psm.cleanupInterval))

	// Start background cleanup goroutine
	psm.wg.Add(1)
	go psm.cleanupWorker()

	return nil
}

// Stop stops the producer state manager
func (psm *ProducerStateManager) Stop(ctx context.Context) error {
	psm.stopOnce.Do(func() {
		psm.logger.Info("stopping producer state manager")
		close(psm.stopCh)
	})

	psm.wg.Wait()
	psm.logger.Info("producer state manager stopped")
	return nil
}

// RegisterActivity registers producer activity and updates state
func (psm *ProducerStateManager) RegisterActivity(producerID string, epoch uint64, topic string, sequence uint64, isDuplicate bool) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	now := time.Now()

	state, exists := psm.producers[producerID]
	if !exists {
		// New producer
		state = &ProducerState{
			ID:           producerID,
			Epoch:        epoch,
			LastSequence: make(map[string]uint64),
			FirstSeen:    now,
			LastActivity: now,
			Topics:       make(map[string]bool),
		}
		psm.producers[producerID] = state

		psm.logger.Info("registered new producer",
			zap.String("producer_id", producerID),
			zap.Uint64("epoch", epoch),
			zap.String("topic", topic))
	}

	// Update state
	state.LastActivity = now
	state.Topics[topic] = true

	// Check for epoch change
	if epoch != state.Epoch {
		psm.logger.Info("producer epoch changed",
			zap.String("producer_id", producerID),
			zap.Uint64("old_epoch", state.Epoch),
			zap.Uint64("new_epoch", epoch))

		// Reset sequence tracking for new epoch
		state.Epoch = epoch
		state.LastSequence = make(map[string]uint64)
	}

	// Update sequence tracking
	if lastSeq, exists := state.LastSequence[topic]; !exists || sequence > lastSeq {
		state.LastSequence[topic] = sequence
	}

	// Update counters
	state.MessageCount++
	if isDuplicate {
		state.DuplicateCount++
	}
}

// GetProducerState returns the state for a producer
func (psm *ProducerStateManager) GetProducerState(producerID string) (*ProducerState, bool) {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	state, exists := psm.producers[producerID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid concurrent access issues
	stateCopy := &ProducerState{
		ID:             state.ID,
		Epoch:          state.Epoch,
		LastSequence:   make(map[string]uint64),
		FirstSeen:      state.FirstSeen,
		LastActivity:   state.LastActivity,
		MessageCount:   state.MessageCount,
		DuplicateCount: state.DuplicateCount,
		Topics:         make(map[string]bool),
	}

	for topic, seq := range state.LastSequence {
		stateCopy.LastSequence[topic] = seq
	}

	for topic := range state.Topics {
		stateCopy.Topics[topic] = true
	}

	return stateCopy, true
}

// ListProducers returns a list of all active producer states
func (psm *ProducerStateManager) ListProducers() []*ProducerState {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	producers := make([]*ProducerState, 0, len(psm.producers))
	for _, state := range psm.producers {
		// Create a copy
		stateCopy := &ProducerState{
			ID:             state.ID,
			Epoch:          state.Epoch,
			LastSequence:   make(map[string]uint64),
			FirstSeen:      state.FirstSeen,
			LastActivity:   state.LastActivity,
			MessageCount:   state.MessageCount,
			DuplicateCount: state.DuplicateCount,
			Topics:         make(map[string]bool),
		}

		for topic, seq := range state.LastSequence {
			stateCopy.LastSequence[topic] = seq
		}

		for topic := range state.Topics {
			stateCopy.Topics[topic] = true
		}

		producers = append(producers, stateCopy)
	}

	return producers
}

// GetActiveProducers returns the number of active producers
func (psm *ProducerStateManager) GetActiveProducers() int {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	return len(psm.producers)
}

// GetProducersForTopic returns producers that have sent messages to a specific topic
func (psm *ProducerStateManager) GetProducersForTopic(topic string) []*ProducerState {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	var producers []*ProducerState
	for _, state := range psm.producers {
		if state.Topics[topic] {
			// Create a copy
			stateCopy := &ProducerState{
				ID:             state.ID,
				Epoch:          state.Epoch,
				LastSequence:   make(map[string]uint64),
				FirstSeen:      state.FirstSeen,
				LastActivity:   state.LastActivity,
				MessageCount:   state.MessageCount,
				DuplicateCount: state.DuplicateCount,
				Topics:         make(map[string]bool),
			}

			for t, seq := range state.LastSequence {
				stateCopy.LastSequence[t] = seq
			}

			for t := range state.Topics {
				stateCopy.Topics[t] = true
			}

			producers = append(producers, stateCopy)
		}
	}

	return producers
}

// IsSequenceInOrder checks if a sequence number is in order for a producer
func (psm *ProducerStateManager) IsSequenceInOrder(producerID string, epoch uint64, topic string, sequence uint64) bool {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	state, exists := psm.producers[producerID]
	if !exists {
		// New producer, any sequence is acceptable
		return true
	}

	// Check epoch
	if epoch != state.Epoch {
		// Different epoch, reset sequence tracking
		return true
	}

	lastSeq, exists := state.LastSequence[topic]
	if !exists {
		// No previous sequence for this topic
		return true
	}

	// Sequence should be greater than last seen sequence
	return sequence > lastSeq
}

// GetExpectedNextSequence returns the expected next sequence number for a producer on a topic
func (psm *ProducerStateManager) GetExpectedNextSequence(producerID string, epoch uint64, topic string) uint64 {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	state, exists := psm.producers[producerID]
	if !exists {
		return 1 // Start from 1 for new producers
	}

	// Check epoch
	if epoch != state.Epoch {
		return 1 // New epoch, start from 1
	}

	lastSeq, exists := state.LastSequence[topic]
	if !exists {
		return 1 // No previous sequence for this topic
	}

	return lastSeq + 1
}

// ValidateProducerKey validates a producer key format and structure
func (psm *ProducerStateManager) ValidateProducerKey(key models.ProducerKey) error {
	if key.ID == "" {
		return fmt.Errorf("producer ID cannot be empty")
	}

	if len(key.ID) > 255 {
		return fmt.Errorf("producer ID too long: max 255 characters")
	}

	if key.Epoch == 0 {
		return fmt.Errorf("producer epoch must be positive")
	}

	return nil
}

// RemoveProducer removes a producer from tracking
func (psm *ProducerStateManager) RemoveProducer(producerID string) bool {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	_, exists := psm.producers[producerID]
	if exists {
		delete(psm.producers, producerID)
		psm.logger.Info("removed producer", zap.String("producer_id", producerID))
	}

	return exists
}

// cleanupWorker runs periodic cleanup of inactive producers
func (psm *ProducerStateManager) cleanupWorker() {
	defer psm.wg.Done()

	ticker := time.NewTicker(psm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			psm.cleanupInactiveProducers()
		case <-psm.stopCh:
			return
		}
	}
}

// cleanupInactiveProducers removes producers that have been inactive for too long
func (psm *ProducerStateManager) cleanupInactiveProducers() {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	cutoff := time.Now().Add(-psm.maxInactiveTime)
	removed := 0

	for producerID, state := range psm.producers {
		if state.LastActivity.Before(cutoff) {
			delete(psm.producers, producerID)
			removed++
		}
	}

	if removed > 0 {
		psm.logger.Info("cleaned up inactive producers",
			zap.Int("removed", removed),
			zap.Duration("inactive_threshold", psm.maxInactiveTime))
	}
}

// GetStats returns statistics about producer state management
func (psm *ProducerStateManager) GetStats() *ProducerManagerStats {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	stats := &ProducerManagerStats{
		ActiveProducers:   len(psm.producers),
		TotalMessages:     0,
		TotalDuplicates:   0,
		TopicCount:        make(map[string]int),
		EpochDistribution: make(map[uint64]int),
	}

	topicSet := make(map[string]bool)

	for _, state := range psm.producers {
		stats.TotalMessages += state.MessageCount
		stats.TotalDuplicates += state.DuplicateCount
		stats.EpochDistribution[state.Epoch]++

		for topic := range state.Topics {
			topicSet[topic] = true
			stats.TopicCount[topic]++
		}
	}

	stats.UniqueTopics = len(topicSet)

	return stats
}

// ProducerManagerStats holds statistics about producer state management
type ProducerManagerStats struct {
	ActiveProducers   int            `json:"active_producers"`
	TotalMessages     uint64         `json:"total_messages"`
	TotalDuplicates   uint64         `json:"total_duplicates"`
	UniqueTopics      int            `json:"unique_topics"`
	TopicCount        map[string]int `json:"topic_count"`        // topic -> producer count
	EpochDistribution map[uint64]int `json:"epoch_distribution"` // epoch -> producer count
}

// UpdateConfig updates the producer state manager configuration
func (psm *ProducerStateManager) UpdateConfig(maxInactiveTime, cleanupInterval time.Duration) error {
	if maxInactiveTime <= 0 {
		return fmt.Errorf("max inactive time must be positive")
	}
	if cleanupInterval <= 0 {
		return fmt.Errorf("cleanup interval must be positive")
	}

	psm.mu.Lock()
	psm.maxInactiveTime = maxInactiveTime
	psm.cleanupInterval = cleanupInterval
	psm.mu.Unlock()

	psm.logger.Info("updated producer state manager configuration",
		zap.Duration("max_inactive_time", maxInactiveTime),
		zap.Duration("cleanup_interval", cleanupInterval))

	return nil
}

// Clear removes all producer states (useful for testing)
func (psm *ProducerStateManager) Clear() {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	count := len(psm.producers)
	psm.producers = make(map[string]*ProducerState)

	psm.logger.Info("cleared all producer states", zap.Int("cleared", count))
}

// IsHealthy returns true if the producer state manager is operating normally
func (psm *ProducerStateManager) IsHealthy() bool {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	// Simple health check - ensure the producers map is accessible
	return psm.producers != nil
}
