package delivery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

var (
	ErrDLQNotEnabled = fmt.Errorf("DLQ not enabled for topic")
	ErrDLQFull       = fmt.Errorf("DLQ has reached capacity limit")
)

// DLQManager manages dead letter queue operations
type DLQManager struct {
	mu       sync.RWMutex
	dlqHeaps map[string]*DLQHeap // topic -> DLQ heap
	logger   *zap.Logger
	config   *DLQConfig
}

// DLQConfig holds configuration for dead letter queue management
type DLQConfig struct {
	MaxCapacity        uint64        `json:"max_capacity"`        // Max messages per DLQ
	RetentionTime      time.Duration `json:"retention_time"`      // How long to keep DLQ messages
	EnableCompaction   bool          `json:"enable_compaction"`   // Enable periodic cleanup
	CompactionInterval time.Duration `json:"compaction_interval"` // How often to run cleanup
}

// DLQHeap represents a dead letter queue for a specific topic
type DLQHeap struct {
	topic      string
	messages   []*DLQMessage
	messageMap map[models.MessageID]*DLQMessage
	mu         sync.RWMutex
	logger     *zap.Logger
	config     *DLQConfig
	stats      DLQStats
}

// DLQMessage represents a message in the dead letter queue
type DLQMessage struct {
	Message            *models.Message `json:"message"`
	OriginalTopic      string          `json:"original_topic"`
	MovedToDLQAt       time.Time       `json:"moved_to_dlq_at"`
	FinalAttempts      uint32          `json:"final_attempts"`
	FinalFailureReason string          `json:"final_failure_reason"`
	RetentionDeadline  time.Time       `json:"retention_deadline"`
}

// DLQStats holds statistics for a DLQ heap
type DLQStats struct {
	MessageCount        uint64    `json:"message_count"`
	TotalMoved          uint64    `json:"total_moved"`
	TotalReplayed       uint64    `json:"total_replayed"`
	TotalDeleted        uint64    `json:"total_deleted"`
	TotalExpired        uint64    `json:"total_expired"`
	LastCompaction      time.Time `json:"last_compaction"`
	OldestMessage       time.Time `json:"oldest_message"`
	RetentionViolations uint64    `json:"retention_violations"`
}

// DefaultDLQConfig returns sensible defaults for DLQ configuration
func DefaultDLQConfig() *DLQConfig {
	return &DLQConfig{
		MaxCapacity:        10000,              // 10K messages per DLQ
		RetentionTime:      7 * 24 * time.Hour, // 7 days
		EnableCompaction:   true,
		CompactionInterval: 1 * time.Hour, // Clean up every hour
	}
}

// NewDLQManager creates a new dead letter queue manager
func NewDLQManager(config *DLQConfig, logger *zap.Logger) *DLQManager {
	if config == nil {
		config = DefaultDLQConfig()
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	return &DLQManager{
		dlqHeaps: make(map[string]*DLQHeap),
		logger:   logger,
		config:   config,
	}
}

// GetOrCreateDLQ gets or creates a DLQ heap for a topic
func (dm *DLQManager) GetOrCreateDLQ(topic string) *DLQHeap {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if heap, exists := dm.dlqHeaps[topic]; exists {
		return heap
	}

	heap := &DLQHeap{
		topic:      topic,
		messages:   make([]*DLQMessage, 0),
		messageMap: make(map[models.MessageID]*DLQMessage),
		logger:     dm.logger.With(zap.String("dlq_topic", topic)),
		config:     dm.config,
	}

	dm.dlqHeaps[topic] = heap

	dm.logger.Debug("Created DLQ heap",
		zap.String("component", "dlq_manager"),
		zap.String("topic", topic))

	return heap
}

// MoveToDLQ moves a message to the dead letter queue
func (dm *DLQManager) MoveToDLQ(ctx context.Context, topic string, message *models.Message, failureReason string) error {
	heap := dm.GetOrCreateDLQ(topic)
	return heap.AddMessage(ctx, message, failureReason)
}

// PeekDLQ returns messages from the DLQ without removing them
func (dm *DLQManager) PeekDLQ(ctx context.Context, topic string, count int) ([]*DLQMessage, error) {
	dm.mu.RLock()
	heap, exists := dm.dlqHeaps[topic]
	dm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("DLQ not found for topic: %s", topic)
	}

	return heap.Peek(ctx, count)
}

// ReplayFromDLQ removes messages from DLQ and returns them for re-queueing
func (dm *DLQManager) ReplayFromDLQ(ctx context.Context, topic string, messageIDs []models.MessageID) ([]*models.Message, error) {
	dm.mu.RLock()
	heap, exists := dm.dlqHeaps[topic]
	dm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("DLQ not found for topic: %s", topic)
	}

	return heap.Replay(ctx, messageIDs)
}

// DeleteFromDLQ permanently deletes messages from the DLQ
func (dm *DLQManager) DeleteFromDLQ(ctx context.Context, topic string, messageIDs []models.MessageID) error {
	dm.mu.RLock()
	heap, exists := dm.dlqHeaps[topic]
	dm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("DLQ not found for topic: %s", topic)
	}

	return heap.Delete(ctx, messageIDs)
}

// GetDLQStats returns statistics for a topic's DLQ
func (dm *DLQManager) GetDLQStats(topic string) (*DLQStats, error) {
	dm.mu.RLock()
	heap, exists := dm.dlqHeaps[topic]
	dm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("DLQ not found for topic: %s", topic)
	}

	return heap.GetStats(), nil
}

// CompactAll runs compaction on all DLQ heaps
func (dm *DLQManager) CompactAll(ctx context.Context) error {
	dm.mu.RLock()
	heaps := make([]*DLQHeap, 0, len(dm.dlqHeaps))
	for _, heap := range dm.dlqHeaps {
		heaps = append(heaps, heap)
	}
	dm.mu.RUnlock()

	for _, heap := range heaps {
		if err := heap.Compact(ctx); err != nil {
			dm.logger.Error("Failed to compact DLQ",
				zap.String("topic", heap.topic),
				zap.Error(err))
		}
	}

	return nil
}

// AddMessage adds a message to the DLQ heap
func (dh *DLQHeap) AddMessage(ctx context.Context, message *models.Message, failureReason string) error {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	// Check capacity
	if uint64(len(dh.messages)) >= dh.config.MaxCapacity {
		dh.logger.Warn("DLQ at capacity",
			zap.String("topic", dh.topic),
			zap.Uint64("capacity", dh.config.MaxCapacity))
		return ErrDLQFull
	}

	// Check if message already exists
	if _, exists := dh.messageMap[message.ID]; exists {
		dh.logger.Debug("Message already in DLQ",
			zap.String("message_id", string(message.ID)),
			zap.String("topic", dh.topic))
		return nil
	}

	now := time.Now()
	dlqMessage := &DLQMessage{
		Message:            message,
		OriginalTopic:      dh.topic,
		MovedToDLQAt:       now,
		FinalAttempts:      message.Attempts,
		FinalFailureReason: failureReason,
		RetentionDeadline:  now.Add(dh.config.RetentionTime),
	}

	// Add to both slice and map
	dh.messages = append(dh.messages, dlqMessage)
	dh.messageMap[message.ID] = dlqMessage

	// Update stats
	dh.stats.MessageCount++
	dh.stats.TotalMoved++

	if dh.stats.OldestMessage.IsZero() || now.Before(dh.stats.OldestMessage) {
		dh.stats.OldestMessage = now
	}

	dh.logger.Debug("Message moved to DLQ",
		zap.String("component", "dlq_heap"),
		zap.String("topic", dh.topic),
		zap.String("message_id", string(message.ID)),
		zap.Uint32("attempts", message.Attempts),
		zap.String("failure_reason", failureReason))

	return nil
}

// Peek returns messages without removing them
func (dh *DLQHeap) Peek(ctx context.Context, count int) ([]*DLQMessage, error) {
	dh.mu.RLock()
	defer dh.mu.RUnlock()

	if count <= 0 || count > len(dh.messages) {
		count = len(dh.messages)
	}

	result := make([]*DLQMessage, count)
	copy(result, dh.messages[:count])

	dh.logger.Debug("Peeked DLQ messages",
		zap.String("component", "dlq_heap"),
		zap.String("topic", dh.topic),
		zap.Int("requested", count),
		zap.Int("returned", len(result)))

	return result, nil
}

// Replay removes messages from DLQ and returns them
func (dh *DLQHeap) Replay(ctx context.Context, messageIDs []models.MessageID) ([]*models.Message, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	var replayed []*models.Message
	var toRemove []int

	for _, messageID := range messageIDs {
		dlqMessage, exists := dh.messageMap[messageID]
		if !exists {
			dh.logger.Debug("Message not found in DLQ for replay",
				zap.String("message_id", string(messageID)),
				zap.String("topic", dh.topic))
			continue
		}

		// Reset message for replay
		message := dlqMessage.Message
		message.Attempts = 0 // Reset attempts for replay
		message.Lease = nil  // Clear any lease

		replayed = append(replayed, message)

		// Find index to remove
		for i, msg := range dh.messages {
			if msg.Message.ID == messageID {
				toRemove = append(toRemove, i)
				break
			}
		}

		// Remove from map
		delete(dh.messageMap, messageID)
	}

	// Remove from slice (in reverse order to maintain indices)
	for i := len(toRemove) - 1; i >= 0; i-- {
		idx := toRemove[i]
		dh.messages = append(dh.messages[:idx], dh.messages[idx+1:]...)
	}

	// Update stats
	dh.stats.MessageCount = uint64(len(dh.messages))
	dh.stats.TotalReplayed += uint64(len(replayed))

	dh.logger.Debug("Replayed messages from DLQ",
		zap.String("component", "dlq_heap"),
		zap.String("topic", dh.topic),
		zap.Int("requested", len(messageIDs)),
		zap.Int("replayed", len(replayed)))

	return replayed, nil
}

// Delete permanently removes messages from the DLQ
func (dh *DLQHeap) Delete(ctx context.Context, messageIDs []models.MessageID) error {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	var toRemove []int
	deletedCount := 0

	for _, messageID := range messageIDs {
		if _, exists := dh.messageMap[messageID]; !exists {
			dh.logger.Debug("Message not found in DLQ for deletion",
				zap.String("message_id", string(messageID)),
				zap.String("topic", dh.topic))
			continue
		}

		// Find index to remove
		for i, msg := range dh.messages {
			if msg.Message.ID == messageID {
				toRemove = append(toRemove, i)
				break
			}
		}

		// Remove from map
		delete(dh.messageMap, messageID)
		deletedCount++
	}

	// Remove from slice (in reverse order to maintain indices)
	for i := len(toRemove) - 1; i >= 0; i-- {
		idx := toRemove[i]
		dh.messages = append(dh.messages[:idx], dh.messages[idx+1:]...)
	}

	// Update stats
	dh.stats.MessageCount = uint64(len(dh.messages))
	dh.stats.TotalDeleted += uint64(deletedCount)

	dh.logger.Debug("Deleted messages from DLQ",
		zap.String("component", "dlq_heap"),
		zap.String("topic", dh.topic),
		zap.Int("requested", len(messageIDs)),
		zap.Int("deleted", deletedCount))

	return nil
}

// Compact removes expired messages based on retention policy
func (dh *DLQHeap) Compact(ctx context.Context) error {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	if !dh.config.EnableCompaction {
		return nil
	}

	now := time.Now()
	var toRemove []int
	expiredCount := 0

	// Find expired messages
	for i, dlqMessage := range dh.messages {
		if dlqMessage.RetentionDeadline.Before(now) {
			toRemove = append(toRemove, i)
			delete(dh.messageMap, dlqMessage.Message.ID)
			expiredCount++
		}
	}

	// Remove expired messages (in reverse order)
	for i := len(toRemove) - 1; i >= 0; i-- {
		idx := toRemove[i]
		dh.messages = append(dh.messages[:idx], dh.messages[idx+1:]...)
	}

	// Update stats
	dh.stats.MessageCount = uint64(len(dh.messages))
	dh.stats.TotalExpired += uint64(expiredCount)
	dh.stats.LastCompaction = now

	// Check for retention violations (messages past their deadline)
	violations := uint64(0)
	for _, msg := range dh.messages {
		if msg.RetentionDeadline.Before(now) {
			violations++
		}
	}
	dh.stats.RetentionViolations = violations

	if expiredCount > 0 {
		dh.logger.Debug("Compacted DLQ",
			zap.String("component", "dlq_heap"),
			zap.String("topic", dh.topic),
			zap.Int("expired_removed", expiredCount),
			zap.Uint64("remaining_messages", dh.stats.MessageCount),
			zap.Uint64("retention_violations", violations))
	}

	return nil
}

// GetStats returns current DLQ statistics
func (dh *DLQHeap) GetStats() *DLQStats {
	dh.mu.RLock()
	defer dh.mu.RUnlock()

	// Update current message count
	stats := dh.stats
	stats.MessageCount = uint64(len(dh.messages))

	// Find oldest message
	if len(dh.messages) > 0 {
		oldest := dh.messages[0].MovedToDLQAt
		for _, msg := range dh.messages {
			if msg.MovedToDLQAt.Before(oldest) {
				oldest = msg.MovedToDLQAt
			}
		}
		stats.OldestMessage = oldest
	} else {
		stats.OldestMessage = time.Time{}
	}

	return &stats
}
