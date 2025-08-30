package heap

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// MessageScheduler coordinates between timer heap and main heap
// It runs a background goroutine that periodically promotes ready messages
type MessageScheduler struct {
	timerHeap TimerHeap
	mainHeap  Heap
	logger    *zap.Logger

	// Background processing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Configuration
	checkInterval time.Duration

	// Channels for communication
	promoteChan chan struct{} // Signal to check for ready messages

	// Statistics
	mu    sync.RWMutex
	stats SchedulerStats
}

// SchedulerStats provides statistics about the message scheduler
type SchedulerStats struct {
	TotalPromotions      uint64        `json:"total_promotions"`
	LastPromotionAt      *time.Time    `json:"last_promotion_at,omitempty"`
	AveragePromotionTime time.Duration `json:"average_promotion_time"`
	ScheduledMessages    int           `json:"scheduled_messages"`
	BackgroundErrors     uint64        `json:"background_errors"`
}

// NewMessageScheduler creates a new message scheduler
func NewMessageScheduler(timerHeap TimerHeap, mainHeap Heap, logger *zap.Logger) *MessageScheduler {
	if logger == nil {
		logger = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &MessageScheduler{
		timerHeap:     timerHeap,
		mainHeap:      mainHeap,
		logger:        logger.With(zap.String("component", "message_scheduler")),
		ctx:           ctx,
		cancel:        cancel,
		checkInterval: 100 * time.Millisecond, // Default check interval
		promoteChan:   make(chan struct{}, 1), // Buffered to avoid blocking
	}
}

// Start begins the background scheduling process
func (ms *MessageScheduler) Start() {
	ms.wg.Add(1)
	go ms.backgroundProcessor()

	ms.logger.Info("Message scheduler started",
		zap.Duration("check_interval", ms.checkInterval),
	)
}

// Stop gracefully shuts down the scheduler
func (ms *MessageScheduler) Stop() {
	ms.logger.Info("Stopping message scheduler")

	ms.cancel()
	ms.wg.Wait()

	// Do a final promotion of any ready messages
	ms.promoteReadyMessages()

	ms.logger.Info("Message scheduler stopped")
}

// SetCheckInterval configures how often to check for ready messages
func (ms *MessageScheduler) SetCheckInterval(interval time.Duration) {
	if interval < time.Millisecond {
		interval = time.Millisecond // Minimum 1ms
	}
	ms.checkInterval = interval

	ms.logger.Debug("Check interval updated",
		zap.Duration("new_interval", interval),
	)
}

// TriggerCheck manually triggers a check for ready messages
// This can be called when new messages are scheduled to reduce latency
func (ms *MessageScheduler) TriggerCheck() {
	select {
	case ms.promoteChan <- struct{}{}:
		// Successfully triggered
	default:
		// Channel is full, check is already pending
	}
}

// backgroundProcessor runs the main scheduling loop
func (ms *MessageScheduler) backgroundProcessor() {
	defer ms.wg.Done()

	ticker := time.NewTicker(ms.checkInterval)
	defer ticker.Stop()

	ms.logger.Debug("Background processor started")

	for {
		select {
		case <-ms.ctx.Done():
			ms.logger.Debug("Background processor stopping")
			return

		case <-ticker.C:
			// Regular interval check
			ms.promoteReadyMessages()

		case <-ms.promoteChan:
			// Manual trigger
			ms.promoteReadyMessages()

			// Adjust ticker based on next ready time for efficiency
			if nextTime := ms.timerHeap.NextReadyTime(); nextTime != nil {
				timeUntilNext := time.Until(*nextTime)
				if timeUntilNext > 0 && timeUntilNext < ms.checkInterval {
					// Reset ticker for more responsive handling
					ticker.Reset(timeUntilNext + time.Millisecond)
				}
			}
		}
	}
}

// promoteReadyMessages moves ready messages from timer heap to main heap
func (ms *MessageScheduler) promoteReadyMessages() {
	start := time.Now()

	readyMessages := ms.timerHeap.ProcessReady()
	if len(readyMessages) == 0 {
		return
	}

	promoted := 0
	for _, msg := range readyMessages {
		if err := ms.mainHeap.Insert(msg); err != nil {
			ms.logger.Error("Failed to promote message to main heap",
				zap.String("message_id", string(msg.ID)),
				zap.Error(err),
			)

			ms.mu.Lock()
			ms.stats.BackgroundErrors++
			ms.mu.Unlock()
		} else {
			promoted++
		}
	}

	// Update statistics
	promotionTime := time.Since(start)
	now := time.Now()

	ms.mu.Lock()
	ms.stats.TotalPromotions += uint64(promoted)
	ms.stats.LastPromotionAt = &now
	ms.stats.ScheduledMessages = ms.timerHeap.Size()

	// Update average promotion time using exponential moving average
	if ms.stats.AveragePromotionTime == 0 {
		ms.stats.AveragePromotionTime = promotionTime
	} else {
		// EMA with α = 0.1
		alpha := 0.1
		ms.stats.AveragePromotionTime = time.Duration(
			float64(ms.stats.AveragePromotionTime)*(1-alpha) + float64(promotionTime)*alpha,
		)
	}
	ms.mu.Unlock()

	if promoted > 0 {
		ms.logger.Debug("Messages promoted from timer to main heap",
			zap.Int("promoted", promoted),
			zap.Int("total_ready", len(readyMessages)),
			zap.Duration("promotion_time", promotionTime),
			zap.Int("still_scheduled", ms.timerHeap.Size()),
		)
	}
}

// Stats returns current scheduler statistics
func (ms *MessageScheduler) Stats() SchedulerStats {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	stats := ms.stats
	stats.ScheduledMessages = ms.timerHeap.Size() // Get current count
	return stats
}

// GetNextScheduledTime returns when the next message will be ready
func (ms *MessageScheduler) GetNextScheduledTime() *time.Time {
	return ms.timerHeap.NextReadyTime()
}

// IsHealthy checks if the scheduler is running properly
func (ms *MessageScheduler) IsHealthy() bool {
	select {
	case <-ms.ctx.Done():
		return false
	default:
		return true
	}
}
