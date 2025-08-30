package txn

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// TimeoutCallback is called when a timeout occurs
type TimeoutCallback func()

// TimeoutEntry represents a scheduled timeout
type TimeoutEntry struct {
	ID        string
	ExpiresAt time.Time
	Callback  TimeoutCallback
	index     int // for heap operations
}

// TimeoutHeap implements a min-heap for timeout entries
type TimeoutHeap []*TimeoutEntry

func (h TimeoutHeap) Len() int           { return len(h) }
func (h TimeoutHeap) Less(i, j int) bool { return h[i].ExpiresAt.Before(h[j].ExpiresAt) }
func (h TimeoutHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *TimeoutHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*TimeoutEntry)
	entry.index = n
	*h = append(*h, entry)
}

func (h *TimeoutHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil   // avoid memory leak
	entry.index = -1 // for safety
	*h = old[0 : n-1]
	return entry
}

// TimeoutManagerConfig configures the timeout manager
type TimeoutManagerConfig struct {
	// How often to check for expired timeouts
	TickInterval time.Duration
	// Buffer size for timeout processing
	CallbackBufferSize int
}

// DefaultTimeoutManagerConfig returns a default configuration
func DefaultTimeoutManagerConfig() *TimeoutManagerConfig {
	return &TimeoutManagerConfig{
		TickInterval:       100 * time.Millisecond,
		CallbackBufferSize: 100,
	}
}

// TimeoutManager manages transaction timeouts using a min-heap
type TimeoutManager struct {
	config    *TimeoutManagerConfig
	heap      *TimeoutHeap
	entries   map[string]*TimeoutEntry
	callbacks chan TimeoutCallback
	logger    *zap.Logger
	mu        sync.RWMutex
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewTimeoutManager creates a new timeout manager
func NewTimeoutManager(logger *zap.Logger) *TimeoutManager {
	config := DefaultTimeoutManagerConfig()

	h := &TimeoutHeap{}
	heap.Init(h)

	return &TimeoutManager{
		config:    config,
		heap:      h,
		entries:   make(map[string]*TimeoutEntry),
		callbacks: make(chan TimeoutCallback, config.CallbackBufferSize),
		logger:    logger.Named("timeout-manager"),
		stopCh:    make(chan struct{}),
	}
}

// Start starts the timeout manager
func (tm *TimeoutManager) Start(ctx context.Context) error {
	tm.logger.Info("starting timeout manager",
		zap.Duration("tick_interval", tm.config.TickInterval))

	// Start timer worker
	tm.wg.Add(1)
	go tm.timerWorker()

	// Start callback processor
	tm.wg.Add(1)
	go tm.callbackProcessor()

	tm.logger.Info("timeout manager started")
	return nil
}

// Stop stops the timeout manager
func (tm *TimeoutManager) Stop(ctx context.Context) error {
	tm.logger.Info("stopping timeout manager")

	close(tm.stopCh)
	tm.wg.Wait()

	// Close callbacks channel
	close(tm.callbacks)

	tm.logger.Info("timeout manager stopped")
	return nil
}

// ScheduleTimeout schedules a timeout for the given ID
func (tm *TimeoutManager) ScheduleTimeout(id string, expiresAt time.Time, callback TimeoutCallback) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Cancel existing timeout for this ID if any
	if existing, exists := tm.entries[id]; exists {
		tm.removeEntry(existing)
	}

	// Create new timeout entry
	entry := &TimeoutEntry{
		ID:        id,
		ExpiresAt: expiresAt,
		Callback:  callback,
	}

	// Add to heap and index
	heap.Push(tm.heap, entry)
	tm.entries[id] = entry

	tm.logger.Debug("scheduled timeout",
		zap.String("id", id),
		zap.Time("expires_at", expiresAt))
}

// CancelTimeout cancels a scheduled timeout
func (tm *TimeoutManager) CancelTimeout(id string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	entry, exists := tm.entries[id]
	if !exists {
		return false
	}

	tm.removeEntry(entry)

	tm.logger.Debug("cancelled timeout", zap.String("id", id))
	return true
}

// GetTimeouts returns all scheduled timeouts (for debugging/monitoring)
func (tm *TimeoutManager) GetTimeouts() map[string]time.Time {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make(map[string]time.Time)
	for id, entry := range tm.entries {
		result[id] = entry.ExpiresAt
	}

	return result
}

// Stats returns timeout manager statistics
func (tm *TimeoutManager) Stats() TimeoutManagerStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var nextTimeout *time.Time
	if tm.heap.Len() > 0 {
		nextTimeout = &(*tm.heap)[0].ExpiresAt
	}

	return TimeoutManagerStats{
		ActiveTimeouts: len(tm.entries),
		NextTimeout:    nextTimeout,
	}
}

// TimeoutManagerStats contains timeout manager statistics
type TimeoutManagerStats struct {
	ActiveTimeouts int
	NextTimeout    *time.Time
}

func (tm *TimeoutManager) removeEntry(entry *TimeoutEntry) {
	// Remove from heap
	if entry.index >= 0 && entry.index < tm.heap.Len() {
		heap.Remove(tm.heap, entry.index)
	}

	// Remove from index
	delete(tm.entries, entry.ID)
}

func (tm *TimeoutManager) timerWorker() {
	defer tm.wg.Done()

	ticker := time.NewTicker(tm.config.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.processTimeouts()
		case <-tm.stopCh:
			return
		}
	}
}

func (tm *TimeoutManager) processTimeouts() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	now := time.Now()
	var expired []*TimeoutEntry

	// Collect all expired timeouts
	for tm.heap.Len() > 0 {
		next := (*tm.heap)[0]
		if next.ExpiresAt.After(now) {
			break // No more expired timeouts
		}

		// Remove from heap and index
		entry := heap.Pop(tm.heap).(*TimeoutEntry)
		delete(tm.entries, entry.ID)
		expired = append(expired, entry)
	}

	// Execute callbacks asynchronously
	for _, entry := range expired {
		select {
		case tm.callbacks <- entry.Callback:
			tm.logger.Debug("timeout expired",
				zap.String("id", entry.ID),
				zap.Time("expires_at", entry.ExpiresAt))
		default:
			// Callback buffer is full, execute synchronously
			tm.logger.Warn("callback buffer full, executing timeout callback synchronously",
				zap.String("id", entry.ID))

			// Execute in goroutine to avoid blocking the timer
			go func(cb TimeoutCallback, id string) {
				defer func() {
					if r := recover(); r != nil {
						tm.logger.Error("timeout callback panicked",
							zap.String("id", id),
							zap.Any("panic", r))
					}
				}()
				cb()
			}(entry.Callback, entry.ID)
		}
	}

	if len(expired) > 0 {
		tm.logger.Debug("processed expired timeouts", zap.Int("count", len(expired)))
	}
}

func (tm *TimeoutManager) callbackProcessor() {
	defer tm.wg.Done()

	for {
		select {
		case callback, ok := <-tm.callbacks:
			if !ok {
				return // Channel closed
			}

			// Execute callback with panic protection
			func() {
				defer func() {
					if r := recover(); r != nil {
						tm.logger.Error("timeout callback panicked",
							zap.Any("panic", r))
					}
				}()
				callback()
			}()

		case <-tm.stopCh:
			// Drain remaining callbacks
			for {
				select {
				case callback, ok := <-tm.callbacks:
					if !ok {
						return
					}

					// Execute remaining callbacks
					func() {
						defer func() {
							if r := recover(); r != nil {
								tm.logger.Error("timeout callback panicked during shutdown",
									zap.Any("panic", r))
							}
						}()
						callback()
					}()
				default:
					return // No more callbacks
				}
			}
		}
	}
}
