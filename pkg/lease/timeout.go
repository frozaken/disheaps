package lease

import (
	"context"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// TimeoutHandler manages lease expiration detection and handling
type TimeoutHandler struct {
	mu       sync.RWMutex
	logger   *zap.Logger
	manager  *Manager
	watchers map[models.LeaseToken]*timeoutWatcher

	// Configuration
	checkInterval time.Duration
	gracePeriod   time.Duration

	// Background processing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// timeoutWatcher tracks a specific lease timeout
type timeoutWatcher struct {
	lease       *models.Lease
	topic       string
	partition   uint32
	deadline    time.Time
	gracePeriod time.Duration
	callback    TimeoutCallback
	timer       *time.Timer
	cancelled   bool
	mu          sync.Mutex
}

// TimeoutCallback is called when a lease times out
type TimeoutCallback func(ctx context.Context, lease *models.Lease, topic string, partition uint32)

// TimeoutConfig configures lease timeout handling
type TimeoutConfig struct {
	CheckInterval time.Duration
	GracePeriod   time.Duration
}

// DefaultTimeoutConfig returns sensible defaults for timeout handling
func DefaultTimeoutConfig() *TimeoutConfig {
	return &TimeoutConfig{
		CheckInterval: 5 * time.Second,
		GracePeriod:   2 * time.Second,
	}
}

// NewTimeoutHandler creates a new timeout handler
func NewTimeoutHandler(manager *Manager, config *TimeoutConfig, logger *zap.Logger) *TimeoutHandler {
	if config == nil {
		config = DefaultTimeoutConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &TimeoutHandler{
		logger:        logger.With(zap.String("component", "lease_timeout")),
		manager:       manager,
		watchers:      make(map[models.LeaseToken]*timeoutWatcher),
		checkInterval: config.CheckInterval,
		gracePeriod:   config.GracePeriod,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start begins timeout monitoring
func (th *TimeoutHandler) Start() error {
	th.logger.Info("Starting lease timeout handler",
		zap.Duration("check_interval", th.checkInterval),
		zap.Duration("grace_period", th.gracePeriod))

	// Start periodic timeout checker
	th.wg.Add(1)
	go th.timeoutCheckLoop()

	return nil
}

// Stop gracefully shuts down timeout handling
func (th *TimeoutHandler) Stop() error {
	th.logger.Info("Stopping lease timeout handler")

	th.cancel()

	// Cancel all active watchers
	th.mu.Lock()
	for _, watcher := range th.watchers {
		watcher.cancel()
	}
	th.watchers = make(map[models.LeaseToken]*timeoutWatcher)
	th.mu.Unlock()

	th.wg.Wait()

	th.logger.Info("Lease timeout handler stopped")
	return nil
}

// WatchLease starts monitoring a lease for timeout
func (th *TimeoutHandler) WatchLease(topic string, partition uint32, lease *models.Lease, callback TimeoutCallback) {
	if lease == nil || callback == nil {
		th.logger.Warn("Invalid lease or callback for watching")
		return
	}

	// Calculate timeout with grace period
	now := time.Now()
	deadline := lease.Deadline.Add(th.gracePeriod)

	if deadline.Before(now) {
		// Already expired, call callback immediately
		go callback(th.ctx, lease, topic, partition)
		return
	}

	th.mu.Lock()
	defer th.mu.Unlock()

	// Cancel existing watcher if any
	if existingWatcher, exists := th.watchers[lease.Token]; exists {
		existingWatcher.cancel()
	}

	// Create new watcher
	watcher := &timeoutWatcher{
		lease:       lease,
		topic:       topic,
		partition:   partition,
		deadline:    deadline,
		gracePeriod: th.gracePeriod,
		callback:    callback,
	}

	// Set up timer for timeout
	timeout := deadline.Sub(now)
	watcher.timer = time.AfterFunc(timeout, func() {
		th.handleTimeout(watcher)
	})

	th.watchers[lease.Token] = watcher

	th.logger.Debug("Started watching lease",
		zap.String("token", string(lease.Token)),
		zap.String("message_id", string(lease.MessageID)),
		zap.Time("deadline", deadline),
		zap.Duration("timeout_in", timeout))
}

// UnwatchLease stops monitoring a lease
func (th *TimeoutHandler) UnwatchLease(token models.LeaseToken) {
	th.mu.Lock()
	defer th.mu.Unlock()

	if watcher, exists := th.watchers[token]; exists {
		watcher.cancel()
		delete(th.watchers, token)

		th.logger.Debug("Stopped watching lease",
			zap.String("token", string(token)))
	}
}

// UpdateLeaseWatch updates the timeout for an existing watched lease
func (th *TimeoutHandler) UpdateLeaseWatch(topic string, partition uint32, lease *models.Lease, callback TimeoutCallback) {
	// Simply re-watch the lease - this will cancel the old watcher and create a new one
	th.WatchLease(topic, partition, lease, callback)
}

// GetActiveWatchers returns the number of active lease watchers
func (th *TimeoutHandler) GetActiveWatchers() int {
	th.mu.RLock()
	defer th.mu.RUnlock()
	return len(th.watchers)
}

// handleTimeout processes a lease timeout
func (th *TimeoutHandler) handleTimeout(watcher *timeoutWatcher) {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()

	if watcher.cancelled {
		return
	}

	// Mark as cancelled to prevent duplicate processing
	watcher.cancelled = true

	// Remove from watchers map
	th.mu.Lock()
	delete(th.watchers, watcher.lease.Token)
	th.mu.Unlock()

	th.logger.Debug("Lease timed out",
		zap.String("token", string(watcher.lease.Token)),
		zap.String("message_id", string(watcher.lease.MessageID)),
		zap.String("holder", watcher.lease.Holder),
		zap.Time("deadline", watcher.deadline))

	// Call timeout callback
	if watcher.callback != nil {
		watcher.callback(th.ctx, watcher.lease, watcher.topic, watcher.partition)
	}
}

// cancel stops the watcher's timer
func (w *timeoutWatcher) cancel() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.cancelled = true
	if w.timer != nil {
		w.timer.Stop()
	}
}

// timeoutCheckLoop periodically checks for expired leases as a backup
func (th *TimeoutHandler) timeoutCheckLoop() {
	defer th.wg.Done()

	ticker := time.NewTicker(th.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-th.ctx.Done():
			return
		case <-ticker.C:
			th.checkExpiredLeases()
		}
	}
}

// checkExpiredLeases performs a backup check for expired leases
func (th *TimeoutHandler) checkExpiredLeases() {
	now := time.Now()
	expiredWatchers := make([]*timeoutWatcher, 0)

	th.mu.RLock()
	for _, watcher := range th.watchers {
		if now.After(watcher.deadline) {
			expiredWatchers = append(expiredWatchers, watcher)
		}
	}
	th.mu.RUnlock()

	// Process expired watchers
	for _, watcher := range expiredWatchers {
		th.handleTimeout(watcher)
	}

	if len(expiredWatchers) > 0 {
		th.logger.Debug("Backup timeout check found expired leases",
			zap.Int("count", len(expiredWatchers)))
	}
}

// Close releases all resources
func (th *TimeoutHandler) Close() error {
	return th.Stop()
}
