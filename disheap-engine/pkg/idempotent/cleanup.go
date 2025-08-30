package idempotent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/storage"
	"go.uber.org/zap"
)

// CleanupConfig holds configuration for deduplication cleanup
type CleanupConfig struct {
	// Interval between cleanup runs
	Interval time.Duration `json:"interval"`

	// BatchSize for cleanup operations
	BatchSize int `json:"batch_size"`

	// Timeout for individual cleanup operations
	OperationTimeout time.Duration `json:"operation_timeout"`

	// MaxConcurrentOps limits concurrent cleanup operations
	MaxConcurrentOps int `json:"max_concurrent_ops"`

	// EnableProgressLogging enables detailed progress logging
	EnableProgressLogging bool `json:"enable_progress_logging"`

	// RetryAttempts for failed cleanup operations
	RetryAttempts int `json:"retry_attempts"`

	// RetryDelay between retry attempts
	RetryDelay time.Duration `json:"retry_delay"`
}

// DefaultCleanupConfig returns the default cleanup configuration
func DefaultCleanupConfig() *CleanupConfig {
	return &CleanupConfig{
		Interval:              5 * time.Minute,
		BatchSize:             1000,
		OperationTimeout:      30 * time.Second,
		MaxConcurrentOps:      2,
		EnableProgressLogging: false,
		RetryAttempts:         3,
		RetryDelay:            5 * time.Second,
	}
}

// CleanupManager manages TTL-based cleanup of deduplication entries
type CleanupManager struct {
	config  *CleanupConfig
	storage storage.Storage
	logger  *zap.Logger

	// Background processing
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// Statistics
	mu                  sync.RWMutex
	totalCleaned        uint64
	totalCleanupRuns    uint64
	lastCleanupTime     time.Time
	lastCleanupCount    int
	lastCleanupDuration time.Duration
	failedCleanups      uint64
}

// NewCleanupManager creates a new cleanup manager
func NewCleanupManager(config *CleanupConfig, storage storage.Storage, logger *zap.Logger) *CleanupManager {
	if config == nil {
		config = DefaultCleanupConfig()
	}

	return &CleanupManager{
		config:  config,
		storage: storage,
		logger:  logger.Named("cleanup-manager"),
		stopCh:  make(chan struct{}),
	}
}

// Start begins the cleanup manager
func (cm *CleanupManager) Start(ctx context.Context) error {
	cm.logger.Info("starting cleanup manager",
		zap.Duration("interval", cm.config.Interval),
		zap.Int("batch_size", cm.config.BatchSize),
		zap.Duration("operation_timeout", cm.config.OperationTimeout),
		zap.Int("max_concurrent_ops", cm.config.MaxConcurrentOps))

	// Start background cleanup worker
	cm.wg.Add(1)
	go cm.cleanupWorker()

	return nil
}

// Stop stops the cleanup manager
func (cm *CleanupManager) Stop(ctx context.Context) error {
	cm.stopOnce.Do(func() {
		cm.logger.Info("stopping cleanup manager")
		close(cm.stopCh)
	})

	cm.wg.Wait()
	cm.logger.Info("cleanup manager stopped")
	return nil
}

// cleanupWorker runs the periodic cleanup process
func (cm *CleanupManager) cleanupWorker() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.config.Interval)
	defer ticker.Stop()

	// Run initial cleanup
	cm.runCleanup()

	for {
		select {
		case <-ticker.C:
			cm.runCleanup()
		case <-cm.stopCh:
			return
		}
	}
}

// runCleanup performs a cleanup operation
func (cm *CleanupManager) runCleanup() {
	start := time.Now()

	cm.mu.Lock()
	cm.totalCleanupRuns++
	cm.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), cm.config.OperationTimeout)
	defer cancel()

	cleaned, err := cm.performCleanup(ctx)
	duration := time.Since(start)

	cm.mu.Lock()
	cm.lastCleanupTime = start
	cm.lastCleanupDuration = duration

	if err != nil {
		cm.failedCleanups++
		cm.lastCleanupCount = 0
		cm.logger.Error("cleanup operation failed",
			zap.Error(err),
			zap.Duration("duration", duration))
	} else {
		cm.totalCleaned += uint64(cleaned)
		cm.lastCleanupCount = cleaned

		if cleaned > 0 {
			cm.logger.Info("cleanup completed",
				zap.Int("cleaned", cleaned),
				zap.Duration("duration", duration),
				zap.Uint64("total_cleaned", cm.totalCleaned))
		} else if cm.config.EnableProgressLogging {
			cm.logger.Debug("cleanup completed - no entries to clean",
				zap.Duration("duration", duration))
		}
	}
	cm.mu.Unlock()
}

// performCleanup performs the actual cleanup with retries
func (cm *CleanupManager) performCleanup(ctx context.Context) (int, error) {
	var lastErr error

	for attempt := 0; attempt < cm.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			// Wait before retrying
			select {
			case <-time.After(cm.config.RetryDelay):
			case <-ctx.Done():
				return 0, ctx.Err()
			}

			cm.logger.Debug("retrying cleanup",
				zap.Int("attempt", attempt+1),
				zap.Error(lastErr))
		}

		cleaned, err := cm.storage.CleanupExpiredDedup(ctx, time.Now())
		if err == nil {
			return cleaned, nil
		}

		lastErr = err

		// Check if context was cancelled
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}

	return 0, lastErr
}

// RunCleanupNow performs an immediate cleanup operation
func (cm *CleanupManager) RunCleanupNow(ctx context.Context) (*CleanupResult, error) {
	start := time.Now()

	cleaned, err := cm.performCleanup(ctx)
	duration := time.Since(start)

	result := &CleanupResult{
		EntriesCleaned: cleaned,
		Duration:       duration,
		Success:        err == nil,
		Error:          err,
		Timestamp:      start,
	}

	// Update statistics
	cm.mu.Lock()
	cm.totalCleanupRuns++
	if err == nil {
		cm.totalCleaned += uint64(cleaned)
	} else {
		cm.failedCleanups++
	}
	cm.mu.Unlock()

	return result, err
}

// GetStats returns cleanup statistics
func (cm *CleanupManager) GetStats() *CleanupStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return &CleanupStats{
		TotalCleaned:        cm.totalCleaned,
		TotalCleanupRuns:    cm.totalCleanupRuns,
		FailedCleanups:      cm.failedCleanups,
		LastCleanupTime:     cm.lastCleanupTime,
		LastCleanupCount:    cm.lastCleanupCount,
		LastCleanupDuration: cm.lastCleanupDuration,
		Config:              cm.config,
	}
}

// CleanupStats holds statistics about cleanup operations
type CleanupStats struct {
	TotalCleaned        uint64         `json:"total_cleaned"`
	TotalCleanupRuns    uint64         `json:"total_cleanup_runs"`
	FailedCleanups      uint64         `json:"failed_cleanups"`
	LastCleanupTime     time.Time      `json:"last_cleanup_time"`
	LastCleanupCount    int            `json:"last_cleanup_count"`
	LastCleanupDuration time.Duration  `json:"last_cleanup_duration"`
	Config              *CleanupConfig `json:"config"`
}

// CleanupResult represents the result of a cleanup operation
type CleanupResult struct {
	EntriesCleaned int           `json:"entries_cleaned"`
	Duration       time.Duration `json:"duration"`
	Success        bool          `json:"success"`
	Error          error         `json:"error,omitempty"`
	Timestamp      time.Time     `json:"timestamp"`
}

// UpdateConfig updates the cleanup configuration
func (cm *CleanupManager) UpdateConfig(newConfig *CleanupConfig) error {
	if newConfig == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// Validate configuration
	if newConfig.Interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}
	if newConfig.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	if newConfig.OperationTimeout <= 0 {
		return fmt.Errorf("operation timeout must be positive")
	}
	if newConfig.MaxConcurrentOps <= 0 {
		return fmt.Errorf("max concurrent ops must be positive")
	}
	if newConfig.RetryAttempts < 0 {
		return fmt.Errorf("retry attempts cannot be negative")
	}
	if newConfig.RetryDelay < 0 {
		return fmt.Errorf("retry delay cannot be negative")
	}

	cm.config = newConfig

	cm.logger.Info("updated cleanup configuration",
		zap.Duration("interval", newConfig.Interval),
		zap.Int("batch_size", newConfig.BatchSize),
		zap.Duration("operation_timeout", newConfig.OperationTimeout),
		zap.Int("max_concurrent_ops", newConfig.MaxConcurrentOps),
		zap.Int("retry_attempts", newConfig.RetryAttempts),
		zap.Duration("retry_delay", newConfig.RetryDelay))

	return nil
}

// IsHealthy returns true if the cleanup manager is operating normally
func (cm *CleanupManager) IsHealthy() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Consider healthy if:
	// 1. We haven't had too many recent failures
	// 2. Storage is accessible

	// Check failure rate (more than 50% failures in recent runs is unhealthy)
	if cm.totalCleanupRuns > 0 {
		failureRate := float64(cm.failedCleanups) / float64(cm.totalCleanupRuns)
		if failureRate > 0.5 {
			return false
		}
	}

	// Check if storage is accessible
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cm.storage.Health(ctx); err != nil {
		return false
	}

	return true
}

// GetConfig returns the current cleanup configuration
func (cm *CleanupManager) GetConfig() *CleanupConfig {
	return cm.config
}

// Reset resets cleanup statistics (useful for testing)
func (cm *CleanupManager) Reset() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.totalCleaned = 0
	cm.totalCleanupRuns = 0
	cm.failedCleanups = 0
	cm.lastCleanupTime = time.Time{}
	cm.lastCleanupCount = 0
	cm.lastCleanupDuration = 0

	cm.logger.Info("reset cleanup statistics")
}

// CalculateCleanupEfficiency returns the efficiency of cleanup operations
func (cm *CleanupManager) CalculateCleanupEfficiency() *CleanupEfficiency {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	efficiency := &CleanupEfficiency{
		TotalRuns:       cm.totalCleanupRuns,
		SuccessfulRuns:  cm.totalCleanupRuns - cm.failedCleanups,
		FailedRuns:      cm.failedCleanups,
		TotalCleaned:    cm.totalCleaned,
		AverageDuration: cm.lastCleanupDuration, // Simplified - using last duration
	}

	if cm.totalCleanupRuns > 0 {
		efficiency.SuccessRate = float64(efficiency.SuccessfulRuns) / float64(cm.totalCleanupRuns)
		efficiency.AverageCleanedPerRun = float64(cm.totalCleaned) / float64(cm.totalCleanupRuns)
	}

	return efficiency
}

// CleanupEfficiency represents cleanup operation efficiency metrics
type CleanupEfficiency struct {
	TotalRuns            uint64        `json:"total_runs"`
	SuccessfulRuns       uint64        `json:"successful_runs"`
	FailedRuns           uint64        `json:"failed_runs"`
	SuccessRate          float64       `json:"success_rate"`
	TotalCleaned         uint64        `json:"total_cleaned"`
	AverageCleanedPerRun float64       `json:"average_cleaned_per_run"`
	AverageDuration      time.Duration `json:"average_duration"`
}

// EstimateNextCleanup estimates when the next cleanup will occur
func (cm *CleanupManager) EstimateNextCleanup() time.Time {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.lastCleanupTime.IsZero() {
		return time.Now().Add(cm.config.Interval)
	}

	return cm.lastCleanupTime.Add(cm.config.Interval)
}

// ShouldRunCleanup determines if cleanup should be run based on current conditions
func (cm *CleanupManager) ShouldRunCleanup() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Run if we haven't run cleanup yet
	if cm.lastCleanupTime.IsZero() {
		return true
	}

	// Run if interval has passed
	return time.Since(cm.lastCleanupTime) >= cm.config.Interval
}
