package lease

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// Manager coordinates lease lifecycle across partitions
type Manager struct {
	mu         sync.RWMutex
	logger     *zap.Logger
	registries map[string]*models.LeaseRegistry // keyed by "topic:partition"

	// Configuration
	defaultTimeout       time.Duration
	maxExtensions        uint32
	cleanupInterval      time.Duration
	extensionGracePeriod time.Duration

	// Background processing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Statistics
	stats LeaseManagerStats
}

// LeaseManagerStats tracks lease manager performance
type LeaseManagerStats struct {
	mu                   sync.RWMutex
	TotalLeases          uint64
	ActiveLeases         uint64
	ExpiredLeases        uint64
	ExtendedLeases       uint64
	RevokedLeases        uint64
	CleanupRuns          uint64
	AverageLeaseLifetime time.Duration
	LastCleanupTime      time.Time
}

// Config holds lease manager configuration
type Config struct {
	DefaultTimeout       time.Duration
	MaxExtensions        uint32
	CleanupInterval      time.Duration
	ExtensionGracePeriod time.Duration
}

// DefaultConfig returns sensible defaults for lease management
func DefaultConfig() *Config {
	return &Config{
		DefaultTimeout:       30 * time.Second,
		MaxExtensions:        10,
		CleanupInterval:      10 * time.Second,
		ExtensionGracePeriod: 5 * time.Second,
	}
}

// NewManager creates a new lease manager
func NewManager(config *Config, logger *zap.Logger) *Manager {
	if config == nil {
		config = DefaultConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		logger:               logger.With(zap.String("component", "lease_manager")),
		registries:           make(map[string]*models.LeaseRegistry),
		defaultTimeout:       config.DefaultTimeout,
		maxExtensions:        config.MaxExtensions,
		cleanupInterval:      config.CleanupInterval,
		extensionGracePeriod: config.ExtensionGracePeriod,
		ctx:                  ctx,
		cancel:               cancel,
	}
}

// Start begins background lease management
func (lm *Manager) Start() error {
	lm.logger.Info("Starting lease manager",
		zap.Duration("cleanup_interval", lm.cleanupInterval),
		zap.Duration("default_timeout", lm.defaultTimeout))

	// Start cleanup goroutine
	lm.wg.Add(1)
	go lm.cleanupLoop()

	return nil
}

// Stop gracefully shuts down the lease manager
func (lm *Manager) Stop() error {
	lm.logger.Info("Stopping lease manager")

	lm.cancel()
	lm.wg.Wait()

	lm.logger.Info("Lease manager stopped")
	return nil
}

// GetRegistry returns or creates a lease registry for a topic/partition
func (lm *Manager) GetRegistry(topic string, partition uint32) *models.LeaseRegistry {
	key := fmt.Sprintf("%s:%d", topic, partition)

	lm.mu.RLock()
	registry, exists := lm.registries[key]
	lm.mu.RUnlock()

	if exists {
		return registry
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Double-check after acquiring write lock
	if registry, exists := lm.registries[key]; exists {
		return registry
	}

	registry = models.NewLeaseRegistry(lm.logger)
	lm.registries[key] = registry

	lm.logger.Debug("Created lease registry",
		zap.String("topic", topic),
		zap.Uint32("partition", partition))

	return registry
}

// GrantLease creates and grants a new lease
func (lm *Manager) GrantLease(ctx context.Context, topic string, partition uint32, messageID models.MessageID, holder string, timeout time.Duration) (*models.Lease, error) {
	registry := lm.GetRegistry(topic, partition)

	if timeout == 0 {
		timeout = lm.defaultTimeout
	}

	lease := models.NewLease(messageID, holder, timeout)

	err := registry.Grant(ctx, lease)
	if err != nil {
		lm.logger.Debug("Failed to grant lease",
			zap.String("message_id", string(messageID)),
			zap.String("holder", holder),
			zap.Error(err))
		return nil, fmt.Errorf("failed to grant lease: %w", err)
	}

	// Update statistics
	lm.updateStats(func(stats *LeaseManagerStats) {
		stats.TotalLeases++
		stats.ActiveLeases++
	})

	lm.logger.Debug("Lease granted",
		zap.String("message_id", string(messageID)),
		zap.String("holder", holder),
		zap.String("token", string(lease.Token)),
		zap.Duration("timeout", timeout))

	return lease, nil
}

// ExtendLease extends an existing lease
func (lm *Manager) ExtendLease(ctx context.Context, topic string, partition uint32, token models.LeaseToken, extension time.Duration) (*models.Lease, error) {
	registry := lm.GetRegistry(topic, partition)

	if extension == 0 {
		extension = lm.defaultTimeout
	}

	lease, err := registry.Extend(ctx, token, extension)
	if err != nil {
		lm.logger.Debug("Failed to extend lease",
			zap.String("token", string(token)),
			zap.Error(err))
		return nil, fmt.Errorf("failed to extend lease: %w", err)
	}

	// Check extension limits
	if lease.Extensions > lm.maxExtensions {
		lm.logger.Warn("Lease extension limit exceeded",
			zap.String("message_id", string(lease.MessageID)),
			zap.String("holder", lease.Holder),
			zap.Uint32("extensions", lease.Extensions),
			zap.Uint32("max_extensions", lm.maxExtensions))
		return lease, fmt.Errorf("lease extension limit exceeded: %d > %d", lease.Extensions, lm.maxExtensions)
	}

	// Update statistics
	lm.updateStats(func(stats *LeaseManagerStats) {
		stats.ExtendedLeases++
	})

	lm.logger.Debug("Lease extended",
		zap.String("message_id", string(lease.MessageID)),
		zap.String("holder", lease.Holder),
		zap.String("token", string(token)),
		zap.Duration("extension", extension),
		zap.Time("new_deadline", lease.Deadline))

	return lease, nil
}

// RevokeLease revokes an existing lease
func (lm *Manager) RevokeLease(ctx context.Context, topic string, partition uint32, messageID models.MessageID, token models.LeaseToken) (*models.Lease, error) {
	registry := lm.GetRegistry(topic, partition)

	lease, err := registry.Revoke(ctx, messageID, token)
	if err != nil {
		lm.logger.Debug("Failed to revoke lease",
			zap.String("message_id", string(messageID)),
			zap.String("token", string(token)),
			zap.Error(err))
		return nil, fmt.Errorf("failed to revoke lease: %w", err)
	}

	// Update statistics
	lm.updateStats(func(stats *LeaseManagerStats) {
		stats.RevokedLeases++
		stats.ActiveLeases--
	})

	lm.logger.Debug("Lease revoked",
		zap.String("message_id", string(messageID)),
		zap.String("holder", lease.Holder),
		zap.String("token", string(token)))

	return lease, nil
}

// GetLease retrieves an existing lease
func (lm *Manager) GetLease(topic string, partition uint32, messageID models.MessageID) (*models.Lease, error) {
	registry := lm.GetRegistry(topic, partition)

	lease := registry.GetLease(messageID)
	if lease == nil {
		return nil, fmt.Errorf("lease not found for message %s", messageID)
	}

	return lease, nil
}

// IsLeased checks if a message is currently leased
func (lm *Manager) IsLeased(topic string, partition uint32, messageID models.MessageID) bool {
	lease, err := lm.GetLease(topic, partition, messageID)
	if err != nil {
		return false
	}

	return !lease.IsExpired()
}

// GetStats returns current lease manager statistics
func (lm *Manager) GetStats() LeaseManagerStats {
	lm.stats.mu.RLock()
	defer lm.stats.mu.RUnlock()

	// Update active lease count
	activeCount := uint64(0)
	lm.mu.RLock()
	for _, registry := range lm.registries {
		activeCount += uint64(registry.ActiveLeases())
	}
	lm.mu.RUnlock()

	stats := lm.stats
	stats.ActiveLeases = activeCount
	return stats
}

// cleanupLoop runs periodic cleanup of expired leases
func (lm *Manager) cleanupLoop() {
	defer lm.wg.Done()

	ticker := time.NewTicker(lm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-lm.ctx.Done():
			return
		case <-ticker.C:
			lm.cleanupExpiredLeases()
		}
	}
}

// cleanupExpiredLeases removes expired leases from all registries
func (lm *Manager) cleanupExpiredLeases() {
	lm.mu.RLock()
	registries := make(map[string]*models.LeaseRegistry, len(lm.registries))
	for k, v := range lm.registries {
		registries[k] = v
	}
	lm.mu.RUnlock()

	totalCleaned := 0
	for key, registry := range registries {
		cleaned := registry.CleanExpired()
		if len(cleaned) > 0 {
			lm.logger.Debug("Cleaned expired leases",
				zap.String("registry", key),
				zap.Int("count", len(cleaned)))
			totalCleaned += len(cleaned)
		}
	}

	if totalCleaned > 0 {
		lm.logger.Info("Lease cleanup completed",
			zap.Int("total_cleaned", totalCleaned))

		// Update statistics
		lm.updateStats(func(stats *LeaseManagerStats) {
			stats.ExpiredLeases += uint64(totalCleaned)
			stats.ActiveLeases -= uint64(totalCleaned)
			stats.CleanupRuns++
			stats.LastCleanupTime = time.Now()
		})
	}
}

// updateStats safely updates statistics
func (lm *Manager) updateStats(fn func(*LeaseManagerStats)) {
	lm.stats.mu.Lock()
	defer lm.stats.mu.Unlock()
	fn(&lm.stats)
}

// Close releases all resources
func (lm *Manager) Close() error {
	return lm.Stop()
}
