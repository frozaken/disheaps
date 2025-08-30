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

	lm.logger.Debug("Lease granted",
		zap.String("message_id", string(messageID)),
		zap.String("holder", holder),
		zap.String("token", string(lease.Token)),
		zap.Duration("timeout", timeout))

	return lease, nil
}

// GetLease retrieves an existing lease
func (lm *Manager) GetLease(topic string, partition uint32, messageID models.MessageID) (*models.Lease, error) {
	registry := lm.GetRegistry(topic, partition)

	lease, exists := registry.Get(messageID)
	if !exists {
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

// Close releases all resources
func (lm *Manager) Close() error {
	return nil
}
