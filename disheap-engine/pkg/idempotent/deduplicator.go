package idempotent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
	"go.uber.org/zap"
)

// DeduplicationConfig holds configuration for the deduplicator
type DeduplicationConfig struct {
	// DefaultTTL is the default time-to-live for deduplication entries
	DefaultTTL time.Duration `json:"default_ttl"`

	// MaxTTL is the maximum allowed TTL for deduplication entries
	MaxTTL time.Duration `json:"max_ttl"`

	// MinTTL is the minimum allowed TTL for deduplication entries
	MinTTL time.Duration `json:"min_ttl"`

	// CleanupInterval is how often expired entries are cleaned up
	CleanupInterval time.Duration `json:"cleanup_interval"`

	// MaxMemoryMB is the maximum memory to use for in-memory cache (in MB)
	MaxMemoryMB int `json:"max_memory_mb"`

	// WindowSize is the size of the sliding window for sequence tracking
	WindowSize uint64 `json:"window_size"`
}

// DefaultDeduplicationConfig returns the default configuration
func DefaultDeduplicationConfig() *DeduplicationConfig {
	return &DeduplicationConfig{
		DefaultTTL:      24 * time.Hour,     // 24 hours as per spec
		MaxTTL:          7 * 24 * time.Hour, // 7 days max
		MinTTL:          1 * time.Minute,    // 1 minute min
		CleanupInterval: 5 * time.Minute,    // Clean up every 5 minutes
		MaxMemoryMB:     100,                // 100MB cache limit
		WindowSize:      10000,              // Track last 10K sequences per producer
	}
}

// Deduplicator implements idempotent enqueue with producer sequence tracking
type Deduplicator struct {
	config  *DeduplicationConfig
	storage storage.Storage
	logger  *zap.Logger

	// In-memory cache for fast lookups
	cache *DeduplicationCache

	// Mutex to synchronize check-and-store operations
	mu sync.Mutex

	// Background cleanup
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewDeduplicator creates a new deduplicator instance
func NewDeduplicator(config *DeduplicationConfig, storage storage.Storage, logger *zap.Logger) *Deduplicator {
	if config == nil {
		config = DefaultDeduplicationConfig()
	}

	return &Deduplicator{
		config:  config,
		storage: storage,
		logger:  logger.Named("deduplicator"),
		cache:   NewDeduplicationCache(config.MaxMemoryMB, config.WindowSize),
		stopCh:  make(chan struct{}),
	}
}

// Start begins background cleanup operations
func (d *Deduplicator) Start(ctx context.Context) error {
	d.logger.Info("starting deduplicator",
		zap.Duration("default_ttl", d.config.DefaultTTL),
		zap.Duration("cleanup_interval", d.config.CleanupInterval),
		zap.Int("max_memory_mb", d.config.MaxMemoryMB),
		zap.Uint64("window_size", d.config.WindowSize))

	// Start background cleanup goroutine
	d.wg.Add(1)
	go d.cleanupWorker()

	return nil
}

// Stop stops the deduplicator and cleanup operations
func (d *Deduplicator) Stop(ctx context.Context) error {
	d.stopOnce.Do(func() {
		d.logger.Info("stopping deduplicator")
		close(d.stopCh)
	})

	d.wg.Wait()
	d.logger.Info("deduplicator stopped")
	return nil
}

// CheckAndStore checks for duplicate and stores if not duplicate
// Returns (message_id, is_duplicate, error)
func (d *Deduplicator) CheckAndStore(ctx context.Context, topic string, producerID string, epoch uint64, sequence uint64, messageID models.MessageID, ttl time.Duration) (models.MessageID, bool, error) {
	if ttl == 0 {
		ttl = d.config.DefaultTTL
	}

	// Validate TTL bounds
	if ttl < d.config.MinTTL {
		ttl = d.config.MinTTL
	} else if ttl > d.config.MaxTTL {
		ttl = d.config.MaxTTL
	}

	producerKey := models.ProducerKey{
		ID:    producerID,
		Epoch: epoch,
	}

	// Synchronize the entire check-and-store operation to prevent race conditions
	d.mu.Lock()
	defer d.mu.Unlock()

	// First check in-memory cache for fast lookup
	if existingID, exists := d.cache.Check(topic, producerKey, sequence); exists {
		d.logger.Debug("duplicate found in cache",
			zap.String("topic", topic),
			zap.String("producer_id", producerID),
			zap.Uint64("epoch", epoch),
			zap.Uint64("sequence", sequence),
			zap.String("existing_id", existingID.String()))
		return existingID, true, nil
	}

	// Check persistent storage
	existingID, exists, err := d.storage.CheckDedup(ctx, topic, producerKey, sequence)
	if err != nil {
		d.logger.Error("failed to check dedup in storage",
			zap.String("topic", topic),
			zap.String("producer_id", producerID),
			zap.Uint64("epoch", epoch),
			zap.Uint64("sequence", sequence),
			zap.Error(err))
		return "", false, fmt.Errorf("failed to check dedup: %w", err)
	}

	if exists {
		// Found in storage, update cache for future lookups
		d.cache.Store(topic, producerKey, sequence, existingID)
		d.logger.Debug("duplicate found in storage",
			zap.String("topic", topic),
			zap.String("producer_id", producerID),
			zap.Uint64("epoch", epoch),
			zap.Uint64("sequence", sequence),
			zap.String("existing_id", existingID.String()))
		return existingID, true, nil
	}

	// Not a duplicate, store the new entry
	err = d.storage.StoreDedup(ctx, topic, producerKey, sequence, messageID, ttl)
	if err != nil {
		d.logger.Error("failed to store dedup entry",
			zap.String("topic", topic),
			zap.String("producer_id", producerID),
			zap.Uint64("epoch", epoch),
			zap.Uint64("sequence", sequence),
			zap.String("message_id", messageID.String()),
			zap.Error(err))
		return "", false, fmt.Errorf("failed to store dedup: %w", err)
	}

	// Store in cache for future lookups
	d.cache.Store(topic, producerKey, sequence, messageID)

	d.logger.Debug("stored new dedup entry",
		zap.String("topic", topic),
		zap.String("producer_id", producerID),
		zap.Uint64("epoch", epoch),
		zap.Uint64("sequence", sequence),
		zap.String("message_id", messageID.String()),
		zap.Duration("ttl", ttl))

	return messageID, false, nil
}

// CheckDuplicate only checks for duplicates without storing
func (d *Deduplicator) CheckDuplicate(ctx context.Context, topic string, producerID string, epoch uint64, sequence uint64) (models.MessageID, bool, error) {
	producerKey := models.ProducerKey{
		ID:    producerID,
		Epoch: epoch,
	}

	// First check in-memory cache
	if existingID, exists := d.cache.Check(topic, producerKey, sequence); exists {
		return existingID, true, nil
	}

	// Check persistent storage
	existingID, exists, err := d.storage.CheckDedup(ctx, topic, producerKey, sequence)
	if err != nil {
		return "", false, fmt.Errorf("failed to check dedup: %w", err)
	}

	if exists {
		// Update cache for future lookups
		d.cache.Store(topic, producerKey, sequence, existingID)
	}

	return existingID, exists, nil
}

// StoreDedupEntry stores a deduplication entry
func (d *Deduplicator) StoreDedupEntry(ctx context.Context, topic string, producerID string, epoch uint64, sequence uint64, messageID models.MessageID, ttl time.Duration) error {
	if ttl == 0 {
		ttl = d.config.DefaultTTL
	}

	// Validate TTL bounds
	if ttl < d.config.MinTTL {
		ttl = d.config.MinTTL
	} else if ttl > d.config.MaxTTL {
		ttl = d.config.MaxTTL
	}

	producerKey := models.ProducerKey{
		ID:    producerID,
		Epoch: epoch,
	}

	err := d.storage.StoreDedup(ctx, topic, producerKey, sequence, messageID, ttl)
	if err != nil {
		return fmt.Errorf("failed to store dedup entry: %w", err)
	}

	// Store in cache
	d.cache.Store(topic, producerKey, sequence, messageID)

	return nil
}

// cleanupWorker runs periodic cleanup of expired entries
func (d *Deduplicator) cleanupWorker() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.runCleanup()
		case <-d.stopCh:
			return
		}
	}
}

// runCleanup performs cleanup of expired entries
func (d *Deduplicator) runCleanup() {
	start := time.Now()

	// Clean up expired entries from storage
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cleaned, err := d.storage.CleanupExpiredDedup(ctx, time.Now())
	if err != nil {
		d.logger.Error("failed to cleanup expired dedup entries", zap.Error(err))
		return
	}

	// Clean up expired entries from cache
	cacheEvicted := d.cache.EvictExpired()

	duration := time.Since(start)

	if cleaned > 0 || cacheEvicted > 0 {
		d.logger.Info("cleanup completed",
			zap.Int("storage_cleaned", cleaned),
			zap.Int("cache_evicted", cacheEvicted),
			zap.Duration("duration", duration))
	} else {
		d.logger.Debug("cleanup completed - no entries to clean",
			zap.Duration("duration", duration))
	}
}

// GetStats returns statistics about the deduplicator
func (d *Deduplicator) GetStats() *DeduplicationStats {
	cacheStats := d.cache.GetStats()

	return &DeduplicationStats{
		CacheHits:       cacheStats.Hits,
		CacheMisses:     cacheStats.Misses,
		CacheSize:       cacheStats.Size,
		CacheEvictions:  cacheStats.Evictions,
		CacheMaxSize:    cacheStats.MaxSize,
		WindowSize:      d.config.WindowSize,
		DefaultTTL:      d.config.DefaultTTL,
		CleanupInterval: d.config.CleanupInterval,
	}
}

// DeduplicationStats holds statistics about deduplication performance
type DeduplicationStats struct {
	CacheHits       uint64        `json:"cache_hits"`
	CacheMisses     uint64        `json:"cache_misses"`
	CacheSize       uint64        `json:"cache_size"`
	CacheEvictions  uint64        `json:"cache_evictions"`
	CacheMaxSize    uint64        `json:"cache_max_size"`
	WindowSize      uint64        `json:"window_size"`
	DefaultTTL      time.Duration `json:"default_ttl"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
}

// IsHealthy returns true if the deduplicator is operating normally
func (d *Deduplicator) IsHealthy() bool {
	// Check if cache is functioning
	if d.cache == nil {
		return false
	}

	// Check if storage is accessible
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := d.storage.Health(ctx); err != nil {
		return false
	}

	return true
}

// GetConfig returns the current deduplicator configuration
func (d *Deduplicator) GetConfig() *DeduplicationConfig {
	return d.config
}

// UpdateConfig updates the deduplicator configuration
func (d *Deduplicator) UpdateConfig(newConfig *DeduplicationConfig) error {
	if newConfig == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// Validate configuration
	if newConfig.DefaultTTL <= 0 {
		return fmt.Errorf("default TTL must be positive")
	}
	if newConfig.MaxTTL < newConfig.DefaultTTL {
		return fmt.Errorf("max TTL must be >= default TTL")
	}
	if newConfig.MinTTL > newConfig.DefaultTTL {
		return fmt.Errorf("min TTL must be <= default TTL")
	}
	if newConfig.CleanupInterval <= 0 {
		return fmt.Errorf("cleanup interval must be positive")
	}
	if newConfig.MaxMemoryMB <= 0 {
		return fmt.Errorf("max memory MB must be positive")
	}
	if newConfig.WindowSize == 0 {
		return fmt.Errorf("window size must be positive")
	}

	d.config = newConfig
	d.cache.UpdateConfig(newConfig.MaxMemoryMB, newConfig.WindowSize)

	d.logger.Info("updated deduplicator configuration",
		zap.Duration("default_ttl", newConfig.DefaultTTL),
		zap.Duration("cleanup_interval", newConfig.CleanupInterval),
		zap.Int("max_memory_mb", newConfig.MaxMemoryMB),
		zap.Uint64("window_size", newConfig.WindowSize))

	return nil
}
