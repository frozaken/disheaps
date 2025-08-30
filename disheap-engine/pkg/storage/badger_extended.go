package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// StoreLease stores a lease in the database
func (bs *BadgerStorage) StoreLease(ctx context.Context, topic string, partition uint32, lease *models.Lease) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("store_lease", time.Since(start))
	}()

	// Validate lease
	if err := lease.Validate(); err != nil {
		return fmt.Errorf("lease validation failed: %w", err)
	}

	key := bs.keyBuilder.LeaseKey(topic, partition, lease.MessageID)

	// Serialize lease
	data, err := json.Marshal(lease)
	if err != nil {
		return ErrStorageOperation{
			Operation: "serialize_lease",
			Key:       key,
			Cause:     err,
		}
	}

	// Store with TTL based on lease deadline
	err = bs.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), data)

		// Set TTL to lease deadline + grace period
		ttl := time.Until(lease.Deadline) + (5 * time.Minute) // 5min grace
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}

		return txn.SetEntry(entry)
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "store_lease",
			Key:       key,
			Cause:     err,
		}
	}

	bs.recordSize("lease", len(data))
	bs.logger.Debug("Lease stored successfully",
		zap.String("token", string(lease.Token)),
		zap.String("message_id", string(lease.MessageID)),
		zap.String("holder", lease.Holder),
	)

	return nil
}

// GetLease retrieves a lease from the database
func (bs *BadgerStorage) GetLease(ctx context.Context, topic string, partition uint32, msgID models.MessageID) (*models.Lease, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("get_lease", time.Since(start))
	}()

	key := bs.keyBuilder.LeaseKey(topic, partition, msgID)

	var lease models.Lease
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &lease)
		})
	})

	if err != nil {
		if err == ErrNotFound {
			return nil, err
		}
		return nil, ErrStorageOperation{
			Operation: "get_lease",
			Key:       key,
			Cause:     err,
		}
	}

	return &lease, nil
}

// DeleteLease removes a lease from the database
func (bs *BadgerStorage) DeleteLease(ctx context.Context, topic string, partition uint32, msgID models.MessageID) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("delete_lease", time.Since(start))
	}()

	key := bs.keyBuilder.LeaseKey(topic, partition, msgID)

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "delete_lease",
			Key:       key,
			Cause:     err,
		}
	}

	return nil
}

// ListExpiredLeases retrieves expired leases
func (bs *BadgerStorage) ListExpiredLeases(ctx context.Context, before time.Time, limit int) ([]*models.Lease, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("list_expired_leases", time.Since(start))
	}()

	var expiredLeases []*models.Lease

	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(PrefixLease)
		count := 0

		for it.Seek(prefix); it.ValidForPrefix(prefix) && count < limit; it.Next() {
			item := it.Item()

			var lease models.Lease
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &lease)
			})
			if err != nil {
				bs.logger.Warn("Failed to unmarshal lease",
					zap.Error(err),
					zap.String("key", string(item.Key())),
				)
				continue
			}

			if lease.Deadline.Before(before) {
				expiredLeases = append(expiredLeases, &lease)
				count++
			}
		}

		return nil
	})

	if err != nil {
		return nil, ErrStorageOperation{
			Operation: "list_expired_leases",
			Key:       "lease_prefix",
			Cause:     err,
		}
	}

	return expiredLeases, nil
}

// StoreDedup stores a deduplication entry with TTL
func (bs *BadgerStorage) StoreDedup(ctx context.Context, topic string, producerKey models.ProducerKey, seq uint64, msgID models.MessageID, ttl time.Duration) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("store_dedup", time.Since(start))
	}()

	key := bs.keyBuilder.DedupKey(topic, producerKey, seq)

	// Store message ID as value
	data := []byte(string(msgID))

	err := bs.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), data).WithTTL(ttl)
		return txn.SetEntry(entry)
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "store_dedup",
			Key:       key,
			Cause:     err,
		}
	}

	bs.recordSize("dedup", len(data))
	bs.logger.Debug("Dedup entry stored",
		zap.String("producer_key", producerKey.String()),
		zap.Uint64("sequence", seq),
		zap.String("message_id", string(msgID)),
		zap.Duration("ttl", ttl),
	)

	return nil
}

// CheckDedup checks if a deduplication entry exists
func (bs *BadgerStorage) CheckDedup(ctx context.Context, topic string, producerKey models.ProducerKey, seq uint64) (models.MessageID, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return "", false, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("check_dedup", time.Since(start))
	}()

	key := bs.keyBuilder.DedupKey(topic, producerKey, seq)

	var msgID models.MessageID
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			msgID = models.MessageID(string(val))
			return nil
		})
	})

	if err != nil {
		if err == ErrNotFound {
			return "", false, nil
		}
		return "", false, ErrStorageOperation{
			Operation: "check_dedup",
			Key:       key,
			Cause:     err,
		}
	}

	return msgID, true, nil
}

// CleanupExpiredDedup removes expired deduplication entries
func (bs *BadgerStorage) CleanupExpiredDedup(ctx context.Context, before time.Time) (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("cleanup_expired_dedup", time.Since(start))
	}()

	// BadgerDB automatically handles TTL expiration, but we can force cleanup
	cleaned := 0

	// Run value log GC to clean up expired entries
	err := bs.db.RunValueLogGC(0.7) // Aggressive cleanup
	if err != nil && err != badger.ErrNoRewrite {
		return 0, ErrStorageOperation{
			Operation: "cleanup_expired_dedup",
			Key:       "value_log_gc",
			Cause:     err,
		}
	}

	bs.logger.Debug("Expired dedup cleanup completed",
		zap.Int("cleaned_count", cleaned),
	)

	return cleaned, nil
}

// StoreSnapshot stores a snapshot (heap or timer)
func (bs *BadgerStorage) StoreSnapshot(ctx context.Context, topic string, partition uint32, snapshotType SnapshotType, data []byte) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("store_snapshot", time.Since(start))
	}()

	key := bs.keyBuilder.SnapshotKey(topic, partition, snapshotType)

	err := bs.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), data)
		return txn.SetEntry(entry)
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "store_snapshot",
			Key:       key,
			Cause:     err,
		}
	}

	bs.recordSize("snapshot", len(data))
	bs.logger.Debug("Snapshot stored",
		zap.String("topic", topic),
		zap.Uint32("partition", partition),
		zap.String("type", snapshotType.String()),
		zap.Int("size_bytes", len(data)),
	)

	return nil
}

// GetSnapshot retrieves a snapshot
func (bs *BadgerStorage) GetSnapshot(ctx context.Context, topic string, partition uint32, snapshotType SnapshotType) ([]byte, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("get_snapshot", time.Since(start))
	}()

	key := bs.keyBuilder.SnapshotKey(topic, partition, snapshotType)

	var data []byte
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			data = make([]byte, len(val))
			copy(data, val)
			return nil
		})
	})

	if err != nil {
		if err == ErrNotFound {
			return nil, err
		}
		return nil, ErrStorageOperation{
			Operation: "get_snapshot",
			Key:       key,
			Cause:     err,
		}
	}

	return data, nil
}

// DeleteSnapshot removes a snapshot
func (bs *BadgerStorage) DeleteSnapshot(ctx context.Context, topic string, partition uint32, snapshotType SnapshotType) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("delete_snapshot", time.Since(start))
	}()

	key := bs.keyBuilder.SnapshotKey(topic, partition, snapshotType)

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "delete_snapshot",
			Key:       key,
			Cause:     err,
		}
	}

	return nil
}

// StoreTopicConfig stores topic configuration
func (bs *BadgerStorage) StoreTopicConfig(ctx context.Context, config *models.TopicConfig) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("store_topic_config", time.Since(start))
	}()

	// Validate topic name according to INTERFACE_CONTRACTS.md
	if err := ValidateTopicName(config.Name); err != nil {
		return fmt.Errorf("invalid topic name: %w", err)
	}

	// Validate config
	if err := config.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	key := bs.keyBuilder.TopicConfigKey(config.Name)

	// Serialize config
	data, err := json.Marshal(config)
	if err != nil {
		return ErrStorageOperation{
			Operation: "serialize_topic_config",
			Key:       key,
			Cause:     err,
		}
	}

	err = bs.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), data)
		return txn.SetEntry(entry)
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "store_topic_config",
			Key:       key,
			Cause:     err,
		}
	}

	bs.recordSize("topic_config", len(data))
	return nil
}

// GetTopicConfig retrieves topic configuration
func (bs *BadgerStorage) GetTopicConfig(ctx context.Context, topic string) (*models.TopicConfig, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("get_topic_config", time.Since(start))
	}()

	key := bs.keyBuilder.TopicConfigKey(topic)

	var config models.TopicConfig
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &config)
		})
	})

	if err != nil {
		if err == ErrNotFound {
			return nil, err
		}
		return nil, ErrStorageOperation{
			Operation: "get_topic_config",
			Key:       key,
			Cause:     err,
		}
	}

	return &config, nil
}

// DeleteTopicConfig removes topic configuration
func (bs *BadgerStorage) DeleteTopicConfig(ctx context.Context, topic string) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("delete_topic_config", time.Since(start))
	}()

	key := bs.keyBuilder.TopicConfigKey(topic)

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "delete_topic_config",
			Key:       key,
			Cause:     err,
		}
	}

	return nil
}

// ListTopicConfigs retrieves all topic configurations
func (bs *BadgerStorage) ListTopicConfigs(ctx context.Context) ([]*models.TopicConfig, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("list_topic_configs", time.Since(start))
	}()

	prefix := []byte(bs.keyBuilder.TopicConfigPrefix())
	var configs []*models.TopicConfig

	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			var config models.TopicConfig
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &config)
			})
			if err != nil {
				bs.logger.Warn("Failed to unmarshal topic config",
					zap.Error(err),
					zap.String("key", string(item.Key())),
				)
				continue
			}

			configs = append(configs, &config)
		}

		return nil
	})

	if err != nil {
		return nil, ErrStorageOperation{
			Operation: "list_topic_configs",
			Key:       string(prefix),
			Cause:     err,
		}
	}

	return configs, nil
}

// Compact performs manual compaction
func (bs *BadgerStorage) Compact(ctx context.Context) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("compact", time.Since(start))
	}()

	// Run value log GC multiple times for thorough cleanup
	for i := 0; i < 3; i++ {
		err := bs.db.RunValueLogGC(0.5)
		if err != nil {
			if err == badger.ErrNoRewrite {
				break // No more cleanup needed
			}
			return ErrStorageOperation{
				Operation: "compact",
				Key:       fmt.Sprintf("gc_run_%d", i),
				Cause:     err,
			}
		}
	}

	bs.logger.Info("Manual compaction completed")
	return nil
}

// Stats returns storage statistics
func (bs *BadgerStorage) Stats(ctx context.Context) (StorageStats, error) {
	if err := bs.checkOpen(); err != nil {
		return StorageStats{}, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("stats", time.Since(start))
	}()

	lsm, vlog := bs.db.Size()

	stats := StorageStats{
		TotalBytes:      uint64(lsm + vlog),
		IsHealthy:       true,
		LastHealthCheck: time.Now(),
		EngineStats: map[string]interface{}{
			"lsm_size":  lsm,
			"vlog_size": vlog,
		},
	}

	// Count different data types by scanning keys
	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys for counting
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())

			if len(key) == 0 {
				continue
			}

			// Extract prefix from key (everything before first slash)
			parts := strings.Split(key, "/")
			if len(parts) == 0 {
				continue
			}
			prefix := parts[0]

			switch prefix {
			case PrefixMessage:
				stats.TotalMessages++
			case PrefixLease:
				stats.TotalLeases++
			case PrefixDedup:
				stats.TotalDedupEntries++
			case PrefixHeapSnapshot, PrefixTimerSnapshot:
				stats.TotalSnapshots++
			}
		}

		return nil
	})

	if err != nil {
		bs.logger.Warn("Failed to collect detailed stats", zap.Error(err))
		stats.IsHealthy = false
	}

	return stats, nil
}

// Batch creates a new storage batch for atomic operations
func (bs *BadgerStorage) Batch() StorageBatch {
	return NewBadgerBatch(bs)
}
