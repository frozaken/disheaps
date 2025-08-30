package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// StorePartitionInfo stores partition information
func (bs *BadgerStorage) StorePartitionInfo(ctx context.Context, info *models.PartitionInfo) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("store_partition_info", time.Since(start))
	}()

	// Validate topic name according to INTERFACE_CONTRACTS.md
	if err := ValidateTopicName(info.Topic); err != nil {
		return ErrInvalidConfig{Field: "topic", Issue: err.Error()}
	}

	key := bs.keyBuilder.PartitionInfoKey(info.Topic, uint32(info.ID))

	// Update last updated timestamp
	info.LastUpdated = time.Now()

	// Serialize partition info
	data, err := json.Marshal(info)
	if err != nil {
		return ErrStorageOperation{
			Operation: "serialize_partition_info",
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
			Operation: "store_partition_info",
			Key:       key,
			Cause:     err,
		}
	}

	bs.recordSize("partition_info", len(data))
	bs.logger.Debug("Partition info stored successfully",
		zap.String("topic", info.Topic),
		zap.Uint32("partition_id", uint32(info.ID)),
		zap.String("leader_node", info.LeaderNode),
		zap.Bool("is_healthy", info.IsHealthy),
		zap.Bool("is_throttled", info.IsThrottled),
	)

	return nil
}

// GetPartitionInfo retrieves partition information
func (bs *BadgerStorage) GetPartitionInfo(ctx context.Context, topic string, partition uint32) (*models.PartitionInfo, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("get_partition_info", time.Since(start))
	}()

	key := bs.keyBuilder.PartitionInfoKey(topic, partition)

	var info models.PartitionInfo
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &info)
		})
	})

	if err != nil {
		if err == ErrNotFound {
			return nil, err
		}
		return nil, ErrStorageOperation{
			Operation: "get_partition_info",
			Key:       key,
			Cause:     err,
		}
	}

	return &info, nil
}

// ListPartitionInfos retrieves all partition information for a topic
func (bs *BadgerStorage) ListPartitionInfos(ctx context.Context, topic string) ([]*models.PartitionInfo, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("list_partition_infos", time.Since(start))
	}()

	prefix := []byte(bs.keyBuilder.PartitionInfoPrefix(topic))
	var infos []*models.PartitionInfo

	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			var info models.PartitionInfo
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &info)
			})
			if err != nil {
				bs.logger.Warn("Failed to unmarshal partition info",
					zap.Error(err),
					zap.String("key", string(item.Key())),
				)
				continue
			}

			infos = append(infos, &info)
		}

		return nil
	})

	if err != nil {
		return nil, ErrStorageOperation{
			Operation: "list_partition_infos",
			Key:       string(prefix),
			Cause:     err,
		}
	}

	return infos, nil
}

// UpdatePartitionHealth updates partition health status
func (bs *BadgerStorage) UpdatePartitionHealth(ctx context.Context, topic string, partition uint32, isHealthy bool, reason string) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("update_partition_health", time.Since(start))
	}()

	// Get current partition info
	info, err := bs.GetPartitionInfo(ctx, topic, partition)
	if err != nil {
		if IsNotFound(err) {
			// Create new partition info if it doesn't exist
			info = models.NewPartitionInfo(models.PartitionID(partition), topic, "", nil)
		} else {
			return err
		}
	}

	// Update health status
	info.IsHealthy = isHealthy
	info.LastUpdated = time.Now()

	if !isHealthy && reason != "" {
		info.Throttle(reason)
	} else if isHealthy {
		info.Unthrottle()
	}

	// Store updated info
	return bs.StorePartitionInfo(ctx, info)
}

// GetPartitionsByTopic returns partition information grouped by topic
func (bs *BadgerStorage) GetPartitionsByTopic(ctx context.Context) (map[string][]*models.PartitionInfo, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("get_partitions_by_topic", time.Since(start))
	}()

	result := make(map[string][]*models.PartitionInfo)
	prefix := []byte(bs.keyBuilder.PartitionInfoPrefix(""))

	// Remove trailing slash to get all partition info entries
	if len(prefix) > 0 && prefix[len(prefix)-1] == '/' {
		prefix = prefix[:len(prefix)-1]
	}

	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 50
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			var info models.PartitionInfo
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &info)
			})
			if err != nil {
				bs.logger.Warn("Failed to unmarshal partition info",
					zap.Error(err),
					zap.String("key", string(item.Key())),
				)
				continue
			}

			result[info.Topic] = append(result[info.Topic], &info)
		}

		return nil
	})

	if err != nil {
		return nil, ErrStorageOperation{
			Operation: "get_partitions_by_topic",
			Key:       string(prefix),
			Cause:     err,
		}
	}

	return result, nil
}

// DeletePartitionInfo removes partition information
func (bs *BadgerStorage) DeletePartitionInfo(ctx context.Context, topic string, partition uint32) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("delete_partition_info", time.Since(start))
	}()

	key := bs.keyBuilder.PartitionInfoKey(topic, partition)

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "delete_partition_info",
			Key:       key,
			Cause:     err,
		}
	}

	bs.logger.Debug("Partition info deleted",
		zap.String("topic", topic),
		zap.Uint32("partition", partition),
	)

	return nil
}
