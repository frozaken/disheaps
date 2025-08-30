package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// BadgerBatch implements StorageBatch for atomic operations
type BadgerBatch struct {
	storage    *BadgerStorage
	operations []batchOperation
	size       int
}

// batchOperation represents a single operation in a batch
type batchOperation struct {
	operationType string
	key           string
	value         []byte
	ttl           time.Duration
}

// NewBadgerBatch creates a new batch
func NewBadgerBatch(storage *BadgerStorage) *BadgerBatch {
	return &BadgerBatch{
		storage:    storage,
		operations: make([]batchOperation, 0),
		size:       0,
	}
}

// StoreMessage adds a message store operation to the batch
func (bb *BadgerBatch) StoreMessage(msg *models.Message) error {
	if bb.size >= bb.storage.config.MaxBatchSize {
		return ErrBatchTooLarge
	}

	// Validate message according to interface contracts
	if err := msg.Validate(1024 * 1024); err != nil {
		return fmt.Errorf("message validation failed: %w", err)
	}

	key := bb.storage.keyBuilder.MessageKey(msg.Topic, msg.PartitionID, msg.ID)

	// Serialize message
	data, err := json.Marshal(msg)
	if err != nil {
		return ErrStorageOperation{
			Operation: "serialize_message_batch",
			Key:       key,
			Cause:     err,
		}
	}

	bb.operations = append(bb.operations, batchOperation{
		operationType: "set",
		key:           key,
		value:         data,
		ttl:           bb.storage.config.DefaultTTL,
	})

	bb.size++
	return nil
}

// DeleteMessage adds a message delete operation to the batch
func (bb *BadgerBatch) DeleteMessage(topic string, partition uint32, msgID models.MessageID) error {
	if bb.size >= bb.storage.config.MaxBatchSize {
		return ErrBatchTooLarge
	}

	key := bb.storage.keyBuilder.MessageKey(topic, partition, msgID)

	bb.operations = append(bb.operations, batchOperation{
		operationType: "delete",
		key:           key,
	})

	bb.size++
	return nil
}

// StoreLease adds a lease store operation to the batch
func (bb *BadgerBatch) StoreLease(topic string, partition uint32, lease *models.Lease) error {
	if bb.size >= bb.storage.config.MaxBatchSize {
		return ErrBatchTooLarge
	}

	// Validate lease
	if err := lease.Validate(); err != nil {
		return fmt.Errorf("lease validation failed: %w", err)
	}

	key := bb.storage.keyBuilder.LeaseKey(topic, partition, lease.MessageID)

	// Serialize lease
	data, err := json.Marshal(lease)
	if err != nil {
		return ErrStorageOperation{
			Operation: "serialize_lease_batch",
			Key:       key,
			Cause:     err,
		}
	}

	// Calculate TTL based on lease deadline + grace period
	ttl := time.Until(lease.Deadline) + (5 * time.Minute)
	if ttl <= 0 {
		ttl = 5 * time.Minute // Minimum grace period
	}

	bb.operations = append(bb.operations, batchOperation{
		operationType: "set",
		key:           key,
		value:         data,
		ttl:           ttl,
	})

	bb.size++
	return nil
}

// DeleteLease adds a lease delete operation to the batch
func (bb *BadgerBatch) DeleteLease(topic string, partition uint32, msgID models.MessageID) error {
	if bb.size >= bb.storage.config.MaxBatchSize {
		return ErrBatchTooLarge
	}

	key := bb.storage.keyBuilder.LeaseKey(topic, partition, msgID)

	bb.operations = append(bb.operations, batchOperation{
		operationType: "delete",
		key:           key,
	})

	bb.size++
	return nil
}

// StoreDedup adds a dedup store operation to the batch
func (bb *BadgerBatch) StoreDedup(topic string, producerKey models.ProducerKey, seq uint64, msgID models.MessageID, ttl time.Duration) error {
	if bb.size >= bb.storage.config.MaxBatchSize {
		return ErrBatchTooLarge
	}

	key := bb.storage.keyBuilder.DedupKey(topic, producerKey, seq)
	data := []byte(string(msgID))

	bb.operations = append(bb.operations, batchOperation{
		operationType: "set",
		key:           key,
		value:         data,
		ttl:           ttl,
	})

	bb.size++
	return nil
}

// StoreTopicConfig adds a topic config store operation to the batch
func (bb *BadgerBatch) StoreTopicConfig(config *models.TopicConfig) error {
	if bb.size >= bb.storage.config.MaxBatchSize {
		return ErrBatchTooLarge
	}

	// Validate topic name according to INTERFACE_CONTRACTS.md
	if err := ValidateTopicName(config.Name); err != nil {
		return fmt.Errorf("invalid topic name: %w", err)
	}

	// Validate config
	if err := config.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	key := bb.storage.keyBuilder.TopicConfigKey(config.Name)

	// Serialize config
	data, err := json.Marshal(config)
	if err != nil {
		return ErrStorageOperation{
			Operation: "serialize_topic_config_batch",
			Key:       key,
			Cause:     err,
		}
	}

	bb.operations = append(bb.operations, batchOperation{
		operationType: "set",
		key:           key,
		value:         data,
	})

	bb.size++
	return nil
}

// StorePartitionInfo adds a partition info store operation to the batch
func (bb *BadgerBatch) StorePartitionInfo(info *models.PartitionInfo) error {
	if bb.size >= bb.storage.config.MaxBatchSize {
		return ErrBatchTooLarge
	}

	key := bb.storage.keyBuilder.PartitionInfoKey(info.Topic, uint32(info.ID))

	// Serialize partition info
	data, err := json.Marshal(info)
	if err != nil {
		return ErrStorageOperation{
			Operation: "serialize_partition_info_batch",
			Key:       key,
			Cause:     err,
		}
	}

	bb.operations = append(bb.operations, batchOperation{
		operationType: "set",
		key:           key,
		value:         data,
	})

	bb.size++
	return nil
}

// Size returns the number of operations in the batch
func (bb *BadgerBatch) Size() int {
	return bb.size
}

// Commit applies all operations in the batch atomically
func (bb *BadgerBatch) Commit(ctx context.Context) error {
	if err := bb.storage.checkOpen(); err != nil {
		return err
	}

	if bb.size == 0 {
		return nil // Nothing to commit
	}

	start := time.Now()
	defer func() {
		bb.storage.recordOperation("batch_commit", time.Since(start))
	}()

	// Execute all operations in a single transaction
	err := bb.storage.db.Update(func(txn *badger.Txn) error {
		for _, op := range bb.operations {
			switch op.operationType {
			case "set":
				entry := badger.NewEntry([]byte(op.key), op.value)
				if op.ttl > 0 {
					entry = entry.WithTTL(op.ttl)
				}
				if err := txn.SetEntry(entry); err != nil {
					return fmt.Errorf("failed to set key %s: %w", op.key, err)
				}

			case "delete":
				if err := txn.Delete([]byte(op.key)); err != nil {
					return fmt.Errorf("failed to delete key %s: %w", op.key, err)
				}

			default:
				return fmt.Errorf("unknown operation type: %s", op.operationType)
			}
		}
		return nil
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "batch_commit",
			Key:       fmt.Sprintf("batch_size_%d", bb.size),
			Cause:     err,
		}
	}

	// Record metrics for batch operation
	bb.storage.storageOps.Add(ctx, int64(bb.size),
		metric.WithAttributes(attribute.String("operation", "batch_commit")))

	bb.storage.logger.Debug("Batch committed successfully",
		zap.Int("operations_count", bb.size),
		zap.Duration("duration", time.Since(start)),
	)

	// Clear operations after successful commit
	bb.operations = bb.operations[:0]
	bb.size = 0

	return nil
}

// Rollback discards all operations in the batch
func (bb *BadgerBatch) Rollback() error {
	bb.storage.logger.Debug("Batch rolled back",
		zap.Int("operations_count", bb.size),
	)

	// Simply clear the operations
	bb.operations = bb.operations[:0]
	bb.size = 0

	return nil
}
