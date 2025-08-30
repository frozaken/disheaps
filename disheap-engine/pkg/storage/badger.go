package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// BadgerStorage implements the Storage interface using BadgerDB
type BadgerStorage struct {
	db     *badger.DB
	config *Config
	logger *zap.Logger

	// Key building and parsing
	keyBuilder *KeyBuilder
	keyParser  *KeyParser

	// Metrics (following INTERFACE_CONTRACTS.md)
	meter         metric.Meter
	storageOps    metric.Int64Counter
	storageSize   metric.Int64Histogram
	operationTime metric.Float64Histogram

	// Lifecycle
	mu     sync.RWMutex
	closed bool

	// Background tasks
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
	wg            sync.WaitGroup
}

// NewBadgerStorage creates a new BadgerDB storage instance
func NewBadgerStorage(config *Config, logger *zap.Logger) (*BadgerStorage, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	// Initialize OpenTelemetry meter for observability (INTERFACE_CONTRACTS.md)
	meter := otel.Meter("disheap.engine.storage")

	storageOps, err := meter.Int64Counter(
		"storage_operations_total",
		metric.WithDescription("Total number of storage operations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage operations counter: %w", err)
	}

	storageSize, err := meter.Int64Histogram(
		"storage_size_bytes",
		metric.WithDescription("Storage operation size in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage size histogram: %w", err)
	}

	operationTime, err := meter.Float64Histogram(
		"storage_operation_duration_ms",
		metric.WithDescription("Storage operation duration in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create operation time histogram: %w", err)
	}

	bs := &BadgerStorage{
		config:        config,
		logger:        logger.With(zap.String("component", "badger_storage")),
		keyBuilder:    NewKeyBuilder(),
		keyParser:     NewKeyParser(),
		meter:         meter,
		storageOps:    storageOps,
		storageSize:   storageSize,
		operationTime: operationTime,
		stopCleanup:   make(chan struct{}),
	}

	return bs, nil
}

// Open initializes the BadgerDB connection and starts background tasks
func (bs *BadgerStorage) Open(ctx context.Context) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.db != nil {
		return ErrExists
	}

	// Configure BadgerDB options
	opts := badger.DefaultOptions(bs.config.Path)
	opts.Logger = &badgerLogger{logger: bs.logger}
	opts.SyncWrites = bs.config.SyncWrites
	opts.ValueThreshold = bs.config.ValueThreshold
	opts.NumMemtables = bs.config.NumMemtables
	opts.NumLevelZeroTables = bs.config.NumLevelZeroTables
	opts.NumCompactors = bs.config.NumCompactors

	if bs.config.ValueLogPath != "" {
		opts.ValueDir = bs.config.ValueLogPath
	}

	// Open database
	db, err := badger.Open(opts)
	if err != nil {
		return ErrStorageOperation{
			Operation: "open",
			Key:       bs.config.Path,
			Cause:     err,
		}
	}

	bs.db = db
	bs.closed = false

	// Start background cleanup task
	bs.startCleanupTask()

	bs.logger.Info("BadgerDB storage opened successfully",
		zap.String("path", bs.config.Path),
		zap.Bool("sync_writes", bs.config.SyncWrites),
	)

	return nil
}

// Close shuts down the storage and cleans up resources
func (bs *BadgerStorage) Close(ctx context.Context) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.closed || bs.db == nil {
		return nil
	}

	// Stop background tasks
	close(bs.stopCleanup)
	if bs.cleanupTicker != nil {
		bs.cleanupTicker.Stop()
	}
	bs.wg.Wait()

	// Close database
	err := bs.db.Close()
	bs.db = nil
	bs.closed = true

	if err != nil {
		return ErrStorageOperation{
			Operation: "close",
			Key:       bs.config.Path,
			Cause:     err,
		}
	}

	bs.logger.Info("BadgerDB storage closed successfully")
	return nil
}

// Health checks the storage health
func (bs *BadgerStorage) Health(ctx context.Context) error {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed || bs.db == nil {
		return ErrClosed
	}

	// Perform a simple read operation to verify health
	return bs.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("__health_check__"))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})
}

// StoreMessage stores a message in the database
func (bs *BadgerStorage) StoreMessage(ctx context.Context, msg *models.Message) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("store_message", time.Since(start))
	}()

	// Validate message according to interface contracts
	if err := msg.Validate(1024 * 1024); err != nil { // 1MB default max
		return fmt.Errorf("message validation failed: %w", err)
	}

	key := bs.keyBuilder.MessageKey(msg.Topic, msg.PartitionID, msg.ID)

	// Serialize message
	data, err := json.Marshal(msg)
	if err != nil {
		return ErrStorageOperation{
			Operation: "serialize_message",
			Key:       key,
			Cause:     err,
		}
	}

	// Store in database
	err = bs.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), data)
		if bs.config.DefaultTTL > 0 {
			entry = entry.WithTTL(bs.config.DefaultTTL)
		}
		return txn.SetEntry(entry)
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "store_message",
			Key:       key,
			Cause:     err,
		}
	}

	bs.recordSize("message", len(data))
	bs.logger.Debug("Message stored successfully",
		zap.String("message_id", string(msg.ID)),
		zap.String("topic", msg.Topic),
		zap.Uint32("partition", msg.PartitionID),
	)

	return nil
}

// GetMessage retrieves a message from the database
func (bs *BadgerStorage) GetMessage(ctx context.Context, topic string, partition uint32, msgID models.MessageID) (*models.Message, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("get_message", time.Since(start))
	}()

	key := bs.keyBuilder.MessageKey(topic, partition, msgID)

	var msg models.Message
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &msg)
		})
	})

	if err != nil {
		if err == ErrNotFound {
			return nil, err
		}
		return nil, ErrStorageOperation{
			Operation: "get_message",
			Key:       key,
			Cause:     err,
		}
	}

	return &msg, nil
}

// DeleteMessage removes a message from the database
func (bs *BadgerStorage) DeleteMessage(ctx context.Context, topic string, partition uint32, msgID models.MessageID) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("delete_message", time.Since(start))
	}()

	key := bs.keyBuilder.MessageKey(topic, partition, msgID)

	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err != nil {
		return ErrStorageOperation{
			Operation: "delete_message",
			Key:       key,
			Cause:     err,
		}
	}

	return nil
}

// ListMessages retrieves a list of messages for a topic/partition
func (bs *BadgerStorage) ListMessages(ctx context.Context, topic string, partition uint32, limit int) ([]*models.Message, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		bs.recordOperation("list_messages", time.Since(start))
	}()

	prefix := []byte(bs.keyBuilder.MessagePrefix(topic, partition))
	var messages []*models.Message

	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix) && count < limit; it.Next() {
			item := it.Item()

			var msg models.Message
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &msg)
			})
			if err != nil {
				bs.logger.Warn("Failed to unmarshal message",
					zap.Error(err),
					zap.String("key", string(item.Key())),
				)
				continue
			}

			messages = append(messages, &msg)
			count++
		}

		return nil
	})

	if err != nil {
		return nil, ErrStorageOperation{
			Operation: "list_messages",
			Key:       string(prefix),
			Cause:     err,
		}
	}

	return messages, nil
}

// checkOpen verifies the storage is open and ready
func (bs *BadgerStorage) checkOpen() error {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	if bs.closed || bs.db == nil {
		return ErrClosed
	}

	return nil
}

// recordOperation records metrics for storage operations (INTERFACE_CONTRACTS.md)
func (bs *BadgerStorage) recordOperation(operation string, duration time.Duration) {
	ctx := context.Background()

	bs.storageOps.Add(ctx, 1,
		metric.WithAttributes(attribute.String("operation", operation)))

	bs.operationTime.Record(ctx, float64(duration.Nanoseconds())/1e6,
		metric.WithAttributes(attribute.String("operation", operation)))
}

// recordSize records size metrics for storage operations
func (bs *BadgerStorage) recordSize(dataType string, size int) {
	ctx := context.Background()

	bs.storageSize.Record(ctx, int64(size),
		metric.WithAttributes(attribute.String("data_type", dataType)))
}

// startCleanupTask starts the background cleanup task
func (bs *BadgerStorage) startCleanupTask() {
	bs.cleanupTicker = time.NewTicker(bs.config.CompactionInterval)

	bs.wg.Add(1)
	go func() {
		defer bs.wg.Done()

		for {
			select {
			case <-bs.cleanupTicker.C:
				if err := bs.runCleanup(); err != nil {
					bs.logger.Error("Cleanup task failed", zap.Error(err))
				}
			case <-bs.stopCleanup:
				return
			}
		}
	}()
}

// runCleanup performs database cleanup and compaction
func (bs *BadgerStorage) runCleanup() error {
	bs.logger.Debug("Running storage cleanup")

	// Run garbage collection
	if err := bs.db.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
		return fmt.Errorf("value log GC failed: %w", err)
	}

	// Clean up expired dedup entries
	if _, err := bs.CleanupExpiredDedup(context.Background(), time.Now()); err != nil {
		bs.logger.Warn("Failed to cleanup expired dedup entries", zap.Error(err))
	}

	bs.logger.Debug("Storage cleanup completed")
	return nil
}

// badgerLogger adapts zap.Logger to badger.Logger interface
type badgerLogger struct {
	logger *zap.Logger
}

func (bl *badgerLogger) Errorf(format string, args ...interface{}) {
	bl.logger.Error(fmt.Sprintf(format, args...))
}

func (bl *badgerLogger) Warningf(format string, args ...interface{}) {
	bl.logger.Warn(fmt.Sprintf(format, args...))
}

func (bl *badgerLogger) Infof(format string, args ...interface{}) {
	bl.logger.Info(fmt.Sprintf(format, args...))
}

func (bl *badgerLogger) Debugf(format string, args ...interface{}) {
	bl.logger.Debug(fmt.Sprintf(format, args...))
}
