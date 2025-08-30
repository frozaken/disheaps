package raft

import (
	"compress/gzip"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
)

// HeapSnapshot implements the raft.FSMSnapshot interface for heap state snapshots
type HeapSnapshot struct {
	// Core state
	LastIndex uint64    `json:"last_index"`
	LastTerm  uint64    `json:"last_term"`
	AppliedAt time.Time `json:"applied_at"`
	CreatedAt time.Time `json:"created_at"`

	// Transaction state
	PendingTxns map[string]*BatchPrepareEntry `json:"pending_txns"`

	// Statistics
	AppliedEntries uint64 `json:"applied_entries"`
	AppliedBatches uint64 `json:"applied_batches"`
	FailedOps      uint64 `json:"failed_ops"`
	SnapshotCount  uint64 `json:"snapshot_count"`

	// Storage state (will be persisted separately)
	Storage storage.Storage `json:"-"`
	Logger  *zap.Logger     `json:"-"`
}

// Persist writes the snapshot data to the provided sink
func (s *HeapSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	startTime := time.Now()

	// Create gzip writer for compression
	gzipWriter := gzip.NewWriter(sink)
	defer gzipWriter.Close()

	// Create gob encoder
	encoder := gob.NewEncoder(gzipWriter)

	// Create snapshot metadata
	metadata := &SnapshotMetadata{
		Version:        SnapshotVersion,
		LastIndex:      s.LastIndex,
		LastTerm:       s.LastTerm,
		CreatedAt:      s.CreatedAt,
		AppliedAt:      s.AppliedAt,
		AppliedEntries: s.AppliedEntries,
		AppliedBatches: s.AppliedBatches,
		FailedOps:      s.FailedOps,
		SnapshotCount:  s.SnapshotCount,
		PendingTxns:    len(s.PendingTxns),
	}

	// Write metadata
	if err := encoder.Encode(metadata); err != nil {
		s.Logger.Error("failed to encode snapshot metadata", zap.Error(err))
		return fmt.Errorf("failed to encode snapshot metadata: %w", err)
	}

	// Write pending transactions
	if err := encoder.Encode(s.PendingTxns); err != nil {
		s.Logger.Error("failed to encode pending transactions", zap.Error(err))
		return fmt.Errorf("failed to encode pending transactions: %w", err)
	}

	// Create storage snapshot data
	storageSnapshot, err := s.createStorageSnapshot()
	if err != nil {
		s.Logger.Error("failed to create storage snapshot", zap.Error(err))
		return fmt.Errorf("failed to create storage snapshot: %w", err)
	}

	// Write storage snapshot
	if err := encoder.Encode(storageSnapshot); err != nil {
		s.Logger.Error("failed to encode storage snapshot", zap.Error(err))
		return fmt.Errorf("failed to encode storage snapshot: %w", err)
	}

	// Flush and close writers
	if err := gzipWriter.Close(); err != nil {
		s.Logger.Error("failed to close gzip writer", zap.Error(err))
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	duration := time.Since(startTime)

	s.Logger.Info("snapshot persisted successfully",
		zap.Uint64("last_index", s.LastIndex),
		zap.Uint64("last_term", s.LastTerm),
		zap.Duration("duration", duration),
		zap.Int("pending_txns", len(s.PendingTxns)))

	return nil
}

// Release is called when the snapshot is no longer needed
func (s *HeapSnapshot) Release() {
	s.Logger.Debug("releasing snapshot",
		zap.Uint64("last_index", s.LastIndex),
		zap.Uint64("last_term", s.LastTerm))

	// Clear references to help with garbage collection
	s.PendingTxns = nil
	s.Storage = nil
	s.Logger = nil
}

// Restore restores the FSM state from a snapshot reader
func (s *HeapSnapshot) Restore(reader io.ReadCloser) error {
	defer reader.Close()

	startTime := time.Now()

	// Create gzip reader for decompression
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	// Create gob decoder
	decoder := gob.NewDecoder(gzipReader)

	// Read metadata
	var metadata SnapshotMetadata
	if err := decoder.Decode(&metadata); err != nil {
		return fmt.Errorf("failed to decode snapshot metadata: %w", err)
	}

	// Validate snapshot version
	if metadata.Version != SnapshotVersion {
		return fmt.Errorf("unsupported snapshot version: %d (expected %d)",
			metadata.Version, SnapshotVersion)
	}

	// Restore snapshot state from metadata
	s.LastIndex = metadata.LastIndex
	s.LastTerm = metadata.LastTerm
	s.CreatedAt = metadata.CreatedAt
	s.AppliedAt = metadata.AppliedAt
	s.AppliedEntries = metadata.AppliedEntries
	s.AppliedBatches = metadata.AppliedBatches
	s.FailedOps = metadata.FailedOps
	s.SnapshotCount = metadata.SnapshotCount

	// Read pending transactions
	var pendingTxns map[string]*BatchPrepareEntry
	if err := decoder.Decode(&pendingTxns); err != nil {
		return fmt.Errorf("failed to decode pending transactions: %w", err)
	}
	s.PendingTxns = pendingTxns

	// Read storage snapshot
	var storageSnapshot StorageSnapshot
	if err := decoder.Decode(&storageSnapshot); err != nil {
		return fmt.Errorf("failed to decode storage snapshot: %w", err)
	}

	// Restore storage state
	if err := s.restoreStorageSnapshot(&storageSnapshot); err != nil {
		return fmt.Errorf("failed to restore storage snapshot: %w", err)
	}

	duration := time.Since(startTime)

	if s.Logger != nil {
		s.Logger.Info("snapshot restored successfully",
			zap.Uint64("last_index", s.LastIndex),
			zap.Uint64("last_term", s.LastTerm),
			zap.Duration("duration", duration),
			zap.Int("pending_txns", len(s.PendingTxns)))
	}

	return nil
}

// createStorageSnapshot creates a snapshot of the storage layer state
func (s *HeapSnapshot) createStorageSnapshot() (*StorageSnapshot, error) {
	if s.Storage == nil {
		return &StorageSnapshot{}, nil
	}

	// Get all topic configurations
	topicConfigs, err := s.Storage.ListTopicConfigs(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}

	snapshot := &StorageSnapshot{
		Topics:    make(map[string]*TopicSnapshotData),
		CreatedAt: time.Now(),
	}

	// Snapshot each topic
	for _, topicConfig := range topicConfigs {
		topicData, err := s.createTopicSnapshot(topicConfig.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic snapshot for %s: %w", topicConfig.Name, err)
		}
		snapshot.Topics[topicConfig.Name] = topicData
	}

	return snapshot, nil
}

// createTopicSnapshot creates a snapshot of a specific topic
func (s *HeapSnapshot) createTopicSnapshot(topic string) (*TopicSnapshotData, error) {
	// Get topic configuration
	config, err := s.Storage.GetTopicConfig(context.Background(), topic)
	if err != nil {
		return nil, fmt.Errorf("failed to load topic config: %w", err)
	}

	topicData := &TopicSnapshotData{
		Config:     config,
		Partitions: make(map[uint32]*PartitionSnapshotData),
	}

	// Snapshot each partition
	for i := uint32(0); i < config.Partitions; i++ {
		partitionData, err := s.createPartitionSnapshot(topic, i)
		if err != nil {
			s.Logger.Warn("failed to create partition snapshot",
				zap.String("topic", topic),
				zap.Uint32("partition", i),
				zap.Error(err))
			continue
		}
		topicData.Partitions[i] = partitionData
	}

	return topicData, nil
}

// createPartitionSnapshot creates a snapshot of a specific partition
func (s *HeapSnapshot) createPartitionSnapshot(topic string, partitionID uint32) (*PartitionSnapshotData, error) {
	// Get partition messages (this is a simplified approach - in practice,
	// we might want to use more efficient serialization)
	messages, err := s.Storage.ListMessages(context.Background(), topic, partitionID, 1000) // Limit for safety
	if err != nil {
		return nil, fmt.Errorf("failed to list messages: %w", err)
	}

	// Get partition info (instead of stats, since that's what's available)
	partitionInfo, err := s.Storage.GetPartitionInfo(context.Background(), topic, partitionID)
	if err != nil {
		// Partition info might not exist, continue without it
		partitionInfo = nil
	}

	// Convert messages to interface{} slice
	messageInterfaces := make([]interface{}, len(messages))
	for i, msg := range messages {
		messageInterfaces[i] = msg
	}

	return &PartitionSnapshotData{
		Topic:       topic,
		PartitionID: partitionID,
		Messages:    messageInterfaces,
		Stats:       partitionInfo, // Store partition info instead of stats
		SnapshotAt:  time.Now(),
	}, nil
}

// restoreStorageSnapshot restores the storage layer from a snapshot
func (s *HeapSnapshot) restoreStorageSnapshot(snapshot *StorageSnapshot) error {
	if s.Storage == nil {
		return nil // No storage to restore
	}

	// Restore each topic
	for topicName, topicData := range snapshot.Topics {
		if err := s.restoreTopicSnapshot(topicName, topicData); err != nil {
			return fmt.Errorf("failed to restore topic %s: %w", topicName, err)
		}
	}

	return nil
}

// restoreTopicSnapshot restores a specific topic from snapshot data
func (s *HeapSnapshot) restoreTopicSnapshot(topicName string, topicData *TopicSnapshotData) error {
	// Restore topic configuration
	if topicData.Config != nil {
		if config, ok := topicData.Config.(*models.TopicConfig); ok {
			if err := s.Storage.StoreTopicConfig(context.Background(), config); err != nil {
				return fmt.Errorf("failed to restore topic config: %w", err)
			}
		}
	}

	// Restore each partition
	for partitionID, partitionData := range topicData.Partitions {
		if err := s.restorePartitionSnapshot(partitionID, partitionData); err != nil {
			s.Logger.Warn("failed to restore partition",
				zap.String("topic", topicName),
				zap.Uint32("partition", partitionID),
				zap.Error(err))
			continue
		}
	}

	return nil
}

// restorePartitionSnapshot restores a specific partition from snapshot data
func (s *HeapSnapshot) restorePartitionSnapshot(partitionID uint32, partitionData *PartitionSnapshotData) error {
	// Restore messages
	for _, msgInterface := range partitionData.Messages {
		if message, ok := msgInterface.(*models.Message); ok {
			if err := s.Storage.StoreMessage(context.Background(), message); err != nil {
				return fmt.Errorf("failed to restore message %s: %w", message.ID.String(), err)
			}
		}
	}

	return nil
}

// SnapshotMetadata contains metadata about a snapshot
type SnapshotMetadata struct {
	Version        int       `json:"version"`
	LastIndex      uint64    `json:"last_index"`
	LastTerm       uint64    `json:"last_term"`
	CreatedAt      time.Time `json:"created_at"`
	AppliedAt      time.Time `json:"applied_at"`
	AppliedEntries uint64    `json:"applied_entries"`
	AppliedBatches uint64    `json:"applied_batches"`
	FailedOps      uint64    `json:"failed_ops"`
	SnapshotCount  uint64    `json:"snapshot_count"`
	PendingTxns    int       `json:"pending_txns"`
}

// StorageSnapshot represents the entire storage layer state
type StorageSnapshot struct {
	Topics    map[string]*TopicSnapshotData `json:"topics"`
	CreatedAt time.Time                     `json:"created_at"`
}

// TopicSnapshotData represents the state of a single topic
type TopicSnapshotData struct {
	Config     interface{}                       `json:"config"` // models.TopicConfig
	Partitions map[uint32]*PartitionSnapshotData `json:"partitions"`
}

// PartitionSnapshotData represents the state of a single partition
type PartitionSnapshotData struct {
	Topic       string        `json:"topic"`
	PartitionID uint32        `json:"partition_id"`
	Messages    []interface{} `json:"messages"` // []*models.Message
	Stats       interface{}   `json:"stats"`    // *models.PartitionStats
	SnapshotAt  time.Time     `json:"snapshot_at"`
}

// Snapshot version for compatibility checking
const SnapshotVersion = 1

// SnapshotConfig contains configuration for snapshot behavior
type SnapshotConfig struct {
	// Retention settings
	RetainCount    int           `json:"retain_count"`    // Number of snapshots to retain
	RetainInterval time.Duration `json:"retain_interval"` // Time to retain snapshots

	// Compression settings
	CompressionEnabled bool `json:"compression_enabled"` // Whether to compress snapshots
	CompressionLevel   int  `json:"compression_level"`   // Compression level (1-9)

	// Performance settings
	MaxConcurrentOps int `json:"max_concurrent_ops"` // Max concurrent snapshot operations
	BufferSize       int `json:"buffer_size"`        // I/O buffer size

	// Validation settings
	ValidateOnRestore bool `json:"validate_on_restore"` // Whether to validate on restore
	ChecksumEnabled   bool `json:"checksum_enabled"`    // Whether to include checksums
}

// DefaultSnapshotConfig returns the default snapshot configuration
func DefaultSnapshotConfig() *SnapshotConfig {
	return &SnapshotConfig{
		RetainCount:        5,
		RetainInterval:     24 * time.Hour,
		CompressionEnabled: true,
		CompressionLevel:   6,
		MaxConcurrentOps:   4,
		BufferSize:         64 * 1024, // 64KB
		ValidateOnRestore:  true,
		ChecksumEnabled:    true,
	}
}

// SnapshotManager manages snapshot lifecycle and cleanup
type SnapshotManager struct {
	config *SnapshotConfig
	logger *zap.Logger
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(config *SnapshotConfig, logger *zap.Logger) *SnapshotManager {
	if config == nil {
		config = DefaultSnapshotConfig()
	}

	return &SnapshotManager{
		config: config,
		logger: logger.Named("snapshot-manager"),
	}
}

// ShouldSnapshot determines if a new snapshot should be created
func (sm *SnapshotManager) ShouldSnapshot(lastSnapshotIndex, currentIndex uint64, lastSnapshotTime time.Time) bool {
	// Snapshot if we've applied a significant number of entries since last snapshot
	const snapshotThreshold = 1000
	if currentIndex-lastSnapshotIndex >= snapshotThreshold {
		return true
	}

	// Snapshot if enough time has passed since last snapshot
	if time.Since(lastSnapshotTime) >= sm.config.RetainInterval {
		return true
	}

	return false
}

// GetConfig returns the snapshot configuration
func (sm *SnapshotManager) GetConfig() *SnapshotConfig {
	return sm.config
}

// UpdateConfig updates the snapshot configuration
func (sm *SnapshotManager) UpdateConfig(newConfig *SnapshotConfig) {
	sm.config = newConfig
	sm.logger.Info("updated snapshot configuration",
		zap.Int("retain_count", newConfig.RetainCount),
		zap.Duration("retain_interval", newConfig.RetainInterval),
		zap.Bool("compression_enabled", newConfig.CompressionEnabled))
}
