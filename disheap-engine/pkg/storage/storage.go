package storage

import (
	"context"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
)

// Storage defines the interface for persistent storage operations
type Storage interface {
	// Message operations
	StoreMessage(ctx context.Context, msg *models.Message) error
	GetMessage(ctx context.Context, topic string, partition uint32, msgID models.MessageID) (*models.Message, error)
	DeleteMessage(ctx context.Context, topic string, partition uint32, msgID models.MessageID) error
	ListMessages(ctx context.Context, topic string, partition uint32, limit int) ([]*models.Message, error)

	// Lease operations
	StoreLease(ctx context.Context, topic string, partition uint32, lease *models.Lease) error
	GetLease(ctx context.Context, topic string, partition uint32, msgID models.MessageID) (*models.Lease, error)
	DeleteLease(ctx context.Context, topic string, partition uint32, msgID models.MessageID) error
	ListExpiredLeases(ctx context.Context, before time.Time, limit int) ([]*models.Lease, error)

	// Deduplication operations
	StoreDedup(ctx context.Context, topic string, producerKey models.ProducerKey, seq uint64, msgID models.MessageID, ttl time.Duration) error
	CheckDedup(ctx context.Context, topic string, producerKey models.ProducerKey, seq uint64) (models.MessageID, bool, error)
	CleanupExpiredDedup(ctx context.Context, before time.Time) (int, error)

	// Snapshot operations
	StoreSnapshot(ctx context.Context, topic string, partition uint32, snapshotType SnapshotType, data []byte) error
	GetSnapshot(ctx context.Context, topic string, partition uint32, snapshotType SnapshotType) ([]byte, error)
	DeleteSnapshot(ctx context.Context, topic string, partition uint32, snapshotType SnapshotType) error

	// Topic configuration operations
	StoreTopicConfig(ctx context.Context, config *models.TopicConfig) error
	GetTopicConfig(ctx context.Context, topic string) (*models.TopicConfig, error)
	DeleteTopicConfig(ctx context.Context, topic string) error
	ListTopicConfigs(ctx context.Context) ([]*models.TopicConfig, error)

	// Partition info operations
	StorePartitionInfo(ctx context.Context, info *models.PartitionInfo) error
	GetPartitionInfo(ctx context.Context, topic string, partition uint32) (*models.PartitionInfo, error)
	ListPartitionInfos(ctx context.Context, topic string) ([]*models.PartitionInfo, error)

	// Transaction operations
	Batch() StorageBatch

	// Lifecycle operations
	Open(ctx context.Context) error
	Close(ctx context.Context) error
	Health(ctx context.Context) error

	// Maintenance operations
	Compact(ctx context.Context) error
	Stats(ctx context.Context) (StorageStats, error)
}

// StorageBatch provides atomic transaction capabilities
type StorageBatch interface {
	// Message operations
	StoreMessage(msg *models.Message) error
	DeleteMessage(topic string, partition uint32, msgID models.MessageID) error

	// Lease operations
	StoreLease(topic string, partition uint32, lease *models.Lease) error
	DeleteLease(topic string, partition uint32, msgID models.MessageID) error

	// Dedup operations
	StoreDedup(topic string, producerKey models.ProducerKey, seq uint64, msgID models.MessageID, ttl time.Duration) error

	// Config operations
	StoreTopicConfig(config *models.TopicConfig) error
	StorePartitionInfo(info *models.PartitionInfo) error

	// Transaction control
	Commit(ctx context.Context) error
	Rollback() error
	Size() int // Number of operations in batch
}

// SnapshotType represents different types of snapshots
type SnapshotType int

const (
	SnapshotTypeHeap SnapshotType = iota
	SnapshotTypeTimer
	SnapshotTypeRaft
)

func (st SnapshotType) String() string {
	switch st {
	case SnapshotTypeHeap:
		return "heap"
	case SnapshotTypeTimer:
		return "timer"
	case SnapshotTypeRaft:
		return "raft"
	default:
		return "unknown"
	}
}

// StorageStats provides storage engine statistics
type StorageStats struct {
	// Size information
	TotalBytes     uint64 `json:"total_bytes"`
	MessagesBytes  uint64 `json:"messages_bytes"`
	LeasesBytes    uint64 `json:"leases_bytes"`
	DedupBytes     uint64 `json:"dedup_bytes"`
	SnapshotsBytes uint64 `json:"snapshots_bytes"`

	// Count information
	TotalMessages     uint64 `json:"total_messages"`
	TotalLeases       uint64 `json:"total_leases"`
	TotalDedupEntries uint64 `json:"total_dedup_entries"`
	TotalSnapshots    uint64 `json:"total_snapshots"`

	// Performance information
	LastCompactionAt *time.Time `json:"last_compaction_at,omitempty"`
	CompactionCount  uint64     `json:"compaction_count"`

	// Health information
	IsHealthy       bool      `json:"is_healthy"`
	LastHealthCheck time.Time `json:"last_health_check"`

	// Engine-specific stats (BadgerDB)
	EngineStats interface{} `json:"engine_stats,omitempty"`
}

// Config holds storage configuration
type Config struct {
	// BadgerDB configuration
	Path               string `yaml:"path" json:"path"`
	ValueLogPath       string `yaml:"value_log_path" json:"value_log_path,omitempty"`
	SyncWrites         bool   `yaml:"sync_writes" json:"sync_writes"`
	TableLoadingMode   int    `yaml:"table_loading_mode" json:"table_loading_mode"`
	ValueThreshold     int64  `yaml:"value_threshold" json:"value_threshold"`
	NumMemtables       int    `yaml:"num_memtables" json:"num_memtables"`
	NumLevelZeroTables int    `yaml:"num_level_zero_tables" json:"num_level_zero_tables"`
	NumCompactors      int    `yaml:"num_compactors" json:"num_compactors"`

	// Retention configuration
	DefaultTTL         time.Duration `yaml:"default_ttl" json:"default_ttl"`
	DedupTTL           time.Duration `yaml:"dedup_ttl" json:"dedup_ttl"`
	CompactionInterval time.Duration `yaml:"compaction_interval" json:"compaction_interval"`

	// Performance tuning
	MaxBatchSize  int           `yaml:"max_batch_size" json:"max_batch_size"`
	MaxBatchDelay time.Duration `yaml:"max_batch_delay" json:"max_batch_delay"`
}

// DefaultConfig returns a default storage configuration
func DefaultConfig() *Config {
	return &Config{
		Path:               "./data",
		SyncWrites:         true,
		TableLoadingMode:   2,    // LoadToRAM
		ValueThreshold:     1024, // 1KB
		NumMemtables:       3,
		NumLevelZeroTables: 2,
		NumCompactors:      2,
		DefaultTTL:         24 * time.Hour,
		DedupTTL:           24 * time.Hour,
		CompactionInterval: time.Hour,
		MaxBatchSize:       1000,
		MaxBatchDelay:      10 * time.Millisecond,
	}
}

// Validate validates the storage configuration
func (c *Config) Validate() error {
	if c.Path == "" {
		return ErrInvalidConfig{Field: "path", Issue: "cannot be empty"}
	}
	if c.MaxBatchSize <= 0 {
		return ErrInvalidConfig{Field: "max_batch_size", Issue: "must be positive"}
	}
	if c.MaxBatchDelay < 0 {
		return ErrInvalidConfig{Field: "max_batch_delay", Issue: "cannot be negative"}
	}
	return nil
}
