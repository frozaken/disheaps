package models

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

var (
	// ErrInvalidTopicName is returned when a topic name is invalid
	ErrInvalidTopicName = errors.New("invalid topic name")
	// ErrInvalidPartitionCount is returned when partition count is invalid
	ErrInvalidPartitionCount = errors.New("invalid partition count")
	// ErrInvalidReplicationFactor is returned when replication factor is invalid
	ErrInvalidReplicationFactor = errors.New("invalid replication factor")
)

// Topic name validation pattern (alphanumeric, hyphens, underscores)
var topicNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// HeapMode represents the heap mode (MIN or MAX)
type HeapMode int

const (
	// MinHeap mode (smallest priority first)
	MinHeap HeapMode = iota
	// MaxHeap mode (largest priority first)
	MaxHeap
)

// String returns string representation of heap mode
func (hm HeapMode) String() string {
	switch hm {
	case MinHeap:
		return "MIN"
	case MaxHeap:
		return "MAX"
	default:
		return "UNKNOWN"
	}
}

// DLQPolicyType represents the type of DLQ policy
type DLQPolicyType string

const (
	DLQPolicyNone       DLQPolicyType = "none"
	DLQPolicyMaxRetries DLQPolicyType = "max_retries"
	DLQPolicyTTL        DLQPolicyType = "ttl"
)

// DLQPolicy represents dead letter queue configuration
type DLQPolicy struct {
	Type          DLQPolicyType `json:"type"`
	MaxRetries    int32         `json:"max_retries,omitempty"`
	MaxTTL        time.Duration `json:"max_ttl,omitempty"`
	RetentionTime time.Duration `json:"retention_time"`
}

// Validate validates the DLQ policy
func (dlq *DLQPolicy) Validate() error {
	switch dlq.Type {
	case DLQPolicyMaxRetries:
		if dlq.MaxRetries <= 0 {
			return errors.New("DLQ max retries must be positive")
		}
	case DLQPolicyTTL:
		if dlq.MaxTTL <= 0 {
			return errors.New("DLQ max TTL must be positive")
		}
	case DLQPolicyNone:
		// No validation needed
	default:
		return errors.New("invalid DLQ policy type")
	}

	if dlq.RetentionTime <= 0 {
		return errors.New("DLQ retention time must be positive")
	}
	return nil
}

// TopicConfig represents the configuration for a topic (heap)
type TopicConfig struct {
	// Basic configuration
	Name              string   `json:"name"`
	Mode              HeapMode `json:"mode"`
	Partitions        uint32   `json:"partitions"`
	ReplicationFactor uint32   `json:"replication_factor"`
	TopKBound         uint32   `json:"top_k_bound"`

	// Retention and timeouts
	RetentionTime            time.Duration `json:"retention_time"`
	VisibilityTimeoutDefault time.Duration `json:"visibility_timeout_default"`

	// Retry configuration
	MaxRetriesDefault uint32 `json:"max_retries_default"`

	// Payload limits
	MaxPayloadBytes uint64 `json:"max_payload_bytes"`

	// Features
	CompressionEnabled bool      `json:"compression_enabled"`
	DLQPolicy          DLQPolicy `json:"dlq_policy"`

	// Metadata
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   uint64    `json:"version"` // For optimistic locking
}

// NewTopicConfig creates a new topic configuration with defaults
func NewTopicConfig(name string, mode HeapMode, partitions, replicationFactor uint32) *TopicConfig {
	now := time.Now()
	return &TopicConfig{
		Name:                     name,
		Mode:                     mode,
		Partitions:               partitions,
		ReplicationFactor:        replicationFactor,
		TopKBound:                100,              // Default top-K bound
		RetentionTime:            24 * time.Hour,   // 24 hours
		VisibilityTimeoutDefault: 30 * time.Second, // 30 seconds
		MaxRetriesDefault:        3,                // 3 retries
		MaxPayloadBytes:          1048576,          // 1MB default
		CompressionEnabled:       true,
		DLQPolicy: DLQPolicy{
			Type:          DLQPolicyMaxRetries,
			MaxRetries:    3,
			RetentionTime: 7 * 24 * time.Hour, // 7 days
		},
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}
}

// Validate performs comprehensive validation of the topic configuration
func (tc *TopicConfig) Validate() error {
	// Validate topic name
	if tc.Name == "" {
		return fmt.Errorf("%w: name cannot be empty", ErrInvalidTopicName)
	}
	if len(tc.Name) > 255 {
		return fmt.Errorf("%w: name too long (max 255 chars)", ErrInvalidTopicName)
	}
	if !topicNamePattern.MatchString(tc.Name) {
		return fmt.Errorf("%w: name must contain only alphanumeric characters, hyphens, and underscores", ErrInvalidTopicName)
	}

	// Validate partition count
	if tc.Partitions == 0 {
		return fmt.Errorf("%w: must have at least 1 partition", ErrInvalidPartitionCount)
	}
	if tc.Partitions > 1000 { // reasonable upper bound
		return fmt.Errorf("%w: too many partitions (max 1000)", ErrInvalidPartitionCount)
	}

	// Validate replication factor
	if tc.ReplicationFactor < 1 {
		return fmt.Errorf("%w: must be at least 1", ErrInvalidReplicationFactor)
	}
	if tc.ReplicationFactor > 7 { // reasonable upper bound
		return fmt.Errorf("%w: too high (max 7)", ErrInvalidReplicationFactor)
	}
	if tc.ReplicationFactor > tc.Partitions {
		return fmt.Errorf("%w: cannot exceed partition count", ErrInvalidReplicationFactor)
	}

	// Validate bounds and limits
	if tc.TopKBound == 0 {
		return errors.New("top-K bound must be positive")
	}
	if tc.TopKBound > 10000 { // reasonable upper bound
		return errors.New("top-K bound too large (max 10000)")
	}

	// Validate timeouts
	if tc.RetentionTime <= 0 {
		return errors.New("retention time must be positive")
	}
	if tc.RetentionTime > 365*24*time.Hour { // 1 year max
		return errors.New("retention time too long (max 1 year)")
	}

	if tc.VisibilityTimeoutDefault <= 0 {
		return errors.New("visibility timeout must be positive")
	}
	if tc.VisibilityTimeoutDefault > 12*time.Hour { // 12 hours max
		return errors.New("visibility timeout too long (max 12 hours)")
	}

	// Validate payload size
	if tc.MaxPayloadBytes == 0 {
		return errors.New("max payload bytes must be positive")
	}
	if tc.MaxPayloadBytes > 256*1024*1024 { // 256MB max
		return errors.New("max payload bytes too large (max 256MB)")
	}

	// Validate DLQ policy
	if err := tc.DLQPolicy.Validate(); err != nil {
		return fmt.Errorf("invalid DLQ policy: %w", err)
	}

	return nil
}

// IsMinHeap returns true if this is a min-heap
func (tc *TopicConfig) IsMinHeap() bool {
	return tc.Mode == MinHeap
}

// IsMaxHeap returns true if this is a max-heap
func (tc *TopicConfig) IsMaxHeap() bool {
	return tc.Mode == MaxHeap
}

// SpineFanout calculates the spine index fan-out based on top-K and partitions
// This determines how many candidates each partition contributes to the spine index
func (tc *TopicConfig) SpineFanout() uint32 {
	// Fan-out = ceil(top_k_bound / partitions), minimum 1, maximum 10
	fanout := (tc.TopKBound + tc.Partitions - 1) / tc.Partitions
	if fanout < 1 {
		fanout = 1
	}
	if fanout > 10 {
		fanout = 10
	}
	return fanout
}

// PartitionID represents a partition identifier
type PartitionID uint32

// PartitionKey represents a partition routing key
type PartitionKey string

// PartitionInfo represents metadata about a partition
type PartitionInfo struct {
	ID            PartitionID `json:"id"`
	Topic         string      `json:"topic"`
	LeaderNode    string      `json:"leader_node"`
	ReplicaNodes  []string    `json:"replica_nodes"`
	IsHealthy     bool        `json:"is_healthy"`
	MessageCount  uint64      `json:"message_count"`
	InflightCount uint64      `json:"inflight_count"`
	LastUpdated   time.Time   `json:"last_updated"`

	// Throttling state (during rebalancing/snapshots)
	IsThrottled    bool   `json:"is_throttled"`
	ThrottleReason string `json:"throttle_reason,omitempty"`
}

// NewPartitionInfo creates a new partition info
func NewPartitionInfo(id PartitionID, topic string, leaderNode string, replicaNodes []string) *PartitionInfo {
	return &PartitionInfo{
		ID:           id,
		Topic:        topic,
		LeaderNode:   leaderNode,
		ReplicaNodes: replicaNodes,
		IsHealthy:    true,
		LastUpdated:  time.Now(),
	}
}

// IsLeader returns true if the given node is the leader for this partition
func (pi *PartitionInfo) IsLeader(nodeID string) bool {
	return pi.LeaderNode == nodeID
}

// IsReplica returns true if the given node is a replica for this partition
func (pi *PartitionInfo) IsReplica(nodeID string) bool {
	for _, replica := range pi.ReplicaNodes {
		if replica == nodeID {
			return true
		}
	}
	return false
}

// AllNodes returns all nodes (leader + replicas) for this partition
func (pi *PartitionInfo) AllNodes() []string {
	nodes := make([]string, 0, len(pi.ReplicaNodes)+1)
	nodes = append(nodes, pi.LeaderNode)
	nodes = append(nodes, pi.ReplicaNodes...)
	return nodes
}

// Throttle marks the partition as throttled with a reason
func (pi *PartitionInfo) Throttle(reason string) {
	pi.IsThrottled = true
	pi.ThrottleReason = reason
	pi.LastUpdated = time.Now()
}

// Unthrottle removes throttling from the partition
func (pi *PartitionInfo) Unthrottle() {
	pi.IsThrottled = false
	pi.ThrottleReason = ""
	pi.LastUpdated = time.Now()
}
