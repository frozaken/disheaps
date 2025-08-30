package storage

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
)

// Key schema implementation following the spec:
// m/<topic>/<part>/<msg_id> -> payload+meta
// l/<topic>/<part>/<msg_id> -> lease state
// d/<topic>/<producer>/<epoch>/<seq> -> msg_id (TTL)
// c/<topic>/<part>/heap_snapshot -> binary blob
// t/<topic>/<part>/timer_snapshot -> binary blob
// tc/<topic> -> topic config
// pi/<topic>/<part> -> partition info

// Key prefixes for different data types
const (
	PrefixMessage       = "m"
	PrefixLease         = "l"
	PrefixDedup         = "d"
	PrefixHeapSnapshot  = "c"
	PrefixTimerSnapshot = "t"
	PrefixTopicConfig   = "tc"
	PrefixPartitionInfo = "pi"
)

// KeyBuilder provides methods to construct storage keys according to the schema
type KeyBuilder struct{}

// NewKeyBuilder creates a new key builder
func NewKeyBuilder() *KeyBuilder {
	return &KeyBuilder{}
}

// MessageKey constructs a key for message storage
// Format: m/<topic>/<partition>/<message_id>
func (kb *KeyBuilder) MessageKey(topic string, partition uint32, msgID models.MessageID) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	if !msgID.IsValid() {
		panic(fmt.Sprintf("invalid message ID: %s", msgID))
	}
	return fmt.Sprintf("%s/%s/%d/%s", PrefixMessage, topic, partition, msgID)
}

// LeaseKey constructs a key for lease storage
// Format: l/<topic>/<partition>/<message_id>
func (kb *KeyBuilder) LeaseKey(topic string, partition uint32, msgID models.MessageID) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	if !msgID.IsValid() {
		panic(fmt.Sprintf("invalid message ID: %s", msgID))
	}
	return fmt.Sprintf("%s/%s/%d/%s", PrefixLease, topic, partition, msgID)
}

// DedupKey constructs a key for deduplication storage
// Format: d/<topic>/<producer_id>/<epoch>/<sequence>
func (kb *KeyBuilder) DedupKey(topic string, producerKey models.ProducerKey, seq uint64) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	if producerKey.ID == "" {
		panic("producer ID cannot be empty")
	}
	return fmt.Sprintf("%s/%s/%s/%d/%d", PrefixDedup, topic, producerKey.ID, producerKey.Epoch, seq)
}

// SnapshotKey constructs a key for snapshot storage
// Format: c/<topic>/<partition> (heap) or t/<topic>/<partition> (timer)
func (kb *KeyBuilder) SnapshotKey(topic string, partition uint32, snapshotType SnapshotType) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}

	prefix := PrefixHeapSnapshot
	if snapshotType == SnapshotTypeTimer {
		prefix = PrefixTimerSnapshot
	}

	return fmt.Sprintf("%s/%s/%d", prefix, topic, partition)
}

// TopicConfigKey constructs a key for topic configuration storage
// Format: tc/<topic>
func (kb *KeyBuilder) TopicConfigKey(topic string) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	return fmt.Sprintf("%s/%s", PrefixTopicConfig, topic)
}

// PartitionInfoKey constructs a key for partition info storage
// Format: pi/<topic>/<partition>
func (kb *KeyBuilder) PartitionInfoKey(topic string, partition uint32) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	return fmt.Sprintf("%s/%s/%d", PrefixPartitionInfo, topic, partition)
}

// MessagePrefix returns a prefix for scanning message keys
// Format: m/<topic>/<partition>/
func (kb *KeyBuilder) MessagePrefix(topic string, partition uint32) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	return fmt.Sprintf("%s/%s/%d/", PrefixMessage, topic, partition)
}

// LeasePrefix returns a prefix for scanning lease keys
// Format: l/<topic>/<partition>/
func (kb *KeyBuilder) LeasePrefix(topic string, partition uint32) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	return fmt.Sprintf("%s/%s/%d/", PrefixLease, topic, partition)
}

// DedupPrefix returns a prefix for scanning dedup keys
// Format: d/<topic>/
func (kb *KeyBuilder) DedupPrefix(topic string) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	return fmt.Sprintf("%s/%s/", PrefixDedup, topic)
}

// TopicConfigPrefix returns a prefix for scanning topic config keys
// Format: tc/
func (kb *KeyBuilder) TopicConfigPrefix() string {
	return fmt.Sprintf("%s/", PrefixTopicConfig)
}

// PartitionInfoPrefix returns a prefix for scanning partition info keys
// Format: pi/<topic>/
func (kb *KeyBuilder) PartitionInfoPrefix(topic string) string {
	if err := validateTopic(topic); err != nil {
		panic(fmt.Sprintf("invalid topic: %v", err))
	}
	return fmt.Sprintf("%s/%s/", PrefixPartitionInfo, topic)
}

// KeyParser provides methods to parse storage keys back to components
type KeyParser struct{}

// NewKeyParser creates a new key parser
func NewKeyParser() *KeyParser {
	return &KeyParser{}
}

// ParsedKey represents a parsed storage key
type ParsedKey struct {
	Prefix       string
	Topic        string
	Partition    uint32
	MessageID    models.MessageID
	ProducerID   string
	Epoch        uint64
	Sequence     uint64
	HasPartition bool
}

// ParseKey parses a storage key into its components
func (kp *KeyParser) ParseKey(key string) (*ParsedKey, error) {
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return nil, ErrInvalidKey{Key: key, Issue: "insufficient parts"}
	}

	parsed := &ParsedKey{
		Prefix: parts[0],
		Topic:  parts[1],
	}

	switch parsed.Prefix {
	case PrefixMessage, PrefixLease:
		if len(parts) != 4 {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid message/lease key format"}
		}
		partition, err := strconv.ParseUint(parts[2], 10, 32)
		if err != nil {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid partition"}
		}
		parsed.Partition = uint32(partition)
		parsed.HasPartition = true
		parsed.MessageID = models.MessageID(parts[3])
		if !parsed.MessageID.IsValid() {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid message ID"}
		}

	case PrefixDedup:
		if len(parts) != 5 {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid dedup key format"}
		}
		parsed.ProducerID = parts[2]
		epoch, err := strconv.ParseUint(parts[3], 10, 64)
		if err != nil {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid epoch"}
		}
		parsed.Epoch = epoch
		seq, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid sequence"}
		}
		parsed.Sequence = seq

	case PrefixHeapSnapshot, PrefixTimerSnapshot:
		if len(parts) != 3 {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid snapshot key format"}
		}
		partition, err := strconv.ParseUint(parts[2], 10, 32)
		if err != nil {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid partition"}
		}
		parsed.Partition = uint32(partition)
		parsed.HasPartition = true

	case PrefixTopicConfig:
		if len(parts) != 2 {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid topic config key format"}
		}
		// Topic already set

	case PrefixPartitionInfo:
		if len(parts) != 3 {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid partition info key format"}
		}
		partition, err := strconv.ParseUint(parts[2], 10, 32)
		if err != nil {
			return nil, ErrInvalidKey{Key: key, Issue: "invalid partition"}
		}
		parsed.Partition = uint32(partition)
		parsed.HasPartition = true

	default:
		return nil, ErrInvalidKey{Key: key, Issue: "unknown prefix"}
	}

	return parsed, nil
}

// TTLKey represents a key with TTL information
type TTLKey struct {
	Key       string
	TTL       time.Duration
	ExpiresAt time.Time
}

// NewTTLKey creates a new TTL key
func NewTTLKey(key string, ttl time.Duration) *TTLKey {
	return &TTLKey{
		Key:       key,
		TTL:       ttl,
		ExpiresAt: time.Now().Add(ttl),
	}
}

// IsExpired checks if the TTL key has expired
func (tk *TTLKey) IsExpired() bool {
	return time.Now().After(tk.ExpiresAt)
}

// validateTopic validates topic name according to interface contracts
// Pattern: ^[a-zA-Z0-9_-]+$, Max Length: 255 characters
func validateTopic(topic string) error {
	if topic == "" {
		return ErrInvalidConfig{Field: "topic", Issue: "cannot be empty"}
	}
	if len(topic) > 255 {
		return ErrInvalidConfig{Field: "topic", Issue: "exceeds maximum length of 255 characters"}
	}

	// Validate pattern: ^[a-zA-Z0-9_-]+$
	for _, r := range topic {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-') {
			return ErrInvalidConfig{Field: "topic", Issue: "contains invalid characters, only alphanumeric, underscore, and hyphen allowed"}
		}
	}

	return nil
}

// ValidateTopicName validates a topic name according to interface contracts
func ValidateTopicName(topic string) error {
	return validateTopic(topic)
}
