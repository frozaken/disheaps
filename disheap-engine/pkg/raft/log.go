package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
)

func init() {
	// Register types that will be encoded as interfaces
	gob.Register(&models.TopicConfig{})
	gob.Register(&models.Message{})
	gob.Register(&models.PartitionInfo{})
}

// EntryType represents the type of Raft log entry
type EntryType int

const (
	EntryEnqueue EntryType = iota
	EntryAck
	EntryNack
	EntryLeaseExtend
	EntryBatchPrepare
	EntryBatchCommit
	EntryBatchAbort
	EntryConfigUpdate
	EntrySnapshotMark
)

// String returns the string representation of EntryType
func (e EntryType) String() string {
	switch e {
	case EntryEnqueue:
		return "ENQUEUE"
	case EntryAck:
		return "ACK"
	case EntryNack:
		return "NACK"
	case EntryLeaseExtend:
		return "LEASE_EXTEND"
	case EntryBatchPrepare:
		return "BATCH_PREPARE"
	case EntryBatchCommit:
		return "BATCH_COMMIT"
	case EntryBatchAbort:
		return "BATCH_ABORT"
	case EntryConfigUpdate:
		return "CONFIG_UPDATE"
	case EntrySnapshotMark:
		return "SNAPSHOT_MARK"
	default:
		return fmt.Sprintf("UNKNOWN_%d", int(e))
	}
}

// LogEntry represents a Raft log entry containing heap operation data
type LogEntry struct {
	Type      EntryType `json:"type"`
	Topic     string    `json:"topic"`
	Partition uint32    `json:"partition"`
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

// EnqueueEntry represents an enqueue operation log entry
type EnqueueEntry struct {
	Message       *models.Message `json:"message"`
	ProducerID    string          `json:"producer_id"`
	ProducerSeq   uint64          `json:"producer_seq"`
	TransactionID string          `json:"transaction_id,omitempty"`
}

// AckEntry represents an acknowledgment log entry
type AckEntry struct {
	MessageID  string `json:"message_id"`
	LeaseToken string `json:"lease_token"`
	Consumer   string `json:"consumer"`
}

// NackEntry represents a negative acknowledgment log entry
type NackEntry struct {
	MessageID    string        `json:"message_id"`
	LeaseToken   string        `json:"lease_token"`
	Consumer     string        `json:"consumer"`
	Reason       string        `json:"reason"`
	RetryAfter   time.Duration `json:"retry_after"`
	BackoffLevel int           `json:"backoff_level"`
}

// LeaseExtendEntry represents a lease extension log entry
type LeaseExtendEntry struct {
	MessageID  string        `json:"message_id"`
	LeaseToken string        `json:"lease_token"`
	Consumer   string        `json:"consumer"`
	Extension  time.Duration `json:"extension"`
}

// BatchPrepareEntry represents a two-phase commit prepare entry
type BatchPrepareEntry struct {
	TransactionID string            `json:"transaction_id"`
	Messages      []*models.Message `json:"messages"`
	ProducerID    string            `json:"producer_id"`
	ProducerSeq   uint64            `json:"producer_seq"`
	PreparedAt    time.Time         `json:"prepared_at"`
}

// BatchCommitEntry represents a two-phase commit commit entry
type BatchCommitEntry struct {
	TransactionID string    `json:"transaction_id"`
	CommittedAt   time.Time `json:"committed_at"`
}

// BatchAbortEntry represents a two-phase commit abort entry
type BatchAbortEntry struct {
	TransactionID string    `json:"transaction_id"`
	Reason        string    `json:"reason"`
	AbortedAt     time.Time `json:"aborted_at"`
}

// ConfigUpdateEntry represents a configuration update log entry
type ConfigUpdateEntry struct {
	ConfigType string      `json:"config_type"` // "topic" or "partition"
	ConfigID   string      `json:"config_id"`   // topic name or partition ID
	OldConfig  interface{} `json:"old_config"`
	NewConfig  interface{} `json:"new_config"`
}

// SnapshotMarkEntry represents a snapshot marker log entry
type SnapshotMarkEntry struct {
	SnapshotID   string    `json:"snapshot_id"`
	LastIndex    uint64    `json:"last_index"`
	LastTerm     uint64    `json:"last_term"`
	SnapshotPath string    `json:"snapshot_path"`
	CreatedAt    time.Time `json:"created_at"`
}

// NewLogEntry creates a new log entry with the given type and data
func NewLogEntry(entryType EntryType, topic string, partition uint32, data interface{}) (*LogEntry, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	if err := encoder.Encode(data); err != nil {
		return nil, fmt.Errorf("failed to encode log entry data: %w", err)
	}

	return &LogEntry{
		Type:      entryType,
		Topic:     topic,
		Partition: partition,
		Data:      buf.Bytes(),
		Timestamp: time.Now().UTC(),
	}, nil
}

// DecodeData decodes the log entry data into the provided interface
func (le *LogEntry) DecodeData(target interface{}) error {
	if len(le.Data) == 0 {
		return fmt.Errorf("log entry has no data to decode")
	}

	buf := bytes.NewBuffer(le.Data)
	decoder := gob.NewDecoder(buf)

	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("failed to decode log entry data: %w", err)
	}

	return nil
}

// Size returns the serialized size of the log entry in bytes
func (le *LogEntry) Size() int {
	// Calculate approximate size: type (4) + topic length (4) + topic + partition (4) +
	// data length (4) + data + timestamp (8 for Unix nano)
	size := 4 + 4 + len(le.Topic) + 4 + 4 + len(le.Data) + 8
	return size
}

// Encode serializes the log entry to bytes (alias for Serialize)
func (le *LogEntry) Encode() ([]byte, error) {
	return le.Serialize()
}

// Serialize serializes the log entry to bytes
func (le *LogEntry) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Write entry type
	if err := binary.Write(&buf, binary.LittleEndian, uint32(le.Type)); err != nil {
		return nil, fmt.Errorf("failed to write entry type: %w", err)
	}

	// Write topic
	topicBytes := []byte(le.Topic)
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(topicBytes))); err != nil {
		return nil, fmt.Errorf("failed to write topic length: %w", err)
	}
	buf.Write(topicBytes)

	// Write partition
	if err := binary.Write(&buf, binary.LittleEndian, le.Partition); err != nil {
		return nil, fmt.Errorf("failed to write partition: %w", err)
	}

	// Write data
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(le.Data))); err != nil {
		return nil, fmt.Errorf("failed to write data length: %w", err)
	}
	buf.Write(le.Data)

	// Write timestamp
	if err := binary.Write(&buf, binary.LittleEndian, le.Timestamp.UnixNano()); err != nil {
		return nil, fmt.Errorf("failed to write timestamp: %w", err)
	}

	return buf.Bytes(), nil
}

// DeserializeLogEntry deserializes bytes into a log entry
func DeserializeLogEntry(data []byte) (*LogEntry, error) {
	buf := bytes.NewBuffer(data)
	entry := &LogEntry{}

	// Read entry type
	var entryType uint32
	if err := binary.Read(buf, binary.LittleEndian, &entryType); err != nil {
		return nil, fmt.Errorf("failed to read entry type: %w", err)
	}
	entry.Type = EntryType(entryType)

	// Read topic
	var topicLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %w", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := buf.Read(topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic: %w", err)
	}
	entry.Topic = string(topicBytes)

	// Read partition
	if err := binary.Read(buf, binary.LittleEndian, &entry.Partition); err != nil {
		return nil, fmt.Errorf("failed to read partition: %w", err)
	}

	// Read data
	var dataLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
		return nil, fmt.Errorf("failed to read data length: %w", err)
	}
	entry.Data = make([]byte, dataLen)
	if _, err := buf.Read(entry.Data); err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	// Read timestamp
	var timestampNano int64
	if err := binary.Read(buf, binary.LittleEndian, &timestampNano); err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}
	entry.Timestamp = time.Unix(0, timestampNano)

	return entry, nil
}

// ValidateLogEntry validates the structure and content of a log entry
func ValidateLogEntry(entry *LogEntry) error {
	if entry == nil {
		return fmt.Errorf("log entry is nil")
	}

	if entry.Topic == "" {
		return fmt.Errorf("log entry topic cannot be empty")
	}

	if entry.Type < EntryEnqueue || entry.Type > EntrySnapshotMark {
		return fmt.Errorf("invalid log entry type: %d", entry.Type)
	}

	if entry.Timestamp.IsZero() {
		return fmt.Errorf("log entry timestamp cannot be zero")
	}

	// Validate entry-specific data based on type
	switch entry.Type {
	case EntryEnqueue:
		var enqueueEntry EnqueueEntry
		if err := entry.DecodeData(&enqueueEntry); err != nil {
			return fmt.Errorf("invalid enqueue entry data: %w", err)
		}
		if enqueueEntry.Message == nil {
			return fmt.Errorf("enqueue entry must have a message")
		}
		if enqueueEntry.ProducerID == "" {
			return fmt.Errorf("enqueue entry must have a producer ID")
		}

	case EntryAck:
		var ackEntry AckEntry
		if err := entry.DecodeData(&ackEntry); err != nil {
			return fmt.Errorf("invalid ack entry data: %w", err)
		}
		if ackEntry.MessageID == "" {
			return fmt.Errorf("ack entry must have a message ID")
		}
		if ackEntry.LeaseToken == "" {
			return fmt.Errorf("ack entry must have a lease token")
		}

	case EntryNack:
		var nackEntry NackEntry
		if err := entry.DecodeData(&nackEntry); err != nil {
			return fmt.Errorf("invalid nack entry data: %w", err)
		}
		if nackEntry.MessageID == "" {
			return fmt.Errorf("nack entry must have a message ID")
		}
		if nackEntry.LeaseToken == "" {
			return fmt.Errorf("nack entry must have a lease token")
		}
	}

	return nil
}

// LogEntryBatch represents a batch of log entries for efficient processing
type LogEntryBatch struct {
	Entries   []*LogEntry `json:"entries"`
	TotalSize int         `json:"total_size"`
	BatchID   string      `json:"batch_id"`
	CreatedAt time.Time   `json:"created_at"`
}

// NewLogEntryBatch creates a new batch with the given entries
func NewLogEntryBatch(entries []*LogEntry) *LogEntryBatch {
	totalSize := 0
	for _, entry := range entries {
		totalSize += entry.Size()
	}

	return &LogEntryBatch{
		Entries:   entries,
		TotalSize: totalSize,
		BatchID:   fmt.Sprintf("batch_%d", time.Now().UnixNano()),
		CreatedAt: time.Now().UTC(),
	}
}

// AddEntry adds an entry to the batch and updates the total size
func (batch *LogEntryBatch) AddEntry(entry *LogEntry) {
	batch.Entries = append(batch.Entries, entry)
	batch.TotalSize += entry.Size()
}

// IsEmpty returns true if the batch contains no entries
func (batch *LogEntryBatch) IsEmpty() bool {
	return len(batch.Entries) == 0
}

// Count returns the number of entries in the batch
func (batch *LogEntryBatch) Count() int {
	return len(batch.Entries)
}
