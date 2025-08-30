package models

import (
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// ErrInvalidMessageID is returned when a message ID is invalid
	ErrInvalidMessageID = errors.New("invalid message ID")
	// ErrEmptyPayload is returned when payload is empty when not allowed
	ErrEmptyPayload = errors.New("empty payload not allowed")
	// ErrPayloadTooLarge is returned when payload exceeds size limit
	ErrPayloadTooLarge = errors.New("payload too large")
)

// MessageID represents a unique message identifier (ULID)
type MessageID string

// NewMessageID generates a new ULID-based message ID
func NewMessageID() MessageID {
	return MessageID(ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String())
}

// ParseMessageID parses and validates a message ID string
func ParseMessageID(s string) (MessageID, error) {
	if _, err := ulid.Parse(s); err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidMessageID, s)
	}
	return MessageID(s), nil
}

// String returns the string representation of the message ID
func (m MessageID) String() string {
	return string(m)
}

// IsValid checks if the message ID is valid
func (m MessageID) IsValid() bool {
	_, err := ulid.Parse(string(m))
	return err == nil
}

// IsZero checks if the message ID is zero/empty
func (m MessageID) IsZero() bool {
	return string(m) == ""
}

// Time extracts the timestamp from the ULID message ID
func (m MessageID) Time() time.Time {
	if id, err := ulid.Parse(string(m)); err == nil {
		return ulid.Time(id.Time())
	}
	return time.Time{}
}

// Message represents a message in the heap
type Message struct {
	// Core fields
	ID          MessageID `json:"id"`
	Topic       string    `json:"topic"`
	PartitionID uint32    `json:"partition_id"`
	Priority    int64     `json:"priority"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
	Payload     []byte    `json:"payload"`

	// Producer tracking for idempotency
	ProducerID string `json:"producer_id,omitempty"`
	Epoch      uint64 `json:"epoch,omitempty"`
	Sequence   uint64 `json:"sequence,omitempty"`

	// Retry handling
	Attempts   uint32 `json:"attempts"`
	MaxRetries uint32 `json:"max_retries"`
	RetryCount int32  `json:"retry_count"`

	// Delayed visibility
	NotBefore *time.Time `json:"not_before,omitempty"`

	// Leasing state (nil when not leased)
	Lease *MessageLease `json:"lease,omitempty"`
}

// NewMessage creates a new message with generated ID and current timestamp
func NewMessage(topic string, partitionID uint32, priority int64, payload []byte) *Message {
	now := time.Now()
	return &Message{
		ID:          NewMessageID(),
		Topic:       topic,
		PartitionID: partitionID,
		Priority:    priority,
		EnqueuedAt:  now,
		Payload:     payload,
		Attempts:    0,
		MaxRetries:  3, // default
	}
}

// Validate performs comprehensive validation of the message
func (m *Message) Validate(maxPayloadSize uint64) error {
	if !m.ID.IsValid() {
		return fmt.Errorf("%w: %s", ErrInvalidMessageID, m.ID)
	}

	if m.Topic == "" {
		return errors.New("topic cannot be empty")
	}

	if len(m.Payload) == 0 {
		return ErrEmptyPayload
	}

	if uint64(len(m.Payload)) > maxPayloadSize {
		return fmt.Errorf("%w: %d bytes exceeds limit of %d", ErrPayloadTooLarge, len(m.Payload), maxPayloadSize)
	}

	if m.EnqueuedAt.IsZero() {
		return errors.New("enqueued_at cannot be zero")
	}

	// Validate lease if present
	if m.Lease != nil {
		if err := m.Lease.Validate(); err != nil {
			return fmt.Errorf("invalid lease: %w", err)
		}
	}

	return nil
}

// IsLeased returns true if the message is currently leased
func (m *Message) IsLeased() bool {
	return m.Lease != nil && !m.Lease.IsExpired()
}

// IsReady returns true if the message is ready to be popped
func (m *Message) IsReady() bool {
	if m.NotBefore != nil && time.Now().Before(*m.NotBefore) {
		return false
	}
	return !m.IsLeased()
}

// CanRetry returns true if the message can be retried
func (m *Message) CanRetry() bool {
	return m.Attempts < m.MaxRetries
}

// IncrementAttempts increments the attempt counter
func (m *Message) IncrementAttempts() {
	m.Attempts++
}

// SetBackoff sets the not_before time for backoff
func (m *Message) SetBackoff(backoffDuration time.Duration) {
	notBefore := time.Now().Add(backoffDuration)
	m.NotBefore = &notBefore
}

// ClearBackoff removes the not_before restriction
func (m *Message) ClearBackoff() {
	m.NotBefore = nil
}

// ComparisonKey returns a comparable key for heap ordering
// Messages are ordered by (priority, enqueued_time, id) for deterministic ordering
func (m *Message) ComparisonKey() MessageComparisonKey {
	return MessageComparisonKey{
		Priority:   m.Priority,
		EnqueuedAt: m.EnqueuedAt,
		ID:         m.ID,
	}
}

// ToProtoTimestamp converts time to protobuf timestamp
func (m *Message) ToProtoTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// MessageComparisonKey provides a comparable key for heap ordering
type MessageComparisonKey struct {
	Priority   int64
	EnqueuedAt time.Time
	ID         MessageID
}

// Less returns true if this key should be ordered before the other key
// For MIN heap: lower priority comes first
// For MAX heap: higher priority comes first (handled by caller)
func (k MessageComparisonKey) Less(other MessageComparisonKey, isMaxHeap bool) bool {
	// Primary sort: priority
	if k.Priority != other.Priority {
		if isMaxHeap {
			return k.Priority > other.Priority // Higher priority first in max heap
		}
		return k.Priority < other.Priority // Lower priority first in min heap
	}

	// Secondary sort: enqueued time (earlier first)
	if !k.EnqueuedAt.Equal(other.EnqueuedAt) {
		return k.EnqueuedAt.Before(other.EnqueuedAt)
	}

	// Tertiary sort: message ID (lexicographic for deterministic ordering)
	return k.ID < other.ID
}

// ProducerKey represents a unique producer identification
type ProducerKey struct {
	ID    string
	Epoch uint64
}

// String returns string representation of producer key
func (pk ProducerKey) String() string {
	return fmt.Sprintf("%s:%d", pk.ID, pk.Epoch)
}

// BatchOperationType represents the type of operation in a batch
type BatchOperationType int

const (
	// BatchOperationEnqueue enqueues a message
	BatchOperationEnqueue BatchOperationType = iota
	// BatchOperationAck acknowledges a message
	BatchOperationAck
	// BatchOperationNack negative acknowledges a message
	BatchOperationNack
)

// String returns string representation of batch operation type
func (bot BatchOperationType) String() string {
	switch bot {
	case BatchOperationEnqueue:
		return "enqueue"
	case BatchOperationAck:
		return "ack"
	case BatchOperationNack:
		return "nack"
	default:
		return "unknown"
	}
}

// BatchOperation represents a single operation in a transaction batch
type BatchOperation struct {
	// Type of operation
	Type BatchOperationType
	// Topic and partition for the operation
	Topic     string
	Partition int
	// For enqueue operations
	Message *Message
	// For ack/nack operations
	MessageID  MessageID
	LeaseToken LeaseToken
}
