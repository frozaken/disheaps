package models

import (
	"strings"
	"testing"
	"time"
)

func TestNewMessageID(t *testing.T) {
	id1 := NewMessageID()
	id2 := NewMessageID()

	// IDs should be different
	if id1 == id2 {
		t.Error("NewMessageID should generate unique IDs")
	}

	// IDs should be valid ULIDs
	if !id1.IsValid() {
		t.Error("Generated ID should be valid")
	}
	if !id2.IsValid() {
		t.Error("Generated ID should be valid")
	}
}

func TestParseMessageID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid ULID", "01H5GBVMG3D1SQW1YB4XFSJKHR", false},
		{"empty string", "", true},
		{"too short", "123", true},
		{"too long", "01H5GBVMG3D1SQW1YB4XFSJKHR123", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := ParseMessageID(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMessageID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !id.IsValid() {
				t.Error("Parsed ID should be valid")
			}
		})
	}
}

func TestMessageID_Time(t *testing.T) {
	now := time.Now()
	id := NewMessageID()

	// ID time should be close to now (within a few seconds)
	idTime := id.Time()
	if idTime.Before(now.Add(-5*time.Second)) || idTime.After(now.Add(5*time.Second)) {
		t.Errorf("ID time %v should be close to %v", idTime, now)
	}
}

func TestNewMessage(t *testing.T) {
	topic := "test-topic"
	partitionID := uint32(5)
	priority := int64(100)
	payload := []byte("test payload")

	msg := NewMessage(topic, partitionID, priority, payload)

	if msg.Topic != topic {
		t.Errorf("Topic = %v, want %v", msg.Topic, topic)
	}
	if msg.PartitionID != partitionID {
		t.Errorf("PartitionID = %v, want %v", msg.PartitionID, partitionID)
	}
	if msg.Priority != priority {
		t.Errorf("Priority = %v, want %v", msg.Priority, priority)
	}
	if string(msg.Payload) != string(payload) {
		t.Errorf("Payload = %v, want %v", string(msg.Payload), string(payload))
	}
	if !msg.ID.IsValid() {
		t.Error("Message ID should be valid")
	}
	if msg.Attempts != 0 {
		t.Errorf("Attempts = %v, want 0", msg.Attempts)
	}
	if msg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %v, want 3", msg.MaxRetries)
	}
}

func TestMessage_Validate(t *testing.T) {
	tests := []struct {
		name            string
		msg             *Message
		maxPayloadSize  uint64
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "valid message",
			msg: &Message{
				ID:          NewMessageID(),
				Topic:       "test-topic",
				PartitionID: 1,
				Priority:    100,
				EnqueuedAt:  time.Now(),
				Payload:     []byte("test"),
				MaxRetries:  3,
			},
			maxPayloadSize: 1000,
			wantErr:        false,
		},
		{
			name: "invalid message ID",
			msg: &Message{
				ID:          MessageID("invalid"),
				Topic:       "test-topic",
				PartitionID: 1,
				Priority:    100,
				EnqueuedAt:  time.Now(),
				Payload:     []byte("test"),
			},
			maxPayloadSize:  1000,
			wantErr:         true,
			wantErrContains: "invalid message ID",
		},
		{
			name: "empty topic",
			msg: &Message{
				ID:          NewMessageID(),
				Topic:       "",
				PartitionID: 1,
				Priority:    100,
				EnqueuedAt:  time.Now(),
				Payload:     []byte("test"),
			},
			maxPayloadSize:  1000,
			wantErr:         true,
			wantErrContains: "topic cannot be empty",
		},
		{
			name: "empty payload",
			msg: &Message{
				ID:          NewMessageID(),
				Topic:       "test-topic",
				PartitionID: 1,
				Priority:    100,
				EnqueuedAt:  time.Now(),
				Payload:     []byte{},
			},
			maxPayloadSize:  1000,
			wantErr:         true,
			wantErrContains: "empty payload",
		},
		{
			name: "payload too large",
			msg: &Message{
				ID:          NewMessageID(),
				Topic:       "test-topic",
				PartitionID: 1,
				Priority:    100,
				EnqueuedAt:  time.Now(),
				Payload:     make([]byte, 1001),
			},
			maxPayloadSize:  1000,
			wantErr:         true,
			wantErrContains: "payload too large",
		},
		{
			name: "zero enqueued time",
			msg: &Message{
				ID:          NewMessageID(),
				Topic:       "test-topic",
				PartitionID: 1,
				Priority:    100,
				EnqueuedAt:  time.Time{},
				Payload:     []byte("test"),
			},
			maxPayloadSize:  1000,
			wantErr:         true,
			wantErrContains: "enqueued_at cannot be zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.msg.Validate(tt.maxPayloadSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.wantErrContains != "" {
				if !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Errorf("Error should contain %q, got %v", tt.wantErrContains, err)
				}
			}
		})
	}
}

func TestMessage_IsLeased(t *testing.T) {
	msg := NewMessage("topic", 1, 100, []byte("test"))

	// Initially not leased
	if msg.IsLeased() {
		t.Error("Message should not be leased initially")
	}

	// Add valid lease
	lease := NewLease(msg.ID, "holder-1", 30*time.Second)
	msg.Lease = &MessageLease{
		Token:      lease.Token,
		Holder:     lease.Holder,
		GrantedAt:  lease.GrantedAt,
		Deadline:   lease.Deadline,
		Extensions: 0,
	}
	if !msg.IsLeased() {
		t.Error("Message should be leased after adding valid lease")
	}

	// Expired lease
	msg.Lease.Deadline = time.Now().Add(-time.Second)
	if msg.IsLeased() {
		t.Error("Message should not be leased with expired lease")
	}
}

func TestMessage_IsReady(t *testing.T) {
	msg := NewMessage("topic", 1, 100, []byte("test"))

	// Initially ready
	if !msg.IsReady() {
		t.Error("Message should be ready initially")
	}

	// Not ready with backoff
	future := time.Now().Add(time.Hour)
	msg.NotBefore = &future
	if msg.IsReady() {
		t.Error("Message should not be ready with future not_before")
	}

	// Ready with past not_before
	past := time.Now().Add(-time.Hour)
	msg.NotBefore = &past
	if !msg.IsReady() {
		t.Error("Message should be ready with past not_before")
	}

	// Not ready when leased
	msg.NotBefore = nil
	lease2 := NewLease(msg.ID, "holder-1", 30*time.Second)
	msg.Lease = &MessageLease{
		Token:      lease2.Token,
		Holder:     lease2.Holder,
		GrantedAt:  lease2.GrantedAt,
		Deadline:   lease2.Deadline,
		Extensions: 0,
	}
	if msg.IsReady() {
		t.Error("Message should not be ready when leased")
	}
}

func TestMessage_CanRetry(t *testing.T) {
	msg := NewMessage("topic", 1, 100, []byte("test"))
	msg.MaxRetries = 3

	// Can retry initially
	if !msg.CanRetry() {
		t.Error("Message should be able to retry initially")
	}

	// Can retry after some attempts
	msg.Attempts = 2
	if !msg.CanRetry() {
		t.Error("Message should be able to retry with attempts < max")
	}

	// Cannot retry after max attempts
	msg.Attempts = 3
	if msg.CanRetry() {
		t.Error("Message should not be able to retry with attempts >= max")
	}
}

func TestMessage_SetBackoff(t *testing.T) {
	msg := NewMessage("topic", 1, 100, []byte("test"))

	backoffDuration := 5 * time.Minute
	msg.SetBackoff(backoffDuration)

	if msg.NotBefore == nil {
		t.Error("NotBefore should be set after SetBackoff")
	}

	// Should be approximately now + backoff duration
	expected := time.Now().Add(backoffDuration)
	if msg.NotBefore.Before(expected.Add(-time.Second)) || msg.NotBefore.After(expected.Add(time.Second)) {
		t.Errorf("NotBefore should be close to %v, got %v", expected, *msg.NotBefore)
	}
}

func TestMessage_ClearBackoff(t *testing.T) {
	msg := NewMessage("topic", 1, 100, []byte("test"))

	// Set backoff first
	msg.SetBackoff(time.Hour)
	if msg.NotBefore == nil {
		t.Error("NotBefore should be set")
	}

	// Clear backoff
	msg.ClearBackoff()
	if msg.NotBefore != nil {
		t.Error("NotBefore should be nil after ClearBackoff")
	}
}

func TestMessage_IncrementAttempts(t *testing.T) {
	msg := NewMessage("topic", 1, 100, []byte("test"))

	initial := msg.Attempts
	msg.IncrementAttempts()

	if msg.Attempts != initial+1 {
		t.Errorf("Attempts = %v, want %v", msg.Attempts, initial+1)
	}
}

func TestMessageComparisonKey_Less(t *testing.T) {
	now := time.Now()

	key1 := MessageComparisonKey{
		Priority:   100,
		EnqueuedAt: now,
		ID:         MessageID("key1"),
	}

	key2 := MessageComparisonKey{
		Priority:   200,
		EnqueuedAt: now,
		ID:         MessageID("key2"),
	}

	key3 := MessageComparisonKey{
		Priority:   100,
		EnqueuedAt: now.Add(time.Second),
		ID:         MessageID("key3"),
	}

	// Test MIN heap ordering
	if !key1.Less(key2, false) {
		t.Error("Lower priority should come first in min heap")
	}
	if key2.Less(key1, false) {
		t.Error("Higher priority should come later in min heap")
	}

	// Test MAX heap ordering
	if key1.Less(key2, true) {
		t.Error("Lower priority should come later in max heap")
	}
	if !key2.Less(key1, true) {
		t.Error("Higher priority should come first in max heap")
	}

	// Test time ordering (same priority)
	if !key1.Less(key3, false) {
		t.Error("Earlier time should come first")
	}

	// Test ID ordering (same priority and time)
	key4 := MessageComparisonKey{
		Priority:   100,
		EnqueuedAt: now,
		ID:         MessageID("key4"),
	}

	if !key1.Less(key4, false) {
		t.Error("Lexicographically smaller ID should come first")
	}
}

func TestProducerKey_String(t *testing.T) {
	pk := ProducerKey{
		ID:    "producer-123",
		Epoch: 42,
	}

	expected := "producer-123:42"
	if pk.String() != expected {
		t.Errorf("String() = %v, want %v", pk.String(), expected)
	}
}
