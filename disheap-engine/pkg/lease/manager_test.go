package lease

import (
	"context"
	"testing"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap/zaptest"
)

func TestLeaseManager_BasicOperations(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		DefaultTimeout:  10 * time.Second,
		MaxExtensions:   5,
		CleanupInterval: 100 * time.Millisecond,
	}

	manager := NewManager(config, logger)
	defer manager.Close()

	ctx := context.Background()
	topic := "test-topic"
	partition := uint32(1)
	messageID := models.MessageID("test-message-1")
	holder := "test-consumer"

	// Test lease granting
	lease, err := manager.GrantLease(ctx, topic, partition, messageID, holder, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to grant lease: %v", err)
	}

	if lease.MessageID != messageID {
		t.Errorf("Expected message ID %s, got %s", messageID, lease.MessageID)
	}

	if lease.Holder != holder {
		t.Errorf("Expected holder %s, got %s", holder, lease.Holder)
	}

	// Test lease retrieval
	retrievedLease, err := manager.GetLease(topic, partition, messageID)
	if err != nil {
		t.Fatalf("Failed to get lease: %v", err)
	}

	if retrievedLease.Token != lease.Token {
		t.Errorf("Expected token %s, got %s", lease.Token, retrievedLease.Token)
	}

	// Test lease checking
	if !manager.IsLeased(topic, partition, messageID) {
		t.Error("Message should be leased")
	}
}

func TestLeaseManager_MultipleRegistries(t *testing.T) {
	logger := zaptest.NewLogger(t)
	manager := NewManager(DefaultConfig(), logger)
	defer manager.Close()

	ctx := context.Background()

	// Create leases in different partitions
	topic1, partition1 := "topic1", uint32(1)
	topic2, partition2 := "topic2", uint32(2)

	messageID1 := models.MessageID("message-1")
	messageID2 := models.MessageID("message-2")

	_, err := manager.GrantLease(ctx, topic1, partition1, messageID1, "consumer1", 0)
	if err != nil {
		t.Fatalf("Failed to grant lease1: %v", err)
	}

	_, err = manager.GrantLease(ctx, topic2, partition2, messageID2, "consumer2", 0)
	if err != nil {
		t.Fatalf("Failed to grant lease2: %v", err)
	}

	// Verify leases are in separate registries
	registry1 := manager.GetRegistry(topic1, partition1)
	registry2 := manager.GetRegistry(topic2, partition2)

	if registry1 == registry2 {
		t.Error("Expected different registries for different topic/partition combinations")
	}

	// Verify each lease is only in its respective registry
	if !manager.IsLeased(topic1, partition1, messageID1) {
		t.Error("Message1 should be leased in registry1")
	}

	if !manager.IsLeased(topic2, partition2, messageID2) {
		t.Error("Message2 should be leased in registry2")
	}

	if manager.IsLeased(topic1, partition1, messageID2) {
		t.Error("Message2 should not be leased in registry1")
	}

	if manager.IsLeased(topic2, partition2, messageID1) {
		t.Error("Message1 should not be leased in registry2")
	}
}
