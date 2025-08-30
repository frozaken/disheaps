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

	// Start the manager
	err := manager.Start()
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

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

	// Test lease extension
	extendedLease, err := manager.ExtendLease(ctx, topic, partition, lease.Token, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to extend lease: %v", err)
	}

	if !extendedLease.Deadline.After(lease.Deadline) {
		t.Error("Extended lease deadline should be after original deadline")
	}

	// Test lease revocation
	revokedLease, err := manager.RevokeLease(ctx, topic, partition, messageID, lease.Token)
	if err != nil {
		t.Fatalf("Failed to revoke lease: %v", err)
	}

	if revokedLease.MessageID != messageID {
		t.Errorf("Expected revoked message ID %s, got %s", messageID, revokedLease.MessageID)
	}

	// Test lease no longer exists
	if manager.IsLeased(topic, partition, messageID) {
		t.Error("Message should not be leased after revocation")
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

	lease1, err := manager.GrantLease(ctx, topic1, partition1, messageID1, "consumer1", 0)
	if err != nil {
		t.Fatalf("Failed to grant lease1: %v", err)
	}

	lease2, err := manager.GrantLease(ctx, topic2, partition2, messageID2, "consumer2", 0)
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

	// Test cleanup affects correct registry
	_, err = manager.RevokeLease(ctx, topic1, partition1, messageID1, lease1.Token)
	if err != nil {
		t.Fatalf("Failed to revoke lease1: %v", err)
	}

	// Verify lease2 is still active
	if !manager.IsLeased(topic2, partition2, messageID2) {
		t.Error("Message2 should still be leased after revoking lease1")
	}
}

func TestLeaseManager_Statistics(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		DefaultTimeout:  100 * time.Millisecond,
		CleanupInterval: 50 * time.Millisecond,
	}

	manager := NewManager(config, logger)
	defer manager.Close()

	err := manager.Start()
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	ctx := context.Background()
	topic := "test-topic"
	partition := uint32(1)

	// Initial stats
	stats := manager.GetStats()
	if stats.TotalLeases != 0 {
		t.Errorf("Expected 0 total leases initially, got %d", stats.TotalLeases)
	}

	// Grant some leases
	messageID1 := models.MessageID("message-1")
	messageID2 := models.MessageID("message-2")

	lease1, err := manager.GrantLease(ctx, topic, partition, messageID1, "consumer1", 0)
	if err != nil {
		t.Fatalf("Failed to grant lease1: %v", err)
	}

	lease2, err := manager.GrantLease(ctx, topic, partition, messageID2, "consumer2", 0)
	if err != nil {
		t.Fatalf("Failed to grant lease2: %v", err)
	}

	// Check stats after granting
	stats = manager.GetStats()
	if stats.TotalLeases != 2 {
		t.Errorf("Expected 2 total leases, got %d", stats.TotalLeases)
	}

	// Extend a lease
	_, err = manager.ExtendLease(ctx, topic, partition, lease1.Token, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to extend lease: %v", err)
	}

	stats = manager.GetStats()
	if stats.ExtendedLeases != 1 {
		t.Errorf("Expected 1 extended lease, got %d", stats.ExtendedLeases)
	}

	// Revoke a lease
	_, err = manager.RevokeLease(ctx, topic, partition, messageID2, lease2.Token)
	if err != nil {
		t.Fatalf("Failed to revoke lease: %v", err)
	}

	stats = manager.GetStats()
	if stats.RevokedLeases != 1 {
		t.Errorf("Expected 1 revoked lease, got %d", stats.RevokedLeases)
	}

	// Wait for lease to expire and cleanup to run
	time.Sleep(200 * time.Millisecond)

	stats = manager.GetStats()
	if stats.CleanupRuns == 0 {
		t.Error("Expected at least one cleanup run")
	}

	if stats.ExpiredLeases == 0 {
		t.Error("Expected at least one expired lease")
	}
}

func TestLeaseManager_CleanupExpiredLeases(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		DefaultTimeout:  50 * time.Millisecond,
		CleanupInterval: 25 * time.Millisecond,
	}

	manager := NewManager(config, logger)
	defer manager.Close()

	err := manager.Start()
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	ctx := context.Background()
	topic := "test-topic"
	partition := uint32(1)
	messageID := models.MessageID("test-message")

	// Grant a short-lived lease
	lease, err := manager.GrantLease(ctx, topic, partition, messageID, "consumer", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to grant lease: %v", err)
	}

	// Verify lease exists
	if !manager.IsLeased(topic, partition, messageID) {
		t.Error("Message should be leased initially")
	}

	// Wait for lease to expire and cleanup to occur
	time.Sleep(150 * time.Millisecond)

	// Verify lease was cleaned up
	if manager.IsLeased(topic, partition, messageID) {
		t.Error("Message should not be leased after expiration and cleanup")
	}

	// Verify statistics were updated
	stats := manager.GetStats()
	if stats.ExpiredLeases == 0 {
		t.Error("Expected at least one expired lease in statistics")
	}

	if stats.CleanupRuns == 0 {
		t.Error("Expected at least one cleanup run in statistics")
	}

	// Try to extend expired lease - should fail
	_, err = manager.ExtendLease(ctx, topic, partition, lease.Token, time.Second)
	if err == nil {
		t.Error("Expected error when trying to extend expired lease")
	}
}

func TestLeaseManager_MaxExtensions(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &Config{
		DefaultTimeout:  time.Second,
		MaxExtensions:   2,           // Limit to 2 extensions
		CleanupInterval: time.Minute, // Long interval so cleanup doesn't interfere
	}

	manager := NewManager(config, logger)
	defer manager.Close()

	ctx := context.Background()
	topic := "test-topic"
	partition := uint32(1)
	messageID := models.MessageID("test-message")

	// Grant initial lease
	lease, err := manager.GrantLease(ctx, topic, partition, messageID, "consumer", time.Second)
	if err != nil {
		t.Fatalf("Failed to grant lease: %v", err)
	}

	// First extension should succeed
	_, err = manager.ExtendLease(ctx, topic, partition, lease.Token, time.Second)
	if err != nil {
		t.Fatalf("First extension should succeed: %v", err)
	}

	// Second extension should succeed
	_, err = manager.ExtendLease(ctx, topic, partition, lease.Token, time.Second)
	if err != nil {
		t.Fatalf("Second extension should succeed: %v", err)
	}

	// Third extension should fail (exceeds MaxExtensions)
	_, err = manager.ExtendLease(ctx, topic, partition, lease.Token, time.Second)
	if err == nil {
		t.Error("Third extension should fail due to MaxExtensions limit")
	}
}
