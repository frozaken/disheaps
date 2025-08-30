package models

import (
	"context"
	"testing"
	"time"
)

func TestLeaseToken(t *testing.T) {
	token := NewLeaseToken()
	if token == "" {
		t.Error("NewLeaseToken should not return empty string")
	}

	// Different tokens should be different
	token2 := NewLeaseToken()
	if token == token2 {
		t.Error("NewLeaseToken should generate unique tokens")
	}

	// Test string conversion
	if token.String() != string(token) {
		t.Error("String() should return the string representation")
	}
}

func TestLease(t *testing.T) {
	messageID := NewMessageID()
	holder := "consumer-1"
	timeout := 30 * time.Second

	lease := NewLease(messageID, holder, timeout)

	// Check basic fields
	if lease.MessageID != messageID {
		t.Errorf("MessageID = %v, want %v", lease.MessageID, messageID)
	}
	if lease.Holder != holder {
		t.Errorf("Holder = %v, want %v", lease.Holder, holder)
	}
	if lease.Token == "" {
		t.Error("Token should not be empty")
	}

	// Check timestamps
	if lease.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
	if lease.GrantedAt.IsZero() {
		t.Error("GrantedAt should not be zero")
	}
	if lease.Deadline.IsZero() {
		t.Error("Deadline should not be zero")
	}

	expectedDeadline := lease.GrantedAt.Add(timeout)
	if !lease.Deadline.Equal(expectedDeadline) {
		t.Errorf("Deadline = %v, want %v", lease.Deadline, expectedDeadline)
	}
}

func TestLeaseValidation(t *testing.T) {
	messageID := NewMessageID()
	holder := "consumer-1"
	timeout := 30 * time.Second

	lease := NewLease(messageID, holder, timeout)

	// Valid lease should pass
	if err := lease.Validate(); err != nil {
		t.Errorf("Valid lease should pass validation, got %v", err)
	}

	// Test invalid cases
	testCases := []struct {
		name     string
		modifier func(*Lease)
	}{
		{
			name: "empty token",
			modifier: func(l *Lease) {
				l.Token = ""
			},
		},
		{
			name: "empty holder",
			modifier: func(l *Lease) {
				l.Holder = ""
			},
		},
		{
			name: "zero created time",
			modifier: func(l *Lease) {
				l.CreatedAt = time.Time{}
			},
		},
		{
			name: "zero granted time",
			modifier: func(l *Lease) {
				l.GrantedAt = time.Time{}
			},
		},
		{
			name: "zero deadline",
			modifier: func(l *Lease) {
				l.Deadline = time.Time{}
			},
		},
		{
			name: "deadline before granted",
			modifier: func(l *Lease) {
				l.Deadline = l.GrantedAt.Add(-1 * time.Hour)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			invalidLease := NewLease(messageID, holder, timeout)
			tc.modifier(invalidLease)

			if err := invalidLease.Validate(); err == nil {
				t.Errorf("%s should fail validation", tc.name)
			}
		})
	}
}

func TestLeaseIsExpired(t *testing.T) {
	messageID := NewMessageID()
	holder := "consumer-1"

	// Create an expired lease
	expiredLease := NewLease(messageID, holder, 1*time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	if !expiredLease.IsExpired() {
		t.Error("Expired lease should return true for IsExpired()")
	}

	// Create a non-expired lease
	activeLease := NewLease(messageID, holder, 1*time.Hour)

	if activeLease.IsExpired() {
		t.Error("Active lease should return false for IsExpired()")
	}
}

func TestLeaseAge(t *testing.T) {
	messageID := NewMessageID()
	holder := "consumer-1"
	timeout := 30 * time.Second

	lease := NewLease(messageID, holder, timeout)
	time.Sleep(1 * time.Millisecond) // Small delay to ensure age > 0

	age := lease.Age()
	if age <= 0 {
		t.Errorf("Age should be positive, got %v", age)
	}
	if age > 1*time.Second {
		t.Errorf("Age should be less than 1s, got %v", age)
	}
}

func TestLeaseRegistry(t *testing.T) {
	ctx := context.Background()
	registry := NewLeaseRegistry(nil)

	if registry.Count() != 0 {
		t.Errorf("New registry should have 0 leases, got %v", registry.Count())
	}

	// Grant a lease
	messageID := NewMessageID()
	holder := "holder-1"
	duration := 30 * time.Second

	// Create a lease
	lease := NewLease(messageID, holder, duration)
	err := registry.Grant(ctx, lease)
	if err != nil {
		t.Errorf("Grant should not return an error, got %v", err)
	}

	if registry.Count() != 1 {
		t.Errorf("Registry should have 1 lease, got %v", registry.Count())
	}

	// Get by message ID
	retrieved, exists := registry.Get(messageID)
	if !exists {
		t.Error("Should be able to get lease by message ID")
	}
	if retrieved.Token != lease.Token {
		t.Errorf("Retrieved lease token = %v, want %v", retrieved.Token, lease.Token)
	}

	// Get by token
	retrieved, exists = registry.GetByToken(lease.Token)
	if !exists {
		t.Error("Should be able to get lease by token")
	}
	if retrieved.MessageID != messageID {
		t.Errorf("Retrieved lease MessageID = %v, want %v", retrieved.MessageID, messageID)
	}

	// Revoke the lease
	revokedLease, err := registry.Revoke(ctx, messageID, lease.Token)
	if err != nil {
		t.Errorf("Revoke should not return an error, got %v", err)
	}
	if revokedLease.Token != lease.Token {
		t.Errorf("Revoked lease token = %v, want %v", revokedLease.Token, lease.Token)
	}

	if registry.Count() != 0 {
		t.Errorf("Registry should have 0 leases after revoke, got %v", registry.Count())
	}

	_, exists = registry.Get(messageID)
	if exists {
		t.Error("Should not be able to get revoked lease")
	}
}

func TestLeaseRegistryCleanExpired(t *testing.T) {
	ctx := context.Background()
	registry := NewLeaseRegistry(nil)

	// Grant a lease that will expire quickly
	messageID := NewMessageID()
	holder := "holder-1"

	expiredLease := NewLease(messageID, holder, 1*time.Millisecond)
	_ = registry.Grant(ctx, expiredLease)

	// Grant a lease that won't expire
	messageID2 := NewMessageID()
	activeLease := NewLease(messageID2, holder, 1*time.Hour)
	_ = registry.Grant(ctx, activeLease)

	if registry.Count() != 2 {
		t.Errorf("Registry should have 2 leases, got %v", registry.Count())
	}

	// Wait for expiration
	time.Sleep(2 * time.Millisecond)

	// Clean expired leases
	cleaned := registry.CleanExpired()
	if len(cleaned) != 1 {
		t.Errorf("Should have cleaned 1 expired lease, got %v", len(cleaned))
	}

	if registry.Count() != 1 {
		t.Errorf("Registry should have 1 lease after cleaning, got %v", registry.Count())
	}

	// Active lease should still exist
	_, exists := registry.Get(messageID2)
	if !exists {
		t.Error("Active lease should still exist after cleaning")
	}

	// Expired lease should be gone
	_, exists = registry.Get(messageID)
	if exists {
		t.Error("Expired lease should be removed after cleaning")
	}
}

func TestLeaseRegistryExtend(t *testing.T) {
	ctx := context.Background()
	registry := NewLeaseRegistry(nil)

	// Grant a lease
	messageID := NewMessageID()
	holder := "holder-1"
	duration := 30 * time.Second

	lease := NewLease(messageID, holder, duration)
	_ = registry.Grant(ctx, lease)

	originalDeadline := lease.Deadline
	extension := 30 * time.Second

	// Extend the lease
	extendedLease, err := registry.Extend(ctx, lease.Token, extension)
	if err != nil {
		t.Errorf("Extend should not return an error, got %v", err)
	}

	if !extendedLease.Deadline.After(originalDeadline) {
		t.Error("Extended lease deadline should be after original deadline")
	}

	expectedDeadline := originalDeadline.Add(extension)
	if !extendedLease.Deadline.Equal(expectedDeadline) {
		t.Errorf("Extended deadline = %v, want %v", extendedLease.Deadline, expectedDeadline)
	}

	// Verify the lease in registry is updated
	retrieved, exists := registry.Get(messageID)
	if !exists {
		t.Error("Should be able to get extended lease")
	}
	if !retrieved.Deadline.Equal(expectedDeadline) {
		t.Error("Registry should have updated deadline")
	}
}

func TestLeaseRegistryActiveLeases(t *testing.T) {
	ctx := context.Background()
	registry := NewLeaseRegistry(nil)

	if registry.ActiveLeases() != 0 {
		t.Errorf("Empty registry should have 0 active leases, got %v", registry.ActiveLeases())
	}

	// Add active lease
	messageID1 := NewMessageID()
	activeLease := NewLease(messageID1, "holder-1", 1*time.Hour)
	_ = registry.Grant(ctx, activeLease)

	// Add expired lease
	messageID2 := NewMessageID()
	expiredLease := NewLease(messageID2, "holder-2", 1*time.Millisecond)
	_ = registry.Grant(ctx, expiredLease)
	time.Sleep(2 * time.Millisecond)

	if registry.ActiveLeases() != 1 {
		t.Errorf("Should have 1 active lease, got %v", registry.ActiveLeases())
	}
}

func TestLeaseRegistryDuplicateGrant(t *testing.T) {
	ctx := context.Background()
	registry := NewLeaseRegistry(nil)

	messageID := NewMessageID()
	holder := "holder-1"
	duration := 30 * time.Second

	// Grant first lease
	lease1 := NewLease(messageID, holder, duration)
	err := registry.Grant(ctx, lease1)
	if err != nil {
		t.Errorf("First grant should succeed, got %v", err)
	}

	// Try to grant lease for same message
	lease2 := NewLease(messageID, holder, duration)
	err = registry.Grant(ctx, lease2)
	if err == nil {
		t.Error("Duplicate grant should fail")
	}

	// Try to grant lease with same token
	otherMessageID := NewMessageID()
	lease3 := NewLease(otherMessageID, holder, duration)
	lease3.Token = lease1.Token
	err = registry.Grant(ctx, lease3)
	if err == nil {
		t.Error("Grant with duplicate token should fail")
	}
}
