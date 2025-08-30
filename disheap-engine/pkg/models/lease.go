package models

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	// ErrInvalidLeaseToken is returned when a lease token is invalid
	ErrInvalidLeaseToken = errors.New("invalid lease token")
	// ErrLeaseExpired is returned when a lease has expired
	ErrLeaseExpired = errors.New("lease expired")
)

// LeaseToken represents an opaque lease token
type LeaseToken string

// NewLeaseToken generates a new cryptographically secure lease token
func NewLeaseToken() LeaseToken {
	// Generate 32 random bytes and encode as base64
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		panic(fmt.Sprintf("failed to generate lease token: %v", err))
	}
	return LeaseToken(base64.URLEncoding.EncodeToString(bytes))
}

// String returns the string representation of the lease token
func (lt LeaseToken) String() string {
	return string(lt)
}

// IsValid checks if the lease token is valid (not empty and proper format)
func (lt LeaseToken) IsValid() bool {
	if string(lt) == "" {
		return false
	}

	// Attempt to decode to verify it's valid base64
	_, err := base64.URLEncoding.DecodeString(string(lt))
	return err == nil
}

// Lease represents a message lease for exactly-once delivery
type Lease struct {
	Token     LeaseToken `json:"token"`
	Holder    string     `json:"holder"` // Consumer/client identifier
	CreatedAt time.Time  `json:"created_at"`
	GrantedAt time.Time  `json:"granted_at"` // Same as CreatedAt for compatibility
	Deadline  time.Time  `json:"deadline"`
	MessageID MessageID  `json:"message_id"`

	// Extension tracking
	OriginalDuration time.Duration `json:"original_duration"`
	ExtensionCount   uint32        `json:"extension_count"`
	MaxExtensions    uint32        `json:"max_extensions"`
}

// NewLease creates a new lease for a message
func NewLease(messageID MessageID, holder string, duration time.Duration) *Lease {
	now := time.Now()
	return &Lease{
		Token:            NewLeaseToken(),
		Holder:           holder,
		CreatedAt:        now,
		GrantedAt:        now, // Same as CreatedAt
		Deadline:         now.Add(duration),
		MessageID:        messageID,
		OriginalDuration: duration,
		ExtensionCount:   0,
		MaxExtensions:    5, // Default max extensions
	}
}

// Validate performs validation of the lease
func (l *Lease) Validate() error {
	if !l.Token.IsValid() {
		return fmt.Errorf("%w: %s", ErrInvalidLeaseToken, l.Token)
	}

	if l.Holder == "" {
		return errors.New("lease holder cannot be empty")
	}

	if !l.MessageID.IsValid() {
		return fmt.Errorf("lease has invalid message ID: %s", l.MessageID)
	}

	if l.CreatedAt.IsZero() {
		return errors.New("lease created_at cannot be zero")
	}

	if l.GrantedAt.IsZero() {
		return errors.New("lease granted_at cannot be zero")
	}

	if l.Deadline.IsZero() {
		return errors.New("lease deadline cannot be zero")
	}

	if l.Deadline.Before(l.CreatedAt) {
		return errors.New("lease deadline cannot be before created_at")
	}

	return nil
}

// IsExpired returns true if the lease has expired
func (l *Lease) IsExpired() bool {
	return time.Now().After(l.Deadline)
}

// TimeUntilExpiry returns the duration until the lease expires
func (l *Lease) TimeUntilExpiry() time.Duration {
	remaining := time.Until(l.Deadline)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// CanExtend returns true if the lease can be extended
func (l *Lease) CanExtend() bool {
	return l.ExtensionCount < l.MaxExtensions && !l.IsExpired()
}

// Extend extends the lease by the given duration
func (l *Lease) Extend(duration time.Duration) error {
	if !l.CanExtend() {
		if l.ExtensionCount >= l.MaxExtensions {
			return errors.New("lease extension limit reached")
		}
		return ErrLeaseExpired
	}

	l.Deadline = l.Deadline.Add(duration)
	l.ExtensionCount++
	return nil
}

// Renew renews the lease with a new deadline from now
func (l *Lease) Renew(duration time.Duration) {
	l.Deadline = time.Now().Add(duration)
	// Reset extension count on renewal
	l.ExtensionCount = 0
}

// Age returns how long the lease has been active
func (l *Lease) Age() time.Duration {
	return time.Since(l.CreatedAt)
}

// LeaseRegistry manages active leases for a partition
type LeaseRegistry struct {
	mu     sync.RWMutex
	leases map[MessageID]*Lease
	tokens map[LeaseToken]*Lease
	logger *zap.Logger
}

// NewLeaseRegistry creates a new lease registry
func NewLeaseRegistry(logger *zap.Logger) *LeaseRegistry {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &LeaseRegistry{
		leases: make(map[MessageID]*Lease),
		tokens: make(map[LeaseToken]*Lease),
		logger: logger,
	}
}

// Grant registers an existing lease
func (lr *LeaseRegistry) Grant(ctx context.Context, lease *Lease) error {
	if lease == nil {
		return fmt.Errorf("lease cannot be nil")
	}

	lr.mu.Lock()
	defer lr.mu.Unlock()

	// Check if message already has a lease
	if existing, exists := lr.leases[lease.MessageID]; exists {
		return fmt.Errorf("message %s already has lease %s", lease.MessageID, existing.Token)
	}

	// Check if token already exists
	if _, exists := lr.tokens[lease.Token]; exists {
		return fmt.Errorf("lease token %s already exists", lease.Token)
	}

	lr.leases[lease.MessageID] = lease
	lr.tokens[lease.Token] = lease

	lr.logger.Debug("Lease granted",
		zap.String("message_id", string(lease.MessageID)),
		zap.String("holder", lease.Holder),
		zap.String("token", string(lease.Token)),
	)

	return nil
}

// Revoke removes a lease and returns it
func (lr *LeaseRegistry) Revoke(ctx context.Context, messageID MessageID, token LeaseToken) (*Lease, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	lease, exists := lr.leases[messageID]
	if !exists {
		return nil, fmt.Errorf("lease not found for message %s", messageID)
	}

	if lease.Token != token {
		return nil, fmt.Errorf("token mismatch for message %s", messageID)
	}

	delete(lr.leases, messageID)
	delete(lr.tokens, token)

	lr.logger.Debug("Lease revoked",
		zap.String("message_id", string(messageID)),
		zap.String("holder", lease.Holder),
		zap.String("token", string(token)),
	)

	return lease, nil
}

// Extend extends a lease duration and returns the updated lease
func (lr *LeaseRegistry) Extend(ctx context.Context, token LeaseToken, extension time.Duration) (*Lease, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	lease, exists := lr.tokens[token]
	if !exists {
		return nil, fmt.Errorf("lease token %s not found", token)
	}

	if lease.IsExpired() {
		return nil, fmt.Errorf("lease %s is expired", token)
	}

	if !lease.CanExtend() {
		return nil, fmt.Errorf("lease %s cannot be extended", token)
	}

	if err := lease.Extend(extension); err != nil {
		return nil, err
	}

	lr.logger.Debug("Lease extended",
		zap.String("message_id", string(lease.MessageID)),
		zap.String("holder", lease.Holder),
		zap.String("token", string(token)),
		zap.Duration("extension", extension),
		zap.Time("new_deadline", lease.Deadline),
	)

	return lease, nil
}

// ActiveLeases returns the count of active (non-expired) leases
func (lr *LeaseRegistry) ActiveLeases() int {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	count := 0
	for _, lease := range lr.leases {
		if !lease.IsExpired() {
			count++
		}
	}
	return count
}

// Get retrieves a lease by message ID
func (lr *LeaseRegistry) Get(messageID MessageID) (*Lease, bool) {
	lease, exists := lr.leases[messageID]
	return lease, exists
}

// GetByToken retrieves a lease by token
func (lr *LeaseRegistry) GetByToken(token LeaseToken) (*Lease, bool) {
	for _, lease := range lr.leases {
		if lease.Token == token {
			return lease, true
		}
	}
	return nil, false
}

// Release releases a lease for a message
func (lr *LeaseRegistry) Release(messageID MessageID) bool {
	_, exists := lr.leases[messageID]
	if exists {
		delete(lr.leases, messageID)
	}
	return exists
}

// ReleaseByToken releases a lease by token
func (lr *LeaseRegistry) ReleaseByToken(token LeaseToken) bool {
	for messageID, lease := range lr.leases {
		if lease.Token == token {
			delete(lr.leases, messageID)
			return true
		}
	}
	return false
}

// CleanExpired removes all expired leases and returns the message IDs
func (lr *LeaseRegistry) CleanExpired() []MessageID {
	var expired []MessageID
	for messageID, lease := range lr.leases {
		if lease.IsExpired() {
			expired = append(expired, messageID)
			delete(lr.leases, messageID)
		}
	}
	return expired
}

// Count returns the number of active leases
func (lr *LeaseRegistry) Count() int {
	return len(lr.leases)
}

// List returns all active leases (copy)
func (lr *LeaseRegistry) List() []*Lease {
	leases := make([]*Lease, 0, len(lr.leases))
	for _, lease := range lr.leases {
		leases = append(leases, lease)
	}
	return leases
}

// IsLeased returns true if a message is currently leased
func (lr *LeaseRegistry) IsLeased(messageID MessageID) bool {
	lease, exists := lr.leases[messageID]
	return exists && !lease.IsExpired()
}

// GetLeaseByToken returns a lease by its token
func (lr *LeaseRegistry) GetLeaseByToken(token LeaseToken) *Lease {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	return lr.tokens[token]
}
