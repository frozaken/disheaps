package delivery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

var (
	ErrMessageNotFound    = errors.New("message not found")
	ErrInvalidLease       = errors.New("invalid lease token")
	ErrLeaseExpired       = errors.New("lease has expired")
	ErrAlreadyLeased      = errors.New("message already leased")
	ErrDeliveryTimeout    = errors.New("delivery timeout")
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
)

// DeliveryManager coordinates exactly-once message delivery
type DeliveryManager struct {
	mu           sync.RWMutex
	activeLeases map[models.MessageID]*models.MessageLease
	logger       *zap.Logger

	// Configuration
	maxLeaseExtensions uint32
	maxLeaseExtension  time.Duration
	leaseTimeout       time.Duration

	// Metrics
	deliveredCount uint64
	ackedCount     uint64
	nackedCount    uint64
	timeoutCount   uint64
}

// DeliveryConfig holds configuration for the delivery manager
type DeliveryConfig struct {
	MaxLeaseExtensions  uint32        `json:"max_lease_extensions"`
	MaxLeaseExtension   time.Duration `json:"max_lease_extension"`
	DefaultLeaseTimeout time.Duration `json:"default_lease_timeout"`
}

// NewDeliveryManager creates a new delivery manager
func NewDeliveryManager(config *DeliveryConfig, logger *zap.Logger) *DeliveryManager {
	if config == nil {
		config = &DeliveryConfig{
			MaxLeaseExtensions:  10,
			MaxLeaseExtension:   5 * time.Minute,
			DefaultLeaseTimeout: 30 * time.Second,
		}
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	return &DeliveryManager{
		activeLeases:       make(map[models.MessageID]*models.MessageLease),
		logger:             logger,
		maxLeaseExtensions: config.MaxLeaseExtensions,
		maxLeaseExtension:  config.MaxLeaseExtension,
		leaseTimeout:       config.DefaultLeaseTimeout,
	}
}

// LeaseMessage creates a lease for a message to enable exactly-once delivery
func (dm *DeliveryManager) LeaseMessage(ctx context.Context, messageID models.MessageID, holder string, timeout time.Duration) (*models.MessageLease, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if message is already leased
	if existingLease, exists := dm.activeLeases[messageID]; exists {
		if !existingLease.IsExpired() {
			dm.logger.Debug("Message already leased",
				zap.String("message_id", string(messageID)),
				zap.String("existing_holder", existingLease.Holder),
				zap.String("requested_holder", holder))
			return nil, ErrAlreadyLeased
		}
		// Clean up expired lease
		delete(dm.activeLeases, messageID)
	}

	// Use default timeout if not specified
	if timeout <= 0 {
		timeout = dm.leaseTimeout
	}

	// Create new lease
	lease := &models.MessageLease{
		Token:      models.NewLeaseToken(),
		Holder:     holder,
		GrantedAt:  time.Now(),
		Deadline:   time.Now().Add(timeout),
		Extensions: 0,
	}

	dm.activeLeases[messageID] = lease

	dm.logger.Debug("Message leased",
		zap.String("component", "delivery_manager"),
		zap.String("message_id", string(messageID)),
		zap.String("holder", holder),
		zap.String("token", string(lease.Token)),
		zap.Duration("timeout", timeout))

	return lease, nil
}

// AckMessage acknowledges successful message processing and removes the lease
func (dm *DeliveryManager) AckMessage(ctx context.Context, messageID models.MessageID, token models.LeaseToken) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	lease, exists := dm.activeLeases[messageID]
	if !exists {
		dm.logger.Debug("Lease not found for ack",
			zap.String("message_id", string(messageID)),
			zap.String("token", string(token)))
		return ErrMessageNotFound
	}

	if lease.Token != token {
		dm.logger.Debug("Invalid lease token for ack",
			zap.String("message_id", string(messageID)),
			zap.String("expected_token", string(lease.Token)),
			zap.String("provided_token", string(token)))
		return ErrInvalidLease
	}

	if lease.IsExpired() {
		dm.logger.Debug("Lease expired for ack",
			zap.String("message_id", string(messageID)),
			zap.String("token", string(token)),
			zap.Time("deadline", lease.Deadline))
		delete(dm.activeLeases, messageID)
		return ErrLeaseExpired
	}

	// Remove successful lease
	delete(dm.activeLeases, messageID)
	dm.ackedCount++

	dm.logger.Debug("Message acknowledged",
		zap.String("component", "delivery_manager"),
		zap.String("message_id", string(messageID)),
		zap.String("holder", lease.Holder),
		zap.String("token", string(token)))

	return nil
}

// NackMessage handles message processing failure and removes the lease
func (dm *DeliveryManager) NackMessage(ctx context.Context, messageID models.MessageID, token models.LeaseToken) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	lease, exists := dm.activeLeases[messageID]
	if !exists {
		dm.logger.Debug("Lease not found for nack",
			zap.String("message_id", string(messageID)),
			zap.String("token", string(token)))
		return ErrMessageNotFound
	}

	if lease.Token != token {
		dm.logger.Debug("Invalid lease token for nack",
			zap.String("message_id", string(messageID)),
			zap.String("expected_token", string(lease.Token)),
			zap.String("provided_token", string(token)))
		return ErrInvalidLease
	}

	// Remove failed lease (caller should handle retry logic)
	delete(dm.activeLeases, messageID)
	dm.nackedCount++

	dm.logger.Debug("Message nacked",
		zap.String("component", "delivery_manager"),
		zap.String("message_id", string(messageID)),
		zap.String("holder", lease.Holder),
		zap.String("token", string(token)))

	return nil
}

// ExtendLease extends the deadline of an active lease
func (dm *DeliveryManager) ExtendLease(ctx context.Context, messageID models.MessageID, token models.LeaseToken, extension time.Duration) (*models.MessageLease, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	lease, exists := dm.activeLeases[messageID]
	if !exists {
		dm.logger.Debug("Lease not found for extension",
			zap.String("message_id", string(messageID)),
			zap.String("token", string(token)))
		return nil, ErrMessageNotFound
	}

	if lease.Token != token {
		dm.logger.Debug("Invalid lease token for extension",
			zap.String("message_id", string(messageID)),
			zap.String("expected_token", string(lease.Token)),
			zap.String("provided_token", string(token)))
		return nil, ErrInvalidLease
	}

	if lease.IsExpired() {
		dm.logger.Debug("Lease expired for extension",
			zap.String("message_id", string(messageID)),
			zap.String("token", string(token)),
			zap.Time("deadline", lease.Deadline))
		delete(dm.activeLeases, messageID)
		return nil, ErrLeaseExpired
	}

	// Check extension limits
	if uint32(lease.Extensions) >= dm.maxLeaseExtensions {
		dm.logger.Debug("Maximum lease extensions exceeded",
			zap.String("message_id", string(messageID)),
			zap.Uint32("extensions", uint32(lease.Extensions)),
			zap.Uint32("max_extensions", dm.maxLeaseExtensions))
		return nil, fmt.Errorf("maximum lease extensions (%d) exceeded", dm.maxLeaseExtensions)
	}

	if extension > dm.maxLeaseExtension {
		dm.logger.Debug("Extension too large, capping",
			zap.String("message_id", string(messageID)),
			zap.Duration("requested", extension),
			zap.Duration("max_extension", dm.maxLeaseExtension))
		extension = dm.maxLeaseExtension
	}

	// Extend the lease
	oldDeadline := lease.Deadline
	lease.Deadline = lease.Deadline.Add(extension)
	lease.Extensions++

	dm.logger.Debug("Lease extended",
		zap.String("component", "delivery_manager"),
		zap.String("message_id", string(messageID)),
		zap.String("holder", lease.Holder),
		zap.String("token", string(token)),
		zap.Duration("extension", extension),
		zap.Time("old_deadline", oldDeadline),
		zap.Time("new_deadline", lease.Deadline),
		zap.Uint32("total_extensions", uint32(lease.Extensions)))

	return lease, nil
}

// IsMessageLeased checks if a message currently has an active lease
func (dm *DeliveryManager) IsMessageLeased(messageID models.MessageID) bool {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	lease, exists := dm.activeLeases[messageID]
	if !exists {
		return false
	}

	if lease.IsExpired() {
		// Clean up expired lease in background
		go func() {
			dm.mu.Lock()
			defer dm.mu.Unlock()
			if expiredLease, stillExists := dm.activeLeases[messageID]; stillExists && expiredLease.IsExpired() {
				delete(dm.activeLeases, messageID)
				dm.timeoutCount++
			}
		}()
		return false
	}

	return true
}

// GetActiveLease retrieves the active lease for a message
func (dm *DeliveryManager) GetActiveLease(messageID models.MessageID) (*models.MessageLease, bool) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	lease, exists := dm.activeLeases[messageID]
	if !exists || lease.IsExpired() {
		return nil, false
	}

	return lease, true
}

// CleanExpiredLeases removes all expired leases
func (dm *DeliveryManager) CleanExpiredLeases(ctx context.Context) []models.MessageID {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var expired []models.MessageID
	now := time.Now()

	for messageID, lease := range dm.activeLeases {
		if lease.Deadline.Before(now) {
			expired = append(expired, messageID)
			delete(dm.activeLeases, messageID)
			dm.timeoutCount++
		}
	}

	if len(expired) > 0 {
		dm.logger.Debug("Cleaned expired leases",
			zap.String("component", "delivery_manager"),
			zap.Int("count", len(expired)))
	}

	return expired
}

// Stats returns delivery manager statistics
func (dm *DeliveryManager) Stats() DeliveryStats {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	return DeliveryStats{
		ActiveLeases:   uint64(len(dm.activeLeases)),
		DeliveredCount: dm.deliveredCount,
		AckedCount:     dm.ackedCount,
		NackedCount:    dm.nackedCount,
		TimeoutCount:   dm.timeoutCount,
	}
}

// DeliveryStats holds delivery manager metrics
type DeliveryStats struct {
	ActiveLeases   uint64 `json:"active_leases"`
	DeliveredCount uint64 `json:"delivered_count"`
	AckedCount     uint64 `json:"acked_count"`
	NackedCount    uint64 `json:"nacked_count"`
	TimeoutCount   uint64 `json:"timeout_count"`
}
