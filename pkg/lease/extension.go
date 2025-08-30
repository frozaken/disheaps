package lease

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// ExtensionPolicy defines how lease extensions are handled
type ExtensionPolicy struct {
	MaxExtensions      uint32        // Maximum number of extensions allowed
	ExtensionDuration  time.Duration // Default extension duration
	MaxTotalLifetime   time.Duration // Maximum total lease lifetime
	BackoffMultiplier  float64       // Backoff multiplier for successive extensions
	MaxBackoffDuration time.Duration // Maximum backoff duration
	GracePeriod        time.Duration // Grace period before considering lease expired
}

// DefaultExtensionPolicy returns sensible defaults for lease extensions
func DefaultExtensionPolicy() *ExtensionPolicy {
	return &ExtensionPolicy{
		MaxExtensions:      10,
		ExtensionDuration:  30 * time.Second,
		MaxTotalLifetime:   10 * time.Minute,
		BackoffMultiplier:  1.5,
		MaxBackoffDuration: 2 * time.Minute,
		GracePeriod:        5 * time.Second,
	}
}

// ExtensionManager handles lease extension logic with policies and backoff
type ExtensionManager struct {
	mu      sync.RWMutex
	logger  *zap.Logger
	manager *Manager
	policy  *ExtensionPolicy

	// Extension tracking
	extensions map[models.LeaseToken]*extensionState
}

// extensionState tracks the state of lease extensions
type extensionState struct {
	lease           *models.Lease
	originalTimeout time.Duration
	totalExtensions uint32
	lastExtension   time.Time
	backoffDuration time.Duration
}

// ExtensionResult represents the result of an extension attempt
type ExtensionResult struct {
	Success             bool
	Lease               *models.Lease
	NextAllowedAt       *time.Time
	RemainingExtensions uint32
	Error               error
}

// NewExtensionManager creates a new extension manager
func NewExtensionManager(manager *Manager, policy *ExtensionPolicy, logger *zap.Logger) *ExtensionManager {
	if policy == nil {
		policy = DefaultExtensionPolicy()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	return &ExtensionManager{
		logger:     logger.With(zap.String("component", "lease_extension")),
		manager:    manager,
		policy:     policy,
		extensions: make(map[models.LeaseToken]*extensionState),
	}
}

// ExtendLease attempts to extend a lease with policy enforcement
func (em *ExtensionManager) ExtendLease(ctx context.Context, topic string, partition uint32, token models.LeaseToken, requestedDuration time.Duration) *ExtensionResult {
	// Get current lease
	lease, err := em.manager.GetLease(topic, partition, models.MessageID(""))
	if err != nil {
		// Try to find lease by token in the registry
		registry := em.manager.GetRegistry(topic, partition)
		lease = registry.GetLeaseByToken(token)
		if lease == nil {
			return &ExtensionResult{
				Success: false,
				Error:   fmt.Errorf("lease not found for token %s", token),
			}
		}
	}

	// Validate lease token matches
	if lease.Token != token {
		return &ExtensionResult{
			Success: false,
			Error:   fmt.Errorf("lease token mismatch"),
		}
	}

	// Check if lease is already expired (with grace period)
	now := time.Now()
	if now.After(lease.Deadline.Add(em.policy.GracePeriod)) {
		return &ExtensionResult{
			Success: false,
			Error:   fmt.Errorf("lease has expired beyond grace period"),
		}
	}

	// Get or create extension state
	em.mu.Lock()
	state, exists := em.extensions[token]
	if !exists {
		state = &extensionState{
			lease:           lease,
			originalTimeout: lease.Deadline.Sub(lease.CreatedAt),
			totalExtensions: 0,
			lastExtension:   lease.CreatedAt,
			backoffDuration: 0,
		}
		em.extensions[token] = state
	}
	em.mu.Unlock()

	// Apply extension policies
	result := em.applyExtensionPolicies(state, requestedDuration)
	if !result.Success {
		return result
	}

	// Calculate actual extension duration
	extensionDuration := em.calculateExtensionDuration(state, requestedDuration)

	// Attempt the extension
	extendedLease, err := em.manager.ExtendLease(ctx, topic, partition, token, extensionDuration)
	if err != nil {
		return &ExtensionResult{
			Success: false,
			Error:   fmt.Errorf("failed to extend lease: %w", err),
		}
	}

	// Update extension state
	em.updateExtensionState(state, extensionDuration)

	em.logger.Debug("Lease extended successfully",
		zap.String("token", string(token)),
		zap.String("message_id", string(lease.MessageID)),
		zap.Duration("extension", extensionDuration),
		zap.Uint32("total_extensions", state.totalExtensions),
		zap.Time("new_deadline", extendedLease.Deadline))

	return &ExtensionResult{
		Success:             true,
		Lease:               extendedLease,
		RemainingExtensions: em.policy.MaxExtensions - state.totalExtensions,
	}
}

// applyExtensionPolicies checks if the extension is allowed by policy
func (em *ExtensionManager) applyExtensionPolicies(state *extensionState, requestedDuration time.Duration) *ExtensionResult {
	now := time.Now()

	// Check maximum extensions limit
	if state.totalExtensions >= em.policy.MaxExtensions {
		return &ExtensionResult{
			Success: false,
			Error:   fmt.Errorf("maximum extensions exceeded: %d >= %d", state.totalExtensions, em.policy.MaxExtensions),
		}
	}

	// Check maximum total lifetime
	totalLifetime := now.Sub(state.lease.CreatedAt) + requestedDuration
	if totalLifetime > em.policy.MaxTotalLifetime {
		return &ExtensionResult{
			Success: false,
			Error:   fmt.Errorf("maximum total lifetime would be exceeded: %v > %v", totalLifetime, em.policy.MaxTotalLifetime),
		}
	}

	// Check backoff timing
	if state.backoffDuration > 0 {
		nextAllowedAt := state.lastExtension.Add(state.backoffDuration)
		if now.Before(nextAllowedAt) {
			return &ExtensionResult{
				Success:       false,
				NextAllowedAt: &nextAllowedAt,
				Error:         fmt.Errorf("extension request too soon, backoff until %v", nextAllowedAt),
			}
		}
	}

	return &ExtensionResult{Success: true}
}

// calculateExtensionDuration computes the actual extension duration
func (em *ExtensionManager) calculateExtensionDuration(state *extensionState, requestedDuration time.Duration) time.Duration {
	// Use requested duration if specified, otherwise use policy default
	if requestedDuration == 0 {
		requestedDuration = em.policy.ExtensionDuration
	}

	// Apply backoff if this isn't the first extension
	if state.totalExtensions > 0 {
		backoffFactor := math.Pow(em.policy.BackoffMultiplier, float64(state.totalExtensions))
		adjustedDuration := time.Duration(float64(requestedDuration) * backoffFactor)

		// Cap at maximum backoff duration
		if adjustedDuration > em.policy.MaxBackoffDuration {
			adjustedDuration = em.policy.MaxBackoffDuration
		}

		return adjustedDuration
	}

	return requestedDuration
}

// updateExtensionState updates the tracking state after successful extension
func (em *ExtensionManager) updateExtensionState(state *extensionState, extensionDuration time.Duration) {
	em.mu.Lock()
	defer em.mu.Unlock()

	state.totalExtensions++
	state.lastExtension = time.Now()

	// Calculate backoff duration for next extension
	if state.totalExtensions > 1 {
		backoffFactor := math.Pow(em.policy.BackoffMultiplier, float64(state.totalExtensions-1))
		state.backoffDuration = time.Duration(float64(time.Second) * backoffFactor)

		if state.backoffDuration > em.policy.MaxBackoffDuration {
			state.backoffDuration = em.policy.MaxBackoffDuration
		}
	}
}

// GetExtensionState returns the current extension state for a lease
func (em *ExtensionManager) GetExtensionState(token models.LeaseToken) (*extensionState, bool) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	state, exists := em.extensions[token]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent race conditions
	stateCopy := *state
	return &stateCopy, true
}

// CleanupExtensionState removes tracking state for revoked/expired leases
func (em *ExtensionManager) CleanupExtensionState(token models.LeaseToken) {
	em.mu.Lock()
	defer em.mu.Unlock()

	delete(em.extensions, token)

	em.logger.Debug("Cleaned up extension state",
		zap.String("token", string(token)))
}

// GetExtensionStats returns statistics about lease extensions
func (em *ExtensionManager) GetExtensionStats() ExtensionStats {
	em.mu.RLock()
	defer em.mu.RUnlock()

	stats := ExtensionStats{
		TotalTrackedLeases: uint64(len(em.extensions)),
	}

	now := time.Now()
	for _, state := range em.extensions {
		stats.TotalExtensions += uint64(state.totalExtensions)

		if state.totalExtensions >= em.policy.MaxExtensions {
			stats.MaxedOutLeases++
		}

		if state.backoffDuration > 0 && now.Before(state.lastExtension.Add(state.backoffDuration)) {
			stats.BackoffLeases++
		}

		leaseAge := now.Sub(state.lease.CreatedAt)
		if leaseAge > stats.MaxLeaseAge {
			stats.MaxLeaseAge = leaseAge
		}

		stats.AverageLeaseAge += leaseAge
	}

	if len(em.extensions) > 0 {
		stats.AverageLeaseAge /= time.Duration(len(em.extensions))
	}

	return stats
}

// ExtensionStats provides statistics about lease extensions
type ExtensionStats struct {
	TotalTrackedLeases uint64
	TotalExtensions    uint64
	MaxedOutLeases     uint64
	BackoffLeases      uint64
	MaxLeaseAge        time.Duration
	AverageLeaseAge    time.Duration
}

// Close releases all resources
func (em *ExtensionManager) Close() error {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.extensions = make(map[models.LeaseToken]*extensionState)
	return nil
}
