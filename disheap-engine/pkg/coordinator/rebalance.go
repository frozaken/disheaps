package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// RebalanceStatus represents the current status of a rebalancing operation
type RebalanceStatus struct {
	Topic        string         `json:"topic"`
	State        RebalanceState `json:"state"`
	StartedAt    time.Time      `json:"started_at"`
	CompletedAt  *time.Time     `json:"completed_at,omitempty"`
	Progress     float64        `json:"progress"` // 0.0 to 1.0
	EstimatedETA *time.Duration `json:"estimated_eta,omitempty"`
	CurrentPhase string         `json:"current_phase"`
	Error        string         `json:"error,omitempty"`

	// Detailed progress
	PartitionsToMove    []models.PartitionID `json:"partitions_to_move"`
	PartitionsMoved     []models.PartitionID `json:"partitions_moved"`
	MessagesTransferred uint64               `json:"messages_transferred"`
	BytesTransferred    uint64               `json:"bytes_transferred"`
}

// RebalanceState represents the state of a rebalancing operation
type RebalanceState int

const (
	RebalanceStateIdle RebalanceState = iota
	RebalanceStatePlanning
	RebalanceStatePreparing
	RebalanceStateTransferring
	RebalanceStateFinalizing
	RebalanceStateCompleted
	RebalanceStateFailed
	RebalanceStateCancelled
)

func (rs RebalanceState) String() string {
	switch rs {
	case RebalanceStateIdle:
		return "idle"
	case RebalanceStatePlanning:
		return "planning"
	case RebalanceStatePreparing:
		return "preparing"
	case RebalanceStateTransferring:
		return "transferring"
	case RebalanceStateFinalizing:
		return "finalizing"
	case RebalanceStateCompleted:
		return "completed"
	case RebalanceStateFailed:
		return "failed"
	case RebalanceStateCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// RebalanceConfig holds configuration for rebalancing operations
type RebalanceConfig struct {
	// Timing
	ThrottleInterval    time.Duration `json:"throttle_interval"`     // Delay between operations
	HealthCheckInterval time.Duration `json:"health_check_interval"` // Health check frequency
	TimeoutPerPartition time.Duration `json:"timeout_per_partition"` // Max time per partition
	OverallTimeout      time.Duration `json:"overall_timeout"`       // Max total time

	// Batching
	TransferBatchSize      int `json:"transfer_batch_size"`      // Messages per batch
	MaxConcurrentTransfers int `json:"max_concurrent_transfers"` // Parallel transfers

	// Safety
	RequireHealthyCluster  bool `json:"require_healthy_cluster"`  // Only rebalance if cluster healthy
	MaxUnhealthyPartitions int  `json:"max_unhealthy_partitions"` // Max unhealthy partitions to allow
	EnableRollback         bool `json:"enable_rollback"`          // Enable rollback on failure
}

// DefaultRebalanceConfig returns a default rebalance configuration
func DefaultRebalanceConfig() RebalanceConfig {
	return RebalanceConfig{
		ThrottleInterval:    100 * time.Millisecond,
		HealthCheckInterval: 5 * time.Second,
		TimeoutPerPartition: 30 * time.Minute,
		OverallTimeout:      4 * time.Hour,

		TransferBatchSize:      1000,
		MaxConcurrentTransfers: 4,

		RequireHealthyCluster:  true,
		MaxUnhealthyPartitions: 1,
		EnableRollback:         true,
	}
}

// Rebalancer manages partition rebalancing operations
type Rebalancer struct {
	mu     sync.RWMutex
	config RebalanceConfig
	logger *zap.Logger

	// Active rebalance operations
	operations map[string]*RebalanceStatus // topic -> status

	// Statistics
	completedRebalances uint64
	failedRebalances    uint64
}

// NewRebalancer creates a new rebalancer
func NewRebalancer(config RebalanceConfig, logger *zap.Logger) *Rebalancer {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &Rebalancer{
		config:     config,
		logger:     logger,
		operations: make(map[string]*RebalanceStatus),
	}
}

// TriggerRebalance starts a rebalancing operation for a topic
func (r *Rebalancer) TriggerRebalance(
	ctx context.Context,
	topic string,
	currentPartitions []models.PartitionID,
	targetPartitions []models.PartitionID,
) error {

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if rebalance is already in progress
	if status, exists := r.operations[topic]; exists {
		if status.State != RebalanceStateCompleted &&
			status.State != RebalanceStateFailed &&
			status.State != RebalanceStateCancelled {
			return fmt.Errorf("rebalance already in progress for topic %s (state: %s)", topic, status.State)
		}
	}

	// Create new rebalance status
	status := &RebalanceStatus{
		Topic:            topic,
		State:            RebalanceStatePlanning,
		StartedAt:        time.Now(),
		Progress:         0.0,
		CurrentPhase:     "planning",
		PartitionsToMove: r.calculatePartitionsToMove(currentPartitions, targetPartitions),
	}

	r.operations[topic] = status

	r.logger.Info("Starting rebalance operation",
		zap.String("component", "rebalancer"),
		zap.String("topic", topic),
		zap.Any("current_partitions", currentPartitions),
		zap.Any("target_partitions", targetPartitions),
		zap.Int("partitions_to_move", len(status.PartitionsToMove)))

	// Start rebalance in background
	go r.executeRebalance(ctx, topic, currentPartitions, targetPartitions)

	return nil
}

// GetRebalanceStatus returns the current status of a rebalancing operation
func (r *Rebalancer) GetRebalanceStatus(topic string) (*RebalanceStatus, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	status, exists := r.operations[topic]
	if !exists {
		return nil, fmt.Errorf("no rebalance operation found for topic %s", topic)
	}

	// Return a copy to prevent external modification
	statusCopy := *status
	return &statusCopy, nil
}

// CancelRebalance attempts to cancel an ongoing rebalancing operation
func (r *Rebalancer) CancelRebalance(topic string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	status, exists := r.operations[topic]
	if !exists {
		return fmt.Errorf("no rebalance operation found for topic %s", topic)
	}

	if status.State == RebalanceStateCompleted || status.State == RebalanceStateFailed {
		return fmt.Errorf("rebalance for topic %s already completed (state: %s)", topic, status.State)
	}

	status.State = RebalanceStateCancelled
	status.CurrentPhase = "cancelled"
	now := time.Now()
	status.CompletedAt = &now

	r.logger.Info("Cancelled rebalance operation",
		zap.String("component", "rebalancer"),
		zap.String("topic", topic))

	return nil
}

// executeRebalance performs the actual rebalancing operation
func (r *Rebalancer) executeRebalance(
	ctx context.Context,
	topic string,
	currentPartitions []models.PartitionID,
	targetPartitions []models.PartitionID,
) {

	// Update status function
	updateStatus := func(state RebalanceState, phase string, progress float64, err error) {
		r.mu.Lock()
		defer r.mu.Unlock()

		if status, exists := r.operations[topic]; exists {
			status.State = state
			status.CurrentPhase = phase
			status.Progress = progress
			if err != nil {
				status.Error = err.Error()
			}
			if state == RebalanceStateCompleted || state == RebalanceStateFailed || state == RebalanceStateCancelled {
				now := time.Now()
				status.CompletedAt = &now
			}
		}
	}

	// Create context with timeout
	rebalanceCtx, cancel := context.WithTimeout(ctx, r.config.OverallTimeout)
	defer cancel()

	// Phase 1: Planning
	r.logger.Info("Starting rebalance planning phase",
		zap.String("component", "rebalancer"), zap.String("topic", topic))

	updateStatus(RebalanceStatePlanning, "analyzing partition distribution", 0.1, nil)

	plan, err := r.createRebalancePlan(currentPartitions, targetPartitions)
	if err != nil {
		r.logger.Error("Failed to create rebalance plan",
			zap.Error(err), zap.String("topic", topic))
		updateStatus(RebalanceStateFailed, "planning failed", 0.1, err)
		r.mu.Lock()
		r.failedRebalances++
		r.mu.Unlock()
		return
	}

	// Phase 2: Preparing
	updateStatus(RebalanceStatePreparing, "preparing partitions for transfer", 0.2, nil)

	if err := r.preparePartitions(rebalanceCtx, plan); err != nil {
		r.logger.Error("Failed to prepare partitions",
			zap.Error(err), zap.String("topic", topic))
		updateStatus(RebalanceStateFailed, "preparation failed", 0.2, err)
		r.mu.Lock()
		r.failedRebalances++
		r.mu.Unlock()
		return
	}

	// Phase 3: Transferring
	updateStatus(RebalanceStateTransferring, "transferring partition data", 0.3, nil)

	if err := r.transferPartitions(rebalanceCtx, topic, plan, updateStatus); err != nil {
		r.logger.Error("Failed to transfer partitions",
			zap.Error(err), zap.String("topic", topic))
		updateStatus(RebalanceStateFailed, "transfer failed", 0.7, err)
		r.mu.Lock()
		r.failedRebalances++
		r.mu.Unlock()
		return
	}

	// Phase 4: Finalizing
	updateStatus(RebalanceStateFinalizing, "finalizing partition assignments", 0.9, nil)

	if err := r.finalizeRebalance(rebalanceCtx, plan); err != nil {
		r.logger.Error("Failed to finalize rebalance",
			zap.Error(err), zap.String("topic", topic))
		updateStatus(RebalanceStateFailed, "finalization failed", 0.9, err)
		r.mu.Lock()
		r.failedRebalances++
		r.mu.Unlock()
		return
	}

	// Completed successfully
	updateStatus(RebalanceStateCompleted, "completed successfully", 1.0, nil)
	r.mu.Lock()
	r.completedRebalances++
	r.mu.Unlock()

	r.logger.Info("Rebalance operation completed successfully",
		zap.String("component", "rebalancer"), zap.String("topic", topic))
}

// RebalancePlan represents a plan for rebalancing partitions
type RebalancePlan struct {
	Topic              string                   `json:"topic"`
	PartitionsToAdd    []models.PartitionID     `json:"partitions_to_add"`
	PartitionsToRemove []models.PartitionID     `json:"partitions_to_remove"`
	PartitionsToMove   []PartitionMoveOperation `json:"partitions_to_move"`
	EstimatedDuration  time.Duration            `json:"estimated_duration"`
}

// PartitionMoveOperation represents a single partition move
type PartitionMoveOperation struct {
	PartitionID       models.PartitionID `json:"partition_id"`
	SourceNode        string             `json:"source_node"`
	TargetNode        string             `json:"target_node"`
	EstimatedMessages uint64             `json:"estimated_messages"`
	EstimatedBytes    uint64             `json:"estimated_bytes"`
}

// calculatePartitionsToMove determines which partitions need to be moved
func (r *Rebalancer) calculatePartitionsToMove(
	current []models.PartitionID,
	target []models.PartitionID,
) []models.PartitionID {

	currentSet := make(map[models.PartitionID]bool)
	for _, p := range current {
		currentSet[p] = true
	}

	targetSet := make(map[models.PartitionID]bool)
	for _, p := range target {
		targetSet[p] = true
	}

	var toMove []models.PartitionID

	// Partitions that exist in current but not in target need to be removed
	for p := range currentSet {
		if !targetSet[p] {
			toMove = append(toMove, p)
		}
	}

	// Partitions that exist in target but not in current need to be added
	for p := range targetSet {
		if !currentSet[p] {
			toMove = append(toMove, p)
		}
	}

	return toMove
}

// createRebalancePlan creates a detailed plan for the rebalancing operation
func (r *Rebalancer) createRebalancePlan(
	current []models.PartitionID,
	target []models.PartitionID,
) (*RebalancePlan, error) {

	plan := &RebalancePlan{
		PartitionsToAdd:    []models.PartitionID{},
		PartitionsToRemove: []models.PartitionID{},
		PartitionsToMove:   []PartitionMoveOperation{},
	}

	currentSet := make(map[models.PartitionID]bool)
	for _, p := range current {
		currentSet[p] = true
	}

	// Determine partitions to add and remove
	for _, p := range target {
		if !currentSet[p] {
			plan.PartitionsToAdd = append(plan.PartitionsToAdd, p)
		}
	}

	targetSet := make(map[models.PartitionID]bool)
	for _, p := range target {
		targetSet[p] = true
	}

	for _, p := range current {
		if !targetSet[p] {
			plan.PartitionsToRemove = append(plan.PartitionsToRemove, p)
		}
	}

	// Estimate duration
	totalOperations := len(plan.PartitionsToAdd) + len(plan.PartitionsToRemove) + len(plan.PartitionsToMove)
	plan.EstimatedDuration = time.Duration(totalOperations) * r.config.TimeoutPerPartition / 4

	return plan, nil
}

// preparePartitions prepares partitions for rebalancing
func (r *Rebalancer) preparePartitions(ctx context.Context, plan *RebalancePlan) error {
	// Mark partitions as throttled during rebalancing
	// This prevents new operations from interfering with the rebalance

	r.logger.Debug("Preparing partitions for rebalancing",
		zap.String("component", "rebalancer"),
		zap.Int("partitions_to_add", len(plan.PartitionsToAdd)),
		zap.Int("partitions_to_remove", len(plan.PartitionsToRemove)),
		zap.Int("partitions_to_move", len(plan.PartitionsToMove)))

	// Implementation would throttle partitions here
	// For now, we simulate preparation time
	time.Sleep(r.config.ThrottleInterval)

	return nil
}

// transferPartitions performs the actual data transfer
func (r *Rebalancer) transferPartitions(
	ctx context.Context,
	topic string,
	plan *RebalancePlan,
	updateStatus func(RebalanceState, string, float64, error),
) error {

	totalOperations := len(plan.PartitionsToAdd) + len(plan.PartitionsToRemove) + len(plan.PartitionsToMove)
	if totalOperations == 0 {
		return nil // Nothing to transfer
	}

	completed := 0

	// Process partition additions
	for _, partitionID := range plan.PartitionsToAdd {
		if err := r.addPartition(ctx, topic, partitionID); err != nil {
			return fmt.Errorf("failed to add partition %d: %w", partitionID, err)
		}

		completed++
		progress := 0.3 + (0.6 * float64(completed) / float64(totalOperations))
		updateStatus(RebalanceStateTransferring, fmt.Sprintf("added partition %d", partitionID), progress, nil)

		// Throttle to avoid overwhelming the system
		time.Sleep(r.config.ThrottleInterval)
	}

	// Process partition removals
	for _, partitionID := range plan.PartitionsToRemove {
		if err := r.removePartition(ctx, topic, partitionID); err != nil {
			return fmt.Errorf("failed to remove partition %d: %w", partitionID, err)
		}

		completed++
		progress := 0.3 + (0.6 * float64(completed) / float64(totalOperations))
		updateStatus(RebalanceStateTransferring, fmt.Sprintf("removed partition %d", partitionID), progress, nil)

		time.Sleep(r.config.ThrottleInterval)
	}

	// Process partition moves
	for _, moveOp := range plan.PartitionsToMove {
		if err := r.movePartition(ctx, topic, moveOp); err != nil {
			return fmt.Errorf("failed to move partition %d: %w", moveOp.PartitionID, err)
		}

		completed++
		progress := 0.3 + (0.6 * float64(completed) / float64(totalOperations))
		updateStatus(RebalanceStateTransferring, fmt.Sprintf("moved partition %d", moveOp.PartitionID), progress, nil)

		time.Sleep(r.config.ThrottleInterval)
	}

	return nil
}

// finalizeRebalance completes the rebalancing operation
func (r *Rebalancer) finalizeRebalance(ctx context.Context, plan *RebalancePlan) error {
	// Unthrottle partitions and update routing tables

	r.logger.Debug("Finalizing rebalance operation",
		zap.String("component", "rebalancer"))

	// Simulate finalization time
	time.Sleep(r.config.ThrottleInterval)

	return nil
}

// Placeholder implementations for partition operations
func (r *Rebalancer) addPartition(ctx context.Context, topic string, partitionID models.PartitionID) error {
	r.logger.Debug("Adding partition",
		zap.String("topic", topic), zap.Uint32("partition_id", uint32(partitionID)))
	return nil
}

func (r *Rebalancer) removePartition(ctx context.Context, topic string, partitionID models.PartitionID) error {
	r.logger.Debug("Removing partition",
		zap.String("topic", topic), zap.Uint32("partition_id", uint32(partitionID)))
	return nil
}

func (r *Rebalancer) movePartition(ctx context.Context, topic string, moveOp PartitionMoveOperation) error {
	r.logger.Debug("Moving partition",
		zap.String("topic", topic),
		zap.Uint32("partition_id", uint32(moveOp.PartitionID)),
		zap.String("source", moveOp.SourceNode),
		zap.String("target", moveOp.TargetNode))
	return nil
}

// GetStats returns rebalancer statistics
func (r *Rebalancer) GetStats() RebalancerStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	activeOperations := 0
	for _, status := range r.operations {
		if status.State != RebalanceStateCompleted &&
			status.State != RebalanceStateFailed &&
			status.State != RebalanceStateCancelled {
			activeOperations++
		}
	}

	return RebalancerStats{
		ActiveOperations:    activeOperations,
		CompletedRebalances: r.completedRebalances,
		FailedRebalances:    r.failedRebalances,
		TotalOperations:     len(r.operations),
	}
}

// RebalancerStats holds statistics about the rebalancer
type RebalancerStats struct {
	ActiveOperations    int    `json:"active_operations"`
	CompletedRebalances uint64 `json:"completed_rebalances"`
	FailedRebalances    uint64 `json:"failed_rebalances"`
	TotalOperations     int    `json:"total_operations"`
}
