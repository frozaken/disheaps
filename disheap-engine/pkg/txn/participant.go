package txn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/partition"
	"go.uber.org/zap"
)

// ParticipantState represents the state of a participant in a transaction
type ParticipantState int

const (
	ParticipantIdle ParticipantState = iota
	ParticipantPreparing
	ParticipantPrepared
	ParticipantCommitting
	ParticipantCommitted
	ParticipantAborting
	ParticipantAborted
)

func (s ParticipantState) String() string {
	switch s {
	case ParticipantIdle:
		return "idle"
	case ParticipantPreparing:
		return "preparing"
	case ParticipantPrepared:
		return "prepared"
	case ParticipantCommitting:
		return "committing"
	case ParticipantCommitted:
		return "committed"
	case ParticipantAborting:
		return "aborting"
	case ParticipantAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// ParticipantTransaction represents a participant's view of a transaction
type ParticipantTransaction struct {
	ID          string
	Operations  []*models.BatchOperation
	State       ParticipantState
	PreparedAt  *time.Time
	CommittedAt *time.Time
	AbortedAt   *time.Time
	Error       error
	mu          sync.RWMutex
}

// PartitionParticipantConfig configures a partition participant
type PartitionParticipantConfig struct {
	// Maximum number of concurrent transactions
	MaxConcurrentTxns int
	// How long to keep prepared transactions before auto-abort
	PreparedTimeout time.Duration
	// Cleanup interval for completed transactions
	CleanupInterval time.Duration
	// How long to keep completed transactions
	RetentionTime time.Duration
}

// DefaultPartitionParticipantConfig returns a default configuration
func DefaultPartitionParticipantConfig() *PartitionParticipantConfig {
	return &PartitionParticipantConfig{
		MaxConcurrentTxns: 100,
		PreparedTimeout:   5 * time.Minute,
		CleanupInterval:   1 * time.Minute,
		RetentionTime:     10 * time.Minute,
	}
}

// PartitionParticipant implements the Participant interface for partition operations
type PartitionParticipant struct {
	config       *PartitionParticipantConfig
	partition    partition.Partition
	transactions map[string]*ParticipantTransaction
	timeouts     *TimeoutManager
	logger       *zap.Logger
	mu           sync.RWMutex
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewPartitionParticipant creates a new partition participant
func NewPartitionParticipant(
	config *PartitionParticipantConfig,
	partition partition.Partition,
	logger *zap.Logger,
) *PartitionParticipant {
	if config == nil {
		config = DefaultPartitionParticipantConfig()
	}

	return &PartitionParticipant{
		config:       config,
		partition:    partition,
		transactions: make(map[string]*ParticipantTransaction),
		timeouts:     NewTimeoutManager(logger.Named("timeout-manager")),
		logger:       logger.Named("partition-participant"),
		stopCh:       make(chan struct{}),
	}
}

// Start starts the participant
func (pp *PartitionParticipant) Start(ctx context.Context) error {
	pp.logger.Info("starting partition participant")

	// Start timeout manager
	if err := pp.timeouts.Start(ctx); err != nil {
		return fmt.Errorf("failed to start timeout manager: %w", err)
	}

	// Start cleanup worker
	pp.wg.Add(1)
	go pp.cleanupWorker()

	pp.logger.Info("partition participant started")
	return nil
}

// Stop stops the participant
func (pp *PartitionParticipant) Stop(ctx context.Context) error {
	pp.logger.Info("stopping partition participant")

	close(pp.stopCh)
	pp.wg.Wait()

	if err := pp.timeouts.Stop(ctx); err != nil {
		pp.logger.Warn("error stopping timeout manager", zap.Error(err))
	}

	pp.logger.Info("partition participant stopped")
	return nil
}

// Prepare implements the prepare phase of 2PC
func (pp *PartitionParticipant) Prepare(ctx context.Context, txnID string, ops []*models.BatchOperation) error {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	pp.logger.Debug("received prepare request",
		zap.String("txn_id", txnID),
		zap.Int("operations", len(ops)))

	// Check if we already have this transaction
	if txn, exists := pp.transactions[txnID]; exists {
		txn.mu.RLock()
		state := txn.State
		txn.mu.RUnlock()

		if state == ParticipantPrepared {
			pp.logger.Debug("transaction already prepared",
				zap.String("txn_id", txnID))
			return nil
		}
		return fmt.Errorf("transaction %s already exists in state %s", txnID, state.String())
	}

	// Check concurrent transaction limit
	if len(pp.transactions) >= pp.config.MaxConcurrentTxns {
		return fmt.Errorf("maximum concurrent transactions reached: %d", pp.config.MaxConcurrentTxns)
	}

	// Create participant transaction
	txn := &ParticipantTransaction{
		ID:         txnID,
		Operations: ops,
		State:      ParticipantPreparing,
	}

	pp.transactions[txnID] = txn

	// Validate operations can be performed
	if err := pp.validateOperations(ctx, ops); err != nil {
		delete(pp.transactions, txnID)
		return fmt.Errorf("operations validation failed: %w", err)
	}

	// Mark as prepared
	now := time.Now()
	txn.mu.Lock()
	txn.State = ParticipantPrepared
	txn.PreparedAt = &now
	txn.mu.Unlock()

	// Schedule timeout for prepared state
	timeoutAt := now.Add(pp.config.PreparedTimeout)
	pp.timeouts.ScheduleTimeout(txnID, timeoutAt, func() {
		pp.handlePreparedTimeout(txnID)
	})

	pp.logger.Info("transaction prepared successfully",
		zap.String("txn_id", txnID),
		zap.Int("operations", len(ops)))

	return nil
}

// Commit implements the commit phase of 2PC
func (pp *PartitionParticipant) Commit(ctx context.Context, txnID string) error {
	pp.mu.RLock()
	txn, exists := pp.transactions[txnID]
	pp.mu.RUnlock()

	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	txn.mu.Lock()
	if txn.State != ParticipantPrepared {
		state := txn.State
		txn.mu.Unlock()
		if state == ParticipantCommitted {
			return nil // Already committed
		}
		return fmt.Errorf("transaction %s not in prepared state: %s", txnID, state.String())
	}
	txn.State = ParticipantCommitting
	txn.mu.Unlock()

	pp.logger.Debug("committing transaction",
		zap.String("txn_id", txnID),
		zap.Int("operations", len(txn.Operations)))

	// Apply operations to partition
	if err := pp.applyOperations(ctx, txn.Operations); err != nil {
		// Mark as failed but don't change state back to prepared
		txn.mu.Lock()
		txn.Error = err
		txn.mu.Unlock()
		return fmt.Errorf("failed to apply operations: %w", err)
	}

	// Mark as committed
	now := time.Now()
	txn.mu.Lock()
	txn.State = ParticipantCommitted
	txn.CommittedAt = &now
	txn.mu.Unlock()

	// Cancel prepared timeout since transaction is complete
	pp.timeouts.CancelTimeout(txnID)

	pp.logger.Info("transaction committed successfully",
		zap.String("txn_id", txnID))

	return nil
}

// Abort implements the abort phase of 2PC
func (pp *PartitionParticipant) Abort(ctx context.Context, txnID string) error {
	pp.mu.RLock()
	txn, exists := pp.transactions[txnID]
	pp.mu.RUnlock()

	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	txn.mu.Lock()
	if txn.State == ParticipantAborted {
		txn.mu.Unlock()
		return nil // Already aborted
	}
	if txn.State == ParticipantCommitted {
		state := txn.State
		txn.mu.Unlock()
		return fmt.Errorf("cannot abort committed transaction %s (state: %s)", txnID, state.String())
	}
	txn.State = ParticipantAborting
	txn.mu.Unlock()

	pp.logger.Debug("aborting transaction",
		zap.String("txn_id", txnID))

	// No rollback needed since we haven't applied operations yet
	// (operations are only applied in commit phase)

	// Mark as aborted
	now := time.Now()
	txn.mu.Lock()
	txn.State = ParticipantAborted
	txn.AbortedAt = &now
	txn.mu.Unlock()

	// Cancel prepared timeout since transaction is complete
	pp.timeouts.CancelTimeout(txnID)

	pp.logger.Info("transaction aborted successfully",
		zap.String("txn_id", txnID))

	return nil
}

// GetTransaction returns information about a transaction
func (pp *PartitionParticipant) GetTransaction(txnID string) (*ParticipantTransaction, error) {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	txn, exists := pp.transactions[txnID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", txnID)
	}

	return txn, nil
}

// GetTransactions returns all transactions
func (pp *PartitionParticipant) GetTransactions() map[string]*ParticipantTransaction {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	result := make(map[string]*ParticipantTransaction)
	for id, txn := range pp.transactions {
		result[id] = txn
	}

	return result
}

func (pp *PartitionParticipant) validateOperations(ctx context.Context, ops []*models.BatchOperation) error {
	for i, op := range ops {
		switch op.Type {
		case models.BatchOperationEnqueue:
			// Validate that we can enqueue the message
			if op.Message == nil {
				return fmt.Errorf("operation %d: message is required for enqueue", i)
			}

			// Additional validation could include:
			// - Check if topic/partition matches this participant
			// - Validate message size limits
			// - Check priority bounds
			// - Validate payload format

		case models.BatchOperationAck, models.BatchOperationNack:
			// Validate that message ID is provided
			if op.MessageID.IsZero() {
				return fmt.Errorf("operation %d: message ID is required for %s", i, op.Type.String())
			}

			// Additional validation could include:
			// - Check if message exists
			// - Validate lease token
			// - Check message is in leased state

		default:
			return fmt.Errorf("operation %d: unsupported operation type: %s", i, op.Type.String())
		}
	}

	return nil
}

func (pp *PartitionParticipant) applyOperations(ctx context.Context, ops []*models.BatchOperation) error {
	// Apply operations to the partition in order
	// This is where the actual state changes happen

	for i, op := range ops {
		switch op.Type {
		case models.BatchOperationEnqueue:
			if err := pp.partition.Enqueue(ctx, op.Message); err != nil {
				return fmt.Errorf("operation %d (enqueue): %w", i, err)
			}

		case models.BatchOperationAck:
			if err := pp.partition.Ack(ctx, op.MessageID, op.LeaseToken); err != nil {
				return fmt.Errorf("operation %d (ack): %w", i, err)
			}

		case models.BatchOperationNack:
			if err := pp.partition.Nack(ctx, op.MessageID, op.LeaseToken, nil); err != nil {
				return fmt.Errorf("operation %d (nack): %w", i, err)
			}

		default:
			return fmt.Errorf("operation %d: unsupported operation type: %s", i, op.Type.String())
		}
	}

	return nil
}

func (pp *PartitionParticipant) handlePreparedTimeout(txnID string) {
	pp.logger.Warn("prepared transaction timed out, auto-aborting",
		zap.String("txn_id", txnID))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pp.Abort(ctx, txnID); err != nil {
		pp.logger.Error("failed to abort timed out prepared transaction",
			zap.String("txn_id", txnID),
			zap.Error(err))
	}
}

func (pp *PartitionParticipant) cleanupWorker() {
	defer pp.wg.Done()

	ticker := time.NewTicker(pp.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pp.cleanup()
		case <-pp.stopCh:
			return
		}
	}
}

func (pp *PartitionParticipant) cleanup() {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	now := time.Now()
	var toDelete []string

	for txnID, txn := range pp.transactions {
		txn.mu.RLock()
		shouldDelete := false

		// Delete transactions that have been complete for longer than retention time
		if txn.State == ParticipantCommitted && txn.CommittedAt != nil {
			if now.Sub(*txn.CommittedAt) > pp.config.RetentionTime {
				shouldDelete = true
			}
		} else if txn.State == ParticipantAborted && txn.AbortedAt != nil {
			if now.Sub(*txn.AbortedAt) > pp.config.RetentionTime {
				shouldDelete = true
			}
		}

		txn.mu.RUnlock()

		if shouldDelete {
			toDelete = append(toDelete, txnID)
		}
	}

	for _, txnID := range toDelete {
		delete(pp.transactions, txnID)
		pp.logger.Debug("cleaned up completed transaction", zap.String("txn_id", txnID))
	}

	if len(toDelete) > 0 {
		pp.logger.Info("cleaned up completed transactions", zap.Int("count", len(toDelete)))
	}
}
