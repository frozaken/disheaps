// Package txn implements Two-Phase Commit (2PC) protocol for atomic batch operations
package txn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"go.uber.org/zap"
)

// TransactionState represents the state of a distributed transaction
type TransactionState int

const (
	StateInit TransactionState = iota
	StatePreparing
	StatePrepared
	StateCommitting
	StateCommitted
	StateAborting
	StateAborted
)

func (s TransactionState) String() string {
	switch s {
	case StateInit:
		return "init"
	case StatePreparing:
		return "preparing"
	case StatePrepared:
		return "prepared"
	case StateCommitting:
		return "committing"
	case StateCommitted:
		return "committed"
	case StateAborting:
		return "aborting"
	case StateAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// Transaction represents a distributed transaction
type Transaction struct {
	ID           string
	Operations   []*models.BatchOperation
	Participants []ParticipantID
	State        TransactionState
	CreatedAt    time.Time
	PreparedAt   *time.Time
	CommittedAt  *time.Time
	AbortedAt    *time.Time
	TimeoutAt    time.Time
	Error        error
	mu           sync.RWMutex
}

// ParticipantID identifies a participant in the transaction
type ParticipantID struct {
	Topic     string
	Partition int
}

func (p ParticipantID) String() string {
	return fmt.Sprintf("%s:%d", p.Topic, p.Partition)
}

// CoordinatorConfig configures the transaction coordinator
type CoordinatorConfig struct {
	// Default timeout for transactions
	DefaultTimeout time.Duration
	// Maximum number of concurrent transactions
	MaxConcurrentTxns int
	// Participant operation timeout
	ParticipantTimeout time.Duration
	// Retry configuration
	MaxRetries   int
	RetryDelay   time.Duration
	RetryBackoff float64
	// Cleanup interval for completed transactions
	CleanupInterval time.Duration
	// How long to keep completed transactions for recovery
	RetentionTime time.Duration
}

// DefaultCoordinatorConfig returns a default configuration
func DefaultCoordinatorConfig() *CoordinatorConfig {
	return &CoordinatorConfig{
		DefaultTimeout:     30 * time.Second,
		MaxConcurrentTxns:  1000,
		ParticipantTimeout: 10 * time.Second,
		MaxRetries:         3,
		RetryDelay:         100 * time.Millisecond,
		RetryBackoff:       1.5,
		CleanupInterval:    5 * time.Minute,
		RetentionTime:      1 * time.Hour,
	}
}

// Participant interface that must be implemented by transaction participants
type Participant interface {
	// Prepare phase - check if operation can be committed
	Prepare(ctx context.Context, txnID string, ops []*models.BatchOperation) error
	// Commit phase - actually apply the operations
	Commit(ctx context.Context, txnID string) error
	// Abort phase - rollback any prepared state
	Abort(ctx context.Context, txnID string) error
}

// Coordinator manages distributed transactions using 2PC protocol
type Coordinator struct {
	config       *CoordinatorConfig
	participants map[ParticipantID]Participant
	transactions map[string]*Transaction
	timeouts     *TimeoutManager
	logger       *zap.Logger
	mu           sync.RWMutex
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewCoordinator creates a new transaction coordinator
func NewCoordinator(config *CoordinatorConfig, logger *zap.Logger) *Coordinator {
	if config == nil {
		config = DefaultCoordinatorConfig()
	}

	return &Coordinator{
		config:       config,
		participants: make(map[ParticipantID]Participant),
		transactions: make(map[string]*Transaction),
		timeouts:     NewTimeoutManager(logger.Named("timeout-manager")),
		logger:       logger.Named("txn-coordinator"),
		stopCh:       make(chan struct{}),
	}
}

// RegisterParticipant registers a participant for transactions
func (c *Coordinator) RegisterParticipant(id ParticipantID, participant Participant) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.participants[id] = participant
	c.logger.Info("registered transaction participant",
		zap.String("participant_id", id.String()))
}

// UnregisterParticipant removes a participant
func (c *Coordinator) UnregisterParticipant(id ParticipantID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.participants, id)
	c.logger.Info("unregistered transaction participant",
		zap.String("participant_id", id.String()))
}

// Start starts the coordinator
func (c *Coordinator) Start(ctx context.Context) error {
	c.logger.Info("starting transaction coordinator")

	// Start timeout manager
	if err := c.timeouts.Start(ctx); err != nil {
		return fmt.Errorf("failed to start timeout manager: %w", err)
	}

	// Start cleanup worker
	c.wg.Add(1)
	go c.cleanupWorker()

	c.logger.Info("transaction coordinator started")
	return nil
}

// Stop stops the coordinator
func (c *Coordinator) Stop(ctx context.Context) error {
	c.logger.Info("stopping transaction coordinator")

	close(c.stopCh)
	c.wg.Wait()

	if err := c.timeouts.Stop(ctx); err != nil {
		c.logger.Warn("error stopping timeout manager", zap.Error(err))
	}

	c.logger.Info("transaction coordinator stopped")
	return nil
}

// BeginTransaction starts a new distributed transaction
func (c *Coordinator) BeginTransaction(ctx context.Context, operations []*models.BatchOperation) (*Transaction, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check concurrent transaction limit
	if len(c.transactions) >= c.config.MaxConcurrentTxns {
		return nil, fmt.Errorf("maximum concurrent transactions reached: %d", c.config.MaxConcurrentTxns)
	}

	// Generate transaction ID
	txnID := models.NewMessageID().String()

	// Determine participants based on operations
	participantSet := make(map[ParticipantID]struct{})
	for _, op := range operations {
		pid := ParticipantID{
			Topic:     op.Topic,
			Partition: op.Partition,
		}
		participantSet[pid] = struct{}{}
	}

	participants := make([]ParticipantID, 0, len(participantSet))
	for pid := range participantSet {
		participants = append(participants, pid)
	}

	// Verify all participants are registered
	for _, pid := range participants {
		if _, exists := c.participants[pid]; !exists {
			return nil, fmt.Errorf("participant not registered: %s", pid.String())
		}
	}

	txn := &Transaction{
		ID:           txnID,
		Operations:   operations,
		Participants: participants,
		State:        StateInit,
		CreatedAt:    time.Now(),
		TimeoutAt:    time.Now().Add(c.config.DefaultTimeout),
	}

	c.transactions[txnID] = txn

	// Schedule timeout
	c.timeouts.ScheduleTimeout(txnID, txn.TimeoutAt, func() {
		c.handleTimeout(txnID)
	})

	c.logger.Info("began transaction",
		zap.String("txn_id", txnID),
		zap.Int("operations", len(operations)),
		zap.Int("participants", len(participants)))

	return txn, nil
}

// CommitTransaction executes the two-phase commit protocol
func (c *Coordinator) CommitTransaction(ctx context.Context, txnID string) error {
	txn, err := c.getTransaction(txnID)
	if err != nil {
		return err
	}

	c.logger.Info("starting 2PC for transaction",
		zap.String("txn_id", txnID),
		zap.Int("participants", len(txn.Participants)))

	// Phase 1: Prepare
	if err := c.preparePhase(ctx, txn); err != nil {
		// If prepare fails, abort the transaction
		abortErr := c.abortTransaction(ctx, txn)
		if abortErr != nil {
			c.logger.Error("failed to abort transaction after prepare failure",
				zap.String("txn_id", txnID),
				zap.Error(abortErr))
		}
		return fmt.Errorf("prepare phase failed: %w", err)
	}

	// Phase 2: Commit
	if err := c.commitPhase(ctx, txn); err != nil {
		return fmt.Errorf("commit phase failed: %w", err)
	}

	return nil
}

// AbortTransaction aborts the transaction
func (c *Coordinator) AbortTransaction(ctx context.Context, txnID string) error {
	txn, err := c.getTransaction(txnID)
	if err != nil {
		return err
	}

	return c.abortTransaction(ctx, txn)
}

// GetTransaction returns transaction information
func (c *Coordinator) GetTransaction(txnID string) (*Transaction, error) {
	return c.getTransaction(txnID)
}

func (c *Coordinator) getTransaction(txnID string) (*Transaction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	txn, exists := c.transactions[txnID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", txnID)
	}

	return txn, nil
}

func (c *Coordinator) preparePhase(ctx context.Context, txn *Transaction) error {
	txn.mu.Lock()
	txn.State = StatePreparing
	txn.mu.Unlock()

	c.logger.Debug("starting prepare phase",
		zap.String("txn_id", txn.ID),
		zap.Int("participants", len(txn.Participants)))

	// Group operations by participant
	participantOps := make(map[ParticipantID][]*models.BatchOperation)
	for _, op := range txn.Operations {
		pid := ParticipantID{Topic: op.Topic, Partition: op.Partition}
		participantOps[pid] = append(participantOps[pid], op)
	}

	// Send prepare to all participants
	var wg sync.WaitGroup
	errCh := make(chan error, len(txn.Participants))

	for _, pid := range txn.Participants {
		wg.Add(1)
		go func(pid ParticipantID) {
			defer wg.Done()

			participant := c.participants[pid]
			ops := participantOps[pid]

			prepareCtx, cancel := context.WithTimeout(ctx, c.config.ParticipantTimeout)
			defer cancel()

			c.logger.Debug("sending prepare to participant",
				zap.String("txn_id", txn.ID),
				zap.String("participant", pid.String()),
				zap.Int("operations", len(ops)))

			if err := participant.Prepare(prepareCtx, txn.ID, ops); err != nil {
				c.logger.Error("participant prepare failed",
					zap.String("txn_id", txn.ID),
					zap.String("participant", pid.String()),
					zap.Error(err))
				errCh <- fmt.Errorf("prepare failed for %s: %w", pid.String(), err)
				return
			}

			c.logger.Debug("participant prepared successfully",
				zap.String("txn_id", txn.ID),
				zap.String("participant", pid.String()))
		}(pid)
	}

	// Wait for all prepare responses
	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		return err
	}

	// All participants prepared successfully
	now := time.Now()
	txn.mu.Lock()
	txn.State = StatePrepared
	txn.PreparedAt = &now
	txn.mu.Unlock()

	c.logger.Info("all participants prepared successfully",
		zap.String("txn_id", txn.ID))

	return nil
}

func (c *Coordinator) commitPhase(ctx context.Context, txn *Transaction) error {
	txn.mu.Lock()
	txn.State = StateCommitting
	txn.mu.Unlock()

	c.logger.Debug("starting commit phase",
		zap.String("txn_id", txn.ID))

	// Send commit to all participants
	var wg sync.WaitGroup
	errCh := make(chan error, len(txn.Participants))

	for _, pid := range txn.Participants {
		wg.Add(1)
		go func(pid ParticipantID) {
			defer wg.Done()

			participant := c.participants[pid]

			commitCtx, cancel := context.WithTimeout(ctx, c.config.ParticipantTimeout)
			defer cancel()

			c.logger.Debug("sending commit to participant",
				zap.String("txn_id", txn.ID),
				zap.String("participant", pid.String()))

			if err := participant.Commit(commitCtx, txn.ID); err != nil {
				c.logger.Error("participant commit failed",
					zap.String("txn_id", txn.ID),
					zap.String("participant", pid.String()),
					zap.Error(err))
				errCh <- fmt.Errorf("commit failed for %s: %w", pid.String(), err)
				return
			}

			c.logger.Debug("participant committed successfully",
				zap.String("txn_id", txn.ID),
				zap.String("participant", pid.String()))
		}(pid)
	}

	// Wait for all commit responses
	wg.Wait()
	close(errCh)

	// Check for errors
	var commitErrors []error
	for err := range errCh {
		commitErrors = append(commitErrors, err)
	}

	// Even if some commits failed, the transaction is considered committed
	// because all participants prepared successfully
	now := time.Now()
	txn.mu.Lock()
	txn.State = StateCommitted
	txn.CommittedAt = &now
	if len(commitErrors) > 0 {
		txn.Error = fmt.Errorf("commit phase had %d errors: %v", len(commitErrors), commitErrors)
	}
	txn.mu.Unlock()

	// Cancel timeout since transaction is complete
	c.timeouts.CancelTimeout(txn.ID)

	if len(commitErrors) > 0 {
		c.logger.Warn("transaction committed with errors",
			zap.String("txn_id", txn.ID),
			zap.Int("errors", len(commitErrors)))
		return txn.Error
	}

	c.logger.Info("transaction committed successfully",
		zap.String("txn_id", txn.ID))

	return nil
}

func (c *Coordinator) abortTransaction(ctx context.Context, txn *Transaction) error {
	txn.mu.Lock()
	if txn.State == StateAborted {
		txn.mu.Unlock()
		return nil // Already aborted
	}
	txn.State = StateAborting
	txn.mu.Unlock()

	c.logger.Debug("aborting transaction",
		zap.String("txn_id", txn.ID))

	// Send abort to all participants
	var wg sync.WaitGroup
	errCh := make(chan error, len(txn.Participants))

	for _, pid := range txn.Participants {
		wg.Add(1)
		go func(pid ParticipantID) {
			defer wg.Done()

			participant := c.participants[pid]

			abortCtx, cancel := context.WithTimeout(ctx, c.config.ParticipantTimeout)
			defer cancel()

			c.logger.Debug("sending abort to participant",
				zap.String("txn_id", txn.ID),
				zap.String("participant", pid.String()))

			if err := participant.Abort(abortCtx, txn.ID); err != nil {
				c.logger.Error("participant abort failed",
					zap.String("txn_id", txn.ID),
					zap.String("participant", pid.String()),
					zap.Error(err))
				errCh <- fmt.Errorf("abort failed for %s: %w", pid.String(), err)
				return
			}

			c.logger.Debug("participant aborted successfully",
				zap.String("txn_id", txn.ID),
				zap.String("participant", pid.String()))
		}(pid)
	}

	// Wait for all abort responses
	wg.Wait()
	close(errCh)

	// Collect errors but don't fail the abort
	var abortErrors []error
	for err := range errCh {
		abortErrors = append(abortErrors, err)
	}

	now := time.Now()
	txn.mu.Lock()
	txn.State = StateAborted
	txn.AbortedAt = &now
	if len(abortErrors) > 0 {
		txn.Error = fmt.Errorf("abort phase had %d errors: %v", len(abortErrors), abortErrors)
	}
	txn.mu.Unlock()

	// Cancel timeout since transaction is complete
	c.timeouts.CancelTimeout(txn.ID)

	if len(abortErrors) > 0 {
		c.logger.Warn("transaction aborted with errors",
			zap.String("txn_id", txn.ID),
			zap.Int("errors", len(abortErrors)))
	} else {
		c.logger.Info("transaction aborted successfully",
			zap.String("txn_id", txn.ID))
	}

	return nil
}

func (c *Coordinator) handleTimeout(txnID string) {
	c.logger.Warn("transaction timed out", zap.String("txn_id", txnID))

	ctx, cancel := context.WithTimeout(context.Background(), c.config.ParticipantTimeout)
	defer cancel()

	if err := c.AbortTransaction(ctx, txnID); err != nil {
		c.logger.Error("failed to abort timed out transaction",
			zap.String("txn_id", txnID),
			zap.Error(err))
	}
}

func (c *Coordinator) cleanupWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Coordinator) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var toDelete []string

	for txnID, txn := range c.transactions {
		txn.mu.RLock()
		shouldDelete := false

		// Delete transactions that have been complete for longer than retention time
		if txn.State == StateCommitted && txn.CommittedAt != nil {
			if now.Sub(*txn.CommittedAt) > c.config.RetentionTime {
				shouldDelete = true
			}
		} else if txn.State == StateAborted && txn.AbortedAt != nil {
			if now.Sub(*txn.AbortedAt) > c.config.RetentionTime {
				shouldDelete = true
			}
		}

		txn.mu.RUnlock()

		if shouldDelete {
			toDelete = append(toDelete, txnID)
		}
	}

	for _, txnID := range toDelete {
		delete(c.transactions, txnID)
		c.logger.Debug("cleaned up completed transaction", zap.String("txn_id", txnID))
	}

	if len(toDelete) > 0 {
		c.logger.Info("cleaned up completed transactions", zap.Int("count", len(toDelete)))
	}
}
