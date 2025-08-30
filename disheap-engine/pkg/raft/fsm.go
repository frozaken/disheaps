package raft

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/partition"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
)

// HeapFSM implements the Raft finite state machine for heap operations
type HeapFSM struct {
	mu sync.RWMutex

	// Core components
	partitions map[string]map[uint32]partition.Partition // topic -> partition_id -> partition
	storage    storage.Storage
	logger     *zap.Logger

	// State tracking
	lastAppliedIndex uint64
	lastAppliedTerm  uint64
	appliedAt        time.Time

	// Transaction state for 2PC
	pendingTxns map[string]*BatchPrepareEntry // transaction_id -> prepare entry
	txnMu       sync.RWMutex

	// Metrics
	appliedEntries   uint64
	appliedBatches   uint64
	failedOperations uint64
	snapshotCount    uint64
	restoreCount     uint64
}

// NewHeapFSM creates a new heap finite state machine
func NewHeapFSM(storage storage.Storage, logger *zap.Logger) *HeapFSM {
	return &HeapFSM{
		partitions:  make(map[string]map[uint32]partition.Partition),
		storage:     storage,
		logger:      logger.Named("raft-fsm"),
		pendingTxns: make(map[string]*BatchPrepareEntry),
		appliedAt:   time.Now(),
	}
}

// Apply applies a Raft log entry to the state machine
func (fsm *HeapFSM) Apply(log *raft.Log) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// Track application progress
	fsm.lastAppliedIndex = log.Index
	fsm.lastAppliedTerm = log.Term
	fsm.appliedAt = time.Now()
	fsm.appliedEntries++

	// Deserialize the log entry
	entry, err := DeserializeLogEntry(log.Data)
	if err != nil {
		fsm.logger.Error("failed to deserialize log entry",
			zap.Uint64("index", log.Index),
			zap.Uint64("term", log.Term),
			zap.Error(err))
		fsm.failedOperations++
		return &FSMApplyResult{Success: false, Error: err}
	}

	// Validate log entry
	if err := ValidateLogEntry(entry); err != nil {
		fsm.logger.Error("invalid log entry",
			zap.Uint64("index", log.Index),
			zap.String("type", entry.Type.String()),
			zap.Error(err))
		fsm.failedOperations++
		return &FSMApplyResult{Success: false, Error: err}
	}

	// Apply the entry based on its type
	result := fsm.applyEntry(entry)
	result.Index = log.Index
	result.Term = log.Term
	result.AppliedAt = fsm.appliedAt

	if !result.Success {
		fsm.failedOperations++
	}

	fsm.logger.Debug("applied log entry",
		zap.Uint64("index", log.Index),
		zap.Uint64("term", log.Term),
		zap.String("type", entry.Type.String()),
		zap.String("topic", entry.Topic),
		zap.Uint32("partition", entry.Partition),
		zap.Bool("success", result.Success))

	return result
}

// applyEntry applies a specific log entry to the appropriate partition
func (fsm *HeapFSM) applyEntry(entry *LogEntry) *FSMApplyResult {
	ctx := context.Background()

	switch entry.Type {
	case EntryEnqueue:
		return fsm.applyEnqueue(ctx, entry)
	case EntryAck:
		return fsm.applyAck(ctx, entry)
	case EntryNack:
		return fsm.applyNack(ctx, entry)
	case EntryLeaseExtend:
		return fsm.applyLeaseExtend(ctx, entry)
	case EntryBatchPrepare:
		return fsm.applyBatchPrepare(ctx, entry)
	case EntryBatchCommit:
		return fsm.applyBatchCommit(ctx, entry)
	case EntryBatchAbort:
		return fsm.applyBatchAbort(ctx, entry)
	case EntryConfigUpdate:
		return fsm.applyConfigUpdate(ctx, entry)
	case EntrySnapshotMark:
		return fsm.applySnapshotMark(ctx, entry)
	default:
		return &FSMApplyResult{
			Success: false,
			Error:   fmt.Errorf("unknown entry type: %v", entry.Type),
		}
	}
}

// applyEnqueue applies an enqueue operation
func (fsm *HeapFSM) applyEnqueue(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var enqueueEntry EnqueueEntry
	if err := entry.DecodeData(&enqueueEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	partition := fsm.getPartition(entry.Topic, entry.Partition)
	if partition == nil {
		return &FSMApplyResult{
			Success: false,
			Error:   fmt.Errorf("partition %s/%d not found", entry.Topic, entry.Partition),
		}
	}

	// Apply enqueue to partition
	if err := partition.Enqueue(ctx, enqueueEntry.Message); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"message_id":   enqueueEntry.Message.ID,
			"producer_id":  enqueueEntry.ProducerID,
			"producer_seq": enqueueEntry.ProducerSeq,
		},
	}
}

// applyAck applies an acknowledgment operation
func (fsm *HeapFSM) applyAck(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var ackEntry AckEntry
	if err := entry.DecodeData(&ackEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	partition := fsm.getPartition(entry.Topic, entry.Partition)
	if partition == nil {
		return &FSMApplyResult{
			Success: false,
			Error:   fmt.Errorf("partition %s/%d not found", entry.Topic, entry.Partition),
		}
	}

	// Apply ack to partition
	msgID := models.MessageID(ackEntry.MessageID)
	leaseToken := models.LeaseToken(ackEntry.LeaseToken)
	if err := partition.Ack(ctx, msgID, leaseToken); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"message_id":  ackEntry.MessageID,
			"lease_token": ackEntry.LeaseToken,
			"consumer":    ackEntry.Consumer,
		},
	}
}

// applyNack applies a negative acknowledgment operation
func (fsm *HeapFSM) applyNack(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var nackEntry NackEntry
	if err := entry.DecodeData(&nackEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	partition := fsm.getPartition(entry.Topic, entry.Partition)
	if partition == nil {
		return &FSMApplyResult{
			Success: false,
			Error:   fmt.Errorf("partition %s/%d not found", entry.Topic, entry.Partition),
		}
	}

	// Apply nack to partition
	msgID := models.MessageID(nackEntry.MessageID)
	leaseToken := models.LeaseToken(nackEntry.LeaseToken)
	backoff := &nackEntry.RetryAfter
	if err := partition.Nack(ctx, msgID, leaseToken, backoff); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"message_id":    nackEntry.MessageID,
			"lease_token":   nackEntry.LeaseToken,
			"consumer":      nackEntry.Consumer,
			"reason":        nackEntry.Reason,
			"backoff_level": nackEntry.BackoffLevel,
		},
	}
}

// applyLeaseExtend applies a lease extension operation
func (fsm *HeapFSM) applyLeaseExtend(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var extendEntry LeaseExtendEntry
	if err := entry.DecodeData(&extendEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	partition := fsm.getPartition(entry.Topic, entry.Partition)
	if partition == nil {
		return &FSMApplyResult{
			Success: false,
			Error:   fmt.Errorf("partition %s/%d not found", entry.Topic, entry.Partition),
		}
	}

	// Apply lease extension to partition
	leaseToken := models.LeaseToken(extendEntry.LeaseToken)
	if err := partition.ExtendLease(ctx, leaseToken, extendEntry.Extension); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"message_id":  extendEntry.MessageID,
			"lease_token": extendEntry.LeaseToken,
			"consumer":    extendEntry.Consumer,
			"extension":   extendEntry.Extension.String(),
		},
	}
}

// applyBatchPrepare applies a batch prepare operation (2PC phase 1)
func (fsm *HeapFSM) applyBatchPrepare(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var prepareEntry BatchPrepareEntry
	if err := entry.DecodeData(&prepareEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	// Store the prepared transaction
	fsm.txnMu.Lock()
	fsm.pendingTxns[prepareEntry.TransactionID] = &prepareEntry
	fsm.txnMu.Unlock()

	fsm.logger.Debug("prepared batch transaction",
		zap.String("transaction_id", prepareEntry.TransactionID),
		zap.String("producer_id", prepareEntry.ProducerID),
		zap.Int("message_count", len(prepareEntry.Messages)))

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"transaction_id": prepareEntry.TransactionID,
			"producer_id":    prepareEntry.ProducerID,
			"message_count":  len(prepareEntry.Messages),
			"prepared_at":    prepareEntry.PreparedAt,
		},
	}
}

// applyBatchCommit applies a batch commit operation (2PC phase 2)
func (fsm *HeapFSM) applyBatchCommit(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var commitEntry BatchCommitEntry
	if err := entry.DecodeData(&commitEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	// Get the prepared transaction
	fsm.txnMu.Lock()
	prepareEntry, exists := fsm.pendingTxns[commitEntry.TransactionID]
	if exists {
		delete(fsm.pendingTxns, commitEntry.TransactionID)
	}
	fsm.txnMu.Unlock()

	if !exists {
		return &FSMApplyResult{
			Success: false,
			Error:   fmt.Errorf("prepared transaction %s not found", commitEntry.TransactionID),
		}
	}

	// Commit all messages in the batch
	partition := fsm.getPartition(entry.Topic, entry.Partition)
	if partition == nil {
		return &FSMApplyResult{
			Success: false,
			Error:   fmt.Errorf("partition %s/%d not found", entry.Topic, entry.Partition),
		}
	}

	for _, message := range prepareEntry.Messages {
		if err := partition.Enqueue(ctx, message); err != nil {
			fsm.logger.Error("failed to commit message in batch",
				zap.String("transaction_id", commitEntry.TransactionID),
				zap.String("message_id", message.ID.String()),
				zap.Error(err))
			return &FSMApplyResult{Success: false, Error: err}
		}
	}

	fsm.appliedBatches++

	fsm.logger.Debug("committed batch transaction",
		zap.String("transaction_id", commitEntry.TransactionID),
		zap.Int("message_count", len(prepareEntry.Messages)))

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"transaction_id": commitEntry.TransactionID,
			"message_count":  len(prepareEntry.Messages),
			"committed_at":   commitEntry.CommittedAt,
		},
	}
}

// applyBatchAbort applies a batch abort operation (2PC abort)
func (fsm *HeapFSM) applyBatchAbort(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var abortEntry BatchAbortEntry
	if err := entry.DecodeData(&abortEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	// Remove the prepared transaction
	fsm.txnMu.Lock()
	delete(fsm.pendingTxns, abortEntry.TransactionID)
	fsm.txnMu.Unlock()

	fsm.logger.Debug("aborted batch transaction",
		zap.String("transaction_id", abortEntry.TransactionID),
		zap.String("reason", abortEntry.Reason))

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"transaction_id": abortEntry.TransactionID,
			"reason":         abortEntry.Reason,
			"aborted_at":     abortEntry.AbortedAt,
		},
	}
}

// applyConfigUpdate applies a configuration update
func (fsm *HeapFSM) applyConfigUpdate(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var configEntry ConfigUpdateEntry
	if err := entry.DecodeData(&configEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	// Apply configuration update based on type
	switch configEntry.ConfigType {
	case "topic":
		// Update topic configuration
		if topicConfig, ok := configEntry.NewConfig.(*models.TopicConfig); ok {
			if err := fsm.storage.StoreTopicConfig(ctx, topicConfig); err != nil {
				return &FSMApplyResult{Success: false, Error: err}
			}
		}

	case "partition":
		// Partition configuration updates would be handled here
		// For now, just log the update
		fsm.logger.Info("partition config update applied",
			zap.String("config_id", configEntry.ConfigID))

	default:
		return &FSMApplyResult{
			Success: false,
			Error:   fmt.Errorf("unknown config type: %s", configEntry.ConfigType),
		}
	}

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"config_type": configEntry.ConfigType,
			"config_id":   configEntry.ConfigID,
		},
	}
}

// applySnapshotMark applies a snapshot marker entry
func (fsm *HeapFSM) applySnapshotMark(ctx context.Context, entry *LogEntry) *FSMApplyResult {
	var snapshotEntry SnapshotMarkEntry
	if err := entry.DecodeData(&snapshotEntry); err != nil {
		return &FSMApplyResult{Success: false, Error: err}
	}

	fsm.logger.Info("snapshot marker applied",
		zap.String("snapshot_id", snapshotEntry.SnapshotID),
		zap.Uint64("last_index", snapshotEntry.LastIndex),
		zap.Uint64("last_term", snapshotEntry.LastTerm))

	return &FSMApplyResult{
		Success: true,
		Data: map[string]interface{}{
			"snapshot_id": snapshotEntry.SnapshotID,
			"last_index":  snapshotEntry.LastIndex,
			"last_term":   snapshotEntry.LastTerm,
		},
	}
}

// getPartition retrieves a partition for the given topic and partition ID
func (fsm *HeapFSM) getPartition(topic string, partitionID uint32) partition.Partition {
	topicPartitions, exists := fsm.partitions[topic]
	if !exists {
		return nil
	}

	partition, exists := topicPartitions[partitionID]
	if !exists {
		return nil
	}

	return partition
}

// AddPartition adds a partition to the FSM
func (fsm *HeapFSM) AddPartition(topic string, partitionID uint32, part partition.Partition) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if _, exists := fsm.partitions[topic]; !exists {
		fsm.partitions[topic] = make(map[uint32]partition.Partition)
	}

	fsm.partitions[topic][partitionID] = part

	fsm.logger.Info("added partition to FSM",
		zap.String("topic", topic),
		zap.Uint32("partition", partitionID))
}

// RemovePartition removes a partition from the FSM
func (fsm *HeapFSM) RemovePartition(topic string, partitionID uint32) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if topicPartitions, exists := fsm.partitions[topic]; exists {
		delete(topicPartitions, partitionID)

		// Clean up empty topic
		if len(topicPartitions) == 0 {
			delete(fsm.partitions, topic)
		}
	}

	fsm.logger.Info("removed partition from FSM",
		zap.String("topic", topic),
		zap.Uint32("partition", partitionID))
}

// GetPartitionCount returns the total number of partitions across all topics
func (fsm *HeapFSM) GetPartitionCount() int {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	count := 0
	for _, partitions := range fsm.partitions {
		count += len(partitions)
	}
	return count
}

// Snapshot creates a snapshot of the current FSM state
func (fsm *HeapFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	fsm.snapshotCount++

	snapshot := &HeapSnapshot{
		LastIndex:      fsm.lastAppliedIndex,
		LastTerm:       fsm.lastAppliedTerm,
		AppliedAt:      fsm.appliedAt,
		PendingTxns:    make(map[string]*BatchPrepareEntry),
		AppliedEntries: fsm.appliedEntries,
		AppliedBatches: fsm.appliedBatches,
		FailedOps:      fsm.failedOperations,
		SnapshotCount:  fsm.snapshotCount,
		CreatedAt:      time.Now(),
		Storage:        fsm.storage,
		Logger:         fsm.logger,
	}

	// Copy pending transactions
	fsm.txnMu.RLock()
	for txnID, prepareEntry := range fsm.pendingTxns {
		snapshot.PendingTxns[txnID] = prepareEntry
	}
	fsm.txnMu.RUnlock()

	fsm.logger.Info("created FSM snapshot",
		zap.Uint64("last_index", snapshot.LastIndex),
		zap.Uint64("last_term", snapshot.LastTerm),
		zap.Uint64("applied_entries", snapshot.AppliedEntries),
		zap.Int("pending_txns", len(snapshot.PendingTxns)))

	return snapshot, nil
}

// Restore restores the FSM from a snapshot
func (fsm *HeapFSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	heapSnapshot := &HeapSnapshot{}
	if err := heapSnapshot.Restore(snapshot); err != nil {
		fsm.logger.Error("failed to restore FSM snapshot", zap.Error(err))
		return fmt.Errorf("failed to restore FSM snapshot: %w", err)
	}

	// Restore state
	fsm.lastAppliedIndex = heapSnapshot.LastIndex
	fsm.lastAppliedTerm = heapSnapshot.LastTerm
	fsm.appliedAt = heapSnapshot.AppliedAt
	fsm.appliedEntries = heapSnapshot.AppliedEntries
	fsm.appliedBatches = heapSnapshot.AppliedBatches
	fsm.failedOperations = heapSnapshot.FailedOps
	fsm.snapshotCount = heapSnapshot.SnapshotCount
	fsm.restoreCount++

	// Restore pending transactions
	fsm.txnMu.Lock()
	fsm.pendingTxns = make(map[string]*BatchPrepareEntry)
	for txnID, prepareEntry := range heapSnapshot.PendingTxns {
		fsm.pendingTxns[txnID] = prepareEntry
	}
	fsm.txnMu.Unlock()

	fsm.logger.Info("restored FSM from snapshot",
		zap.Uint64("last_index", fsm.lastAppliedIndex),
		zap.Uint64("last_term", fsm.lastAppliedTerm),
		zap.Uint64("applied_entries", fsm.appliedEntries),
		zap.Int("pending_txns", len(fsm.pendingTxns)))

	return nil
}

// GetStats returns FSM statistics
func (fsm *HeapFSM) GetStats() *FSMStats {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	fsm.txnMu.RLock()
	pendingTxnCount := len(fsm.pendingTxns)
	fsm.txnMu.RUnlock()

	partitionCount := 0
	for _, topicPartitions := range fsm.partitions {
		partitionCount += len(topicPartitions)
	}

	return &FSMStats{
		LastAppliedIndex: fsm.lastAppliedIndex,
		LastAppliedTerm:  fsm.lastAppliedTerm,
		AppliedAt:        fsm.appliedAt,
		AppliedEntries:   fsm.appliedEntries,
		AppliedBatches:   fsm.appliedBatches,
		FailedOperations: fsm.failedOperations,
		SnapshotCount:    fsm.snapshotCount,
		RestoreCount:     fsm.restoreCount,
		PendingTxns:      uint64(pendingTxnCount),
		TopicCount:       uint64(len(fsm.partitions)),
		PartitionCount:   uint64(partitionCount),
	}
}

// FSMApplyResult represents the result of applying a log entry
type FSMApplyResult struct {
	Success   bool                   `json:"success"`
	Error     error                  `json:"error,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Index     uint64                 `json:"index"`
	Term      uint64                 `json:"term"`
	AppliedAt time.Time              `json:"applied_at"`
}

// FSMStats contains statistics about the FSM
type FSMStats struct {
	LastAppliedIndex uint64    `json:"last_applied_index"`
	LastAppliedTerm  uint64    `json:"last_applied_term"`
	AppliedAt        time.Time `json:"applied_at"`
	AppliedEntries   uint64    `json:"applied_entries"`
	AppliedBatches   uint64    `json:"applied_batches"`
	FailedOperations uint64    `json:"failed_operations"`
	SnapshotCount    uint64    `json:"snapshot_count"`
	RestoreCount     uint64    `json:"restore_count"`
	PendingTxns      uint64    `json:"pending_txns"`
	TopicCount       uint64    `json:"topic_count"`
	PartitionCount   uint64    `json:"partition_count"`
}
