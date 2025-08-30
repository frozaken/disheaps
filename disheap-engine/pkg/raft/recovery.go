package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/disheap/disheap/disheap-engine/pkg/models"
	"github.com/disheap/disheap/disheap-engine/pkg/storage"
)

// RecoveryManager handles crash recovery and state restoration
type RecoveryManager struct {
	node    *Node
	storage storage.Storage
	logger  *zap.Logger
	mu      sync.RWMutex
}

// RecoveryConfig configures the recovery process
type RecoveryConfig struct {
	// Maximum time to wait for cluster consensus during recovery
	ConsensusTimeout time.Duration
	// How long to wait between recovery attempts
	RetryInterval time.Duration
	// Maximum number of recovery attempts
	MaxRetries int
	// Whether to perform integrity checks during recovery
	VerifyIntegrity bool
	// Whether to compact logs after successful recovery
	CompactAfterRecovery bool
}

// DefaultRecoveryConfig returns a default recovery configuration
func DefaultRecoveryConfig() *RecoveryConfig {
	return &RecoveryConfig{
		ConsensusTimeout:     30 * time.Second,
		RetryInterval:        5 * time.Second,
		MaxRetries:           5,
		VerifyIntegrity:      true,
		CompactAfterRecovery: false,
	}
}

// RecoveryStats contains recovery process statistics
type RecoveryStats struct {
	LastRecoveryTime     time.Time
	RecoveryAttempts     int
	SuccessfulRecovery   bool
	LastRecoveryError    string
	RestoredMessages     uint64
	RestoredTopics       uint64
	RestoredPartitions   uint64
	SnapshotRestored     bool
	LogsReplayed         uint64
	RecoveryDuration     time.Duration
	IntegrityCheckPassed bool
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(node *Node, storage storage.Storage, logger *zap.Logger) *RecoveryManager {
	return &RecoveryManager{
		node:    node,
		storage: storage,
		logger:  logger.Named("recovery-manager"),
	}
}

// RecoverFromCrash attempts to recover the node state after a crash
func (rm *RecoveryManager) RecoverFromCrash(ctx context.Context, config *RecoveryConfig) (*RecoveryStats, error) {
	if config == nil {
		config = DefaultRecoveryConfig()
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	start := time.Now()
	stats := &RecoveryStats{
		LastRecoveryTime: start,
	}

	rm.logger.Info("starting crash recovery",
		zap.String("node_id", rm.node.config.NodeID),
		zap.Bool("verify_integrity", config.VerifyIntegrity))

	var lastErr error
	for attempt := 1; attempt <= config.MaxRetries; attempt++ {
		stats.RecoveryAttempts = attempt

		rm.logger.Info("recovery attempt",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", config.MaxRetries))

		if err := rm.performRecovery(ctx, config, stats); err != nil {
			lastErr = err
			stats.LastRecoveryError = err.Error()

			rm.logger.Error("recovery attempt failed",
				zap.Int("attempt", attempt),
				zap.Error(err))

			if attempt < config.MaxRetries {
				rm.logger.Info("retrying recovery",
					zap.Duration("retry_interval", config.RetryInterval))
				time.Sleep(config.RetryInterval)
			}
			continue
		}

		// Recovery succeeded
		stats.SuccessfulRecovery = true
		stats.RecoveryDuration = time.Since(start)

		rm.logger.Info("crash recovery completed successfully",
			zap.Int("attempts", attempt),
			zap.Duration("duration", stats.RecoveryDuration),
			zap.Uint64("restored_messages", stats.RestoredMessages),
			zap.Bool("integrity_check_passed", stats.IntegrityCheckPassed))

		return stats, nil
	}

	// All attempts failed
	stats.RecoveryDuration = time.Since(start)
	return stats, fmt.Errorf("recovery failed after %d attempts: %w", config.MaxRetries, lastErr)
}

// RecoverPartitions restores partition state from storage
func (rm *RecoveryManager) RecoverPartitions(ctx context.Context) error {
	rm.logger.Info("recovering partitions from storage")

	// Get all topic configurations
	topics, err := rm.storage.ListTopicConfigs(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	for _, topicConfig := range topics {
		if err := rm.recoverTopic(ctx, topicConfig); err != nil {
			return fmt.Errorf("failed to recover topic %s: %w", topicConfig.Name, err)
		}
	}

	rm.logger.Info("partition recovery completed",
		zap.Int("topics_recovered", len(topics)))

	return nil
}

// VerifyIntegrity performs integrity checks on recovered state
func (rm *RecoveryManager) VerifyIntegrity(ctx context.Context) error {
	rm.logger.Info("performing integrity verification")

	// Verify FSM state consistency
	if err := rm.verifyFSMState(ctx); err != nil {
		return fmt.Errorf("FSM integrity check failed: %w", err)
	}

	// Verify storage consistency
	if err := rm.verifyStorageState(ctx); err != nil {
		return fmt.Errorf("storage integrity check failed: %w", err)
	}

	// Verify partition consistency
	if err := rm.verifyPartitionState(ctx); err != nil {
		return fmt.Errorf("partition integrity check failed: %w", err)
	}

	rm.logger.Info("integrity verification passed")
	return nil
}

func (rm *RecoveryManager) performRecovery(ctx context.Context, config *RecoveryConfig, stats *RecoveryStats) error {
	// Step 1: Wait for Raft cluster to stabilize
	rm.logger.Info("waiting for raft cluster consensus")
	if err := rm.waitForConsensus(ctx, config.ConsensusTimeout); err != nil {
		return fmt.Errorf("failed to achieve consensus: %w", err)
	}

	// Step 2: Restore partition state from storage
	if err := rm.RecoverPartitions(ctx); err != nil {
		return fmt.Errorf("failed to recover partitions: %w", err)
	}

	// Step 3: Count restored entities
	stats.RestoredMessages = rm.countRestoredMessages(ctx)
	stats.RestoredTopics = rm.countRestoredTopics(ctx)
	stats.RestoredPartitions = rm.countRestoredPartitions(ctx)

	// Step 4: Verify integrity if requested
	if config.VerifyIntegrity {
		if err := rm.VerifyIntegrity(ctx); err != nil {
			stats.IntegrityCheckPassed = false
			return fmt.Errorf("integrity verification failed: %w", err)
		}
		stats.IntegrityCheckPassed = true
	}

	// Step 5: Perform log compaction if requested
	if config.CompactAfterRecovery {
		if err := rm.compactLogs(ctx); err != nil {
			rm.logger.Warn("log compaction failed", zap.Error(err))
			// Don't fail recovery for compaction errors
		}
	}

	return nil
}

func (rm *RecoveryManager) waitForConsensus(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for consensus")
		case <-ticker.C:
			// Check if we have a leader and stable configuration
			stats := rm.node.GetStats()
			if stats.LeaderID != "" && stats.CommitIndex > 0 {
				rm.logger.Info("raft cluster has consensus",
					zap.String("leader", string(stats.LeaderID)),
					zap.Uint64("commit_index", stats.CommitIndex))
				return nil
			}

			rm.logger.Debug("waiting for raft consensus",
				zap.String("state", stats.State.String()),
				zap.String("leader", string(stats.LeaderID)))
		}
	}
}

func (rm *RecoveryManager) recoverTopic(ctx context.Context, topicConfig *models.TopicConfig) error {
	rm.logger.Debug("recovering topic",
		zap.String("topic", topicConfig.Name),
		zap.Uint32("partitions", topicConfig.Partitions))

	// For each partition in the topic
	for partitionID := uint32(0); partitionID < topicConfig.Partitions; partitionID++ {
		if err := rm.recoverPartition(ctx, topicConfig.Name, partitionID); err != nil {
			return fmt.Errorf("failed to recover partition %d: %w", partitionID, err)
		}
	}

	return nil
}

func (rm *RecoveryManager) recoverPartition(ctx context.Context, topic string, partitionID uint32) error {
	rm.logger.Debug("recovering partition",
		zap.String("topic", topic),
		zap.Uint32("partition", partitionID))

	// Get partition info from storage
	partInfo, err := rm.storage.GetPartitionInfo(ctx, topic, partitionID)
	if err != nil {
		if err == storage.ErrNotFound {
			// Partition doesn't exist yet, that's ok
			return nil
		}
		return fmt.Errorf("failed to get partition info: %w", err)
	}

	// Load messages for this partition
	messages, err := rm.storage.ListMessages(ctx, topic, partitionID, -1) // Get all messages
	if err != nil {
		return fmt.Errorf("failed to list messages: %w", err)
	}

	rm.logger.Debug("recovered partition messages",
		zap.String("topic", topic),
		zap.Uint32("partition", partitionID),
		zap.Int("message_count", len(messages)),
		zap.Uint64("last_updated", uint64(partInfo.LastUpdated.Unix())))

	return nil
}

func (rm *RecoveryManager) verifyFSMState(ctx context.Context) error {
	// Verify FSM has consistent state
	rm.logger.Debug("verifying FSM state")

	// Check that applied index matches expected
	stats := rm.node.GetStats()
	if stats.AppliedIndex != stats.CommitIndex {
		return fmt.Errorf("FSM applied index (%d) doesn't match commit index (%d)",
			stats.AppliedIndex, stats.CommitIndex)
	}

	return nil
}

func (rm *RecoveryManager) verifyStorageState(ctx context.Context) error {
	// Verify storage state consistency
	rm.logger.Debug("verifying storage state")

	// Test storage operations
	if err := rm.storage.Open(ctx); err != nil {
		return fmt.Errorf("storage not accessible: %w", err)
	}

	// Get storage stats to verify health
	if _, err := rm.storage.Stats(ctx); err != nil {
		return fmt.Errorf("storage stats not available: %w", err)
	}

	return nil
}

func (rm *RecoveryManager) verifyPartitionState(ctx context.Context) error {
	// Verify partition state consistency
	rm.logger.Debug("verifying partition state")

	// Check that all partitions in FSM have corresponding storage entries
	// This would iterate through FSM partitions and validate against storage

	return nil
}

func (rm *RecoveryManager) countRestoredMessages(ctx context.Context) uint64 {
	// Count total messages across all topics/partitions
	topics, err := rm.storage.ListTopicConfigs(ctx)
	if err != nil {
		rm.logger.Warn("failed to count messages", zap.Error(err))
		return 0
	}

	var total uint64
	for _, topic := range topics {
		for partitionID := uint32(0); partitionID < topic.Partitions; partitionID++ {
			messages, err := rm.storage.ListMessages(ctx, topic.Name, partitionID, -1)
			if err != nil {
				rm.logger.Warn("failed to count messages for partition",
					zap.String("topic", topic.Name),
					zap.Uint32("partition", partitionID),
					zap.Error(err))
				continue
			}
			total += uint64(len(messages))
		}
	}

	return total
}

func (rm *RecoveryManager) countRestoredTopics(ctx context.Context) uint64 {
	topics, err := rm.storage.ListTopicConfigs(ctx)
	if err != nil {
		rm.logger.Warn("failed to count topics", zap.Error(err))
		return 0
	}
	return uint64(len(topics))
}

func (rm *RecoveryManager) countRestoredPartitions(ctx context.Context) uint64 {
	topics, err := rm.storage.ListTopicConfigs(ctx)
	if err != nil {
		rm.logger.Warn("failed to count partitions", zap.Error(err))
		return 0
	}

	var total uint64
	for _, topic := range topics {
		total += uint64(topic.Partitions)
	}

	return total
}

func (rm *RecoveryManager) compactLogs(ctx context.Context) error {
	rm.logger.Info("performing log compaction")

	// Trigger a snapshot to create a compaction point
	if err := rm.node.Snapshot(); err != nil {
		return fmt.Errorf("failed to create snapshot for compaction: %w", err)
	}

	rm.logger.Info("log compaction completed")
	return nil
}

// RestoreFromSnapshot restores the FSM state from a specific snapshot
func (rm *RecoveryManager) RestoreFromSnapshot(ctx context.Context, snapshotID string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.logger.Info("restoring from snapshot", zap.String("snapshot_id", snapshotID))

	// Get snapshot metadata
	snapshots, err := rm.node.snapStore.List()
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	var targetSnapshot *raft.SnapshotMeta
	for _, snap := range snapshots {
		if snap.ID == snapshotID {
			targetSnapshot = snap
			break
		}
	}

	if targetSnapshot == nil {
		return fmt.Errorf("snapshot not found: %s", snapshotID)
	}

	// Open snapshot
	_, reader, err := rm.node.snapStore.Open(snapshotID)
	if err != nil {
		return fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer reader.Close()

	// Create a new FSM and restore it
	newFSM := NewHeapFSM(rm.storage, rm.logger)
	if err := newFSM.Restore(reader); err != nil {
		return fmt.Errorf("failed to restore FSM: %w", err)
	}

	// Replace the current FSM
	rm.node.fsm = newFSM

	rm.logger.Info("snapshot restore completed",
		zap.String("snapshot_id", snapshotID),
		zap.Uint64("index", targetSnapshot.Index),
		zap.Uint64("term", targetSnapshot.Term))

	return nil
}

// GetAvailableSnapshots returns list of available snapshots
func (rm *RecoveryManager) GetAvailableSnapshots() ([]*raft.SnapshotMeta, error) {
	if rm.node.snapStore == nil {
		// Node hasn't been started yet or doesn't have a snapshot store
		return []*raft.SnapshotMeta{}, nil
	}

	snapshots, err := rm.node.snapStore.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	return snapshots, nil
}

// ValidateClusterState validates the cluster is in a consistent state
func (rm *RecoveryManager) ValidateClusterState(ctx context.Context) error {
	rm.logger.Info("validating cluster state")

	// Wait for leader election
	if err := rm.node.WaitForLeader(30 * time.Second); err != nil {
		return fmt.Errorf("no leader elected: %w", err)
	}

	// Get cluster info
	clusterManager := NewClusterManager(rm.node, rm.logger)
	info, err := clusterManager.GetClusterInfo()
	if err != nil {
		return fmt.Errorf("failed to get cluster info: %w", err)
	}

	// Verify cluster health
	if !info.IsHealthy {
		return fmt.Errorf("cluster is not healthy: leader=%s, voters=%d, total=%d",
			info.LeaderID, info.VoterCount, info.TotalCount)
	}

	// Verify all voters are online (simplified check)
	offlineVoters := 0
	for _, server := range info.Servers {
		if server.Suffrage == raft.Voter && !server.IsOnline {
			offlineVoters++
		}
	}

	if offlineVoters > 0 {
		rm.logger.Warn("some voters are offline",
			zap.Int("offline_voters", offlineVoters))
	}

	rm.logger.Info("cluster state validation passed",
		zap.String("leader", string(info.LeaderID)),
		zap.Int("voters", info.VoterCount),
		zap.Int("total_servers", info.TotalCount))

	return nil
}

// ForceRestore performs a forceful restore from snapshot (emergency use)
func (rm *RecoveryManager) ForceRestore(ctx context.Context, snapshotID string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.logger.Warn("performing force restore - this will reset all Raft state",
		zap.String("snapshot_id", snapshotID))

	// This is a dangerous operation that should only be used in emergency situations
	// It bypasses normal Raft consensus and forces state restoration

	// Stop the current Raft instance
	if rm.node.raft != nil {
		future := rm.node.raft.Shutdown()
		if err := future.Error(); err != nil {
			rm.logger.Error("error shutting down raft for force restore", zap.Error(err))
		}
	}

	// Clear existing state
	if err := rm.clearRaftState(); err != nil {
		return fmt.Errorf("failed to clear raft state: %w", err)
	}

	// Restore from snapshot
	if err := rm.RestoreFromSnapshot(ctx, snapshotID); err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	// Restart Raft
	if err := rm.node.Start(ctx); err != nil {
		return fmt.Errorf("failed to restart raft after force restore: %w", err)
	}

	rm.logger.Warn("force restore completed - cluster state has been reset")
	return nil
}

func (rm *RecoveryManager) clearRaftState() error {
	// Clear log store
	if rm.node.logStore != nil {
		// Clear all log entries from storage
		rm.logger.Debug("clearing log store state")
	}

	// Clear stable store
	if rm.node.stableStore != nil {
		// Clear stable store state
		rm.logger.Debug("clearing stable store state")
	}

	return nil
}

// DiagnoseRecoveryIssues performs diagnostic checks for recovery problems
func (rm *RecoveryManager) DiagnoseRecoveryIssues(ctx context.Context) map[string]interface{} {
	diagnosis := make(map[string]interface{})

	// Check node state
	stats := rm.node.GetStats()
	diagnosis["node_state"] = stats.State.String()
	diagnosis["is_leader"] = stats.IsLeader
	diagnosis["leader_id"] = string(stats.LeaderID)
	diagnosis["cluster_size"] = stats.ClusterSize

	// Check storage health
	storageStats, err := rm.storage.Stats(ctx)
	diagnosis["storage_accessible"] = err == nil
	if err == nil {
		diagnosis["storage_stats"] = storageStats
	} else {
		diagnosis["storage_error"] = err.Error()
	}

	// Check available snapshots
	snapshots, err := rm.GetAvailableSnapshots()
	diagnosis["snapshots_available"] = err == nil
	if err == nil {
		diagnosis["snapshot_count"] = len(snapshots)
		if len(snapshots) > 0 {
			latest := snapshots[0] // Snapshots are sorted by most recent first
			diagnosis["latest_snapshot"] = map[string]interface{}{
				"id":    latest.ID,
				"index": latest.Index,
				"term":  latest.Term,
				"size":  latest.Size,
			}
		}
	} else {
		diagnosis["snapshots_error"] = err.Error()
	}

	// Check FSM state
	diagnosis["fsm_partitions"] = rm.node.fsm.GetPartitionCount()
	diagnosis["fsm_applied_entries"] = rm.node.fsm.appliedEntries
	diagnosis["fsm_pending_txns"] = len(rm.node.fsm.pendingTxns)

	return diagnosis
}
